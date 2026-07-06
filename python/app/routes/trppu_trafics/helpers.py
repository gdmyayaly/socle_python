"""DSR-679 — Helpers de récupération des trafics pivot (nouvelle structure gold `_3`).

Package autonome : les tables trafics `g_trppu_trafics_{jour,semaine,mois}_3` portent
DIRECTEMENT l'objet TRPPU dans `co_type_objet` (OO/OS/PR/PP/CO/IP) + `trafic_constate`
/ `trafic_prevu` + `co_regate` + `co_annee_comptage` / `co_mois_comptage` /
`co_semaine_comptage`. Aucune jointure, aucun mapping en dur : la requête agrège par
`co_type_objet` et le service restitue tel quel le résultat du SQL (objets dynamiques).
"""

import logging
import os
from calendar import monthrange
from datetime import datetime, timedelta

from fastapi import HTTPException

from app.config import (
    DATABRICKS_CATALOG,
    DATABRICKS_SCHEMA,
    MAX_DATE_RANGE_DAYS,
)

logger = logging.getLogger(__name__)

# --- Tables & colonnes (structure gold réelle `_3`, surchargeables par env) ---
TABLES_PERIODE = {
    "jours": os.getenv("TRAFIC679_TABLE_JOUR", "g_trppu_trafics_jour_3"),
    "semaines": os.getenv("TRAFIC679_TABLE_SEMAINE", "g_trppu_trafics_semaine_3"),
    "mois": os.getenv("TRAFIC679_TABLE_MOIS", "g_trppu_trafics_mois_3"),
}

DATE_COLUMN_PERIODE = {
    "jours": "da_comptage",
    "semaines": "co_semaine_comptage",
    "mois": "co_mois_comptage",
}

TRAFIC_COL_OBJET = os.getenv("TRAFIC679_COL_OBJET", "co_type_objet")  # objet TRPPU (agrégation)
TRAFIC_COL_CONSTATE = os.getenv("TRAFIC679_COL_CONSTATE", "trafic_constate")
TRAFIC_COL_PREVISIONNEL = os.getenv("TRAFIC679_COL_PREVISIONNEL", "trafic_prevu")
TRAFIC_COL_ANNEE = os.getenv("TRAFIC679_COL_ANNEE", "co_annee_comptage")
TRAFIC_COL_MOIS = os.getenv("TRAFIC679_COL_MOIS", "co_mois_comptage")  # partition mois (table jour)

# Alias exposés dans le SELECT agrégé (clés des dicts renvoyés par fetch_all).
OBJ_ALIAS = "co_objet_trppu"
SOMME_ALIAS = "somme"

# Dimensions (références debug uniquement — non utilisées par la requête agrégée).
OBJ_MAPPING_TABLE = os.getenv("TRAFIC679_OBJ_MAPPING_TABLE", "g_trppu_obj_mapping")
ENTITE_TABLE = os.getenv("TRAFIC679_ENTITE_TABLE", "g_trppu_entite")
CALENDRIER_TABLE = os.getenv("TRAFIC679_CALENDRIER_TABLE", "s_commun_calendrier_jour")

PARAMETRES_RAPPEL_PIVOT = (
    "Paramètres attendus : "
    "co_regate (code régate du site), "
    "date_debut (format AAAAMMJJ), "
    "date_fin (format AAAAMMJJ), "
    "date_pivot (format AAAAMMJJ)."
)


# =============================================================================
# Parsing / formatage / rendu SQL
# =============================================================================
def parse_date(value: str, nom_param: str) -> datetime:
    """Parse une date au format AAAAMMJJ ou AAAA-MM-JJ."""
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    msg = (
        f"Format de {nom_param} invalide '{value}'. "
        f"Attendu : AAAAMMJJ ou AAAA-MM-JJ. {PARAMETRES_RAPPEL_PIVOT}"
    )
    raise HTTPException(status_code=400, detail={"error": True, "message": msg, "code": 400})


def render_sql(sql: str, params: dict) -> str:
    """Substitue les paramètres nommés (:name) dans le SQL pour l'affichage debug."""
    rendered = sql
    for key, value in params.items():
        literal = "NULL" if value is None else f"'{str(value).replace(chr(39), chr(39) * 2)}'"
        rendered = rendered.replace(f":{key}", literal)
    return rendered


def fmt_date(dt: datetime, periode: str = "jours") -> str:
    """Formate une date selon la période (jours AAAA-MM-JJ ; semaines AAAA-NS ; mois AAAA-MM)."""
    if periode == "semaines":
        return f"{dt.isocalendar()[0]}-{dt.isocalendar()[1]:02d}"
    if periode == "mois":
        return dt.strftime("%Y-%m")
    return dt.strftime("%Y-%m-%d")


# =============================================================================
# Découpage de l'intervalle en segments mois / semaines / jours
# =============================================================================
def _decompose_semaines_jours(dt_start: datetime, dt_end: datetime):
    """Découpe un intervalle en semaines complètes (lun-dim) et jours restants."""
    parts: list[tuple[str, datetime, datetime]] = []

    days_to_monday = (7 - dt_start.weekday()) % 7
    first_monday = dt_start + timedelta(days=days_to_monday)

    if dt_end.weekday() == 6:
        last_sunday = dt_end
    else:
        last_sunday = dt_end - timedelta(days=dt_end.weekday() + 1)

    if first_monday + timedelta(days=6) <= last_sunday:
        if dt_start < first_monday:
            parts.append(("jours", dt_start, first_monday - timedelta(days=1)))
        parts.append(("semaines", first_monday, last_sunday))
        if last_sunday < dt_end:
            parts.append(("jours", last_sunday + timedelta(days=1), dt_end))
    else:
        parts.append(("jours", dt_start, dt_end))

    return parts


def decompose_auto(dt_debut: datetime, dt_fin: datetime):
    """Découpe un intervalle en requêtes optimales sur mois / semaines / jours."""
    segments: list[tuple[str, datetime, datetime]] = []

    if dt_debut.day == 1:
        mois_start = dt_debut
    else:
        if dt_debut.month == 12:
            mois_start = datetime(dt_debut.year + 1, 1, 1)
        else:
            mois_start = datetime(dt_debut.year, dt_debut.month + 1, 1)

    last_day = monthrange(dt_fin.year, dt_fin.month)[1]
    if dt_fin.day == last_day:
        mois_end = dt_fin
    else:
        mois_end = datetime(dt_fin.year, dt_fin.month, 1) - timedelta(days=1)

    if mois_start <= mois_end:
        if dt_debut < mois_start:
            segments.extend(_decompose_semaines_jours(dt_debut, mois_start - timedelta(days=1)))
        segments.append(("mois", mois_start, mois_end))
        if mois_end < dt_fin:
            segments.extend(_decompose_semaines_jours(mois_end + timedelta(days=1), dt_fin))
    else:
        segments.extend(_decompose_semaines_jours(dt_debut, dt_fin))

    return segments


# =============================================================================
# Validation des paramètres & découpage pivot
# =============================================================================
def validate_params_pivot(co_regate, date_debut, date_fin, date_pivot):
    """Valide les 4 paramètres et retourne (dt_debut, dt_fin, dt_pivot).

    HTTP 400 explicite (paramètres rappelés) si un paramètre manque, si
    date_debut > date_fin, ou si la période dépasse 2 ans.
    """
    manquants = []
    if not co_regate:
        manquants.append("co_regate")
    if not date_debut:
        manquants.append("date_debut")
    if not date_fin:
        manquants.append("date_fin")
    if not date_pivot:
        manquants.append("date_pivot")
    if manquants:
        msg = f"Paramètre(s) manquant(s) : {', '.join(manquants)}. {PARAMETRES_RAPPEL_PIVOT}"
        logger.warning(msg)
        raise HTTPException(status_code=400, detail={"error": True, "message": msg, "code": 400})

    dt_debut = parse_date(date_debut, "date_debut")
    dt_fin = parse_date(date_fin, "date_fin")
    dt_pivot = parse_date(date_pivot, "date_pivot")

    if dt_debut > dt_fin:
        msg = f"date_debut doit être antérieure ou égale à date_fin. {PARAMETRES_RAPPEL_PIVOT}"
        logger.warning(msg)
        raise HTTPException(status_code=400, detail={"error": True, "message": msg, "code": 400})

    ecart = (dt_fin - dt_debut).days
    if ecart > MAX_DATE_RANGE_DAYS:
        msg = (
            f"La période dépasse les 2 ans d'interrogation permis ({MAX_DATE_RANGE_DAYS} jours). "
            f"Écart actuel : {ecart} jours. {PARAMETRES_RAPPEL_PIVOT}"
        )
        logger.warning(msg)
        raise HTTPException(status_code=400, detail={"error": True, "message": msg, "code": 400})

    return dt_debut, dt_fin, dt_pivot


def split_by_pivot(dt_debut, dt_fin, dt_pivot):
    """Découpe la période en (plage réelle, plage prévisionnelle) autour du pivot.

    - réel : dates **strictement antérieures** au pivot -> [dt_debut, min(dt_fin, pivot-1j)]
    - prév : dates **>= pivot**                          -> [max(dt_debut, pivot), dt_fin]

    Découper au jour près évite toute granularité mois/semaine à cheval sur le pivot.
    Retourne (reel, prev), chacun None si la plage est vide (future -> reel=None ;
    passée -> prev=None).
    """
    reel = None
    prev = None

    reel_fin = min(dt_fin, dt_pivot - timedelta(days=1))
    if dt_debut <= reel_fin:
        reel = (dt_debut, reel_fin)

    prev_debut = max(dt_debut, dt_pivot)
    if prev_debut <= dt_fin:
        prev = (prev_debut, dt_fin)

    return reel, prev


# =============================================================================
# Construction des requêtes (agrégation par objet + pruning partitions)
# =============================================================================
def _years_in_range(dt_start: datetime, dt_end: datetime) -> list[int]:
    """Années couvertes par [dt_start, dt_end] (inclus)."""
    return list(range(dt_start.year, dt_end.year + 1))


def _months_in_range(dt_start: datetime, dt_end: datetime) -> list[str]:
    """Codes AAAA-MM couverts par [dt_start, dt_end] (inclus)."""
    mois: list[str] = []
    y, m = dt_start.year, dt_start.month
    while (y, m) <= (dt_end.year, dt_end.month):
        mois.append(f"{y:04d}-{m:02d}")
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return mois


def build_partition_conditions(
    periode: str, ranges: list[tuple[datetime, datetime]], params: dict
) -> list[str]:
    """Prédicats de partition (pruning) : co_annee_comptage IN (...) [+ co_mois_comptage pour le jour].

    Union des années/mois de toutes les plages (superset sûr). Alimente `params` avec des
    clés uniques et retourne la liste des conditions SQL.
    """
    annees: set[int] = set()
    mois: set[str] = set()
    for s, e in ranges:
        annees.update(_years_in_range(s, e))
        if periode == "jours":
            mois.update(_months_in_range(s, e))

    conditions: list[str] = []

    annee_keys: list[str] = []
    for i, an in enumerate(sorted(annees)):
        k = f"part_an_{i}"
        params[k] = an
        annee_keys.append(f":{k}")
    if annee_keys:
        conditions.append(f"{TRAFIC_COL_ANNEE} IN ({', '.join(annee_keys)})")

    if periode == "jours" and mois:
        mois_keys: list[str] = []
        for i, mo in enumerate(sorted(mois)):
            k = f"part_mo_{i}"
            params[k] = mo
            mois_keys.append(f":{k}")
        conditions.append(f"{TRAFIC_COL_MOIS} IN ({', '.join(mois_keys)})")

    return conditions


def build_query(
    periode: str,
    co_regate: str,
    ranges: list[tuple[datetime, datetime]],
    value_col: str,
) -> tuple[str, dict]:
    """Requête agrégée par objet sur la table de la période (sans jointure).

    `SUM(value_col) GROUP BY co_type_objet`, filtre co_regate + dates (BETWEEN par plage,
    OR entre plages) + prédicats de partition. `value_col` est une constante contrôlée.
    """
    table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TABLES_PERIODE[periode]}"
    date_col = DATE_COLUMN_PERIODE[periode]

    params: dict = {"co_regate": co_regate}
    date_conditions: list[str] = []
    for i, (dt_start, dt_end) in enumerate(ranges):
        s_key, e_key = f"dt_start_{i}", f"dt_end_{i}"
        params[s_key] = fmt_date(dt_start, periode)
        params[e_key] = fmt_date(dt_end, periode)
        date_conditions.append(f"{date_col} BETWEEN :{s_key} AND :{e_key}")

    part_conditions = build_partition_conditions(periode, ranges, params)

    where = ["co_regate = :co_regate", f"({' OR '.join(date_conditions)})"]
    where.extend(part_conditions)

    sql = (
        f"SELECT {TRAFIC_COL_OBJET} AS {OBJ_ALIAS}, "
        f"SUM({value_col}) AS {SOMME_ALIAS} "
        f"FROM {table} "
        f"WHERE {' AND '.join(where)} "
        f"GROUP BY {TRAFIC_COL_OBJET}"
    )
    return sql, params


def build_period_queries(
    co_regate: str,
    dt_debut: datetime,
    dt_fin: datetime,
    value_col: str,
    force_jours: bool = False,
) -> list[tuple[str, dict]]:
    """Découpe [dt_debut, dt_fin] en segments mois/semaines/jours et construit une requête
    agrégée par table (max 3). `force_jours` court-circuite le découpage (table jour seule)."""
    if force_jours:
        segments = [("jours", dt_debut, dt_fin)]
    else:
        segments = decompose_auto(dt_debut, dt_fin)
    grouped: dict[str, list[tuple[datetime, datetime]]] = {}
    for periode, s, e in segments:
        grouped.setdefault(periode, []).append((s, e))
    return [build_query(p, co_regate, ranges, value_col) for p, ranges in grouped.items()]


# =============================================================================
# Accumulation dynamique par objet
# =============================================================================
def empty_accumulator() -> dict:
    """Accumulateur dynamique vide : `{objet: {trafic_brut, trafic_previsionnel}}`.

    Aucun objet pré-hydraté — la restitution ne renvoie que les objets présents dans le SQL."""
    return {}


def accumulate_trafics(rows, target_key, acc) -> None:
    """Ajoute la somme agrégée (par objet) dans `acc[objet][target_key]` (dynamique).

    `rows` = lignes `{co_objet_trppu, somme}` renvoyées par `build_query`. L'objet est créé
    à la volée ; une somme nulle compte pour 0.
    """
    for row in rows:
        objet = row.get(OBJ_ALIAS)
        objet = str(objet).strip() if objet is not None else None
        if objet is None:
            continue
        slot = acc.setdefault(objet, {"trafic_brut": 0, "trafic_previsionnel": 0})
        valeur = row.get(SOMME_ALIAS)
        if valeur is None:
            continue
        slot[target_key] += valeur
