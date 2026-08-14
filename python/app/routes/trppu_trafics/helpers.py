"""DSR-679 — Trafics pivot sur les tables gold `_3` (`co_type_objet`, `trafic_constate` /
`trafic_prevu`, partitionnées par année et mois).

L'objet restitué et son libellé viennent de `g_trppu_obj_mapping`, jointe **pré-regroupée**
(cf. `build_mapping_subquery`) : le mapping porte plusieurs lignes par objet, une jointure brute
gonflerait les `SUM`. Les autres dimensions du ticket ne sont pas joignables ici :
`g_trppu_entite` est réduite à un STRUCT et le calendrier silver porte plusieurs lignes par jour.
"""

import logging
import os
import time
from calendar import monthrange
from datetime import datetime, timedelta

from starlette.concurrency import run_in_threadpool

from app.config import (
    DATABRICKS_CATALOG,
    DATABRICKS_SCHEMA,
    MAX_DATE_RANGE_DAYS,
)
from app.db.databricks import databricks
from app.routes.trppu_trafics.errors import erreur_400

logger = logging.getLogger(__name__)

# On ne dispatche plus que sur les tables jour et mois (cf. `decompose_auto`). L'entrée
# `semaines` reste déclarée uniquement pour `scripts/controle_trafics_679.py`, qui compare
# encore les mailles entre elles.
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

ALIAS_TRAFIC = "t"

TRAFIC_COL_REGATE = os.getenv("TRAFIC679_COL_REGATE", "co_regate")
TRAFIC_COL_OBJET = os.getenv("TRAFIC679_COL_OBJET", "co_type_objet")
TRAFIC_COL_CONSTATE = os.getenv("TRAFIC679_COL_CONSTATE", "trafic_constate")
TRAFIC_COL_PREVISIONNEL = os.getenv("TRAFIC679_COL_PREVISIONNEL", "trafic_prevu")
TRAFIC_COL_ANNEE = os.getenv("TRAFIC679_COL_ANNEE", "co_annee_comptage")
TRAFIC_COL_MOIS = os.getenv("TRAFIC679_COL_MOIS", "co_mois_comptage")

# Les tables gold empilent SITE / ETABLISSEMENT / DEXC / DEPARTEMENT / PIC / PFC / NATIONAL /
# SIEGE : sans ce filtre, une régate présente à plusieurs niveaux gonfle silencieusement les SUM.
TRAFIC_COL_NIVEAU = os.getenv(
    "TRAFIC679_COL_NIVEAU", "co_niveau_regroupement_operationnel"
)
NIVEAU_REGROUPEMENT = os.getenv("TRAFIC679_NIVEAU_REGROUPEMENT", "SITE")

# Alias du SELECT agrégé = clés des dicts renvoyés par fetch_all.
OBJ_ALIAS = "co_objet_trppu"
SOMME_ALIAS = "somme"
LIBELLE_ALIAS = "lb_objet_trppu"

# Dimension objets. `MAPPING_COL_CLE` est rapprochée de `TRAFIC_COL_OBJET` côté trafics ;
# `MAPPING_COL_OBJET` porte le code restitué. Les deux valent `co_type_objet` : le mapping CTR
# a déjà été appliqué en amont des tables `_3`.
OBJ_MAPPING_TABLE = os.getenv("TRAFIC679_OBJ_MAPPING_TABLE", "g_trppu_obj_mapping")
MAPPING_COL_CLE = os.getenv("TRAFIC679_MAPPING_COL_CLE", "co_type_objet")
MAPPING_COL_OBJET = os.getenv("TRAFIC679_MAPPING_COL_OBJET", "co_type_objet")
MAPPING_COL_LIBELLE = os.getenv("TRAFIC679_MAPPING_COL_LIBELLE", "lb_type_objet")
ALIAS_MAPPING = "m"
CLE_ALIAS = "cle_jointure"
LIBELLES_TTL_S = 3600

PARAMETRES_RAPPEL_PIVOT = (
    "Paramètres attendus : "
    "co_regate (code régate du site), "
    "date_debut (format AAAAMMJJ), "
    "date_fin (format AAAAMMJJ), "
    "date_pivot (format AAAAMMJJ)."
)

_libelles_cache: dict[str, str] | None = None
_libelles_cache_ts: float = 0.0


def fqtn(table: str, schema: str | None = None) -> str:
    """Nom pleinement qualifié `catalogue.schéma.table` (schéma gold par défaut)."""
    return f"{DATABRICKS_CATALOG}.{schema or DATABRICKS_SCHEMA}.{table}"


def parse_date(value: str, nom_param: str) -> datetime:
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise erreur_400(
        f"Format de {nom_param} invalide '{value}'. "
        f"Attendu : AAAAMMJJ ou AAAA-MM-JJ. {PARAMETRES_RAPPEL_PIVOT}"
    )


def render_sql(sql: str, params: dict) -> str:
    """Substitue les paramètres nommés (:name) pour obtenir un SQL rejouable tel quel.

    Clés substituées de la plus longue à la plus courte, sinon `:part_mo_1` corromprait
    `:part_mo_10`. Les nombres ne sont pas quotés : le rendu reste typé comme l'exécuté.
    """
    rendered = sql
    for key in sorted(params, key=len, reverse=True):
        value = params[key]
        if value is None:
            literal = "NULL"
        elif isinstance(value, bool):
            literal = "true" if value else "false"
        elif isinstance(value, (int, float)):
            literal = str(value)
        else:
            literal = f"'{str(value).replace(chr(39), chr(39) * 2)}'"
        rendered = rendered.replace(f":{key}", literal)
    return rendered


def fmt_date(dt: datetime, periode: str = "jours") -> str:
    """AAAA-MM-JJ (jours) | AAAA-NS ISO (semaines) | AAAA-MM (mois)."""
    if periode == "semaines":
        return f"{dt.isocalendar()[0]}-{dt.isocalendar()[1]:02d}"
    if periode == "mois":
        return dt.strftime("%Y-%m")
    return dt.strftime("%Y-%m-%d")


# --- Exécution tracée ---------------------------------------------------------------------
def executer_requete(sql: str, params: dict | None = None) -> dict:
    """Exécute une requête Databricks sans jamais lever : renvoie une trace.

    Le SQL rendu est posé dans la trace **avant** l'exécution, donc la requête fautive reste
    restituable alors même que Databricks a échoué — c'est ce que consomme le bloc debug du 500.
    Clés : `sql`, `statut` (ok|echec), + `lignes`/`nb_lignes` si ok, + `erreur` si echec.
    """
    trace: dict = {"sql": render_sql(sql, params or {}), "statut": "ok"}
    try:
        rows = databricks.fetch_all(sql, params or None)
    except Exception as e:
        logger.warning("Requête Databricks trafics en échec : %s", e)
        trace["statut"] = "echec"
        trace["erreur"] = str(e)
        return trace
    trace["lignes"] = rows
    trace["nb_lignes"] = len(rows)
    return trace


async def executer_requete_async(sql: str, params: dict | None = None) -> dict:
    """`executer_requete` déporté hors de la boucle d'événements (Databricks est synchrone)."""
    return await run_in_threadpool(executer_requete, sql, params)


# --- Dimension objets ---------------------------------------------------------------------
def build_mapping_subquery() -> str:
    """Mapping pré-regroupé : exactement une ligne par code, sinon la jointure duplique les
    lignes de trafic et gonfle les `SUM` (un `DISTINCT` ne suffirait pas : `MAX` départage)."""
    return (
        f"SELECT {MAPPING_COL_CLE} AS {CLE_ALIAS}, "
        f"MAX({MAPPING_COL_OBJET}) AS {OBJ_ALIAS}, "
        f"MAX({MAPPING_COL_LIBELLE}) AS {LIBELLE_ALIAS} "
        f"FROM {fqtn(OBJ_MAPPING_TABLE)} "
        f"GROUP BY {MAPPING_COL_CLE}"
    )


def build_libelles_query() -> str:
    """Couples (objet, libellé) du mapping — une ligne par objet."""
    return (
        f"SELECT {OBJ_ALIAS}, {LIBELLE_ALIAS} "
        f"FROM ({build_mapping_subquery()}) {ALIAS_MAPPING}"
    )


def fetch_libelles_objets(force: bool = False) -> dict[str, str]:
    """Dictionnaire `{co_type_objet: lb_type_objet}`, en cache pendant `LIBELLES_TTL_S`.

    Appel Databricks synchrone — à envelopper dans `run_in_threadpool` côté route. Les libellés
    étant décoratifs, une indisponibilité du mapping ne fait pas échouer la restitution : on
    renvoie le dernier cache connu et les libellés sortent à `null`.
    """
    global _libelles_cache, _libelles_cache_ts

    maintenant = time.monotonic()
    if (
        not force
        and _libelles_cache is not None
        and maintenant - _libelles_cache_ts < LIBELLES_TTL_S
    ):
        return _libelles_cache

    trace = executer_requete(build_libelles_query())
    if trace["statut"] == "echec":
        logger.warning(
            "Libellés d'objets indisponibles (%s) — restitution sans libellé.", trace["erreur"]
        )
        _libelles_cache = _libelles_cache or {}
        _libelles_cache_ts = maintenant  # évite de réinterroger à chaque requête
        return _libelles_cache

    libelles: dict[str, str] = {}
    for row in trace["lignes"]:
        code, libelle = row.get(OBJ_ALIAS), row.get(LIBELLE_ALIAS)
        if code is not None and libelle is not None:
            libelles.setdefault(str(code).strip(), str(libelle).strip())

    _libelles_cache, _libelles_cache_ts = libelles, maintenant
    return libelles


# --- Découpage de l'intervalle ------------------------------------------------------------
def decompose_auto(dt_debut: datetime, dt_fin: datetime):
    """Découpe un intervalle en requêtes optimales sur mois / jours.

    Seules les tables jour et mois sont interrogées : la maille semaine a été retirée du
    dispatching. Les mois complets partent sur la table mois, les jours des bords (mois
    partiels de début et de fin) sur la table jour.
    """
    segments: list[tuple[str, datetime, datetime]] = []

    if dt_debut.day == 1:
        mois_start = dt_debut
    elif dt_debut.month == 12:
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
            segments.append(("jours", dt_debut, mois_start - timedelta(days=1)))
        segments.append(("mois", mois_start, mois_end))
        if mois_end < dt_fin:
            segments.append(("jours", mois_end + timedelta(days=1), dt_fin))
    else:
        # Aucun mois complet dans la période : tout part sur la table jour.
        segments.append(("jours", dt_debut, dt_fin))

    return segments


# --- Validation & découpage pivot ---------------------------------------------------------
def validate_params_pivot(co_regate, date_debut, date_fin, date_pivot):
    """Valide les 4 paramètres et retourne (dt_debut, dt_fin, dt_pivot)."""
    manquants = [
        nom
        for nom, valeur in (
            ("co_regate", co_regate),
            ("date_debut", date_debut),
            ("date_fin", date_fin),
            ("date_pivot", date_pivot),
        )
        if not valeur
    ]
    if manquants:
        msg = f"Paramètre(s) manquant(s) : {', '.join(manquants)}. {PARAMETRES_RAPPEL_PIVOT}"
        logger.warning(msg)
        raise erreur_400(msg)

    dt_debut = parse_date(date_debut, "date_debut")
    dt_fin = parse_date(date_fin, "date_fin")
    dt_pivot = parse_date(date_pivot, "date_pivot")

    if dt_debut > dt_fin:
        msg = f"date_debut doit être antérieure ou égale à date_fin. {PARAMETRES_RAPPEL_PIVOT}"
        logger.warning(msg)
        raise erreur_400(msg)

    ecart = (dt_fin - dt_debut).days
    if ecart > MAX_DATE_RANGE_DAYS:
        msg = (
            f"La période dépasse les 2 ans d'interrogation permis ({MAX_DATE_RANGE_DAYS} jours). "
            f"Écart actuel : {ecart} jours. {PARAMETRES_RAPPEL_PIVOT}"
        )
        logger.warning(msg)
        raise erreur_400(msg)

    return dt_debut, dt_fin, dt_pivot


def split_by_pivot(dt_debut, dt_fin, dt_pivot):
    """Découpe la période en (plage réelle, plage prévisionnelle) autour du pivot.

    Réel = dates strictement antérieures au pivot, prév = dates >= pivot. Découper au jour près
    évite toute maille mois à cheval sur le pivot. Chaque plage est None si vide
    (période entièrement future -> reel=None ; entièrement passée -> prev=None).
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


def zones_pivot(reel_range, prev_range, inclure_vides: bool = False):
    """Zones à interroger : (plage, colonne de valeur, clé d'accumulation).

    `inclure_vides=True` garde les zones nulles, dont `scripts/controle_trafics_679.py` a besoin
    pour calculer les plages effectives de maille.
    """
    zones = [
        (reel_range, TRAFIC_COL_CONSTATE, "trafic_brut"),
        (prev_range, TRAFIC_COL_PREVISIONNEL, "trafic_previsionnel"),
    ]
    return zones if inclure_vides else [z for z in zones if z[0] is not None]


# --- Construction des requêtes ------------------------------------------------------------
def _years_in_range(dt_start: datetime, dt_end: datetime) -> list[int]:
    return list(range(dt_start.year, dt_end.year + 1))


def _months_in_range(dt_start: datetime, dt_end: datetime) -> list[str]:
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
    """Prédicats de pruning : année (+ mois pour la table jour), union de toutes les plages.

    Toujours portés par la table trafics (alias `t`), jamais par une dimension, sinon le pruning
    de partitions ne s'applique plus. Alimente `params` et retourne les conditions.
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
        conditions.append(f"{ALIAS_TRAFIC}.{TRAFIC_COL_ANNEE} IN ({', '.join(annee_keys)})")

    if periode == "jours" and mois:
        mois_keys: list[str] = []
        for i, mo in enumerate(sorted(mois)):
            k = f"part_mo_{i}"
            params[k] = mo
            mois_keys.append(f":{k}")
        conditions.append(f"{ALIAS_TRAFIC}.{TRAFIC_COL_MOIS} IN ({', '.join(mois_keys)})")

    return conditions


def build_query(
    periode: str,
    co_regate: str,
    ranges: list[tuple[datetime, datetime]],
    value_col: str,
) -> tuple[str, dict]:
    """Requête agrégée par objet sur la table de la période, jointe au mapping pré-regroupé.

    `LEFT JOIN` + `COALESCE` conservent les objets absents du mapping (`PR`, `PPI`) au lieu de
    les fondre dans un groupe `NULL`. `value_col` est une constante contrôlée, injectée dans le
    SQL ; les valeurs restent paramétrées.
    """
    table = fqtn(TABLES_PERIODE[periode])
    objet_expr = f"COALESCE({ALIAS_MAPPING}.{OBJ_ALIAS}, {ALIAS_TRAFIC}.{TRAFIC_COL_OBJET})"
    date_expr = f"{ALIAS_TRAFIC}.{DATE_COLUMN_PERIODE[periode]}"

    params: dict = {"co_regate": co_regate, "niveau": NIVEAU_REGROUPEMENT}
    date_conditions: list[str] = []
    for i, (dt_start, dt_end) in enumerate(ranges):
        s_key, e_key = f"dt_start_{i}", f"dt_end_{i}"
        params[s_key] = fmt_date(dt_start, periode)
        params[e_key] = fmt_date(dt_end, periode)
        date_conditions.append(f"{date_expr} BETWEEN :{s_key} AND :{e_key}")

    where = [
        f"{ALIAS_TRAFIC}.{TRAFIC_COL_REGATE} = :co_regate",
        f"{ALIAS_TRAFIC}.{TRAFIC_COL_NIVEAU} = :niveau",
        f"({' OR '.join(date_conditions)})",
    ]
    where.extend(build_partition_conditions(periode, ranges, params))

    sql = (
        f"SELECT {objet_expr} AS {OBJ_ALIAS}, "
        f"{ALIAS_MAPPING}.{LIBELLE_ALIAS} AS {LIBELLE_ALIAS}, "
        f"SUM({ALIAS_TRAFIC}.{value_col}) AS {SOMME_ALIAS} "
        f"FROM {table} {ALIAS_TRAFIC} "
        f"LEFT JOIN ({build_mapping_subquery()}) {ALIAS_MAPPING} "
        f"ON {ALIAS_MAPPING}.{CLE_ALIAS} = {ALIAS_TRAFIC}.{TRAFIC_COL_OBJET} "
        f"WHERE {' AND '.join(where)} "
        f"GROUP BY 1, 2"
    )
    return sql, params


def build_period_queries(
    co_regate: str,
    dt_debut: datetime,
    dt_fin: datetime,
    value_col: str,
    force_jours: bool = False,
) -> list[tuple[str, dict]]:
    """Une requête agrégée par table (max 2 : jour et mois). `force_jours` force la table jour."""
    segments = (
        [("jours", dt_debut, dt_fin)] if force_jours else decompose_auto(dt_debut, dt_fin)
    )
    grouped: dict[str, list[tuple[datetime, datetime]]] = {}
    for periode, s, e in segments:
        grouped.setdefault(periode, []).append((s, e))
    return [build_query(p, co_regate, ranges, value_col) for p, ranges in grouped.items()]


# --- Accumulation & restitution -----------------------------------------------------------
def empty_accumulator() -> dict:
    """Accumulateur vide : aucun objet pré-hydraté, seuls ceux vus en base sont restitués."""
    return {}


def accumulate_trafics(rows, target_key, acc) -> None:
    """Cumule les lignes `{co_objet_trppu, lb_objet_trppu, somme}` dans `acc[objet][target_key]`.

    Jusqu'à 2 mailles × 2 zones remontent le même objet : on retient le premier libellé non nul.
    """
    for row in rows:
        objet = row.get(OBJ_ALIAS)
        objet = str(objet).strip() if objet is not None else None
        if objet is None:
            continue
        slot = acc.setdefault(
            objet, {"lb_produit": None, "trafic_brut": 0, "trafic_previsionnel": 0}
        )
        if slot["lb_produit"] is None:
            libelle = row.get(LIBELLE_ALIAS)
            if libelle is not None:
                slot["lb_produit"] = str(libelle).strip()
        valeur = row.get(SOMME_ALIAS)
        if valeur is None:
            continue
        slot[target_key] += valeur


def formater_trafics(acc: dict, libelles: dict[str, str] | None = None) -> list[dict]:
    """Accumulateur -> lignes de sortie, triées par objet.

    Le libellé vient de la jointure ; `libelles` n'est un repli que pour
    `scripts/controle_trafics_679.py` en mode `--sql-seulement`, où la requête n'a pas été
    exécutée.
    """
    return [
        {
            "co_produit": objet,
            "lb_produit": acc[objet]["lb_produit"] or (libelles or {}).get(objet),
            "trafic_brut": acc[objet]["trafic_brut"],
            "trafic_previsionnel": acc[objet]["trafic_previsionnel"],
        }
        for objet in sorted(acc)
    ]


def objets_sans_libelle(acc: dict) -> list[str]:
    """Objets des tables trafics absents du mapping (cf. écart PR/PPI vs PQ/EQ)."""
    return sorted(objet for objet in acc if acc[objet]["lb_produit"] is None)
