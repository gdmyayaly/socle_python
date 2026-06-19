"""Constantes et helpers partagés pour la route trafics (mode auto)."""

import json
import logging
import os
from calendar import monthrange
from datetime import datetime, timedelta

from fastapi import HTTPException

from app.config import (
    MAX_DATE_RANGE_DAYS,
)

logger = logging.getLogger(__name__)

# Noms des champs trafic (clé objet du mapping produit + colonnes de valeurs).
# Définis ici en dur pour faciliter la modification, sans passer par l'env.
TRAFIC_COL_OBJET = "lb_type_objet"
TRAFIC_COL_CONSTATE = "trafic_constate"
TRAFIC_COL_PREVISIONNEL = "trafic_prevu"

_TRAFIC_PRODUIT_MAPPING_DEFAUT = {
 "COURRIER - OBJETS ORDINAIRES MENAGE": "OO",
 "COURRIER - OBJETS ORDINAIRES CEDEX": "OO",
 "COURRIER - OBJETS SIGNALES (OS)": "OS",
 "PRESSE": "PR",
 "PPI / E-PAQ": "PP",
 "COLIS": "CO",
 "IMPRIMÉS PUBLICITAIRES (IP)": "IP",
}
try:
    _mapping_env = json.loads(os.getenv("TRAFIC_PRODUIT_MAPPING", "") or "{}")
    TRAFIC_PRODUIT_MAPPING = _mapping_env if _mapping_env else _TRAFIC_PRODUIT_MAPPING_DEFAUT
except json.JSONDecodeError:
    TRAFIC_PRODUIT_MAPPING = _TRAFIC_PRODUIT_MAPPING_DEFAUT


TRAFIC_PRODUITS = tuple(dict.fromkeys(TRAFIC_PRODUIT_MAPPING.values()))

TRAFIC_OBJET_LABELS = tuple(TRAFIC_PRODUIT_MAPPING.keys())

TABLES_PERIODE = {
    "jours": "g_trppu_trafics_jour",
    "semaines": "g_trppu_trafics_semaine",
    "mois": "g_trppu_trafics_mois",
}

DATE_COLUMN_PERIODE = {
    "jours": "da_comptage",
    "semaines": "co_semaine_comptage",
    "mois": "co_mois_comptage",
}

PARAMETRES_RAPPEL = (
    "Paramètres attendus : "
    "co_regate (code régate du site), "
    "date_debut (format AAAAMMJJ ou AAAA-MM-JJ), "
    "date_fin (format AAAAMMJJ ou AAAA-MM-JJ)."
)


def parse_date(value: str, nom_param: str) -> datetime:
    """Parse une date au format AAAAMMJJ ou AAAA-MM-JJ."""
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    msg = (
        f"Format de {nom_param} invalide '{value}'. "
        f"Attendu : AAAAMMJJ ou AAAA-MM-JJ. {PARAMETRES_RAPPEL}"
    )
    raise HTTPException(
        status_code=400,
        detail={"error": True, "message": msg, "code": 400},
    )


def render_sql(sql: str, params: dict) -> str:
    """Substitue les paramètres nommés (:name) dans le SQL pour l'affichage."""
    rendered = sql
    for key, value in params.items():
        literal = "NULL" if value is None else f"'{str(value).replace(chr(39), chr(39) * 2)}'"
        rendered = rendered.replace(f":{key}", literal)
    return rendered


def fmt_date(dt: datetime, periode: str = "jours") -> str:
    """Formate une date selon la période pour les requêtes SQL.

    jours    -> AAAA-MM-JJ
    semaines -> AAAA-NS  (numéro de semaine ISO)
    mois     -> AAAA-MM
    """
    if periode == "semaines":
        return f"{dt.isocalendar()[0]}-{dt.isocalendar()[1]:02d}"
    if periode == "mois":
        return dt.strftime("%Y-%m")
    return dt.strftime("%Y-%m-%d")


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
            segments.extend(
                _decompose_semaines_jours(dt_debut, mois_start - timedelta(days=1))
            )
        segments.append(("mois", mois_start, mois_end))
        if mois_end < dt_fin:
            segments.extend(
                _decompose_semaines_jours(mois_end + timedelta(days=1), dt_fin)
            )
    else:
        segments.extend(_decompose_semaines_jours(dt_debut, dt_fin))

    return segments


def validate_params(co_regate, date_debut, date_fin):
    """Valide les paramètres communs et retourne (dt_debut, dt_fin)."""
    manquants = []
    if not co_regate:
        manquants.append("co_regate")
    if not date_debut:
        manquants.append("date_debut")
    if not date_fin:
        manquants.append("date_fin")
    if manquants:
        msg = f"Paramètre(s) manquant(s) : {', '.join(manquants)}. {PARAMETRES_RAPPEL}"
        logger.warning(msg)
        raise HTTPException(
            status_code=400,
            detail={"error": True, "message": msg, "code": 400},
        )

    dt_debut = parse_date(date_debut, "date_debut")
    dt_fin = parse_date(date_fin, "date_fin")

    if dt_debut > dt_fin:
        msg = f"date_debut doit être antérieure ou égale à date_fin. {PARAMETRES_RAPPEL}"
        logger.warning(msg)
        raise HTTPException(
            status_code=400,
            detail={"error": True, "message": msg, "code": 400},
        )

    ecart = (dt_fin - dt_debut).days
    if ecart > MAX_DATE_RANGE_DAYS:
        msg = (
            f"L'écart entre les dates ne doit pas dépasser 2 ans ({MAX_DATE_RANGE_DAYS} jours). "
            f"Écart actuel : {ecart} jours. {PARAMETRES_RAPPEL}"
        )
        logger.warning(msg)
        raise HTTPException(
            status_code=400,
            detail={"error": True, "message": msg, "code": 400},
        )

    return dt_debut, dt_fin


PARAMETRES_RAPPEL_PIVOT = (
    "Paramètres attendus : "
    "co_regate (code régate du site), "
    "date_debut (format AAAAMMJJ), "
    "date_fin (format AAAAMMJJ), "
    "date_pivot (format AAAAMMJJ)."
)


def validate_params_pivot(co_regate, date_debut, date_fin, date_pivot):
    """DSR-666 : valide les 4 paramètres et retourne (dt_debut, dt_fin, dt_pivot).

    Renvoie un HTTP 400 explicite (paramètres manquants rappelés) si un paramètre
    manque, si date_debut > date_fin, ou si la période dépasse 2 ans.
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
        raise HTTPException(
            status_code=400,
            detail={"error": True, "message": msg, "code": 400},
        )

    dt_debut = parse_date(date_debut, "date_debut")
    dt_fin = parse_date(date_fin, "date_fin")
    dt_pivot = parse_date(date_pivot, "date_pivot")

    if dt_debut > dt_fin:
        msg = f"date_debut doit être antérieure ou égale à date_fin. {PARAMETRES_RAPPEL_PIVOT}"
        logger.warning(msg)
        raise HTTPException(
            status_code=400,
            detail={"error": True, "message": msg, "code": 400},
        )

    ecart = (dt_fin - dt_debut).days
    if ecart > MAX_DATE_RANGE_DAYS:
        msg = (
            f"La période dépasse les 2 ans d'interrogation permis ({MAX_DATE_RANGE_DAYS} jours). "
            f"Écart actuel : {ecart} jours. {PARAMETRES_RAPPEL_PIVOT}"
        )
        logger.warning(msg)
        raise HTTPException(
            status_code=400,
            detail={"error": True, "message": msg, "code": 400},
        )

    return dt_debut, dt_fin, dt_pivot


def split_by_pivot(dt_debut, dt_fin, dt_pivot):
    """Découpe la période en (plage réelle, plage prévisionnelle) autour du pivot.

    - réel : dates **strictement antérieures** au pivot -> [dt_debut, min(dt_fin, pivot-1j)]
    - prév : dates **>= pivot**                          -> [max(dt_debut, pivot), dt_fin]

    Découper en amont (au jour près) évite toute granularité mois/semaine à cheval
    sur le pivot. Retourne (reel, prev), chacun None si la plage correspondante est
    vide (période entièrement future -> reel=None ; entièrement passée -> prev=None).
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


def map_produit(objet_label):
    """Mappe un libellé `lb_type_objet` Databricks vers un code produit (ou None)."""
    if objet_label is None:
        return None
    return TRAFIC_PRODUIT_MAPPING.get(str(objet_label).strip())


def empty_trafics_accumulator():
    """Initialise l'accumulateur {produit: {trafic_brut, trafic_previsionnel}}.

    Les 6 objets cf _TRAFIC_PRODUIT_MAPPING_DEFAUT sont toujours présents (hydratés à 0), même sans trafic.
    """
    return {
        produit: {"trafic_brut": 0, "trafic_previsionnel": 0} for produit in TRAFIC_PRODUITS
    }


def accumulate_trafics(rows, value_col, target_key, acc):
    """Ajoute la somme de `value_col` (par produit) dans `acc[produit][target_key]`.

    Les lignes dont l'objet n'est pas mappé (ou hors des 6 produits) sont ignorées ;
    les valeurs nulles comptent pour 0.
    """
    for row in rows:
        produit = map_produit(row.get(TRAFIC_COL_OBJET))
        if produit is None or produit not in acc:
            continue
        valeur = row.get(value_col)
        if valeur is None:
            continue
        acc[produit][target_key] += valeur
