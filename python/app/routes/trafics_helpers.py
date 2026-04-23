"""Constantes et helpers partagés pour la route trafics (mode auto)."""

import logging
from calendar import monthrange
from datetime import datetime, timedelta

from fastapi import HTTPException

from app.config import MAX_DATE_RANGE_DAYS

logger = logging.getLogger(__name__)

TABLES_PERIODE = {
    "jours": "g_mdp_trafics_jour_actualise",
    "semaines": "g_mdp_trafics_semaine_actualise",
    "mois": "g_mdp_trafics_mois_actualise",
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
