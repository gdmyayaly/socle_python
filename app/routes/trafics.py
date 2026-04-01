import logging
import time
from calendar import monthrange
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query

from app.config import DATABRICKS_CATALOG, DATABRICKS_SCHEMA
from app.db.databricks import databricks

logger = logging.getLogger(__name__)

TABLES_PERIODE = {
    "jours": "g_mdp_trafics_jour_actualise",
    "semaines": "g_mdp_trafics_semaine_actualise",
    "mois": "g_mdp_trafics_mois_actualise",
}

PARAMETRES_RAPPEL = (
    "Paramètres attendus : "
    "periode (jours, semaines, mois, auto), "
    "co_regate (code régate du site), "
    "date_debut (format AAAAMMJJ ou AAAA-MM-JJ), "
    "date_fin (format AAAAMMJJ ou AAAA-MM-JJ)."
)

router = APIRouter(prefix="/trafics", tags=["Trafics"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_date(value: str, nom_param: str) -> datetime:
    """Parse une date au format AAAAMMJJ ou AAAA-MM-JJ."""
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise HTTPException(
        status_code=400,
        detail=(
            f"Format de {nom_param} invalide '{value}'. "
            f"Attendu : AAAAMMJJ ou AAAA-MM-JJ. {PARAMETRES_RAPPEL}"
        ),
    )


def fmt_date(dt: datetime) -> str:
    """Formate une date en AAAA-MM-JJ pour les requêtes SQL."""
    return dt.strftime("%Y-%m-%d")


def _decompose_semaines_jours(dt_start: datetime, dt_end: datetime):
    """Découpe un intervalle en semaines complètes (lun-dim) et jours restants."""
    parts: list[tuple[str, datetime, datetime]] = []

    # Premier lundi >= dt_start
    days_to_monday = (7 - dt_start.weekday()) % 7
    first_monday = dt_start + timedelta(days=days_to_monday)

    # Dernier dimanche <= dt_end
    if dt_end.weekday() == 6:
        last_sunday = dt_end
    else:
        last_sunday = dt_end - timedelta(days=dt_end.weekday() + 1)

    if first_monday + timedelta(days=6) <= last_sunday:
        # Jours avant le premier lundi
        if dt_start < first_monday:
            parts.append(("jours", dt_start, first_monday - timedelta(days=1)))
        # Semaines complètes
        parts.append(("semaines", first_monday, last_sunday))
        # Jours après le dernier dimanche
        if last_sunday < dt_end:
            parts.append(("jours", last_sunday + timedelta(days=1), dt_end))
    else:
        parts.append(("jours", dt_start, dt_end))

    return parts


def decompose_auto(dt_debut: datetime, dt_fin: datetime):
    """Découpe un intervalle en requêtes optimales sur mois / semaines / jours."""
    segments: list[tuple[str, datetime, datetime]] = []

    # --- Bornes des mois complets ---
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
        # Préfixe : jours/semaines avant les mois complets
        if dt_debut < mois_start:
            segments.extend(
                _decompose_semaines_jours(dt_debut, mois_start - timedelta(days=1))
            )
        # Mois complets
        segments.append(("mois", mois_start, mois_end))
        # Suffixe : jours/semaines après les mois complets
        if mois_end < dt_fin:
            segments.extend(
                _decompose_semaines_jours(mois_end + timedelta(days=1), dt_fin)
            )
    else:
        # Pas de mois complet → découper en semaines/jours
        segments.extend(_decompose_semaines_jours(dt_debut, dt_fin))

    return segments


def build_query(periode: str, co_regate: str, dt_start: datetime, dt_end: datetime) -> str:
    """Construit une requête SELECT pour une période donnée."""
    table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TABLES_PERIODE[periode]}"
    return (
        f"SELECT * FROM {table} "
        f"WHERE co_regate = '{co_regate}' "
        f"AND da_comptage BETWEEN '{fmt_date(dt_start)}' AND '{fmt_date(dt_end)}'"
    )


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get("/get_trafics")
def get_trafics(
    periode: str | None = Query(None, description="Période : jours, semaines, mois ou auto"),
    co_regate: str | None = Query(None, description="Code régate de l'entité"),
    date_debut: str | None = Query(None, description="Date de début (AAAAMMJJ ou AAAA-MM-JJ)"),
    date_fin: str | None = Query(None, description="Date de fin (AAAAMMJJ ou AAAA-MM-JJ)"),
):
    """Récupère les trafics par code régate et période de dates.

    En mode **auto**, l'intervalle est découpé dynamiquement en requêtes
    sur les tables mois, semaines et jours pour optimiser les performances.
    """
    logger.info(
        "get_trafics appelé : periode=%s, co_regate=%s, date_debut=%s, date_fin=%s",
        periode, co_regate, date_debut, date_fin,
    )

    # --- Validation des paramètres ---
    manquants = []
    if not periode:
        manquants.append("periode")
    if not co_regate:
        manquants.append("co_regate")
    if not date_debut:
        manquants.append("date_debut")
    if not date_fin:
        manquants.append("date_fin")
    if manquants:
        msg = f"Paramètre(s) manquant(s) : {', '.join(manquants)}. {PARAMETRES_RAPPEL}"
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    periode_lower = periode.lower()
    periodes_valides = (*TABLES_PERIODE, "auto")
    if periode_lower not in periodes_valides:
        msg = (
            f"Période invalide '{periode}'. "
            f"Valeurs acceptées : {', '.join(periodes_valides)}. {PARAMETRES_RAPPEL}"
        )
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    dt_debut = parse_date(date_debut, "date_debut")
    dt_fin = parse_date(date_fin, "date_fin")

    if dt_debut > dt_fin:
        msg = f"date_debut doit être antérieure ou égale à date_fin. {PARAMETRES_RAPPEL}"
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    ecart = (dt_fin - dt_debut).days
    if ecart > 730:
        msg = (
            f"L'écart entre les dates ne doit pas dépasser 2 ans (730 jours). "
            f"Écart actuel : {ecart} jours. {PARAMETRES_RAPPEL}"
        )
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    # --- Construction des requêtes ---
    if periode_lower == "auto":
        segments = decompose_auto(dt_debut, dt_fin)
    else:
        segments = [(periode_lower, dt_debut, dt_fin)]

    queries = [
        {"periode": seg[0], "query": build_query(seg[0], co_regate, seg[1], seg[2])}
        for seg in segments
    ]

    logger.info("Exécution de %d requête(s) pour co_regate=%s", len(queries), co_regate)

    # --- Mode auto : aperçu des requêtes sans exécution ---
    if periode_lower == "auto":
        logger.info("Mode auto (preview) : %d requête(s) générées", len(queries))
        return {
            "mode": "preview",
            "periode": "auto",
            "co_regate": co_regate,
            "date_debut": fmt_date(dt_debut),
            "date_fin": fmt_date(dt_fin),
            "nb_queries": len(queries),
            "queries": [
                {
                    "index": i + 1,
                    "periode": q["periode"],
                    "query": q["query"],
                }
                for i, q in enumerate(queries)
            ],
        }

    # --- Exécution (jours, semaines, mois) ---
    start = time.perf_counter()
    try:
        results = databricks.fetch_all(queries[0]["query"])
    except Exception as e:
        logger.error("Erreur requête (%s) : %s", queries[0]["periode"], e)
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lors de la récupération des trafics ({queries[0]['periode']}).",
        )
    duration_s = round(time.perf_counter() - start, 3)

    logger.info("get_trafics : %d résultats en %.3fs", len(results), duration_s)
    return {
        "count": len(results),
        "execution_time_s": duration_s,
        "periode": periode_lower,
        "co_regate": co_regate,
        "date_debut": fmt_date(dt_debut),
        "date_fin": fmt_date(dt_fin),
        "data": results,
    }
