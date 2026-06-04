"""Route de calcul du nombre de jours ouvrés par semaine entre deux dates."""

import logging
import time
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query

from app.config import MAX_DATE_RANGE_DAYS
from app.services.jours_service import compute_nb_jours

logger = logging.getLogger(__name__)


def _parse_date_souple(value: str, nom: str) -> datetime:
    """Parse une date au format AAAAMMJJ ou AAAA-MM-JJ."""
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise HTTPException(
        status_code=400,
        detail=f"Format de {nom} invalide '{value}'. Attendu : AAAAMMJJ ou AAAA-MM-JJ.",
    )

PARAMETRES_RAPPEL = (
    "Paramètres attendus : "
    "date_debut (format AAAAMMJJ), "
    "date_fin (format AAAAMMJJ)."
)

router = APIRouter(prefix="/trppu-api/calcl_nbr_jours", tags=["Calcul nombre de jours"])

@router.get("/get_nb_jours")
async def get_nb_jours(
    date_debut: str = Query(..., description="Date de début AAAAMMJJ ou AAAA-MM-JJ"),
    date_fin: str = Query(..., description="Date de fin AAAAMMJJ ou AAAA-MM-JJ"),
):
    """DSR-613 : nombre de jours ouvrés (lun-ven) et ouvrables (lun-sam), fériés déduits.

    Service "CalculerNbJours" appelé par "RecupererTrafics". Les jours fériés / fermés
    sont récupérés via l'API jours-fermes (cf. `app.services.jours_fermes_client`).
    """
    dt_debut = _parse_date_souple(date_debut, "date_debut")
    dt_fin = _parse_date_souple(date_fin, "date_fin")
    if dt_debut > dt_fin:
        raise HTTPException(
            status_code=400, detail="date_debut doit être antérieure ou égale à date_fin."
        )
    if (dt_fin - dt_debut).days > MAX_DATE_RANGE_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"L'écart entre les dates ne doit pas dépasser {MAX_DATE_RANGE_DAYS} jours.",
        )

    start = time.perf_counter()
    logger.info("get_nb_jours appelé : date_debut=%s, date_fin=%s", date_debut, date_fin)
    try:
        nbj = await compute_nb_jours(dt_debut.date(), dt_fin.date())
    except Exception as e:
        logger.exception("Erreur calcul nb_jours (debut=%s, fin=%s)", date_debut, date_fin)
        raise HTTPException(status_code=500, detail="Erreur calcul nombre de jours.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    logger.info(
        "get_nb_jours : ouvres=%d, ouvrables=%d en %.2fms",
        nbj.nb_jours_ouvres,
        nbj.nb_jours_ouvrables,
        duration_ms,
    )
    return {
        "date_debut": date_debut,
        "date_fin": date_fin,
        "nb_jours_total": nbj.nb_jours_total,
        "nb_jours_ouvres_bruts": nbj.nb_jours_ouvres_bruts,
        "nb_jours_ouvrables_bruts": nbj.nb_jours_ouvrables_bruts,
        "nb_feries_hors_weekend": nbj.nb_feries_hors_weekend,
        "nb_feries_samedi": nbj.nb_feries_samedi,
        "nbJoursOuvres": nbj.nb_jours_ouvres,
        "nbJoursOuvrables": nbj.nb_jours_ouvrables,
        "execution_time_ms": duration_ms,
    }
