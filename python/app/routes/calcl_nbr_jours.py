"""Route de calcul du nombre de jours ouvrés par semaine entre deux dates."""

import logging
import time
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query

from app.config import MAX_DATE_RANGE_DAYS

logger = logging.getLogger(__name__)

PARAMETRES_RAPPEL = (
    "Paramètres attendus : "
    "date_debut (format AAAAMMJJ), "
    "date_fin (format AAAAMMJJ)."
)

router = APIRouter(prefix="/calcl_nbr_jours", tags=["Calcul nombre de jours"])


@router.get("/get_nbr_jours")
def get_nbr_jours(
    date_debut: str | None = Query(None, description="Date de début au format AAAAMMJJ"),
    date_fin: str | None = Query(None, description="Date de fin au format AAAAMMJJ"),
):
    """Calcule le nombre de jours ouvrés par semaine entre deux dates (dimanche exclu).

    Retourne pour chaque semaine le nombre de jours ouverts (lundi=0 à samedi=5,
    dimanche exclu).
    """
    logger.info(
        "get_nbr_jours appelé : date_debut=%s, date_fin=%s",
        date_debut,
        date_fin,
    )

    # Vérification des paramètres manquants
    manquants = []
    if not date_debut:
        manquants.append("date_debut")
    if not date_fin:
        manquants.append("date_fin")

    if manquants:
        msg = f"Paramètre(s) manquant(s) : {', '.join(manquants)}. {PARAMETRES_RAPPEL}"
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    # Validation du format des dates
    try:
        dt_debut = datetime.strptime(date_debut, "%Y%m%d")
    except ValueError as e:
        msg = f"Format de date_debut invalide '{date_debut}'. Attendu : AAAAMMJJ (ex: 20240101). {PARAMETRES_RAPPEL}"
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg) from e

    try:
        dt_fin = datetime.strptime(date_fin, "%Y%m%d")
    except ValueError as e:
        msg = f"Format de date_fin invalide '{date_fin}'. Attendu : AAAAMMJJ (ex: 20241231). {PARAMETRES_RAPPEL}"
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg) from e

    # Vérification cohérence des dates
    if dt_debut > dt_fin:
        msg = f"date_debut doit être antérieure ou égale à date_fin. {PARAMETRES_RAPPEL}"
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    # Vérification écart max 2 ans (730 jours)
    ecart = (dt_fin - dt_debut).days
    if ecart > MAX_DATE_RANGE_DAYS:
        msg = (
            f"L'écart entre les dates ne doit pas dépasser 2 ans ({MAX_DATE_RANGE_DAYS} jours). "
            f"Écart actuel : {ecart} jours. {PARAMETRES_RAPPEL}"
        )
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    start = time.perf_counter()

    # Calcul du nombre de jours ouverts par semaine (dimanche exclu)
    semaines = {}
    current = dt_debut
    while current <= dt_fin:
        # isocalendar() retourne (année ISO, numéro de semaine, jour de la semaine)
        iso_year, iso_week, _ = current.isocalendar()
        cle_semaine = f"{iso_year}-S{iso_week:02d}"

        if cle_semaine not in semaines:
            semaines[cle_semaine] = {
                "semaine": cle_semaine,
                "date_debut_semaine": None,
                "date_fin_semaine": None,
                "nbr_jours_ouverts": 0,
            }

        # Dimanche = 7 en isocalendar, on l'exclut
        if current.isoweekday() != 7:
            semaines[cle_semaine]["nbr_jours_ouverts"] += 1

        # Mise à jour des bornes de la semaine
        date_str = current.strftime("%Y%m%d")
        if semaines[cle_semaine]["date_debut_semaine"] is None:
            semaines[cle_semaine]["date_debut_semaine"] = date_str
        semaines[cle_semaine]["date_fin_semaine"] = date_str

        current += timedelta(days=1)

    resultat = list(semaines.values())
    total_jours_ouverts = sum(s["nbr_jours_ouverts"] for s in resultat)
    duration_ms = round((time.perf_counter() - start) * 1000, 2)

    logger.info(
        "get_nbr_jours : %d semaines, %d jours ouverts au total en %.2fms",
        len(resultat),
        total_jours_ouverts,
        duration_ms,
    )

    return {
        "date_debut": date_debut,
        "date_fin": date_fin,
        "total_jours_ouverts": total_jours_ouverts,
        "nbr_semaines": len(resultat),
        "execution_time_ms": duration_ms,
        "semaines": resultat,
    }
