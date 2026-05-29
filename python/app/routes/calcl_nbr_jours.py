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


@router.get("/get_nb_jours")
async def get_nb_jours(
    date_debut: str = Query(..., description="Date de début AAAAMMJJ ou AAAA-MM-JJ"),
    date_fin: str = Query(..., description="Date de fin AAAAMMJJ ou AAAA-MM-JJ"),
):
    """DSR-613 : nombre de jours ouvrés (lun-ven) et ouvrables (lun-sam), fériés déduits.

    Service "CalculerNbJours" appelé par "RecupererTrafics". Les jours fériés nationaux
    sont chargés depuis la table `trppu_jours_feries`.
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
