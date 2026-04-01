import logging
import time
from datetime import datetime

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
    "periode (jours, semaines, mois), "
    "code_regate (code régate du site), "
    "date_debut (format AAAAMMJJ), "
    "date_fin (format AAAAMMJJ)."
)

router = APIRouter(prefix="/trafics", tags=["Trafics"])


@router.get("/get_trafics")
def get_trafics(
    periode: str | None = Query(None, description="Période : jours, semaines ou mois"),
    code_regate: str | None = Query(None, description="Code régate de l'entité"),
    date_debut: str | None = Query(None, description="Date de début au format AAAAMMJJ"),
    date_fin: str | None = Query(None, description="Date de fin au format AAAAMMJJ"),
):
    """Récupère les trafics par code régate et période de dates.

    La période détermine la table interrogée (trafics_jours, trafics_semaines, trafics_mois).
    L'écart entre date_debut et date_fin ne doit pas dépasser 2 ans.
    """
    logger.info(
        "get_trafics appelé : periode=%s, code_regate=%s, date_debut=%s, date_fin=%s",
        periode,
        code_regate,
        date_debut,
        date_fin,
    )

    # Vérification des paramètres manquants
    manquants = []
    if not periode:
        manquants.append("periode")
    if not code_regate:
        manquants.append("code_regate")
    if not date_debut:
        manquants.append("date_debut")
    if not date_fin:
        manquants.append("date_fin")

    if manquants:
        msg = f"Paramètre(s) manquant(s) : {', '.join(manquants)}. {PARAMETRES_RAPPEL}"
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    # Validation de la période
    periode_lower = periode.lower()
    if periode_lower not in TABLES_PERIODE:
        msg = f"Période invalide '{periode}'. Valeurs acceptées : jours, semaines, mois. {PARAMETRES_RAPPEL}"
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    # Validation du format des dates
    try:
        dt_debut = datetime.strptime(date_debut, "%Y%m%d")
    except ValueError:
        msg = f"Format de date_debut invalide '{date_debut}'. Attendu : AAAAMMJJ (ex: 20240101). {PARAMETRES_RAPPEL}"
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    try:
        dt_fin = datetime.strptime(date_fin, "%Y%m%d")
    except ValueError:
        msg = f"Format de date_fin invalide '{date_fin}'. Attendu : AAAAMMJJ (ex: 20241231). {PARAMETRES_RAPPEL}"
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    # Vérification cohérence des dates
    if dt_debut > dt_fin:
        msg = f"date_debut doit être antérieure ou égale à date_fin. {PARAMETRES_RAPPEL}"
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    # Vérification écart max 2 ans (730 jours)
    ecart = (dt_fin - dt_debut).days
    if ecart > 730:
        msg = (
            f"L'écart entre les dates ne doit pas dépasser 2 ans (730 jours). "
            f"Écart actuel : {ecart} jours. {PARAMETRES_RAPPEL}"
        )
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    # Construction et exécution de la requête
    table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TABLES_PERIODE[periode_lower]}"
    query = (
        f"SELECT * FROM {table} "
        f"WHERE code_regate = '{code_regate}' AND date_jour BETWEEN '{date_debut}' AND '{date_fin}'"
    )

    logger.info(
        "Exécution requête sur %s pour code_regate=%s entre %s et %s",
        table, code_regate, date_debut, date_fin,
    )

    start = time.perf_counter()
    try:
        results = databricks.fetch_all(query)
    except Exception as e:
        logger.error("Erreur lors de la requête get_trafics : %s", e)
        raise HTTPException(
            status_code=500,
            detail="Erreur lors de la récupération des trafics.",
        )
    duration_ms = round((time.perf_counter() - start) * 1000, 2)

    logger.info("get_trafics : %d résultats retournés en %.2fms", len(results), duration_ms)
    return {
        "count": len(results),
        "execution_time_ms": duration_ms,
        "periode": periode_lower,
        "code_regate": code_regate,
        "date_debut": date_debut,
        "date_fin": date_fin,
        "data": results,
    }
