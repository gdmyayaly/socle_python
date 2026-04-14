"""Routes d'exploration des métadonnées Databricks (catalogues, schémas, tables, colonnes) pour faciliter la visualisation des tables. """

import logging
import time

from fastapi import APIRouter, HTTPException
from app.config import HEALTH_CHECK_QUERY
from app.db.databricks import databricks

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/databricks", tags=["Databricks"])


@router.get("/test")
def databricks_test():
    """Requête de test sur la SQL Warehouse."""
    start = time.perf_counter()
    try:
        result = databricks.fetch_one(HEALTH_CHECK_QUERY)
    except Exception as e:
        logger.error("Erreur lors du test Databricks : %s", e)
        raise HTTPException(
            status_code=500,
            detail="Erreur lors du test de connexion à Databricks.",
        ) from e
    duration_s = round(time.perf_counter() - start, 3)
    return {"test": "ok", "execution_time_s": duration_s, "result": result}
