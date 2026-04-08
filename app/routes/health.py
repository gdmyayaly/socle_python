"""Routes de vérification de santé de l'application et de ses dépendances."""

import logging

from fastapi import APIRouter

from app.config import SKIP_MYSQL
from app.db.databricks import databricks
from app.db.mysql import db

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/")
def root():
    return {"message": "Bienvenue sur l'API trppu"}


@router.get("/health")
async def health():
    if SKIP_MYSQL:
        mysql_status = "skipped"
    else:
        try:
            result = await db.fetch_one("SELECT 1 AS ok")
            mysql_status = "connected" if result else "error"
        except Exception as e:
            logger.warning("Health check MySQL échoué : %s", e)
            mysql_status = "disconnected"

    try:
        result = databricks.fetch_one("SELECT 1 AS ok")
        databricks_status = "connected" if result else "error"
    except Exception as e:
        logger.warning("Health check Databricks échoué : %s", e)
        databricks_status = "disconnected"

    return {"status": "ok", "mysql": mysql_status, "databricks": databricks_status}
