"""Routes de vérification de santé de l'application et de ses dépendances."""

import logging

from fastapi import APIRouter

from app.config import HEALTH_CHECK_QUERY
from app.db.databricks import databricks
from app.db.mysql import db

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/")
def root():
    return {"message": "Bienvenue sur l'API trppu"}


@router.get("/health")
def health():
    """Vérifie que les configurations nécessaires sont présentes."""
    from app.config import (
        MYSQL_HOST, MYSQL_USER, MYSQL_DATABASE,
        DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH,
        DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET,
    )

    mysql_config = bool(MYSQL_HOST and MYSQL_USER and MYSQL_DATABASE)
    databricks_config = bool(
        DATABRICKS_SERVER_HOSTNAME and DATABRICKS_HTTP_PATH
        and DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET
    )

    return {
        "status": "ok" if mysql_config and databricks_config else "missing_config",
        "mysql_config": mysql_config,
        "databricks_config": databricks_config,
    }


@router.get("/health/resources")
async def health_resources():
    """Vérification de la connectivité aux ressources (MySQL, Databricks)."""
    try:
        result = await db.fetch_one(HEALTH_CHECK_QUERY)
        mysql_status = "connected" if result else "error"
    except Exception as e:
        logger.warning("Health check MySQL échoué : %s", e)
        mysql_status = "disconnected"

    try:
        result = databricks.fetch_one(HEALTH_CHECK_QUERY)
        databricks_status = "connected" if result else "error"
    except Exception as e:
        logger.warning("Health check Databricks échoué : %s", e)
        databricks_status = "disconnected"

    return {"status": "ok", "mysql": mysql_status, "databricks": databricks_status}
