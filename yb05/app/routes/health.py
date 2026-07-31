"""Routes de vérification de santé de l'application et de ses dépendances."""

import logging

from fastapi import APIRouter

from app.config import HEALTH_CHECK_QUERY
from app.db.mysql import db_read, db_write

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/")
def root():
    return {"message": "Bienvenue sur l'API yb05"}


@router.get("/health")
def health():
    """Vérifie que les configurations nécessaires sont présentes."""
    from app.config import MYSQL_HOST_WRITE, MYSQL_USER_WRITE, MYSQL_DATABASE

    mysql_config = bool(MYSQL_HOST_WRITE and MYSQL_USER_WRITE and MYSQL_DATABASE)

    return {
        "status": "ok" if mysql_config else "missing_config",
        "mysql_config": mysql_config,
    }


@router.get("/health/resources")
async def health_resources():
    """Vérification de la connectivité aux ressources (MySQL lecture et écriture)."""
    try:
        result = await db_read.fetch_one(HEALTH_CHECK_QUERY)
        mysql_read_status = "connected" if result else "error"
    except Exception as e:
        logger.warning("Health check MySQL (read) échoué : %s", e)
        mysql_read_status = "disconnected"

    try:
        result = await db_write.fetch_one(HEALTH_CHECK_QUERY)
        mysql_write_status = "connected" if result else "error"
    except Exception as e:
        logger.warning("Health check MySQL (write) échoué : %s", e)
        mysql_write_status = "disconnected"

    return {
        "status": "ok",
        "mysql_read": mysql_read_status,
        "mysql_write": mysql_write_status,
    }
