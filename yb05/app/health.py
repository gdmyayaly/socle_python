"""Vérifications de la base de données : configuration, informations serveur, disponibilité.

Ces fonctions ne dépendent d'aucun framework : elles retournent des dictionnaires et sont
utilisables aussi bien depuis la CLI (`app/main.py`) que depuis un module métier.
"""

import logging

from app.config import (
    HEALTH_CHECK_QUERY,
    MYSQL_DATABASE,
    MYSQL_HOST_WRITE,
    MYSQL_USER_WRITE,
)
from app.db.mysql import Database, db_read, db_write

logger = logging.getLogger(__name__)

# Informations du serveur MySQL et du schéma courant.
SERVER_INFO_QUERY = """
    SELECT VERSION()      AS version,
           DATABASE()     AS schema_courant,
           CURRENT_USER() AS utilisateur,
           @@hostname     AS hote_serveur,
           NOW()          AS date_serveur
"""

TABLE_COUNT_QUERY = """
    SELECT COUNT(*) AS nb_tables
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
"""


def check_config() -> dict:
    """Vérifie que la configuration MySQL minimale est présente (aucune connexion ouverte)."""
    mysql_config = bool(MYSQL_HOST_WRITE and MYSQL_USER_WRITE and MYSQL_DATABASE)
    return {
        "status": "ok" if mysql_config else "missing_config",
        "mysql_config": mysql_config,
    }


def describe_connection(db: Database) -> dict:
    """Paramètres de connexion d'une instance `Database`, **sans le mot de passe**."""
    return {
        "host": db.host,
        "port": db.port,
        "user": db.user,
        "database": db.database,
        "max_retries": db.max_retries,
        "retry_delay": db.retry_delay,
    }


async def fetch_server_info(db: Database | None = None) -> dict:
    """Interroge le serveur MySQL : version, schéma courant, utilisateur, nombre de tables.

    Retourne `{"status": "ok", ...}` ou `{"status": "disconnected", "error": "..."}`.
    """
    db = db if db is not None else db_read
    try:
        info = await db.fetch_one(SERVER_INFO_QUERY) or {}
        tables = await db.fetch_one(TABLE_COUNT_QUERY) or {}
    except Exception as e:
        logger.warning("Récupération des informations serveur échouée : %s", e)
        return {"status": "disconnected", "error": str(e)}

    return {
        "status": "ok",
        "version": info.get("version"),
        "schema_courant": info.get("schema_courant"),
        "utilisateur": info.get("utilisateur"),
        "hote_serveur": info.get("hote_serveur"),
        "date_serveur": str(info.get("date_serveur")) if info.get("date_serveur") else None,
        "nb_tables": tables.get("nb_tables"),
    }


async def _ping(db: Database, libelle: str) -> str:
    """Retourne `connected`, `error` ou `disconnected` pour une instance donnée."""
    try:
        result = await db.fetch_one(HEALTH_CHECK_QUERY)
        return "connected" if result else "error"
    except Exception as e:
        logger.warning("Vérification MySQL (%s) échouée : %s", libelle, e)
        return "disconnected"


async def check_resources() -> dict:
    """Teste la disponibilité réelle des deux instances MySQL (lecture et écriture).

    Ne lève jamais : chaque ressource est reportée à `connected`, `error` ou `disconnected`.
    """
    mysql_read_status = await _ping(db_read, "lecture")
    mysql_write_status = await _ping(db_write, "écriture")

    return {
        "status": "ok" if mysql_read_status == mysql_write_status == "connected" else "ko",
        "mysql_read": mysql_read_status,
        "mysql_write": mysql_write_status,
    }
