"""Routes de debug MySQL pour explorer le schéma et exécuter des requêtes de diagnostic."""

import logging
import time

from fastapi import APIRouter, HTTPException, Query

from app.config import MYSQL_DATABASE
from app.db.mysql import db_read

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/mysql", tags=["MySQL Debug"])


@router.get("/test")
async def mysql_test():
    """Requête de test sur la base MySQL (lecture)."""
    start = time.perf_counter()
    try:
        result = await db_read.fetch_one("SELECT 1 AS ok")
    except Exception as e:
        logger.error("Erreur lors du test MySQL : %s", e)
        raise HTTPException(
            status_code=500,
            detail="Erreur lors du test de connexion à MySQL.",
        ) from e
    duration_s = round(time.perf_counter() - start, 3)
    return {"test": "ok", "execution_time_s": duration_s, "result": result}


@router.get("/tables")
async def list_tables():
    """Liste toutes les tables de la base de données."""
    start = time.perf_counter()
    try:
        rows = await db_read.fetch_all(
            "SELECT table_name, table_type, table_rows, table_comment "
            "FROM information_schema.tables "
            "WHERE table_schema = %s "
            "ORDER BY table_name",
            (MYSQL_DATABASE,),
        )
    except Exception as e:
        logger.error("Erreur listing tables : %s", e)
        raise HTTPException(status_code=500, detail="Erreur listing tables.") from e
    duration_s = round(time.perf_counter() - start, 3)
    return {
        "execution_time_s": duration_s,
        "database": MYSQL_DATABASE,
        "count": len(rows),
        "tables": rows,
    }


@router.get("/columns")
async def list_columns(
    table: str = Query(..., description="Nom de la table"),
):
    """Liste les colonnes d'une table avec leur type et commentaire."""
    start = time.perf_counter()
    try:
        rows = await db_read.fetch_all(
            "SELECT column_name, column_type, is_nullable, column_key, "
            "column_default, column_comment "
            "FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "ORDER BY ordinal_position",
            (MYSQL_DATABASE, table),
        )
    except Exception as e:
        logger.error("Erreur listing colonnes de %s : %s", table, e)
        raise HTTPException(status_code=500, detail=f"Erreur listing colonnes de {table}.") from e
    if not rows:
        raise HTTPException(status_code=404, detail=f"Table '{table}' introuvable dans {MYSQL_DATABASE}.")
    duration_s = round(time.perf_counter() - start, 3)
    return {
        "execution_time_s": duration_s,
        "database": MYSQL_DATABASE,
        "table": table,
        "count": len(rows),
        "columns": rows,
    }


@router.get("/indexes")
async def list_indexes(
    table: str = Query(..., description="Nom de la table"),
):
    """Liste les index d'une table."""
    start = time.perf_counter()
    try:
        rows = await db_read.fetch_all(
            "SHOW INDEX FROM " + f"`{MYSQL_DATABASE}`.`{table}`"
        )
    except Exception as e:
        logger.error("Erreur listing index de %s : %s", table, e)
        raise HTTPException(status_code=500, detail=f"Erreur listing index de {table}.") from e
    duration_s = round(time.perf_counter() - start, 3)
    return {
        "execution_time_s": duration_s,
        "table": table,
        "count": len(rows),
        "indexes": rows,
    }


@router.get("/sample")
async def sample_rows(
    table: str = Query(..., description="Nom de la table"),
    limit: int = Query(10, ge=1, le=100, description="Nombre de lignes (max 100)"),
):
    """Retourne un échantillon de lignes d'une table."""
    start = time.perf_counter()
    try:
        # Vérifier que la table existe dans le bon schéma
        check = await db_read.fetch_one(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            (MYSQL_DATABASE, table),
        )
        if not check:
            raise HTTPException(status_code=404, detail=f"Table '{table}' introuvable dans {MYSQL_DATABASE}.")
        rows = await db_read.fetch_all(
            f"SELECT * FROM `{MYSQL_DATABASE}`.`{table}` LIMIT %s",
            (limit,),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Erreur sample de %s : %s", table, e)
        raise HTTPException(status_code=500, detail=f"Erreur sample de {table}.") from e
    duration_s = round(time.perf_counter() - start, 3)
    return {
        "execution_time_s": duration_s,
        "table": table,
        "count": len(rows),
        "data": rows,
    }


@router.get("/schema")
async def full_schema():
    """Retourne le schéma complet : chaque table avec ses colonnes."""
    start = time.perf_counter()
    try:
        tables = await db_read.fetch_all(
            "SELECT table_name, table_rows, table_comment "
            "FROM information_schema.tables "
            "WHERE table_schema = %s ORDER BY table_name",
            (MYSQL_DATABASE,),
        )
        schema = []
        for t in tables:
            cols = await db_read.fetch_all(
                "SELECT column_name, column_type, is_nullable, column_key, column_comment "
                "FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "ORDER BY ordinal_position",
                (MYSQL_DATABASE, t["table_name"]),
            )
            schema.append({**t, "columns": cols})
    except Exception as e:
        logger.error("Erreur schema complet : %s", e)
        raise HTTPException(status_code=500, detail="Erreur récupération schema.") from e
    duration_s = round(time.perf_counter() - start, 3)
    return {
        "execution_time_s": duration_s,
        "database": MYSQL_DATABASE,
        "table_count": len(schema),
        "schema": schema,
    }
