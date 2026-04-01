import logging
import time

from fastapi import APIRouter, HTTPException, Query

from app.config import DATABRICKS_CATALOG, DATABRICKS_SCHEMA, DEBUG_SHOW_QUERY
from app.db.databricks import databricks

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/databricks", tags=["Databricks"])


@router.get("/test")
def databricks_test():
    """Requête de test sur la SQL Warehouse."""
    start = time.perf_counter()
    try:
        result = databricks.fetch_one("SELECT 1 AS ok")
    except Exception as e:
        logger.error("Erreur lors du test Databricks : %s", e)
        raise HTTPException(
            status_code=500,
            detail="Erreur lors du test de connexion à Databricks.",
        )
    duration_s = round(time.perf_counter() - start, 3)
    return {"test": "ok", "execution_time_s": duration_s, "result": result}


@router.get("/catalogs")
def databricks_catalogs():
    """Liste tous les catalogues accessibles."""
    start = time.perf_counter()
    try:
        result = databricks.catalogs()
    except Exception as e:
        logger.error("Erreur lors de la récupération des catalogues : %s", e)
        raise HTTPException(
            status_code=500,
            detail="Erreur lors de la récupération des catalogues.",
        )
    duration_s = round(time.perf_counter() - start, 3)
    return {"execution_time_s": duration_s, "data": result}


@router.get("/schemas")
def databricks_schemas():
    """Liste les schémas du catalogue."""
    start = time.perf_counter()
    try:
        result = databricks.schemas()
    except Exception as e:
        logger.error("Erreur lors de la récupération des schémas : %s", e)
        raise HTTPException(
            status_code=500,
            detail="Erreur lors de la récupération des schémas.",
        )
    duration_s = round(time.perf_counter() - start, 3)
    return {"execution_time_s": duration_s, "data": result}


@router.get("/tables")
def databricks_tables():
    """Liste les tables du schéma par défaut."""
    start = time.perf_counter()
    try:
        result = databricks.tables()
    except Exception as e:
        logger.error("Erreur lors de la récupération des tables : %s", e)
        raise HTTPException(
            status_code=500,
            detail="Erreur lors de la récupération des tables.",
        )
    duration_s = round(time.perf_counter() - start, 3)
    return {"execution_time_s": duration_s, "data": result}


@router.get("/tables/{schema}")
def databricks_tables_by_schema(schema: str):
    """Liste les tables d'un schéma donné."""
    start = time.perf_counter()
    try:
        result = databricks.tables(schema=schema)
    except Exception as e:
        logger.error("Erreur lors de la récupération des tables du schéma '%s' : %s", schema, e)
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lors de la récupération des tables du schéma '{schema}'.",
        )
    duration_s = round(time.perf_counter() - start, 3)
    return {"execution_time_s": duration_s, "data": result}


@router.get("/columns/{table}")
def databricks_columns(table: str):
    """Liste les colonnes d'une table du schéma par défaut."""
    start = time.perf_counter()
    try:
        result = databricks.columns(table=table)
    except Exception as e:
        logger.error("Erreur lors de la récupération des colonnes de '%s' : %s", table, e)
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lors de la récupération des colonnes de la table '{table}'.",
        )
    duration_s = round(time.perf_counter() - start, 3)
    return {"execution_time_s": duration_s, "data": result}


@router.get("/columns/{schema}/{table}")
def databricks_columns_by_schema(schema: str, table: str):
    """Liste les colonnes d'une table dans un schéma donné."""
    start = time.perf_counter()
    try:
        result = databricks.columns(schema=schema, table=table)
    except Exception as e:
        logger.error("Erreur lors de la récupération des colonnes de '%s.%s' : %s", schema, table, e)
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lors de la récupération des colonnes de '{schema}.{table}'.",
        )
    duration_s = round(time.perf_counter() - start, 3)
    return {"execution_time_s": duration_s, "data": result}


@router.get("/trafics_jours")
def databricks_trafics_jours(
    co_regate: str | None = Query(None, description="Code régate de l'entité"),
    da_comptage: str | None = Query(None, description="Date de comptage (AAAA-MM-JJ)"),
    limit: int = Query(10, ge=1, description="Nombre de résultats"),
):
    """Requête simple sur la table trafics jour."""
    table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.g_mdp_trafics_jour_actualise"
    conditions = []
    if co_regate:
        conditions.append(f"co_regate = '{co_regate}'")
    if da_comptage:
        conditions.append(f"da_comptage = '{da_comptage}'")
    where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"SELECT * FROM {table}{where_clause} LIMIT {limit}"

    start = time.perf_counter()
    try:
        results = databricks.fetch_all(query)
    except Exception as e:
        logger.error("Erreur lors de la requête trafics_jours : %s", e)
        raise HTTPException(
            status_code=500,
            detail="Erreur lors de la récupération des trafics jours.",
        )
    duration_s = round(time.perf_counter() - start, 3)

    response = {"count": len(results), "execution_time_s": duration_s, "data": results}
    if DEBUG_SHOW_QUERY:
        response["query"] = query
    return response
