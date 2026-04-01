import logging
import time

from fastapi import APIRouter, HTTPException, Query

from app.config import DATABRICKS_CATALOG, DATABRICKS_SCHEMA
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
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"test": "ok", "execution_time_ms": duration_ms, "result": result}


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
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"execution_time_ms": duration_ms, "data": result}


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
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"execution_time_ms": duration_ms, "data": result}


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
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"execution_time_ms": duration_ms, "data": result}


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
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"execution_time_ms": duration_ms, "data": result}


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
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"execution_time_ms": duration_ms, "data": result}


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
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"execution_time_ms": duration_ms, "data": result}


@router.get("/trafics_jours")
def trafics_jours(
    code_regate: str | None = Query(None, description="Code régate de l'entité"),
    limit: int = 10,
):
    """Récupère les données de la table trafics_jours."""
    table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.trafics_jours"
    if code_regate:
        query = f"SELECT * FROM {table} WHERE co_regate = '{code_regate}' LIMIT {limit}"
    else:
        query = f"SELECT * FROM {table} LIMIT {limit}"
    start = time.perf_counter()
    try:
        results = databricks.fetch_all(query)
    except Exception as e:
        logger.error("Erreur lors de la requête trafics_jours : %s", e)
        raise HTTPException(
            status_code=500,
            detail="Erreur lors de la récupération des trafics jours.",
        )
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"count": len(results), "execution_time_ms": duration_ms, "data": results}


@router.get("/trafics_semaines")
def trafics_semaines(
    code_regate: str | None = Query(None, description="Code régate de l'entité"),
    limit: int = 10,
):
    """Récupère les données de la table trafics_semaines."""
    table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.trafics_semaines"
    if code_regate:
        query = f"SELECT * FROM {table} WHERE co_regate = '{code_regate}' LIMIT {limit}"
    else:
        query = f"SELECT * FROM {table} LIMIT {limit}"
    start = time.perf_counter()
    try:
        results = databricks.fetch_all(query)
    except Exception as e:
        logger.error("Erreur lors de la requête trafics_semaines : %s", e)
        raise HTTPException(
            status_code=500,
            detail="Erreur lors de la récupération des trafics semaines.",
        )
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"count": len(results), "execution_time_ms": duration_ms, "data": results}


@router.get("/trafics_mois")
def trafics_mois(
    code_regate: str | None = Query(None, description="Code régate de l'entité"),
    limit: int = 10,
):
    """Récupère les données de la table trafics_mois."""
    table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.trafics_mois"
    if code_regate:
        query = f"SELECT * FROM {table} WHERE co_regate = '{code_regate}' LIMIT {limit}"
    else:
        query = f"SELECT * FROM {table} LIMIT {limit}"
    start = time.perf_counter()
    try:
        results = databricks.fetch_all(query)
    except Exception as e:
        logger.error("Erreur lors de la requête trafics_mois : %s", e)
        raise HTTPException(
            status_code=500,
            detail="Erreur lors de la récupération des trafics mois.",
        )
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"count": len(results), "execution_time_ms": duration_ms, "data": results}
