import time

from fastapi import APIRouter, Query

from app.config import DATABRICKS_CATALOG, DATABRICKS_SCHEMA
from app.db.databricks import databricks

router = APIRouter(prefix="/databricks", tags=["Databricks"])


@router.get("/test")
def databricks_test():
    """Requête de test sur la SQL Warehouse."""
    start = time.perf_counter()
    result = databricks.fetch_one("SELECT 1 AS ok")
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"test": "ok", "execution_time_ms": duration_ms, "result": result}


@router.get("/catalogs")
def databricks_catalogs():
    """Liste tous les catalogues accessibles."""
    start = time.perf_counter()
    result = databricks.catalogs()
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"execution_time_ms": duration_ms, "data": result}


@router.get("/schemas")
def databricks_schemas():
    """Liste les schémas du catalogue."""
    start = time.perf_counter()
    result = databricks.schemas()
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"execution_time_ms": duration_ms, "data": result}


@router.get("/tables")
def databricks_tables():
    """Liste les tables du schéma par défaut."""
    start = time.perf_counter()
    result = databricks.tables()
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"execution_time_ms": duration_ms, "data": result}


@router.get("/tables/{schema}")
def databricks_tables_by_schema(schema: str):
    """Liste les tables d'un schéma donné."""
    start = time.perf_counter()
    result = databricks.tables(schema=schema)
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"execution_time_ms": duration_ms, "data": result}


@router.get("/columns/{table}")
def databricks_columns(table: str):
    """Liste les colonnes d'une table du schéma par défaut."""
    start = time.perf_counter()
    result = databricks.columns(table=table)
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"execution_time_ms": duration_ms, "data": result}


@router.get("/columns/{schema}/{table}")
def databricks_columns_by_schema(schema: str, table: str):
    """Liste les colonnes d'une table dans un schéma donné."""
    start = time.perf_counter()
    result = databricks.columns(schema=schema, table=table)
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
        query = f"SELECT * FROM {table} WHERE co_regate = %s LIMIT {limit}"
        params = [code_regate]
    else:
        query = f"SELECT * FROM {table} LIMIT {limit}"
        params = None
    start = time.perf_counter()
    results = databricks.fetch_all(query, params=params)
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
        query = f"SELECT * FROM {table} WHERE co_regate = %s LIMIT {limit}"
        params = [code_regate]
    else:
        query = f"SELECT * FROM {table} LIMIT {limit}"
        params = None
    start = time.perf_counter()
    results = databricks.fetch_all(query, params=params)
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
        query = f"SELECT * FROM {table} WHERE co_regate = %s LIMIT {limit}"
        params = [code_regate]
    else:
        query = f"SELECT * FROM {table} LIMIT {limit}"
        params = None
    start = time.perf_counter()
    results = databricks.fetch_all(query, params=params)
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"count": len(results), "execution_time_ms": duration_ms, "data": results}
