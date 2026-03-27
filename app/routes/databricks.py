from fastapi import APIRouter

from app.config import DATABRICKS_CATALOG, DATABRICKS_SCHEMA
from app.db.databricks import databricks

router = APIRouter(prefix="/databricks", tags=["Databricks"])


@router.get("/test")
def databricks_test():
    """Requête de test sur la SQL Warehouse."""
    result = databricks.fetch_one("SELECT 1 AS ok")
    return {"test": "ok", "result": result}


@router.get("/catalogs")
def databricks_catalogs():
    """Liste tous les catalogues accessibles."""
    return databricks.catalogs()


@router.get("/schemas")
def databricks_schemas():
    """Liste les schémas du catalogue."""
    return databricks.schemas()


@router.get("/tables")
def databricks_tables():
    """Liste les tables du schéma par défaut."""
    return databricks.tables()


@router.get("/tables/{schema}")
def databricks_tables_by_schema(schema: str):
    """Liste les tables d'un schéma donné."""
    return databricks.tables(schema=schema)


@router.get("/columns/{table}")
def databricks_columns(table: str):
    """Liste les colonnes d'une table du schéma par défaut."""
    return databricks.columns(table=table)


@router.get("/columns/{schema}/{table}")
def databricks_columns_by_schema(schema: str, table: str):
    """Liste les colonnes d'une table dans un schéma donné."""
    return databricks.columns(schema=schema, table=table)


@router.get("/trafics_jours")
def trafics_jours(limit: int = 10):
    """Récupère les données de la table trafics_jours."""
    return databricks.fetch_all(
        f"SELECT * FROM {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.trafics_jours LIMIT {limit}"
    )


@router.get("/trafics_semaines")
def trafics_semaines(limit: int = 10):
    """Récupère les données de la table trafics_semaines."""
    return databricks.fetch_all(
        f"SELECT * FROM {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.trafics_semaines LIMIT {limit}"
    )


@router.get("/trafics_mois")
def trafics_mois(limit: int = 10):
    """Récupère les données de la table trafics_mois."""
    return databricks.fetch_all(
        f"SELECT * FROM {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.trafics_mois LIMIT {limit}"
    )
