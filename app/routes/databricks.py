from fastapi import APIRouter

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
    """Liste les schémas du catalogue gold."""
    return databricks.schemas()


@router.get("/tables/{schema}")
def databricks_tables(schema: str):
    """Liste les tables d'un schéma donné."""
    return databricks.tables(schema=schema)


@router.get("/columns/{schema}/{table}")
def databricks_columns(schema: str, table: str):
    """Liste les colonnes d'une table."""
    return databricks.columns(schema=schema, table=table)


@router.get("/trafics_jours")
def trafics_jours(limit: int = 10):
    """Récupère les données de la table trafics_jours (catalogue gold)."""
    return databricks.fetch_all(
        f"SELECT * FROM gold.trafics_jours LIMIT {limit}"
    )


@router.get("/trafics_semaines")
def trafics_semaines(limit: int = 10):
    """Récupère les données de la table trafics_semaines (catalogue gold)."""
    return databricks.fetch_all(
        f"SELECT * FROM gold.trafics_semaines LIMIT {limit}"
    )


@router.get("/trafics_mois")
def trafics_mois(limit: int = 10):
    """Récupère les données de la table trafics_mois (catalogue gold)."""
    return databricks.fetch_all(
        f"SELECT * FROM gold.trafics_mois LIMIT {limit}"
    )
