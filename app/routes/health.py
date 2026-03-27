from fastapi import APIRouter

from app.db.databricks import databricks
from app.db.mysql import db

router = APIRouter()


@router.get("/")
def root():
    return {"message": "Bienvenue sur l'API trppu"}


@router.get("/health")
async def health():
    try:
        result = await db.fetch_one("SELECT 1 AS ok")
        mysql_status = "connected" if result else "error"
    except Exception:
        mysql_status = "disconnected"

    try:
        result = databricks.fetch_one("SELECT 1 AS ok")
        databricks_status = "connected" if result else "error"
    except Exception:
        databricks_status = "disconnected"

    return {"status": "ok", "mysql": mysql_status, "databricks": databricks_status}
