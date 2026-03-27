import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app.db.databricks import databricks
from app.db.mysql import db
from app.logger import setup_logging
from app.routes import databricks as databricks_routes
from app.routes import health as health_routes

setup_logging()
log = logging.getLogger("trppu")


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Démarrage de l'application trppu")
    await db.connect()
    try:
        databricks.connect()
    except Exception as e:
        log.error("Impossible de se connecter à Databricks : %s", e)
    yield
    databricks.disconnect()
    await db.disconnect()
    log.info("Arrêt de l'application trppu")


app = FastAPI(title="trppu API", description="API de test trppu", lifespan=lifespan)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    log.info(">>> %s %s", request.method, request.url.path)
    response = await call_next(request)
    duration_ms = (time.time() - start) * 1000
    log.info(
        "<<< %s %s %d (%.1fms)",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response


app.include_router(health_routes.router)
app.include_router(databricks_routes.router)
