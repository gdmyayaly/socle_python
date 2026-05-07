"""Point d'entrée de l'application FastAPI trppu (ys04)."""
 
import logging
import time
from contextlib import asynccontextmanager
 
from fastapi import FastAPI, Request
 
from app.json_formatter import setup_logging
from app.routes import databricks as databricks_routes
from app.routes import health as health_routes
from app.routes import calcl_nbr_jours as calcl_nbr_jours_routes
from app.routes import logs as logs_routes
from app.routes import mysql_debug as mysql_debug_routes
from app.routes import trafics as trafics_routes
from app.routes.trppu_pic_coefficients import router as trppu_pic_coefficients_router
from app.routes.trppu_pic_version import router as trppu_pic_version_router
from app.routes.trppu_produit import router as trppu_produit_router
from app.routes.trppu_scenario import router as trppu_scenario_router
from app.routes.trppu_site import router as trppu_site_router

setup_logging()
log = logging.getLogger("trppu")
 
 
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Démarrage de l'application trppu")
    yield
    log.info("Arrêt de l'application trppu")
 
 
app = FastAPI(title="trppu API YS04", description="API de test trppu YS04", lifespan=lifespan)
 
 
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    log.info(">>> %s %s", request.method, request.url.path)
    try:
        response = await call_next(request)
    except Exception:
        duration_ms = (time.time() - start) * 1000
        log.exception(
            "<<< %s %s 500 (%.1fms) — UNHANDLED",
            request.method,
            request.url.path,
            duration_ms,
        )
        raise
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
app.include_router(mysql_debug_routes.router)
app.include_router(trafics_routes.router)
app.include_router(calcl_nbr_jours_routes.router)
app.include_router(logs_routes.router)
app.include_router(trppu_site_router)
app.include_router(trppu_produit_router)
app.include_router(trppu_pic_version_router)
app.include_router(trppu_pic_coefficients_router)
app.include_router(trppu_scenario_router)

if __name__ == "__main__":
    import uvicorn
    from app.config import APP_ENV
    uvicorn.run("app.main:app", host="0.0.0.0", port=8080, reload=(APP_ENV == "local"))