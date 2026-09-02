"""Point d'entrée de l'application FastAPI trppu (ys04)."""
 
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.staticfiles import StaticFiles

from app.config import (
    CORS_ALLOW_CREDENTIALS,
    CORS_ALLOW_HEADERS,
    CORS_ALLOW_METHODS,
    CORS_ALLOW_ORIGINS,
)
from app.json_formatter import setup_logging
from app.log_utils import (
    ctx,
    reset_id_session_ihm,
    safe_preview,
    set_id_session_ihm,
)
from app.services.jours_fermes_client import JoursFermesAPIError
from app.routes import databricks as databricks_routes
from app.routes import health as health_routes
from app.routes import calcl_nbr_jours as calcl_nbr_jours_routes
from app.routes import mysql_debug as mysql_debug_routes
from app.routes.trppu_trafics import router as trppu_trafics_router
from app.routes.trppu_pic_coefficients import router as trppu_pic_coefficients_router
from app.routes.trppu_pic_version import router as trppu_pic_version_router
from app.routes.trppu_produit import router as trppu_produit_router
from app.routes.trppu_comptages import router as trppu_comptages_router
from app.routes.trppu_neutralisations import router as trppu_neutralisations_router
from app.routes.trppu_audit import router as trppu_audit_router
from app.routes.trppu_optipacc import router as trppu_optipacc_router
from app.routes.trppu_scenario import router as trppu_scenario_router
from app.routes.trppu_scenario_pic import router as trppu_scenario_pic_router
from app.routes.trppu_site import router as trppu_site_router
from app.routes.trppu_tmh import router as trppu_tmh_router
from app.routes.trppu_variations import router as trppu_variations_router

setup_logging()
log = logging.getLogger("trppu")
 
 
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Démarrage de l'application trppu")
    yield
    log.info("Arrêt de l'application trppu")
 
 
app = FastAPI(
    title="trppu API YS04",
    description="API de test trppu YS04",
    lifespan=lifespan,
    # On désactive les docs par défaut (qui chargent les assets depuis le CDN)
    # pour les remplacer par des routes servant les assets en local.
    docs_url=None,
    redoc_url=None,
)

# Assets Swagger UI / ReDoc servis en local (fonctionne sans accès internet)
_STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=f"{app.title} - Swagger UI",
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
        swagger_js_url="/static/swagger-ui/swagger-ui-bundle.js",
        swagger_css_url="/static/swagger-ui/swagger-ui.css",
        swagger_favicon_url="/static/swagger-ui/favicon-32x32.png",
    )


@app.get(app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
async def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html()


@app.get("/redoc", include_in_schema=False)
async def custom_redoc_html():
    return get_redoc_html(
        openapi_url=app.openapi_url,
        title=f"{app.title} - ReDoc",
        redoc_js_url="/static/swagger-ui/redoc.standalone.js",
        redoc_favicon_url="/static/swagger-ui/favicon-32x32.png",
        with_google_fonts=False,
    )


app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials=CORS_ALLOW_CREDENTIALS,
    allow_methods=CORS_ALLOW_METHODS,
    allow_headers=CORS_ALLOW_HEADERS,
)


@app.exception_handler(JoursFermesAPIError)
async def _jours_fermes_unavailable(request: Request, exc: JoursFermesAPIError):
    log.warning(
        "API jours fermés indisponible %s",
        ctx(path=request.url.path, http=503, motif=str(exc)),
    )
    return JSONResponse(
        status_code=503,
        content={"detail": "Service jours fermés indisponible"},
    )


@app.exception_handler(RequestValidationError)
async def _validation_error(request: Request, exc: RequestValidationError):
    """Trace les paramètres invalides/manquants (DSR-661 critère 4) avant de renvoyer le
    422 standard.

    `id_session_ihm` n'est plus relu ici : le middleware `log_requests` l'a posé dans le
    contexte de log, donc `JsonFormatter` l'ajoute d'office à cet enregistrement (champ
    racine `id_session_ihm`) pour le regroupement Kibana.
    """
    # `exc.errors()` embarque les valeurs d'entrée fautives et n'est pas borné :
    # il passe par `safe_preview` pour ne pas déverser un body entier dans le log.
    log.warning(
        "Rejet validation des paramètres %s",
        ctx(
            methode=request.method,
            path=request.url.path,
            http=422,
            erreurs=safe_preview(exc.errors(), max_len=1000),
        ),
    )
    return JSONResponse(
        status_code=422,
        content={"detail": jsonable_encoder(exc.errors())},
    )


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Trace chaque requête et publie `id_session_ihm` dans le contexte de log.

    L'IHM pose ce query param sur tous les appels /trppu-api (cf.
    session-ihm.interceptor.ts). En l'alimentant ici, `JsonFormatter` l'ajoute
    ensuite à *toutes* les lignes de log de la requête — y compris celles émises
    par les handlers d'exception ou les couches basses, sans que les endpoints
    aient à le déclarer.
    """
    token = set_id_session_ihm(request.query_params.get("id_session_ihm"))
    start = time.time()
    try:
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
    finally:
        reset_id_session_ihm(token)
 
 
app.include_router(health_routes.router)
app.include_router(databricks_routes.router)
app.include_router(mysql_debug_routes.router)
app.include_router(trppu_trafics_router)
app.include_router(calcl_nbr_jours_routes.router)
app.include_router(trppu_site_router)
app.include_router(trppu_produit_router)
app.include_router(trppu_pic_version_router)
app.include_router(trppu_pic_coefficients_router)
app.include_router(trppu_scenario_router)
app.include_router(trppu_tmh_router)
app.include_router(trppu_comptages_router)
app.include_router(trppu_variations_router)
app.include_router(trppu_neutralisations_router)
app.include_router(trppu_scenario_pic_router)
app.include_router(trppu_audit_router)
app.include_router(trppu_optipacc_router)

if __name__ == "__main__":
    import uvicorn
    from app.config import APP_ENV
    uvicorn.run("app.main:app", host="0.0.0.0", port=8080, reload=(APP_ENV == "local"))