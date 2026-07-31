"""Point d'entrée de l'application FastAPI socle yb05."""

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
from app.db.mysql import db_read, db_write
from app.json_formatter import setup_logging
from app.routes import health as health_routes

setup_logging()
log = logging.getLogger("yb05")


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Démarrage de l'application yb05")
    yield
    log.info("Arrêt de l'application yb05")
    # Les pools sont créés à la volée (lazy) au premier appel : on ne les ouvre pas
    # au démarrage pour que l'application démarre même sans MySQL joignable, mais
    # on les ferme proprement à l'arrêt.
    await db_read.disconnect()
    await db_write.disconnect()


app = FastAPI(
    title="yb05 API",
    description="Socle technique YB05 (MySQL, logs, health)",
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


@app.exception_handler(RequestValidationError)
async def _validation_error(request: Request, exc: RequestValidationError):
    """Trace les paramètres invalides/manquants avant de renvoyer le 422 standard.

    `id_session_ihm` (query) est repris s'il est présent, pour le regroupement Kibana.
    """
    id_session_ihm = request.query_params.get("id_session_ihm")
    log.warning(
        "Validation des paramètres échouée (%s %s, id_session_ihm=%s) : %s",
        request.method,
        request.url.path,
        id_session_ihm,
        exc.errors(),
    )
    return JSONResponse(
        status_code=422,
        content={"detail": jsonable_encoder(exc.errors())},
    )


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

if __name__ == "__main__":
    import uvicorn
    from app.config import APP_ENV
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=(APP_ENV == "local"))
