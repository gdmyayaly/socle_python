"""Module scénarios : agrège les sous-routers en un router unique."""

from fastapi import APIRouter

from app.routes.scenarios.exclusions import router as _exclusions_router
from app.routes.scenarios.neutralisations import router as _neutralisations_router
from app.routes.scenarios.routes import router as _scenarios_router

router = APIRouter(prefix="/trppu-api")
router.include_router(_scenarios_router)
router.include_router(_neutralisations_router)
router.include_router(_exclusions_router)
