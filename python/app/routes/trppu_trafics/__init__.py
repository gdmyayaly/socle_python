"""DSR-679 — Package trafics pivot (nouvelle structure gold + dimensions).

Implémentation autonome destinée à remplacer l'ancien `app/routes/trafics.py` :
- `routes.py`  : endpoint production `GET /trppu-api/trafics/get_trafics_pivot`
- `helpers.py` : validation, découpage pivot, construction SQL (jointures dimensions),
                 agrégation dynamique
- `debug.py`   : routes `GET /trppu-api/trafics/test/*` (config, aperçu SQL, schémas,
                 échantillons, dry-run) pour stabiliser la requête avant bascule
"""

from fastapi import APIRouter

from .debug import router as debug_router
from .routes import router as pivot_router

router = APIRouter()
router.include_router(pivot_router)
router.include_router(debug_router)

__all__ = ["router"]
