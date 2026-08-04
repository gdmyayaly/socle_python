"""DSR-679 — Package trafics : endpoint de production + routes de contrôle des données gold.

Seul module trafics du projet depuis la suppression de `app/routes/trafics.py` et de
`app/routes/trafics_helpers.py`, qui en dupliquaient la moitié.
"""

from fastapi import APIRouter

from .debug import router as debug_router
from .routes import router as pivot_router

router = APIRouter()
router.include_router(pivot_router)
router.include_router(debug_router)

__all__ = ["router"]
