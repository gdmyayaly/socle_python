"""DSR-679 — Package trafics : endpoint de production des trafics pivot.

Seul module trafics du projet depuis la suppression de `app/routes/trafics.py` et de
`app/routes/trafics_helpers.py`, qui en dupliquaient la moitié. Les contrôles de données gold,
autrefois exposés en `/trppu-api/trafics/test/*`, sont passés en script :
`scripts/controle_trafics_679.py`.
"""

from .routes import router

__all__ = ["router"]
