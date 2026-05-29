"""Schémas Pydantic v2 pour les comptages manuels (table trppu_scenario_comptages_manuels)."""

from datetime import date
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

CO_PRODUIT_PATTERN = r"^[A-Za-z0-9]{1,2}$"


class ComptageCreate(BaseModel):
    """Body POST (DSR-644) : ajout d'un comptage."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    co_produit: str = Field(..., min_length=1, max_length=2, pattern=CO_PRODUIT_PATTERN)
    dt_comptage: Optional[date] = None  # défaut serveur = date du jour
    nb_produit: int = Field(..., ge=0)
    id_rh: str = Field(..., min_length=1)  # crypté en base


class ComptageUpdate(BaseModel):
    """Body PUT (DSR-644) : modification d'un comptage."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    dt_comptage: Optional[date] = None  # défaut serveur = date du jour
    nb_produit: int = Field(..., ge=0)
    id_rh: str = Field(..., min_length=1)


class ComptageOut(BaseModel):
    """Réponse (DSR-653). id_rh non exposé."""

    model_config = ConfigDict(from_attributes=True)

    co_produit: str
    dt_comptage: date
    nb_produit: int
