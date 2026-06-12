"""Schémas Pydantic v2 pour les neutralisations (table trppu_neutralisations)."""

from datetime import date
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class NeutralisationCreate(BaseModel):
    """Body POST (DSR-645). nb_jour calculé serveur ; motif = texte libre utilisateur."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    dt_debut: date
    dt_fin: date
    motif: str = Field(..., min_length=1, max_length=255)
    id_rh: str = Field(..., min_length=1)  # crypté en base

    @model_validator(mode="after")
    def _check(self):
        if self.dt_fin < self.dt_debut:
            raise ValueError("dt_fin doit être >= dt_debut")
        return self


class NeutralisationOut(BaseModel):
    """Réponse de l'ajout d'une neutralisation (DSR-645)."""

    id: int
    dt_debut: date
    dt_fin: date
    nb_jour: int
    motif: str
    action: Literal["created"]


class NeutralisationItem(BaseModel):
    """Une neutralisation (lecture à plat, DSR-652)."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    dt_debut: date
    dt_fin: date
    nb_jour: int
    motif: Optional[str] = None
