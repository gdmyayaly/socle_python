"""Schémas Pydantic v2 pour les neutralisations (table trppu_neutralisations)."""

from datetime import date
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

TypeNeutralisation = Literal["FERIE", "PEAK", "SAISON"]


class NeutralisationCreate(BaseModel):
    """Body POST (DSR-645). Le nb_jour est calculé côté serveur."""

    model_config = ConfigDict(extra="forbid")

    type: TypeNeutralisation
    dt_debut: date
    dt_fin: date
    id_rh: str = Field(..., min_length=1)  # crypté en base

    @model_validator(mode="after")
    def _check(self):
        if self.dt_fin < self.dt_debut:
            raise ValueError("dt_fin doit être >= dt_debut")
        if self.type == "FERIE" and self.dt_debut != self.dt_fin:
            raise ValueError("Un FERIE couvre un seul jour (dt_debut == dt_fin)")
        return self


class NeutralisationOut(BaseModel):
    id: int
    type: TypeNeutralisation
    dt_debut: date
    dt_fin: date
    nb_jour: int
    action: Literal["created", "updated"]


class FerieJour(BaseModel):
    dt: date
    nb_jour: int = 1


class FeriesBloc(BaseModel):
    actif: bool
    nb_jours_total: int
    jours: list[FerieJour]


class PeriodeBloc(BaseModel):
    actif: bool
    dt_debut: Optional[date] = None
    dt_fin: Optional[date] = None
    nb_jour: int = 0


class NeutralisationsOut(BaseModel):
    """Réponse lecture regroupée par type (DSR-652)."""

    feries: FeriesBloc
    peak: PeriodeBloc
    saison: PeriodeBloc
