"""Schémas Pydantic v2 pour les bodies des routes scénarios."""

from datetime import date
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

Statut = Literal["EN COURS", "SIMULATION", "VALIDE", "VERROUILLE", "ARCHIVE"]
TypeNeutre = Literal["FERIE", "PEAK", "LOCAL"]


class ScenarioCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    co_roc: str = Field(min_length=6, max_length=6)
    co_regate: str = Field(min_length=6, max_length=6)
    lb_scenario: str = Field(min_length=1, max_length=20)
    periode_debut: date
    periode_fin: date
    periode_realise_debut: Optional[date] = None
    periode_realise_fin: Optional[date] = None
    periode_prev_debut: Optional[date] = None
    periode_prev_fin: Optional[date] = None
    id_pic_version: int = Field(gt=0)

    @model_validator(mode="after")
    def _check_periodes(self):
        if self.periode_fin < self.periode_debut:
            raise ValueError("periode_fin doit être >= periode_debut")
        if (self.periode_realise_debut and self.periode_realise_fin
                and self.periode_realise_fin < self.periode_realise_debut):
            raise ValueError("periode_realise_fin doit être >= periode_realise_debut")
        if (self.periode_prev_debut and self.periode_prev_fin
                and self.periode_prev_fin < self.periode_prev_debut):
            raise ValueError("periode_prev_fin doit être >= periode_prev_debut")
        return self


class ScenarioUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    lb_scenario: Optional[str] = Field(None, min_length=1, max_length=20)
    periode_debut: Optional[date] = None
    periode_fin: Optional[date] = None
    periode_realise_debut: Optional[date] = None
    periode_realise_fin: Optional[date] = None
    periode_prev_debut: Optional[date] = None
    periode_prev_fin: Optional[date] = None
    id_pic_version: Optional[int] = Field(None, gt=0)

    @model_validator(mode="after")
    def _at_least_one(self):
        if not self.model_dump(exclude_unset=True):
            raise ValueError("Au moins un champ doit être fourni pour la modification.")
        return self


class StatutUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    statut: Statut


class NeutralisationCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dt_debut: date
    dt_fin: date
    type: TypeNeutre

    @model_validator(mode="after")
    def _check_dates(self):
        if self.dt_fin < self.dt_debut:
            raise ValueError("dt_fin doit être >= dt_debut")
        return self


class NeutralisationUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dt_debut: Optional[date] = None
    dt_fin: Optional[date] = None
    type: Optional[TypeNeutre] = None

    @model_validator(mode="after")
    def _at_least_one(self):
        if not self.model_dump(exclude_unset=True):
            raise ValueError("Au moins un champ doit être fourni pour la modification.")
        if self.dt_debut and self.dt_fin and self.dt_fin < self.dt_debut:
            raise ValueError("dt_fin doit être >= dt_debut")
        return self


class ExclusionCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    co_produit: str = Field(min_length=2, max_length=2)
    motif: Optional[str] = Field(None, max_length=255)
