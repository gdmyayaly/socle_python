"""Schémas Pydantic v2 pour la table trppu_scenario."""

from datetime import date, datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

CO_REGATE_PATTERN = r"^[A-Za-z0-9]{6}$"

Statut = Literal["BROUILLON", "SIMULATION", "VALIDE", "PRODUCTION", "ARCHIVE"]
NbJours = Literal[5, 6]


class ScenarioBase(BaseModel):
    co_regate: str = Field(..., min_length=6, max_length=6, pattern=CO_REGATE_PATTERN)
    lb_scenario: str = Field(..., min_length=1, max_length=50)
    co_roc: str = Field(..., min_length=6, max_length=6, pattern=CO_REGATE_PATTERN)


class ScenarioCreate(ScenarioBase):
    """Body POST /scenarios.

    Obligatoires : co_regate, lb_scenario, co_roc.
    nb_jours_semaine : 5 par défaut, contraint à {5, 6}.
    id_pic_version : si non fourni, résolu côté serveur (premier est_par_defaut=1, fallback 1).
    Périodes principales : si None, today-1an / today+1an.
    statut, version_scenario, est_fige : forcés à BROUILLON / 1 / False.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    nb_jours_semaine: NbJours = 5
    id_pic_version: Optional[int] = Field(None, gt=0)
    periode_debut: Optional[date] = None
    periode_fin: Optional[date] = None
    periode_realise_debut: Optional[date] = None
    periode_realise_fin: Optional[date] = None
    periode_prev_debut: Optional[date] = None
    periode_prev_fin: Optional[date] = None
    id_scenario_parent: Optional[int] = Field(None, gt=0)

    @model_validator(mode="after")
    def _check_periodes(self):
        if self.periode_debut and self.periode_fin and self.periode_fin < self.periode_debut:
            raise ValueError("periode_fin doit être >= periode_debut")
        if (
            self.periode_realise_debut
            and self.periode_realise_fin
            and self.periode_realise_fin < self.periode_realise_debut
        ):
            raise ValueError("periode_realise_fin doit être >= periode_realise_debut")
        if (
            self.periode_prev_debut
            and self.periode_prev_fin
            and self.periode_prev_fin < self.periode_prev_debut
        ):
            raise ValueError("periode_prev_fin doit être >= periode_prev_debut")
        return self


class ScenarioOut(BaseModel):
    """Réponse complète pour les endpoints scénario."""

    model_config = ConfigDict(from_attributes=True)

    id_scenario: int
    co_regate: str
    lb_scenario: str
    co_roc: str
    statut: Statut
    dt_creation: datetime
    dt_validation: Optional[datetime] = None
    dt_mise_en_prod: Optional[datetime] = None
    periode_debut: date
    periode_fin: date
    periode_realise_debut: Optional[date] = None
    periode_realise_fin: Optional[date] = None
    periode_prev_debut: Optional[date] = None
    periode_prev_fin: Optional[date] = None
    nb_jours_semaine: int
    id_pic_version: int
    version_scenario: int
    id_scenario_parent: Optional[int] = None
    est_fige: bool


class PeriodeUpdate(BaseModel):
    """PATCH /periodes : MAJ partielle des bornes de période."""

    model_config = ConfigDict(extra="forbid")

    periode_debut: Optional[date] = None
    periode_fin: Optional[date] = None
    periode_realise_debut: Optional[date] = None
    periode_realise_fin: Optional[date] = None
    periode_prev_debut: Optional[date] = None
    periode_prev_fin: Optional[date] = None

    @model_validator(mode="after")
    def _at_least_one(self):
        if not self.model_dump(exclude_unset=True):
            raise ValueError("Au moins un champ doit être fourni.")
        return self


class NbJoursUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    nb_jours_semaine: NbJours


class StatutUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    statut: Statut


class FigeUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    est_fige: bool


class LbScenarioUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    lb_scenario: str = Field(..., min_length=1, max_length=50)


class DuplicateRequest(BaseModel):
    """Body optionnel pour POST /duplicate : permet d'écraser le libellé du clone."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    lb_scenario: Optional[str] = Field(None, min_length=1, max_length=50)
