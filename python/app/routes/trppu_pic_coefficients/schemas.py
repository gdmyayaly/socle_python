"""Schémas Pydantic v2 pour la table trppu_pic_coefficients."""

from datetime import date, datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, model_validator

CO_PRODUIT_PATTERN = r"^[A-Za-z0-9]{2}$"


class JourSemaineEnum(str, Enum):
    LUN = "LUN"
    MAR = "MAR"
    MER = "MER"
    JEU = "JEU"
    VEN = "VEN"
    SAM = "SAM"


class PicCoefBase(BaseModel):
    id_pic_version: int = Field(..., gt=0)
    co_produit: str = Field(..., min_length=2, max_length=2, pattern=CO_PRODUIT_PATTERN)
    jour_semaine: JourSemaineEnum
    dt_effet: date
    dt_fin_effet: date | None = None
    coef_dense: float = Field(..., ge=0, le=999.9999)
    coef_faible1: float = Field(..., ge=0, le=999.9999)
    coef_faible2: float = Field(..., ge=0, le=999.9999)

    @model_validator(mode="after")
    def _check_dates(self) -> "PicCoefBase":
        if self.dt_fin_effet is not None and self.dt_fin_effet <= self.dt_effet:
            raise ValueError("dt_fin_effet doit être strictement supérieure à dt_effet")
        return self


class PicCoefCreate(PicCoefBase):
    """Body POST — id_pic_coef est auto-généré par la base."""

    model_config = ConfigDict(str_strip_whitespace=True)


class PicCoefUpdate(BaseModel):
    """MAJ partielle — id_pic_coef, dt_creation, dt_maj non modifiables.

    Note : modifier `id_pic_version`, `co_produit`, `jour_semaine` ou `dt_effet`
    peut faire entrer en collision avec un autre coefficient existant
    (UNIQUE KEY uq_picc) — l'API renverra alors 409.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    id_pic_version: int | None = Field(None, gt=0)
    co_produit: str | None = Field(None, min_length=2, max_length=2, pattern=CO_PRODUIT_PATTERN)
    jour_semaine: JourSemaineEnum | None = None
    dt_effet: date | None = None
    dt_fin_effet: date | None = None
    coef_dense: float | None = Field(None, ge=0, le=999.9999)
    coef_faible1: float | None = Field(None, ge=0, le=999.9999)
    coef_faible2: float | None = Field(None, ge=0, le=999.9999)


class PicCoefOut(PicCoefBase):
    model_config = ConfigDict(from_attributes=True)

    id_pic_coef: int
    dt_creation: datetime
    dt_maj: datetime
    id_rh_creation: str | None = None


class SoftDeleteResult(BaseModel):
    id_pic_coef: int
    dt_fin_effet: date
    rows_affected: int


class BulkUploadError(BaseModel):
    row: int
    error: str
    raw: dict | None = None


class BulkUploadResult(BaseModel):
    nb_rows_read: int
    nb_inserted: int
    nb_updated: int
    nb_unchanged: int
    nb_errors: int
    errors: list[BulkUploadError] = []
    execution_time_s: float
