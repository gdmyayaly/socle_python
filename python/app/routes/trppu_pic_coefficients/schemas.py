"""Schémas Pydantic v2 pour la table trppu_pic_coefficients.

Aligné sur le schéma réel (dump 03_db_12_06_2026.sql) :
colonnes `coef` (decimal 7,4) + `densite` (0|1|2), `dt_effet`/`dt_fin` (datetime),
enum `jour_semaine` en LUNDI..SAMEDI. Clé naturelle UNIQUE (uq_picc) :
(id_pic_version, co_produit, jour_semaine, densite).
"""

from datetime import date, datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, model_validator

CO_PRODUIT_PATTERN = r"^[A-Za-z0-9]{2}$"


class JourSemaineEnum(str, Enum):
    LUNDI = "LUNDI"
    MARDI = "MARDI"
    MERCREDI = "MERCREDI"
    JEUDI = "JEUDI"
    VENDREDI = "VENDREDI"
    SAMEDI = "SAMEDI"


class PicCoefBase(BaseModel):
    id_pic_version: int = Field(..., gt=0)
    co_produit: str = Field(..., min_length=2, max_length=2, pattern=CO_PRODUIT_PATTERN)
    jour_semaine: JourSemaineEnum
    densite: int = Field(..., ge=0, le=2, description="0=dense, 1=faible1, 2=faible2")
    dt_effet: date
    dt_fin: date | None = None
    coef: float = Field(..., ge=0, le=999.9999, description="decimal(7,4) >= 0")

    @model_validator(mode="after")
    def _check_dates(self) -> "PicCoefBase":
        if self.dt_fin is not None and self.dt_fin <= self.dt_effet:
            raise ValueError("dt_fin doit être strictement supérieure à dt_effet")
        return self


class PicCoefCreate(PicCoefBase):
    """Body POST — id_pic_coef est auto-généré par la base."""

    model_config = ConfigDict(str_strip_whitespace=True)


class PicCoefUpdate(BaseModel):
    """MAJ partielle — id_pic_coef, dt_creation, dt_maj non modifiables.

    Note : modifier `id_pic_version`, `co_produit`, `jour_semaine` ou `densite`
    peut faire entrer en collision avec un autre coefficient existant
    (UNIQUE KEY uq_picc) — l'API renverra alors 409.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    id_pic_version: int | None = Field(None, gt=0)
    co_produit: str | None = Field(None, min_length=2, max_length=2, pattern=CO_PRODUIT_PATTERN)
    jour_semaine: JourSemaineEnum | None = None
    densite: int | None = Field(None, ge=0, le=2)
    dt_effet: date | None = None
    dt_fin: date | None = None
    coef: float | None = Field(None, ge=0, le=999.9999)


class PicCoefOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id_pic_coef: int
    id_pic_version: int
    co_produit: str
    jour_semaine: JourSemaineEnum
    densite: int
    dt_effet: datetime
    dt_fin: datetime | None = None
    coef: float
    dt_creation: datetime
    dt_maj: datetime
    id_rh: str | None = None


class SoftDeleteResult(BaseModel):
    id_pic_coef: int
    dt_fin: date
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
