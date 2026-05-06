"""Schémas Pydantic v2 pour la table trppu_pdi."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

CO_REGATE_PATTERN = r"^[A-Za-z0-9]{6}$"


class PdiBase(BaseModel):
    id_pdi: int = Field(..., gt=0, description="Identifiant PDI (BIGINT, fourni par l'utilisateur)")
    co_regate: str = Field(..., min_length=6, max_length=6, pattern=CO_REGATE_PATTERN)
    lb_pdi: str | None = Field(None, max_length=150)
    est_actif: bool = True


class PdiCreate(PdiBase):
    model_config = ConfigDict(str_strip_whitespace=True)


class PdiUpdate(BaseModel):
    """MAJ partielle (id_pdi non modifiable)."""

    model_config = ConfigDict(str_strip_whitespace=True)

    co_regate: str | None = Field(None, min_length=6, max_length=6, pattern=CO_REGATE_PATTERN)
    lb_pdi: str | None = Field(None, max_length=150)
    est_actif: bool | None = None


class PdiOut(PdiBase):
    model_config = ConfigDict(from_attributes=True)
    dt_maj: datetime


class SoftDeleteResult(BaseModel):
    id_pdi: int
    est_actif: int = 0
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
