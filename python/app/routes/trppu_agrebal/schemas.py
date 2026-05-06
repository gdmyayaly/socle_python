"""Schémas Pydantic v2 pour la table trppu_agrebal."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

CO_REGATE_PATTERN = r"^[A-Za-z0-9]{6}$"


class AgrebalBase(BaseModel):
    id_agrebal: int = Field(..., gt=0, description="Identifiant amas (BIGINT, fourni par l'utilisateur)")
    co_regate: str = Field(..., min_length=6, max_length=6, pattern=CO_REGATE_PATTERN)
    lb_agrebal: str | None = Field(None, max_length=120)
    est_actif: bool = True


class AgrebalCreate(AgrebalBase):
    model_config = ConfigDict(str_strip_whitespace=True)


class AgrebalUpdate(BaseModel):
    """MAJ partielle (id_agrebal non modifiable)."""

    model_config = ConfigDict(str_strip_whitespace=True)

    co_regate: str | None = Field(None, min_length=6, max_length=6, pattern=CO_REGATE_PATTERN)
    lb_agrebal: str | None = Field(None, max_length=120)
    est_actif: bool | None = None


class AgrebalOut(AgrebalBase):
    model_config = ConfigDict(from_attributes=True)
    dt_maj: datetime


class SoftDeleteResult(BaseModel):
    id_agrebal: int
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
