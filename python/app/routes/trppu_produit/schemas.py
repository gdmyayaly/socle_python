"""Schémas Pydantic v2 pour la table trppu_produit."""

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field

CO_PRODUIT_PATTERN = r"^[A-Za-z0-9]{2,3}$"


class ProduitBase(BaseModel):
    co_produit: str = Field(..., min_length=2, max_length=3, pattern=CO_PRODUIT_PATTERN)
    lb_produit: str = Field(..., min_length=1, max_length=80)
    dt_desactivation: date | None = None
    motif_desactivation: str | None = Field(None, max_length=255)


class ProduitCreate(ProduitBase):
    model_config = ConfigDict(str_strip_whitespace=True)


class ProduitUpdate(BaseModel):
    """Mise à jour partielle (co_produit non modifiable, dt_creation auto-géré)."""

    model_config = ConfigDict(str_strip_whitespace=True)

    lb_produit: str | None = Field(None, min_length=1, max_length=80)
    dt_desactivation: date | None = None
    motif_desactivation: str | None = Field(None, max_length=255)


class ProduitOut(ProduitBase):
    model_config = ConfigDict(from_attributes=True)

    dt_creation: datetime


class SoftDeleteResult(BaseModel):
    co_produit: str
    dt_desactivation: date
    motif_desactivation: str | None
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
