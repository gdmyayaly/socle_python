"""Schémas Pydantic v2 pour la table trppu_site."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

CO_REGATE_PATTERN = r"^[A-Za-z0-9]{6}$"


class SiteBase(BaseModel):
    co_regate: str = Field(..., min_length=6, max_length=6, pattern=CO_REGATE_PATTERN)
    lb_regate: str | None = Field(None, max_length=120)
    lb_site: str = Field(..., min_length=1, max_length=120)
    type_site: str = Field(..., min_length=1, max_length=5)
    co_roc: str = Field(..., min_length=6, max_length=6, pattern=CO_REGATE_PATTERN)
    est_actif: bool = True


class SiteCreate(SiteBase):
    """Body POST /trppu-api/sites."""

    model_config = ConfigDict(str_strip_whitespace=True)


class SiteUpdate(BaseModel):
    """Body PUT /trppu-api/sites/{co_regate} — tous les champs sont optionnels."""

    model_config = ConfigDict(str_strip_whitespace=True)

    lb_regate: str | None = Field(None, max_length=120)
    lb_site: str | None = Field(None, min_length=1, max_length=120)
    type_site: str | None = Field(None, min_length=1, max_length=5)
    co_roc: str | None = Field(None, min_length=6, max_length=6, pattern=CO_REGATE_PATTERN)
    est_actif: bool | None = None


class SiteOut(SiteBase):
    """Réponse standard pour un site."""

    model_config = ConfigDict(from_attributes=True)

    dt_maj: datetime


class SoftDeleteResult(BaseModel):
    co_regate: str
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
