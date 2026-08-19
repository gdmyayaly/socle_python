"""Schémas Pydantic v2 pour la table trppu_pic_version."""

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, model_validator

CO_REGATE_PATTERN = r"^[A-Za-z0-9]{6}$"


class NiveauEnum(str, Enum):
    """Niveaux tels qu'ils existent en base (enum trppu_pic_version.niveau).

    SCENARIO est écrit par le module trppu_scenario_pic (surcharge d'un
    coefficient PIC, DSR-661) et par la duplication de scénario : il doit être
    accepté en LECTURE, sans quoi toute version de ce niveau fait échouer la
    validation de PicVersionOut (-> 500 sur GET /pic-versions).
    """

    NATIONAL = "NATIONAL"
    DEX = "DEX"
    SITE = "SITE"
    SCENARIO = "SCENARIO"


class NiveauCreationEnum(str, Enum):
    """Niveaux acceptés en écriture par le CRUD des versions PIC.

    SCENARIO en est volontairement exclu : ces versions sont créées et
    maintenues par trppu_scenario_pic, qui renseigne le vrai id_scenario.
    """

    NATIONAL = "NATIONAL"
    DEX = "DEX"
    SITE = "SITE"


class PicVersionBase(BaseModel):
    lb_pic_version: str | None = Field(None, max_length=80)
    niveau: NiveauEnum
    co_regate: str = Field(..., min_length=6, max_length=6, pattern=CO_REGATE_PATTERN)
    # NOT NULL en base, sans valeur par défaut SQL. Pour une version référentielle
    # (NATIONAL/DEX/SITE) non rattachée à un scénario, 0 fait office de sentinelle.
    # (Les versions de niveau SCENARIO sont créées par le module trppu_scenario_pic,
    # qui renseigne le vrai id_scenario.)
    id_scenario: int = Field(0, ge=0)
    dt_activation: datetime
    dt_desactivation: datetime | None = None
    motif_desactivation: str | None = Field(None, max_length=255)
    commentaire: str | None = Field(None, max_length=500)
    est_par_defaut: bool = False

    @model_validator(mode="after")
    def _check_dates(self) -> "PicVersionBase":
        if self.dt_desactivation is not None and self.dt_desactivation <= self.dt_activation:
            raise ValueError("dt_desactivation doit être strictement supérieure à dt_activation")
        return self


class PicVersionCreate(PicVersionBase):
    """Body POST — id_pic_version est auto-généré par la base."""

    model_config = ConfigDict(str_strip_whitespace=True)

    # Écriture fermée au niveau SCENARIO (cf. NiveauCreationEnum).
    niveau: NiveauCreationEnum


class PicVersionUpdate(BaseModel):
    """MAJ partielle — id_pic_version, dt_creation, dt_maj non modifiables."""

    model_config = ConfigDict(str_strip_whitespace=True)

    lb_pic_version: str | None = Field(None, max_length=80)
    niveau: NiveauCreationEnum | None = None
    co_regate: str | None = Field(None, min_length=6, max_length=6, pattern=CO_REGATE_PATTERN)
    dt_activation: datetime | None = None
    dt_desactivation: datetime | None = None
    motif_desactivation: str | None = Field(None, max_length=255)
    commentaire: str | None = Field(None, max_length=500)
    est_par_defaut: bool | None = None


class PicVersionOut(PicVersionBase):
    model_config = ConfigDict(from_attributes=True)

    id_pic_version: int
    dt_creation: datetime
    dt_maj: datetime
    id_rh_creation: str | None = None
    id_rh_maj: str | None = None


class SoftDeleteResult(BaseModel):
    id_pic_version: int
    dt_desactivation: datetime
    motif_desactivation: str | None
    rows_affected: int


class BulkUploadError(BaseModel):
    row: int
    error: str
    raw: dict | None = None


class BulkUploadResult(BaseModel):
    """Note : pour trppu_pic_version, l'upload est INSERT-only (PK auto-incrément).
    Les compteurs nb_updated et nb_unchanged sont toujours 0."""

    nb_rows_read: int
    nb_inserted: int
    nb_updated: int = 0
    nb_unchanged: int = 0
    nb_errors: int
    errors: list[BulkUploadError] = []
    execution_time_s: float
