"""Schémas Pydantic v2 pour la rétention PIC d'un scénario (DSR-660 / DSR-661)."""

from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

JourSemaine = Literal["LUNDI", "MARDI", "MERCREDI", "JEUDI", "VENDREDI", "SAMEDI"]
CO_PRODUIT_PATTERN = r"^[A-Za-z0-9]{1,2}$"


class PicCoefItem(BaseModel):
    """Un coefficient PIC fusionné (défaut national + surcharge scénario)."""

    co_produit: str
    jour_semaine: JourSemaine
    densite: int = Field(..., ge=0, le=2)  # 0=dense, 1=faible1, 2=faible2
    coef: Decimal
    modifie: bool  # True si surchargé par le scénario


class PicScenarioOut(BaseModel):
    """Réponse lecture (DSR-660)."""

    id_pic_version_defaut: Optional[int] = None
    id_pic_version_scenario: Optional[int] = None
    niveau_scenario: Optional[str] = None
    coefficients: list[PicCoefItem]


class PicCoefUpsert(BaseModel):
    """Body PUT (DSR-661). Ordre/présence des paramètres validés (extra interdit)."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    co_produit: str = Field(..., min_length=1, max_length=2, pattern=CO_PRODUIT_PATTERN)
    jour_semaine: JourSemaine
    densite: int = Field(..., ge=0, le=2)
    coef: Decimal = Field(..., max_digits=7, decimal_places=4)
    id_rh: str = Field(..., min_length=1)  # crypté en base


class PicCoefUpsertResult(BaseModel):
    action: Literal["update", "insert_coef", "insert_version_and_coef"]
    id_pic_version: int
