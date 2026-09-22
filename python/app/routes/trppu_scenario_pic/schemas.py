"""Schémas Pydantic v2 pour la rétention PIC d'un scénario (DSR-660 / DSR-661)."""

from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

JourSemaine = Literal["LUNDI", "MARDI", "MERCREDI", "JEUDI", "VENDREDI", "SAMEDI"]
CO_PRODUIT_PATTERN = r"^[A-Za-z0-9]{1,2}$"

# Borne du lot d'enregistrement multiple. Le tableau de l'IHM compte aujourd'hui
# 6 produits x 16 colonnes (5 jours x 3 densités + samedi) = 96 cellules ; la marge
# absorbe un élargissement du tableau sans laisser passer un payload arbitraire.
MAX_COEFS_BATCH = 500


class PicCoefItem(BaseModel):
    """Un coefficient PIC fusionné (défaut national + surcharge scénario)."""

    id_pic_version: int  # version d'origine de la ligne : défaut (1) ou version scénario
    co_produit: str
    jour_semaine: JourSemaine
    densite: int = Field(..., ge=0, le=2)  # 0=dense, 1=faible1, 2=faible2
    coef: Decimal
    modifie: bool  # True si surchargé par le scénario (id_pic_version != défaut)


class PicScenarioOut(BaseModel):
    """Réponse lecture (DSR-660)."""

    id_pic_version_defaut: Optional[int] = None
    id_pic_version_scenario: Optional[int] = None
    niveau_scenario: Optional[str] = None
    coefficients: list[PicCoefItem]


class PicCoefBatchItem(BaseModel):
    """Une cellule modifiée du tableau PIC, sans l'id_rh (porté par le lot)."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    co_produit: str = Field(..., min_length=1, max_length=2, pattern=CO_PRODUIT_PATTERN)
    jour_semaine: JourSemaine
    densite: int = Field(..., ge=0, le=2)
    coef: Decimal = Field(..., ge=0, max_digits=7, decimal_places=4)  # chk_pic_coefs: coef >= 0


class PicCoefUpsert(PicCoefBatchItem):
    """Body PUT unitaire (DSR-661). Ordre/présence des paramètres validés (extra interdit)."""

    id_rh: str = Field(..., min_length=1)  # crypté en base


class PicCoefUpsertResult(BaseModel):
    action: Literal["update", "insert_coef", "insert_version_and_coef"]
    id_pic_version: int


class PicCoefBatchUpsert(BaseModel):
    """Body PUT du lot : toutes les cellules modifiées, enregistrées d'un bloc.

    Même validation par cellule que l'écriture unitaire ; l'`id_rh` n'est porté
    qu'une fois, il s'applique à toutes les lignes du lot.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    coefficients: list[PicCoefBatchItem] = Field(
        ..., min_length=1, max_length=MAX_COEFS_BATCH
    )
    id_rh: str = Field(..., min_length=1)  # crypté en base


class PicCoefBatchResult(BaseModel):
    """Réponse du lot : le tout-ou-rien rend inutile un détail ligne à ligne."""

    id_scenario: int
    id_pic_version: int
    version_creee: bool  # True si la version PIC du scénario a été créée par ce lot
    nb_inserted: int
    nb_updated: int
