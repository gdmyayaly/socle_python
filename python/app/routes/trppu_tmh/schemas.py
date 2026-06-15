"""Schémas Pydantic v2 pour le module TMH (table trppu_tmh)."""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

CO_PRODUIT_PATTERN = r"^[A-Za-z0-9]{1,2}$"


class TmhOut(BaseModel):
    """Une ligne du tableau TMH (DSR-650)."""

    model_config = ConfigDict(from_attributes=True)

    co_produit: str
    volume_realise: int | None = None
    volume_previsionnel: int | None = None
    moyenne_journaliere: Decimal
    moyenne_hebdo: Decimal
    bl_exclu: bool
    bl_manuel: bool = False  # ligne saisie/modifiée manuellement (cf. DSR-649/665)


class TmhUpsert(BaseModel):
    """Un produit du tableau TMH à enregistrer (DSR-659 / création DSR-634)."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    co_produit: str = Field(..., min_length=1, max_length=2, pattern=CO_PRODUIT_PATTERN)
    volume_realise: int | None = Field(None, ge=0)
    volume_previsionnel: int | None = Field(None, ge=0)
    moyenne_journaliere: Decimal = Field(..., max_digits=12, decimal_places=2)
    moyenne_hebdo: Decimal = Field(..., max_digits=12, decimal_places=2)
    exclusion: bool = False
    manuel: bool = Field(
        False, description="Ligne ajoutée manuellement (True) ou issue d'un calcul auto (False) → bl_manuel."
    )


class TmhBatchUpdate(BaseModel):
    """Body PUT batch (DSR-659)."""

    model_config = ConfigDict(extra="forbid")
    tmh: list[TmhUpsert] = Field(..., min_length=1)
    id_rh: str = Field(
        ..., min_length=1, description="id_rh en clair de l'utilisateur ; crypté serveur avant stockage."
    )


class TmhBatchResult(BaseModel):
    id_scenario: int
    nb_inserted: int
    nb_updated: int


class TmhVolumeUpdate(BaseModel):
    """Body PATCH ciblé d'un trafic initial modifié (DSR-649)."""

    model_config = ConfigDict(extra="forbid")
    volume_realise: int = Field(..., ge=0)
    moyenne_journaliere: Decimal = Field(..., max_digits=12, decimal_places=2)
    moyenne_hebdo: Decimal = Field(..., max_digits=12, decimal_places=2)


class TmhExclusionUpdate(BaseModel):
    """Body PATCH du switch d'exclusion d'un produit TMH (bl_exclu)."""

    model_config = ConfigDict(extra="forbid")
    bl_exclu: bool = Field(
        ..., description="True = produit exclu du calcul, False = inclus."
    )
