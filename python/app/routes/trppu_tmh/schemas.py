"""Schémas Pydantic v2 pour le module TMH (table trppu_tmh).

Depuis la migration du 24/06/2026, la contrainte d'unicité `uq_tmh` porte sur
(`id_tmh`, `id_scenario`, `co_produit`) : un même produit peut donc apparaître
plusieurs fois pour un scénario. La clé fonctionnelle d'une ligne devient
`id_tmh` (et non plus `co_produit`).
"""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

CO_PRODUIT_PATTERN = r"^[A-Za-z0-9]{1,3}$"


class TmhOut(BaseModel):
    """Une ligne du tableau TMH (DSR-650)."""

    model_config = ConfigDict(from_attributes=True)

    id_tmh: int  # identifiant unique de la ligne (clé fonctionnelle)
    co_produit: str
    volume_realise: int | None = None
    volume_previsionnel: int | None = None
    moyenne_journaliere: Decimal
    moyenne_hebdo: Decimal
    bl_exclu: bool
    bl_manuel: bool = False  # ligne saisie/modifiée manuellement (cf. DSR-649/665)
    motif: str | None = None  # justification d'une modif manuelle / exclusion


class _TmhFields(BaseModel):
    """Champs métier communs d'une ligne TMH en écriture."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    co_produit: str = Field(..., min_length=1, max_length=3, pattern=CO_PRODUIT_PATTERN)
    volume_realise: int | None = None  # valeurs négatives autorisées
    volume_previsionnel: int | None = Field(None, ge=0)
    moyenne_journaliere: Decimal = Field(..., max_digits=12, decimal_places=2)
    moyenne_hebdo: Decimal = Field(..., max_digits=12, decimal_places=2)
    exclusion: bool = False
    manuel: bool = Field(
        False, description="Ligne ajoutée manuellement (True) ou issue d'un calcul auto (False) → bl_manuel."
    )
    motif: str | None = Field(
        None, max_length=255, description="Justification d'une modif manuelle / exclusion (→ motif)."
    )


class TmhUpsert(_TmhFields):
    """Un produit du tableau TMH à enregistrer en lot (DSR-659 / création DSR-634).

    `id_tmh` présent → MAJ de la ligne ciblée ; absent → insertion d'une nouvelle
    ligne (un même produit peut être présent plusieurs fois sur un scénario).
    """

    id_tmh: int | None = Field(
        None, gt=0, description="Présent = MAJ de cette ligne ; absent = nouvelle ligne."
    )


class TmhCreate(_TmhFields):
    """Body POST : création d'une nouvelle ligne TMH (produit éventuellement déjà présent)."""

    id_rh: str = Field(
        ..., min_length=1, description="id_rh en clair de l'utilisateur ; crypté serveur avant stockage."
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
    """Body PATCH ciblé d'un trafic initial modifié (DSR-649), par id_tmh."""

    model_config = ConfigDict(extra="forbid")
    volume_realise: int  # valeurs négatives autorisées
    moyenne_journaliere: Decimal = Field(..., max_digits=12, decimal_places=2)
    moyenne_hebdo: Decimal = Field(..., max_digits=12, decimal_places=2)
    motif: str | None = Field(
        None, max_length=255, description="Justification de la modification manuelle (→ motif)."
    )


class TmhExclusionUpdate(BaseModel):
    """Body PATCH du switch d'exclusion d'une ligne TMH (bl_exclu), par id_tmh."""

    model_config = ConfigDict(extra="forbid")
    bl_exclu: bool = Field(
        ..., description="True = produit exclu du calcul, False = inclus."
    )
    motif: str | None = Field(
        None, max_length=255, description="Justification de l'exclusion / réintégration (→ motif)."
    )
