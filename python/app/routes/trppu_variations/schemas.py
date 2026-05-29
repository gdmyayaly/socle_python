"""Schémas Pydantic v2 pour les variations prévisionnelles (trppu_scenario_variations_prev)."""

from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class VariationUpsert(BaseModel):
    """Body PUT (DSR-646). variation_pct peut être négatif ; 0 % => suppression."""

    model_config = ConfigDict(extra="forbid")

    variation_pct: Decimal = Field(..., max_digits=5, decimal_places=2)
    id_rh: str = Field(..., min_length=1)  # crypté en base


class VariationOut(BaseModel):
    """Réponse lecture (DSR-651)."""

    model_config = ConfigDict(from_attributes=True)

    co_produit: str
    variation_pct: Decimal


class VariationUpsertResult(BaseModel):
    co_produit: str
    variation_pct: Optional[Decimal] = None
    action: Literal["created", "updated", "deleted", "noop"]
