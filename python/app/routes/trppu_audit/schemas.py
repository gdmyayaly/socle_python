"""Schémas Pydantic v2 pour la route d'audit id_rh."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class AuditRequest(BaseModel):
    """Body POST : id_rh chiffré + clé de déchiffrement."""

    model_config = ConfigDict(str_strip_whitespace=True)

    id_rh: str = Field(
        ...,
        min_length=1,
        description=(
            "id_rh **chiffré** (token Fernet) dont on veut retrouver les actions. "
            "Un id_rh en clair est aussi accepté si le cryptage est désactivé côté serveur."
        ),
    )
    cle: str = Field(
        ...,
        min_length=1,
        description="Clé / secret de déchiffrement (même valeur que ID_RH_CRYPTO_KEY).",
    )


class ActionOut(BaseModel):
    """Une action effectuée par l'id_rh, retrouvée en base."""

    ressource: str = Field(..., description="Table source de l'action.")
    action: str = Field(..., description="Type d'action (création, MAJ, écriture…).")
    id: int = Field(..., description="Identifiant de la ligne concernée.")
    id_scenario: int | None = Field(None, description="Scénario rattaché si applicable.")
    date: datetime | None = Field(None, description="Date de l'action (au mieux disponible).")
    details: dict[str, Any] = Field(default_factory=dict, description="Contexte de l'action.")


class AuditOut(BaseModel):
    """Réponse : id_rh en clair + liste des actions trouvées."""

    id_rh: str = Field(..., description="id_rh **en clair** (déchiffré depuis le token fourni).")
    nb_actions: int
    actions: list[ActionOut]
