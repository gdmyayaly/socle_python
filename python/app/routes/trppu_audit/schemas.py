"""Schémas Pydantic v2 pour la route d'audit id_rh."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


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


# ---------------------------------------------------------------------------
# DSR-737 — Agrébals et PDI d'un scénario ou d'un site
# ---------------------------------------------------------------------------
# Nommage camelCase : contrat du ticket (`idScenario`, `codeRegate`, `libelleSite`,
# `agrebalUuid`, `pdis`). Entrée par `alias=`, sortie par `serialization_alias=`.

CO_REGATE_PATTERN = r"^[A-Za-z0-9]{6}$"
TYPE_SCENARIO = "SCENARIO"
TYPE_SITE = "SITE"


class AgrebalsPdiRequest(BaseModel):
    """Body : `idScenario` OU `codeRegate` (RG-001), jamais les deux."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, str_strip_whitespace=True)

    id_scenario: int | None = Field(
        None, alias="idScenario", ge=1, description="Recherche par scénario (RG-003)."
    )
    code_regate: str | None = Field(
        None,
        alias="codeRegate",
        pattern=CO_REGATE_PATTERN,
        description="Recherche par site : données actives uniquement (RG-002).",
    )

    @model_validator(mode="after")
    def _un_seul_critere(self):
        if (self.id_scenario is None) == (self.code_regate is None):
            raise ValueError("Renseigner idScenario OU codeRegate (un seul des deux).")
        return self

    @property
    def type_recherche(self) -> str:
        return TYPE_SCENARIO if self.id_scenario is not None else TYPE_SITE


class AgrebalPdisOut(BaseModel):
    """Un Agrébal et ses PDI (RG-004, RG-005)."""

    agrebal_uuid: str = Field(..., serialization_alias="agrebalUuid")
    pdis: list[int]


class AgrebalsPdiOut(BaseModel):
    """Réponse : site, puis Agrébal -> PDI (CA-06)."""

    type_recherche: str = Field(..., serialization_alias="typeRecherche")
    id_scenario: int | None = Field(None, serialization_alias="idScenario")
    code_regate: str = Field(..., serialization_alias="codeRegate")
    libelle_site: str | None = Field(None, serialization_alias="libelleSite")
    nb_agrebals: int = Field(..., serialization_alias="nbAgrebals")
    nb_pdis: int = Field(..., serialization_alias="nbPdis")
    agrebals: list[AgrebalPdisOut]
    message: str | None = Field(
        None, description="Renseigné quand la liste est vide (ex. scénario non calculé)."
    )
