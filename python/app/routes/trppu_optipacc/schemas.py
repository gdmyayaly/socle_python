"""Schémas Pydantic v2 des services OPTIPACC (DSR-689, DSR-690).

Le nommage reproduit les contrats des tickets tels quels : camelCase pour les
entrées et pour les produits (`codeRegate`, `scenarioId`, `codeProduit`,
`volumeBrut`), snake_case pour les scénarios (`id_scenario`, `lb_scenario`,
contrat DSR-690). Les champs restent en snake_case côté Python et portent un
alias ; FastAPI sérialise les `response_model` avec `by_alias=True` par défaut.
"""

from pydantic import BaseModel, ConfigDict, Field

# Aligné sur CO_REGATE_PATTERN (app/routes/trppu_scenario/schemas.py).
CO_REGATE_PATTERN = r"^[A-Za-z0-9]{6}$"

_REQUEST_CONFIG = ConfigDict(
    extra="forbid",
    populate_by_name=True,
    str_strip_whitespace=True,
)


class SiteScenariosRequest(BaseModel):
    """Body du service S_SiteListeScenarios (DSR-690)."""

    model_config = _REQUEST_CONFIG

    code_regate: str = Field(
        ...,
        alias="codeRegate",
        min_length=6,
        max_length=6,
        pattern=CO_REGATE_PATTERN,
    )


class ScenarioItem(BaseModel):
    """Un scénario proposable. snake_case volontaire : contrat DSR-690."""

    model_config = ConfigDict(from_attributes=True)

    id_scenario: int
    lb_scenario: str


class SiteScenariosResponse(BaseModel):
    """Réponse S_SiteListeScenarios.

    `message` n'est renseigné que lorsque aucun scénario n'est éligible (CA4) :
    une liste vide est un résultat, pas une erreur HTTP.
    """

    code_regate: str = Field(..., serialization_alias="codeRegate")
    scenarios: list[ScenarioItem]
    message: str | None = None


class TraficBrutRequest(BaseModel):
    """Body du service S_ScenarioTraficBrut (DSR-689).

    `inclureExclus` est une extension optionnelle du contrat : par défaut les
    produits marqués exclus dans le TMH ne sont pas restitués.
    """

    model_config = _REQUEST_CONFIG

    code_regate: str = Field(
        ...,
        alias="codeRegate",
        min_length=6,
        max_length=6,
        pattern=CO_REGATE_PATTERN,
    )
    scenario_id: int = Field(..., alias="scenarioId", ge=1)
    inclure_exclus: bool = Field(False, alias="inclureExclus")


class ProduitVolume(BaseModel):
    """Volume brut final d'un produit.

    `code_produit` est borné à 3 caractères comme la colonne `co_produit` en base ;
    on ne réutilise pas CO_PRODUIT_PATTERN du module scénario, borné à 2, qui
    rejetterait un produit comme `PPI`.
    """

    code_produit: str = Field(
        ..., serialization_alias="codeProduit", min_length=1, max_length=3
    )
    volume_brut: int = Field(..., serialization_alias="volumeBrut")


class TraficBrutResponse(BaseModel):
    """Réponse S_ScenarioTraficBrut : uniquement la valeur finale (RG6, CA5)."""

    code_regate: str = Field(..., serialization_alias="codeRegate")
    scenario_id: int = Field(..., serialization_alias="scenarioId")
    produits: list[ProduitVolume]
