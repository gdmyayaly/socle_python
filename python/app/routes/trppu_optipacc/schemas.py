"""Schémas Pydantic v2 des services OPTIPACC (DSR-689, DSR-690, DSR-705, DSR-707).

Le nommage reproduit les contrats des tickets tels quels, y compris lorsqu'ils
divergent entre eux : camelCase pour DSR-689/690 (`codeRegate`, `scenarioId`,
`codeProduit`, `volumeBrut`), **snake_case** pour DSR-705 et DSR-707
(`code_regate`, `scenario_id`, `date_mise_en_oeuvre`). Les tickets les plus
récents sont écrits en snake_case et ces modèles-là n'ont donc aucun alias ;
là où un alias existe, l'entrée utilise `alias=` et la sortie
`serialization_alias=` (FastAPI sérialise les `response_model` avec
`by_alias=True` par défaut).

DSR-690 n'a pas de schéma d'entrée : le service est un GET, son unique paramètre
`codeRegate` est déclaré en `Query` dans `routes.py`.
"""

from datetime import date

from pydantic import BaseModel, ConfigDict, Field

# Aligné sur CO_REGATE_PATTERN (app/routes/trppu_scenario/schemas.py).
CO_REGATE_PATTERN = r"^[A-Za-z0-9]{6}$"

_REQUEST_CONFIG = ConfigDict(
    extra="forbid",
    populate_by_name=True,
    str_strip_whitespace=True,
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
    """Body du service S_ScenarioTraficBrut (DSR-689)."""

    model_config = _REQUEST_CONFIG

    code_regate: str = Field(
        ...,
        alias="codeRegate",
        min_length=6,
        max_length=6,
        pattern=CO_REGATE_PATTERN,
    )
    scenario_id: int = Field(..., alias="scenarioId", ge=1)


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


# --- DSR-707 : mise en production ------------------------------------------------


class MiseEnProductionRequest(BaseModel):
    """Body de POST /scenario/mise-en-production (DSR-707).

    Le ticket se contredit sur le nom du champ date : l'exemple de body montre
    `date_mise_en_prod`, la section « Paramètres » et le critère d'acceptation 1
    décrivent `date_mise_en_oeuvre`. Le Cas 1 tranche — une seule date envoyée
    alimente `DT_MISE_EN_OEUVRE` **et** `DT_MISE_EN_PROD` — et RG-API-PROD-006 le
    confirme : « la date transmise constitue la référence officielle de mise en
    œuvre et de mise en production ». On retient donc un champ unique.
    """

    model_config = _REQUEST_CONFIG

    code_regate: str = Field(
        ...,
        min_length=6,
        max_length=6,
        pattern=CO_REGATE_PATTERN,
        description="Code Regate du site concerné",
    )
    scenario_id: int = Field(..., ge=1, description="Identifiant du scénario à mettre en production")
    date_mise_en_oeuvre: date = Field(
        ...,
        description=(
            "Date effective de mise en œuvre de l'organisation (YYYY-MM-DD). "
            "Sert aussi de date de mise en production."
        ),
    )


class MiseEnProductionResponse(BaseModel):
    """Réponse 200 de la mise en production (DSR-707)."""

    scenario_id: int
    code_regate: str
    statut: str  # toujours "EN PRODUCTION" en cas de succès
    date_mise_en_oeuvre: date


# --- DSR-705 : trafics Agrébal (amas) --------------------------------------------

# Un agrebal_uuid est stocké en varchar(45) ; on ne contraint pas le format à un
# UUID canonique, la clé métier restant ce que YB05 a écrit (RG-API-004).
AGREBAL_UUID_MAX_LEN = 45

# Borne la clause IN (...) construite pour le filtre `amas`.
MAX_AMAS_DEMANDES = 1000


class TraficAmasRequest(BaseModel):
    """Body de POST /trafic-amas (DSR-705).

    `page` ne figure pas dans la section « Paramètres » du ticket mais est utilisé
    par les critères d'acceptation 6 et 7 ; il est donc déclaré ici, avec 1 pour
    défaut. Il est ignoré lorsque `amas` est fourni (Cas 8).
    """

    model_config = _REQUEST_CONFIG

    code_regate: str = Field(
        ...,
        min_length=6,
        max_length=6,
        pattern=CO_REGATE_PATTERN,
        description="Code Regate du site concerné",
    )
    scenario_id: int = Field(..., ge=1, description="Identifiant du scénario")
    amas: list[str] | None = Field(
        None,
        max_length=MAX_AMAS_DEMANDES,
        description=(
            "Liste des agrebal_uuid à restituer. Absente : tous les amas calculés "
            "du scénario (RG-API-005)."
        ),
    )
    page: int = Field(1, ge=1, description="Page demandée ; ignorée si `amas` est fourni")


class ProduitVolumes(BaseModel):
    """Volumes d'un produit pour un amas et un jour, ventilés par densité.

    Mapping imposé par DSR-705 : DENSE -> fort, FAIBLE1 -> faible1,
    FAIBLE2 -> faible2.
    """

    produit: str = Field(..., min_length=1, max_length=3)
    fort: int
    faible1: int
    faible2: int


class AmasOut(BaseModel):
    """Un Agrébal et ses trafics, regroupés par jour puis par produit.

    `nom_amas` provient de `trppu_agrebal_pdi.agrebal_nom`, colonne nullable et
    jointe en LEFT JOIN : il peut valoir `null` sans que les trafics disparaissent.
    """

    agrebal_uuid: str = Field(..., max_length=AGREBAL_UUID_MAX_LEN)
    nom_amas: str | None = None
    jours: dict[str, list[ProduitVolumes]]


class PaginationOut(BaseModel):
    """Bloc `pagination` de la réponse DSR-705.

    Toujours présent, y compris en mode filtre `amas` où il décrit l'unique page
    renvoyée (le ticket n'applique alors pas de pagination, Cas 8).
    """

    page: int
    taille_page: int
    nb_amas_total: int
    nb_pages: int
    page_suivante: int | None = None


class TraficAmasResponse(BaseModel):
    """Réponse de POST /trafic-amas.

    `site` et `scenario` reprennent littéralement le nommage du ticket (et non
    `code_regate` / `scenario_id` du body).
    """

    site: str
    scenario: int
    pagination: PaginationOut
    amas: list[AmasOut]
    # C4 : les UUID inconnus sont ignorés, mais tracés dans la réponse et dans les
    # logs. Le ticket impose la trace, pas le nom du champ.
    amas_non_trouves: list[str] = Field(default_factory=list)
