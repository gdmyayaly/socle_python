"""Schémas Pydantic v2 pour la table trppu_scenario."""

from datetime import date, datetime
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

CO_REGATE_PATTERN = r"^[A-Za-z0-9]{6}$"
CO_PRODUIT_PATTERN = r"^[A-Za-z0-9]{1,2}$"

# Reflète l'enum trppu_scenario.statut du schéma (db/db_new.sql) : SIMULATION
# incluse, sans quoi tout scénario en simulation fait échouer la validation
# du response_model (ResponseValidationError -> 500).
Statut = Literal["EN COURS", "SIMULATION", "VALIDE", "EN PRODUCTION", "ARCHIVE"]
NbJours = Literal[5, 6]


class ScenarioTmhItem(BaseModel):
    """Une ligne TMH fournie à la création/MAJ d'un scénario.

    Mirroir de `app.routes.trppu_tmh.schemas.TmhUpsert` (défini ici pour éviter un
    import circulaire entre les modules scénario et TMH). Mêmes noms d'attributs :
    consommé par `trppu_tmh.helpers.upsert_tmh_rows` (duck typing).

    `id_tmh` présent → MAJ de la ligne ciblée ; absent → insertion (un produit peut
    apparaître plusieurs fois pour un scénario, cf. migration 24/06/2026).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    id_tmh: Optional[int] = Field(
        None, gt=0, description="Présent = MAJ de cette ligne ; absent = nouvelle ligne."
    )
    co_produit: str = Field(..., min_length=1, max_length=2, pattern=CO_PRODUIT_PATTERN)
    volume_realise: Optional[int] = Field(None, ge=0)
    volume_previsionnel: Optional[int] = Field(None, ge=0)
    volume_previsionnel_recalcule: Optional[int] = Field(
        None,
        ge=0,
        description=(
            "Prévisionnel après variation % (calcul front). "
            "Absent => réaligné serveur sur volume_previsionnel."
        ),
    )
    moyenne_journaliere: Decimal = Field(..., max_digits=12, decimal_places=2)
    moyenne_hebdo: Decimal = Field(..., max_digits=12, decimal_places=2)
    exclusion: bool = False
    manuel: bool = False  # → bl_manuel (ligne saisie manuellement vs calcul auto)
    motif: Optional[str] = Field(
        None, max_length=255, description="Justification d'une modif manuelle / exclusion (→ motif)."
    )


class ScenarioBase(BaseModel):
    co_regate: str = Field(..., min_length=6, max_length=6, pattern=CO_REGATE_PATTERN)
    lb_scenario: str = Field(..., min_length=1, max_length=20)
    co_roc: str = Field(..., min_length=6, max_length=6, pattern=CO_REGATE_PATTERN)


class ScenarioCreate(ScenarioBase):
    """Body POST /scenarios.

    Obligatoires : co_regate, lb_scenario, co_roc, lb_regate, type_site.
    nb_jours_semaine : 5 par défaut, contraint à {5, 6}.
    id_pic_version : si non fourni, résolu côté serveur (premier est_par_defaut=1, fallback 1).
    Périodes principales : si None, today-1an / today+1an.
    Les colonnes periode_realise_*/periode_prev_* sont **dérivées serveur** depuis
    (periode_debut, periode_fin, today) et ne sont pas acceptées dans le body.
    statut, version_scenario, est_fige : forcés côté serveur à EN COURS / 1 / False.

    lb_regate, type_site : obligatoires. Si le site (co_regate) n'existe pas
    encore dans trppu_site, il est créé avec ces valeurs. Si le site existe
    déjà, ces champs sont ignorés (pas de MAJ).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    lb_regate: str = Field(..., min_length=1, max_length=120)
    type_site: str = Field(..., min_length=1, max_length=5)
    nb_jours_semaine: NbJours = 6  # défaut 6 (jours ouvrables) — cf. DSR-634
    id_pic_version: Optional[int] = Field(None, gt=0)
    periode_debut: Optional[date] = None
    periode_fin: Optional[date] = None
    dt_mise_en_oeuvre: Optional[date] = None  # défaut serveur = date du jour
    id_rh: str = Field(..., min_length=1)  # crypté en base (id_rh_creation/maj)
    tmh: list[ScenarioTmhItem] = Field(
        default_factory=list,
        description="Lignes du tableau TMH à enregistrer (1 par produit).",
    )

    @model_validator(mode="after")
    def _check_periodes(self):
        if self.periode_debut and self.periode_fin and self.periode_fin < self.periode_debut:
            raise ValueError("periode_fin doit être >= periode_debut")
        return self


class ScenarioOut(BaseModel):
    """Réponse complète pour les endpoints scénario."""

    model_config = ConfigDict(from_attributes=True)

    id_scenario: int
    co_regate: str
    lb_scenario: str
    co_roc: str
    statut: Statut
    dt_creation: datetime
    dt_validation: Optional[datetime] = None
    dt_mise_en_oeuvre: Optional[datetime] = None
    dt_mise_en_prod: Optional[datetime] = None
    dt_real_prev: Optional[datetime] = None
    periode_debut: date
    periode_fin: date
    periode_realise_debut: Optional[date] = None
    periode_realise_fin: Optional[date] = None
    periode_prev_debut: Optional[date] = None
    periode_prev_fin: Optional[date] = None
    nb_jours_semaine: int
    nb_jours_ouvres: Optional[int] = None
    nb_jours_ouvrables: Optional[int] = None
    nb_jours_scenario: Optional[int] = None
    id_pic_version: int
    version_scenario: int
    est_fige: bool
    trafic_pdi_calcule: bool = False  # 0 = trafic PDI non calculé, 1 = calculé
    trafic_agrebal_calcule: bool = False  # 0 = trafic agrebal non calculé, 1 = calculé


class ScenarioPeriodesOut(BaseModel):
    """Réponse DSR-655 : périodes + nombres de jours d'un scénario (slider IHM)."""

    model_config = ConfigDict(from_attributes=True)

    periode_debut: date
    periode_fin: date
    periode_realise_debut: Optional[date] = None
    periode_realise_fin: Optional[date] = None
    periode_prev_debut: Optional[date] = None
    periode_prev_fin: Optional[date] = None
    nb_jours_semaine: int
    nb_jours_ouvres: Optional[int] = None
    nb_jours_ouvrables: Optional[int] = None
    nb_jours_scenario: Optional[int] = None


class ScenarioMajRequest(BaseModel):
    """Body PUT /scenarios/{id} (DSR-656) : MAJ d'un scénario EN COURS après recalcul.

    Les bornes réalisé/prév et les nb_jours sont **recalculés serveur** ; dt_real_prev
    et dt_maj = date courante ; id_rh (maj) crypté. `tmh` (optionnel) met à jour les
    trafics recalculés (DSR-659).
    """

    model_config = ConfigDict(extra="forbid")

    periode_debut: date
    periode_fin: date
    nb_jours_semaine: NbJours
    dt_mise_en_oeuvre: Optional[date] = None  # si absent : valeur existante conservée
    id_rh: str = Field(..., min_length=1)
    tmh: list[ScenarioTmhItem] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check(self):
        if self.periode_fin < self.periode_debut:
            raise ValueError("periode_fin doit être >= periode_debut")
        return self


class PeriodeUpdate(BaseModel):
    """PATCH /periodes : MAJ des bornes principales (realise/prev recalculés serveur)."""

    model_config = ConfigDict(extra="forbid")

    periode_debut: Optional[date] = None
    periode_fin: Optional[date] = None

    @model_validator(mode="after")
    def _at_least_one(self):
        if not self.model_dump(exclude_unset=True):
            raise ValueError("Au moins un champ doit être fourni.")
        return self


class NbJoursUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    nb_jours_semaine: NbJours


class StatutUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    statut: Statut


class FigeUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    est_fige: bool


class FigementParStatutRequest(BaseModel):
    """Body PATCH /{id}/figement (DSR-669) : statut IHM pilotant le figement.

    Statut libre (libellé IHM, ex. "validé"/"simulation"/"en cours") ; le mapping
    vers est_fige est résolu serveur (cf. statuts.resolve_fige_from_statut).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    statut: str = Field(..., min_length=1)


class LbScenarioUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    lb_scenario: str = Field(..., min_length=1, max_length=20)


class DuplicateRequest(BaseModel):
    """Body POST /duplicate : id_rh requis (traçabilité du clone), lb_scenario optionnel."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    id_rh: str = Field(..., min_length=1)
    lb_scenario: Optional[str] = Field(None, min_length=1, max_length=20)
