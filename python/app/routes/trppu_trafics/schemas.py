"""DSR-679 — Modèles de réponse de l'endpoint trafics pivot."""

from pydantic import BaseModel


class TraficObjet(BaseModel):
    """Trafics agrégés (somme sur la période) pour un objet TRPPU (co_type_objet)."""

    co_produit: str
    trafic_brut: float
    trafic_previsionnel: float


class NbJours(BaseModel):
    """Nombre de jours ouvrés / ouvrables sur la période (DSR-613)."""

    nbJoursOuvres: int
    nbJoursOuvrables: int


class TraficsPivotResponse(BaseModel):
    """Réponse de GET /trppu-api/trafics/get_trafics_pivot."""

    execution_time_s: float
    co_regate: str
    date_debut: str
    date_fin: str
    date_pivot: str
    is_day: bool
    count: int
    trafics: list[TraficObjet]
    nb_jours: NbJours | None = None
    queries: list[str] | None = None
