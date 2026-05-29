"""Helpers SQL + agrégation pour les neutralisations (trppu_neutralisations)."""

from .schemas import FeriesBloc, FerieJour, NeutralisationsOut, PeriodeBloc

SELECT_NEUTRALISATIONS_SQL = (
    "SELECT id, dt_debut, dt_fin, nb_jour, type "
    "FROM trppu_neutralisations WHERE id_scenario = %s ORDER BY type, dt_debut"
)


def group_neutralisations(rows: list) -> NeutralisationsOut:
    """Regroupe les lignes par type : feries (liste), peak (1), saison (1)."""
    feries = [r for r in rows if r["type"] == "FERIE"]
    peak = next((r for r in rows if r["type"] == "PEAK"), None)
    saison = next((r for r in rows if r["type"] == "SAISON"), None)

    feries_bloc = FeriesBloc(
        actif=bool(feries),
        nb_jours_total=sum(int(r["nb_jour"]) for r in feries),
        jours=[FerieJour(dt=r["dt_debut"], nb_jour=int(r["nb_jour"])) for r in feries],
    )

    def _periode(r) -> PeriodeBloc:
        if not r:
            return PeriodeBloc(actif=False)
        return PeriodeBloc(
            actif=True, dt_debut=r["dt_debut"], dt_fin=r["dt_fin"], nb_jour=int(r["nb_jour"])
        )

    return NeutralisationsOut(
        feries=feries_bloc, peak=_periode(peak), saison=_periode(saison)
    )
