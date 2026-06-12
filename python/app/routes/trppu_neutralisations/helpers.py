"""Helpers SQL pour les neutralisations (trppu_neutralisations)."""

SELECT_NEUTRALISATIONS_SQL = (
    "SELECT id_neutralisation AS id, dt_debut, dt_fin, nb_jour, motif "
    "FROM trppu_neutralisations WHERE id_scenario = %s ORDER BY dt_debut, dt_fin"
)
