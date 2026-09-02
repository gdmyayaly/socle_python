"""Helpers SQL pour les neutralisations (trppu_neutralisations)."""

SELECT_NEUTRALISATIONS_SQL = (
    "SELECT id_neutralisation AS id, dt_debut, dt_fin, nb_jour, motif "
    "FROM trppu_neutralisations WHERE id_scenario = %s ORDER BY dt_debut, dt_fin"
)

# Ligne complète d'une neutralisation, utilisée pour journaliser l'état avant une
# suppression définitive (la clause WHERE est ajoutée par l'appelant).
SELECT_NEUTRALISATION_SQL = (
    "SELECT id_neutralisation, id_scenario, dt_debut, dt_fin, nb_jour, motif "
    "FROM trppu_neutralisations"
)
