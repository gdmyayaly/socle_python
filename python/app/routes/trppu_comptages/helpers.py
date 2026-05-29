"""Helpers SQL pour les comptages manuels (table trppu_scenario_comptages_manuels)."""

SELECT_COMPTAGES_SQL = (
    "SELECT co_produit, dt_comptage, nb_produit "
    "FROM trppu_scenario_comptages_manuels "
    "WHERE id_scenario = %s ORDER BY co_produit, dt_comptage"
)


async def fetch_comptage(tx_or_db, id_scenario: int, co_produit: str):
    """Récupère le comptage (id_scenario, co_produit) ou None."""
    return await tx_or_db.fetch_one(
        "SELECT id_comptage FROM trppu_scenario_comptages_manuels "
        "WHERE id_scenario = %s AND co_produit = %s",
        (id_scenario, co_produit),
    )
