"""Helpers SQL pour les variations prévisionnelles (trppu_scenario_variations_prev)."""

SELECT_VARIATIONS_SQL = (
    "SELECT co_produit, variation_pct "
    "FROM trppu_scenario_variations_prev "
    "WHERE id_scenario = %s ORDER BY co_produit"
)


async def fetch_variation(tx_or_db, id_scenario: int, co_produit: str):
    """Récupère la variation (id_scenario, co_produit) ou None."""
    return await tx_or_db.fetch_one(
        "SELECT id_variation FROM trppu_scenario_variations_prev "
        "WHERE id_scenario = %s AND co_produit = %s",
        (id_scenario, co_produit),
    )
