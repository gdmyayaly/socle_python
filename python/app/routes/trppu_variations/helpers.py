"""Helpers SQL pour les variations prévisionnelles (trppu_scenario_variations_prev).

La liste des variations est pilotée par les produits du TMH du scénario, pas par la
table des variations : cette dernière ne stocke que les écarts ≠ 0 % (le 0 % n'est
pas stocké, cf. routes.upsert_variation). On renvoie donc un produit par co_produit
distinct du TMH ayant au moins une ligne non exclue (bl_exclu = 0), avec la variation
stockée si elle existe, sinon 0 (COALESCE). Deux paramètres id_scenario attendus
(sous-requête TMH + jointure variations).
"""

SELECT_VARIATIONS_SQL = (
    "SELECT t.co_produit, COALESCE(v.variation_pct, 0) AS variation_pct "
    "FROM ("
    "    SELECT co_produit FROM trppu_tmh "
    "    WHERE id_scenario = %s AND bl_exclu = 0 "
    "    GROUP BY co_produit"
    ") AS t "
    "LEFT JOIN trppu_scenario_variations_prev v "
    "    ON v.co_produit = t.co_produit AND v.id_scenario = %s "
    "ORDER BY t.co_produit"
)


async def fetch_variation(tx_or_db, id_scenario: int, co_produit: str):
    """Récupère la ligne de variation (id_scenario, co_produit) ou None.

    Retourne toutes les colonnes utiles : les appelants s'en servent comme test
    d'existence et pour journaliser l'état avant une MAJ ou une suppression
    (cf. api_docs/CONVENTION-LOGS.md). `id_rh` est volontairement exclu du SELECT.
    """
    return await tx_or_db.fetch_one(
        "SELECT id_variation, id_scenario, co_produit, variation_pct "
        "FROM trppu_scenario_variations_prev "
        "WHERE id_scenario = %s AND co_produit = %s",
        (id_scenario, co_produit),
    )
