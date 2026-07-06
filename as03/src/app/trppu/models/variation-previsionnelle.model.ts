/**
 * Variation prévisionnelle d'un produit pour un scénario.
 * Correspond à la table back `trppu_scenario_variations_prev`.
 * Règle métier : une variation à 0 % n'est PAS stockée côté back
 * (valeur par défaut pour tous les produits d'un nouveau scénario).
 */
export interface VariationPrevisionnelle {
  /** Code produit (char(2) en base : OO, OS, PR, CO, PP, IP...). */
  co_produit: string;
  /** Libellé d'affichage (issu du référentiel produit, absent de la réponse back). */
  libelle?: string;
  /** Pourcentage de variation, borné [-100 ; 100]. */
  variation_pct: number;
}
