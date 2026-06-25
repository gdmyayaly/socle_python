/**
 * Une neutralisation d'un scénario, alignée sur la table backend `trppu_neutralisations`
 * (et la réponse `NeutralisationOut` de l'API).
 *
 * - `id` : id_neutralisation, présent uniquement pour les lignes déjà persistées (sert au DELETE).
 * - `dt_debut` / `dt_fin` : dates au format ISO `YYYY-MM-DD`.
 * - `motif` : justification libre (obligatoire côté backend).
 * - `nb_jour` : nombre de jours ouvrés neutralisés, calculé côté serveur (autoritaire).
 *   Le composant en fournit un aperçu client en attendant la réponse du POST.
 */
export interface Neutralisation {
  id?: number;
  dt_debut: string;
  dt_fin: string;
  motif: string;
  nb_jour?: number;
}
