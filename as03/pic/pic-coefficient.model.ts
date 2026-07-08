/**
 * Paramétrage de rétention en PIC (service YS04 - DSR-660 / DSR-661).
 * Un coefficient = une cellule du tableau : (produit, jour, densité).
 */

/** Jour de semaine côté IHM (lundi -> samedi). */
export type JourSemaine = 'LUN' | 'MAR' | 'MER' | 'JEU' | 'VEN' | 'SAM';

/** Jour de semaine côté back YS04 (enum trppu_pic_coefficients.jour_semaine). */
export type JourSemaineApi =
  | 'LUNDI' | 'MARDI' | 'MERCREDI' | 'JEUDI' | 'VENDREDI' | 'SAMEDI';

/** Correspondance IHM -> back (écriture). */
export const JOUR_TO_API: Record<JourSemaine, JourSemaineApi> = {
  LUN: 'LUNDI', MAR: 'MARDI', MER: 'MERCREDI',
  JEU: 'JEUDI', VEN: 'VENDREDI', SAM: 'SAMEDI',
};

/** Correspondance back -> IHM (lecture). */
export const JOUR_FROM_API: Record<JourSemaineApi, JourSemaine> = {
  LUNDI: 'LUN', MARDI: 'MAR', MERCREDI: 'MER',
  JEUDI: 'JEU', VENDREDI: 'VEN', SAMEDI: 'SAM',
};

/** Densité : 0 = dense, 1 = faible 1 (clairsemée), 2 = faible 2 (clairsemée2). */
export type Densite = 0 | 1 | 2;

/** Version PIC par défaut (national) — fallback d'affichage uniquement. */
export const ID_PIC_VERSION_DEFAUT = 1;

/**
 * Coefficient de rétention renvoyé par YS04 (DSR-660), déjà fusionné défaut/scénario.
 * `modifie = true` => coefficient propre au scénario (à afficher en gras/vert).
 */
export interface PicCoefficient {
  co_produit: string;
  jour_semaine: JourSemaine;
  densite: Densite;
  /** Coefficient en pourcentage [0 ; 100] (ex. 21.7 => "21,7%"). null = cellule vide. */
  coef: number | null;
  /** Version d'origine de la ligne : défaut national ou version scénario. */
  id_pic_version: number;
  /** True si la valeur a été surchargée pour le scénario (distinction couleur). */
  modifie: boolean;
}

/**
 * Corps de la mise à jour d'un coefficient (DSR-661), côté IHM.
 * `id_rh` et `id_session_ihm` sont ajoutés par le service au moment de l'appel.
 */
export interface PicCoefUpdate {
  id_scenario: number;
  co_produit: string;
  jour_semaine: JourSemaine;
  coef: number;
  densite: Densite;
}

/** Réponse du PUT YS04 (DSR-661). */
export interface PicCoefUpsertResult {
  action: 'update' | 'insert_coef' | 'insert_version_and_coef';
  id_pic_version: number;
}
