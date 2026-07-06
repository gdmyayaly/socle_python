/**
 * Paramétrage de rétention en PIC (service YS04 - DSR-660 / DSR-661).
 * Un coefficient = une cellule du tableau : (produit, jour, densité).
 */

/** Jour de semaine (lundi -> samedi). */
export type JourSemaine = 'LUN' | 'MAR' | 'MER' | 'JEU' | 'VEN' | 'SAM';

/** Densité : 0 = dense, 1 = faible 1 (clairsemée), 2 = faible 2 (clairsemée2). */
export type Densite = 0 | 1 | 2;

/** Version PIC par défaut (national). Toute autre valeur => propre au scénario. */
export const ID_PIC_VERSION_DEFAUT = 1;

/**
 * Coefficient de rétention renvoyé par YS04 (DSR-660).
 * `id_pic_version` != 1 => coefficient propre au scénario (à afficher en gras/vert).
 */
export interface PicCoefficient {
  co_produit: string;
  jour_semaine: JourSemaine;
  densite: Densite;
  /** Coefficient en pourcentage [0 ; 100] (ex. 21.7 => "21,7%"). null = cellule vide. */
  coef: number | null;
  id_pic_version: number;
}

/**
 * Corps de la mise à jour d'un coefficient (DSR-661).
 * Transmis au service dès qu'une cellule modifiée perd le focus.
 */
export interface PicCoefUpdate {
  id_scenario: number;
  co_produit: string;
  jour_semaine: JourSemaine;
  coef: number;
  densite: Densite;
}
