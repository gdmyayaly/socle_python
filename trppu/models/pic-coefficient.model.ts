/**
* Paramétrage de rétention en PIC (service YS04 - DSR-660 / DSR-661).
* Un coefficient = une cellule du tableau : (produit, jour, densité).
*/



/** Jour de semaine côté IHM (lundi -> samedi). */
export type JourSemaine = 'LUN' | 'MAR' | 'MER' | 'JEU' | 'VEN' | 'SAM';

/** Jour de semaine côté API YS04 (Literal Pydantic du backend). */
export type JourSemaineApi = 'LUNDI' | 'MARDI' | 'MERCREDI' | 'JEUDI' | 'VENDREDI' | 'SAMEDI';

/** Correspondance jour IHM -> jour API. */
export const JOUR_FRONT_TO_API: Record<JourSemaine, JourSemaineApi> = {
 LUN: 'LUNDI',
 MAR: 'MARDI',
 MER: 'MERCREDI',
 JEU: 'JEUDI',
 VEN: 'VENDREDI',
 SAM: 'SAMEDI',
};

/** Correspondance jour API -> jour IHM. */
export const JOUR_API_TO_FRONT: Record<JourSemaineApi, JourSemaine> = {
 LUNDI: 'LUN',
 MARDI: 'MAR',
 MERCREDI: 'MER',
 JEUDI: 'JEU',
 VENDREDI: 'VEN',
 SAMEDI: 'SAM',
};

/** Densité : 0 = dense, 1 = faible 1 (clairsemée), 2 = faible 2 (clairsemée2). */
export type Densite = 0 | 1 | 2;

/** Version PIC par défaut (national). Toute autre valeur => propre au scénario. */
export const ID_PIC_VERSION_DEFAUT = 1;

/**
* Coefficient de rétention affiché dans l'IHM (DSR-660).
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

/** Un coefficient tel que renvoyé par l'API (DSR-660). */
export interface PicCoefItemApi {
 id_pic_version: number;
 co_produit: string;
 jour_semaine: JourSemaineApi;
 densite: Densite;
 coef: number | string; // Decimal sérialisé (nombre ou chaîne selon la config JSON)
 modifie: boolean;
}

/** Enveloppe de la réponse GET /trppu-api/scenarios/{id}/pic-coefficients (DSR-660). */
export interface PicScenarioOut {
 id_pic_version_defaut: number | null;
 id_pic_version_scenario: number | null;
 niveau_scenario: string | null;
 coefficients: PicCoefItemApi[];
}

/** Vue IHM de la réponse DSR-660 : enveloppe + coefficients au format front. */
export interface PicScenarioView {
 idPicVersionDefaut: number;
 /** null si le scénario n'a jamais été surchargé (tout vient du national). */
 idPicVersionScenario: number | null;
 niveauScenario: string | null;
 coefficients: PicCoefficient[];
}

/**
* Corps de la mise à jour d'un coefficient (DSR-661).
* `id_scenario` est passé dans l'URL (le backend refuse tout champ en trop) ;
* transmis au service dès qu'une cellule modifiée perd le focus.
*/
export interface PicCoefUpdate {
 co_produit: string;
 jour_semaine: JourSemaine;
 coef: number;
 densite: Densite;
 /** Identifiant RH de l'utilisateur (crypté côté serveur). Obligatoire. */
 id_rh: string;
}

/** Réponse du PUT /trppu-api/scenarios/{id}/pic-coefficients (DSR-661). */
export interface PicCoefUpsertResult {
 action: 'update' | 'insert_coef' | 'insert_version_and_coef';
 id_pic_version: number;
}
