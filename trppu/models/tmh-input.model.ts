export interface TmhInput {
  id_tmh: number | null;
  co_produit: string;
  volume_realise: number;
  volume_previsionnel: number | null;
  /** Prévisionnel après variation %. Optionnel : absent => le serveur le
   *  réaligne sur volume_previsionnel (valeur de base). */
  volume_previsionnel_recalcule?: number | null;
  moyenne_journaliere: number;
  moyenne_hebdo: number;
  exclusion: boolean | null;
  manuel: boolean | null;
  motif: string | null;
}
