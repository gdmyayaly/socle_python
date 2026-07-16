export interface TmhInput {
  id_tmh: number | null;
  co_produit: string;
  volume_realise: number;
  volume_previsionnel: number | null;
  moyenne_journaliere: number;
  moyenne_hebdo: number;
  exclusion: boolean | null;
  manuel: boolean | null;
  motif: string | null;
}
