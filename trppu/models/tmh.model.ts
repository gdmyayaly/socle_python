export interface Tmh {
  id_tmh: number | null;
  co_produit: string;
  volume_realise: number;
  volume_previsionnel: number | null;
  moyenne_journaliere: string;
  moyenne_hebdo: string;
  bl_exclu: boolean | null;
  bl_manuel: boolean | null;
  motif: string | null;
}
