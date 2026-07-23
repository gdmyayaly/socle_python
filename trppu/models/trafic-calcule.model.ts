export interface TraficCalcule {
  id: number;
  coProduit: string;
  lbProduit: string;
  produit: string;
  exclure: boolean;
  manuel: boolean;
  motif: string;
  manuelCalc: boolean;
  constateBrut: number;
  previsionnelBrut: number;
  /** Prévisionnel recalculé (variation %) ; null/égal à previsionnelBrut = pas de variation. */
  previsionnelRecalcule?: number | null;
  comptageManuel: number;
  volumeBrut: number;
  traficMoyenHebdo: number;
  traficMoyenJourna: number;
}
