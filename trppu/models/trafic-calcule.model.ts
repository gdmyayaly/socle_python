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
  comptageManuel: number;
  volumeBrut: number;
  traficMoyenHebdo: number;
  traficMoyenJourna: number;
}
