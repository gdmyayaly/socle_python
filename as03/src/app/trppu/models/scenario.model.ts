export interface Scenario {
  id: number;
  nom: string;
  dateCreation: string;
  statut: string;
  dateValidation: string | null;
  dateMo: string | null;
  dateProduction: string | null;
}
