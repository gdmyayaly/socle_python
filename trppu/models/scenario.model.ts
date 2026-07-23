import { Periode } from './periode.model';

export interface Scenario {
  id?: number;
  id_scenario?: number;
  lb_scenario?: string;
  co_regate?: string | null;
  co_roc?: string | null;
  statut?: string;
  dt_creation?: string;
  dt_mise_en_oeuvre?: string | null;
  dt_real_prev?: string | null;
  periode?: Periode | null;
  periode_debut?: string | null;
  periode_fin?: string | null;
  periode_realise_debut?: string | null;
  periode_realise_fin?: string | null;
  periode_prev_debut?: string | null;
  periode_prev_fin?: string | null;
  dt_validation?: string | null;
  dt_mise_en_prod?: string | null;
  nb_jours_semaine?: number;
  nb_jours_ouvres?: number;
  nb_jours_ouvrables?: number;
  nb_jours_scenario?: number;
  id_pic_version?: number;
  version_scenario?: number;
  est_fige?: boolean;
  trafic_pdi_calcule?: boolean;
  trafic_agrebal_calcule?: boolean;
}

// Scenario Status Constants
export enum ScenarioStatus {
  EN_COURS = 'EN COURS',
  SIMULATION = 'SIMULATION',
  VALIDE = 'VALIDE',
  EN_PRODUCTION = 'EN PRODUCTION',
  ARCHIVE = 'ARCHIVE'
}

// Scenario Status which can be manually selected by users
export const EDITABLE_STATUSES = [
  ScenarioStatus.EN_COURS,
  ScenarioStatus.SIMULATION,
  ScenarioStatus.VALIDE
];