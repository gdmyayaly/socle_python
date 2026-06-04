export interface Scenario {
  id_scenario: number;
  co_regate: string;
  lb_scenario: string;
  co_roc: string;
  statut: string;
  dt_creation: string;
  dt_validation: string | null;
  dt_mise_en_prod: string | null;
  periode_debut: string | null;
  periode_fin: string | null;
  periode_realise_debut: string | null;
  periode_realise_fin: string | null;
  periode_prev_debut: string | null;
  periode_prev_fin: string | null;
  nb_jours_semaine: number;
  id_pic_version: number;
  version_scenario: number;
  est_fige: boolean;
}
