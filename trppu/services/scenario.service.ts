import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { Scenario } from '../models/scenario.model';
import { TraficCalcule } from '../models/trafic-calcule.model';
import { environment } from '../../../../environments/environment';
import { Tmh } from '../models/tmh.model';
import { TmhInput } from '../models/tmh-input.model';
import { Comptage } from '../models/comptage.model';
import { TraficService } from './trafics.service';

// Request/Update DTOs
export interface ListScenariosFilters {
  co_regate?: string | null;
  co_roc?: string | null;
  statut?: string | null;
  est_fige?: boolean | null;
  limit?: number;
  offset?: number;
}

export interface ScenarioCreate {
  lb_scenario: string;
  co_regate?: string;
  co_roc?: string;
  [key: string]: any; // Additional fields based on backend requirements
}

export interface ScenarioUpdate {
  periode_debut: string;
  periode_fin: string;
  nb_jours_semaine: number;
  dt_mise_en_oeuvre: string;
  id_rh: string;
}

export interface PeriodeUpdate {
  dtDebut: string;
  dtFin: string;
}

export interface NbJoursUpdate {
  nb_jours_semaine: number;
}

export interface StatutUpdate {
  statut: string;
}

export interface FigeUpdate {
  est_fige: boolean;
}

export interface LbScenarioUpdate {
  lbScenario: string;
}

export interface DuplicateRequest {
  lb_scenario?: string;
  [key: string]: any; // Additional fields based on backend requirements
}

export interface TmhOut {
  [key: string]: any;
}

export interface TmhBatchResult {
  [key: string]: any;
}

export interface TmhVolumeUpdate {
  volume_realise: number;
  moyenne_journaliere: string;
  moyenne_hebdo: string;
  motif: string;
}

export interface TmhExclusionUpdate {
  bl_exclu: boolean;
}

export interface ComptageCreate {
  [key: string]: any;
}

export interface ComptageUpdate {
  [key: string]: any;
}

export interface VariationOut {
  [key: string]: any;
}

export interface VariationUpsert {
  [key: string]: any;
}

export interface VariationUpsertResult {
  [key: string]: any;
}

export interface ScenarioEnums {
  [key: string]: any; // Enum values from backend
}

@Injectable({ providedIn: 'root' })
export class ScenarioService {
  readonly apiTrppuUrl = environment.trppuApiUrl;
  private readonly baseUrl = `${this.apiTrppuUrl}/scenarios`;

  constructor(private http: HttpClient, private traficService: TraficService) {}

  /**
   * List scenarios with optional filters
   * @param filters Optional filters for listing scenarios
   * @returns Observable of scenarios array
   */
  listScenarios(filters?: ListScenariosFilters): Observable<Scenario[]> {
    let params = new HttpParams();

    if (filters) {
      if (filters.co_regate !== undefined && filters.co_regate !== null) {
        params = params.set('co_regate', filters.co_regate);
      }
      if (filters.co_roc !== undefined && filters.co_roc !== null) {
        params = params.set('co_roc', filters.co_roc);
      }
      if (filters.statut !== undefined && filters.statut !== null) {
        params = params.set('statut', filters.statut);
      }
      if (filters.est_fige !== undefined && filters.est_fige !== null) {
        params = params.set('est_fige', filters.est_fige.toString());
      }
      if (filters.limit !== undefined) {
        params = params.set('limit', filters.limit.toString());
      }
      if (filters.offset !== undefined) {
        params = params.set('offset', filters.offset.toString());
      }
    }

    return this.http.get<Scenario[]>(this.baseUrl, { params });
  }

  /**
   * Get list of authorized enum values for scenario columns
   * @returns Observable of enum values
   */
  getEnums(): Observable<ScenarioEnums> {
    return this.http.get<ScenarioEnums>(`${this.baseUrl}/enums`);
  }

  /**
   * Create a new scenario
   * @param scenarioData Scenario creation data
   * @returns Observable of created scenario
   */
  createScenario(scenarioData: ScenarioCreate): Observable<Scenario> {
    return this.http.post<Scenario>(this.baseUrl, scenarioData);
  }

  /**
   * Get a specific scenario by ID
   * @param idScenario Scenario ID
   * @returns Observable of scenario
   */
  getScenario(idScenario: number | string): Observable<Scenario> {
    return this.http.get<Scenario>(`${this.baseUrl}/${idScenario}`);
  }

  /**
   * Delete a scenario (soft-delete: transitions to ARCHIVE status)
   * @param idScenario Scenario ID
   * @returns Observable of deleted scenario
   */
  deleteScenario(idScenario: number): Observable<Scenario> {
    return this.http.delete<Scenario>(`${this.baseUrl}/${idScenario}`);
  }

  /**
   * Update scenario 
   * @param idScenario Scenario ID
   * @param data Scenario update data
   * @returns Observable of updated scenario
   */
  updateScenario(idScenario: number, payload: ScenarioUpdate): Observable<Scenario> {
    return this.http.put<Scenario>(`${this.baseUrl}/${idScenario}`, payload);
  }

  /**
   * Update scenario periods (main boundaries)
   * Server recalculates realized/predicted boundaries
   * @param idScenario Scenario ID
   * @param data Period update data
   * @returns Observable of updated scenario
   */
  updatePeriodes(idScenario: number, data: PeriodeUpdate): Observable<Scenario> {
    const payload = {
      periode_debut: data.dtDebut,
      periode_fin: data.dtFin
    };

    return this.http.patch<Scenario>(`${this.baseUrl}/${idScenario}/periodes`, payload);
  }

  /**
   * Update number of days per week
   * @param idScenario Scenario ID
   * @param data Number of days update data
   * @returns Observable of updated scenario
   */
  updateNbJoursSemaine(idScenario: number, data: NbJoursUpdate): Observable<Scenario> {
    return this.http.patch<Scenario>(`${this.baseUrl}/${idScenario}/nb-jours-semaine`, data);
  }

  /**
   * Update scenario status via state machine with automatic side effects
   * Note: VALIDE -> EN PRODUCTION transition not available here, use miseEnProduction() instead
   * @param idScenario Scenario ID
   * @param data Status update data
   * @returns Observable of updated scenario
   */
  updateStatut(idScenario: number, data: StatutUpdate): Observable<Scenario> {
    return this.http.patch<Scenario>(`${this.baseUrl}/${idScenario}/statut`, data);
  }

  /**
   * Update scenario frozen flag
   * Only way to unfreeze a scenario after production
   * @param idScenario Scenario ID
   * @param data Frozen flag update data
   * @returns Observable of updated scenario
   */
  updateEstFige(idScenario: number, data: FigeUpdate): Observable<Scenario> {
    return this.http.patch<Scenario>(`${this.baseUrl}/${idScenario}/est-fige`, data);
  }

  /**
   * Update scenario label
   * @param idScenario Scenario ID
   * @param data Scenario label update data
   * @returns Observable of updated scenario
   */
  updateLbScenario(idScenario: number, data: LbScenarioUpdate): Observable<Scenario> {
    const payload = {
      lb_scenario: data.lbScenario
    };

    return this.http.patch<Scenario>(`${this.baseUrl}/${idScenario}/lb-scenario`, payload);
  }

  /**
   * Put scenario into production
   * Sets: status EN PRODUCTION, dt_mise_en_prod = NOW(), est_fige = 1
   * Only way to reach EN PRODUCTION status, only from VALIDE status
   * @param idScenario Scenario ID
   * @returns Observable of scenario in production
   */
  miseEnProduction(idScenario: number): Observable<Scenario> {
    return this.http.post<Scenario>(`${this.baseUrl}/${idScenario}/mise-en-prod`, {});
  }

  /**
   * Duplicate a scenario
   * Creates new scenario with status EN COURS, version 1, est_fige = 0
   * No parent tracking (id_scenario_parent removed from model)
   * @param idScenario Scenario ID to duplicate
   * @param payload Optional duplicate request data
   * @returns Observable of duplicated scenario
   */
  duplicateScenario(idScenario: number, payload?: DuplicateRequest): Observable<Scenario> {
    return this.http.post<Scenario>(`${this.baseUrl}/${idScenario}/duplicate`, payload || {});
  }

  /**
   * List TMH rows for a scenario
   * GET /scenarios/{id_scenario}/tmh
   */
  listTmh(
    idScenario: string,
    nbJoursOuvrables: number,
    joursSemaine: number,
    idSessionIhm?: string | null
  ): Observable<TraficCalcule[]> {
    let params = new HttpParams();

    if (idSessionIhm !== undefined && idSessionIhm !== null) {
      params = params.set('id_session_ihm', idSessionIhm);
    }

    return this.http.get<any>(`${this.baseUrl}/${idScenario}/tmh`, { params }).pipe(
      map((response) => this.normalizeTraficsResponse(response, nbJoursOuvrables, joursSemaine))
    );
  }

  /**
   * List raw TMH rows for a scenario, without display normalization.
   * GET /scenarios/{id_scenario}/tmh
   * Utilisé par le recalcul des moyennes (TmhRecalculService) qui a besoin
   * des lignes brutes (bl_exclu / bl_manuel / id_tmh) sans agrégation.
   */
  listTmhRaw(idScenario: number): Observable<Tmh[]> {
    return this.http.get<Tmh[]>(`${this.baseUrl}/${idScenario}/tmh`);
  }

  private normalizeTraficsResponse(response: any, nbJoursOuvrables: number, joursSemaine: number): TraficCalcule[] {
    const rows    = Array.isArray(response) ? response : [];
    const trafics = [];
    const manuels = [];

    rows.forEach((row: any, index: number) => {
      if (!row.bl_manuel) {
        manuels[row.co_produit] = index;
      } else if (manuels[row.co_produit] === undefined) {
        manuels[row.co_produit] = index;
      }

      trafics[index] = {
        id: row.id_tmh,
        coProduit: row.co_produit,
        lbProduit: row.co_produit,
        produit: row.co_produit,
        exclure: row.bl_exclu,
        manuel: row.bl_manuel,
        motif: row.motif,
        manuelCalc: false,
        constateBrut: row.volume_realise,
        previsionnelBrut: row.volume_previsionnel,
        comptageManuel: row.bl_manuel ? row.volume_realise : 0,
        volumeBrut: 0,
        traficMoyenHebdo: row.moyenne_hebdo
      };

      if (row.bl_manuel) {
        if (manuels[row.co_produit] !== undefined) {
          trafics[manuels[row.co_produit]].volumeBrut = Number(row?.volume_realise) + trafics[manuels[row.co_produit]].volumeBrut;
          trafics[manuels[row.co_produit]].traficMoyenHebdo = this.traficService.calculateMoyenneHebdo(
              trafics[manuels[row.co_produit]].volumeBrut,
              0,
              nbJoursOuvrables,
              joursSemaine
            );
          trafics[manuels[row.co_produit]].traficMoyenJourna = this.traficService.calculateMoyenneJournaliere(
              trafics[manuels[row.co_produit]].volumeBrut,
              0,
              nbJoursOuvrables
            );
          trafics[manuels[row.co_produit]].manuelCalc = true;
        }
      } else {
        trafics[index].volumeBrut = Number(row?.volume_realise) + Number(row?.volume_previsionnel);
      }
    });

    return Object.values(trafics);
  }

  /**
   * Upsert TMH rows for a scenario (batch)
   * PUT /scenarios/{id_scenario}/tmh
   */
  upsertTmh(idScenario: number, idRh: string, payload: TmhInput[]): Observable<TmhBatchResult> {
    return this.http.put<TmhBatchResult>(`${this.baseUrl}/${idScenario}/tmh`, { 'tmh': payload, 'id_rh': idRh });
  }

  /**
   * Update one TMH product volume
   * PATCH /scenarios/{id_scenario}/tmh/{co_produit}
   */
  updateTmhVolume(idScenario: number, idTmh: number, payload: TmhVolumeUpdate): Observable<TmhOut> {
    return this.http.patch<TmhOut>(`${this.baseUrl}/${idScenario}/tmh/${idTmh}`, payload);
  }

  /**
   * Delete one TMH product volume
   * DELETE /scenarios/{id_scenario}/tmh/{id_tmh}
   */
  deleteTmhVolume(idScenario: number, idTmh: number): Observable<void> {
    return this.http.delete<void>(`${this.baseUrl}/${idScenario}/tmh/${idTmh}`);
  }

  /**
   * Switch de l'exclusion d'un produit TMH (exclusion) du calcul du scénario.
   * PATCH /scenarios/{id_scenario}/tmh/{co_produit}/exclusion
   */
  updateTmhExclusion(idScenario: number, idTmh: number, payload: TmhExclusionUpdate): Observable<TmhOut> {
    return this.http.patch<TmhOut>(`${this.baseUrl}/${idScenario}/tmh/${idTmh}/exclusion`, payload);
  }

  /**
   * List manual counts for a scenario
   * GET /scenarios/{id_scenario}/comptages
   */
  listComptages(idScenario: number, idSessionIhm?: string | null): Observable<Comptage[]> {
    let params = new HttpParams();

    if (idSessionIhm !== undefined && idSessionIhm !== null) {
      params = params.set('id_session_ihm', idSessionIhm);
    }

    return this.http.get<Comptage[]>(`${this.baseUrl}/${idScenario}/comptages`, { params });
  }

  /**
   * Add manual count for one product
   * POST /scenarios/{id_scenario}/comptages
   */
  addComptage(idScenario: number, payload: ComptageCreate): Observable<Comptage> {
    return this.http.post<Comptage>(`${this.baseUrl}/${idScenario}/comptages`, payload);
  }

  /**
   * Update manual count for one product
   * PUT /scenarios/{id_scenario}/comptages/{co_produit}
   */
  updateComptage(idScenario: number, coProduit: string, payload: ComptageUpdate): Observable<Comptage> {
    const encodedCoProduit = encodeURIComponent(coProduit);
    return this.http.put<Comptage>(`${this.baseUrl}/${idScenario}/comptages/${encodedCoProduit}`, payload);
  }

  /**
   * Delete manual count for one product
   * DELETE /scenarios/{id_scenario}/comptages/{co_produit}
   */
  deleteComptage(idScenario: number, coProduit: string): Observable<void> {
    const encodedCoProduit = encodeURIComponent(coProduit);
    return this.http.delete<void>(`${this.baseUrl}/${idScenario}/comptages/${encodedCoProduit}`);
  }

  /**
   * List forecast variations for a scenario
   * GET /scenarios/{id_scenario}/variations
   */
  listVariations(idScenario: number, idSessionIhm?: string | null): Observable<VariationOut[]> {
    let params = new HttpParams();

    if (idSessionIhm !== undefined && idSessionIhm !== null) {
      params = params.set('id_session_ihm', idSessionIhm);
    }

    return this.http.get<VariationOut[]>(`${this.baseUrl}/${idScenario}/variations`, { params });
  }

  /**
   * Upsert forecast variation for one product
   * PUT /scenarios/{id_scenario}/variations/{co_produit}
   */
  upsertVariation(idScenario: number, coProduit: string, payload: VariationUpsert): Observable<VariationUpsertResult> {
    const encodedCoProduit = encodeURIComponent(coProduit);
    return this.http.put<VariationUpsertResult>(`${this.baseUrl}/${idScenario}/variations/${encodedCoProduit}`, payload);
  }

  /**
   * Delete forecast variation for one product
   * DELETE /scenarios/{id_scenario}/variations/{co_produit}
   */
  deleteVariation(idScenario: number, coProduit: string): Observable<void> {
    const encodedCoProduit = encodeURIComponent(coProduit);
    return this.http.delete<void>(`${this.baseUrl}/${idScenario}/variations/${encodedCoProduit}`);
  }
}
