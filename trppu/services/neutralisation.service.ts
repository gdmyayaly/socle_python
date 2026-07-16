import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { Neutralisation } from '../models/neutralisation.model';
import { environment } from '../../../../environments/environment';

/** Corps du POST (DSR-645). Le back calcule et stocke `nb_jour`. */
export interface NeutralisationCreate {
  dt_debut: string;
  dt_fin: string;
  motif: string;
  id_rh: string;
}

/**
 * Accès API des périodes neutralisées d'un scénario (DSR-645 / DSR-652).
 * Les erreurs HTTP remontent brutes : les appelants gèrent les statuts
 * métier (409 = période déjà neutralisée, 422 = aucun jour ouvré).
 */
@Injectable({ providedIn: 'root' })
export class NeutralisationService {
  private readonly baseUrl = `${environment.trppuApiUrl}/scenarios`;

  constructor(private http: HttpClient) {}

  /** GET /scenarios/{id}/neutralisations */
  list(idScenario: number): Observable<Neutralisation[]> {
    return this.http.get<Neutralisation[]>(
      `${this.baseUrl}/${idScenario}/neutralisations`
    );
  }

  /** POST /scenarios/{id}/neutralisations */
  create(idScenario: number, payload: NeutralisationCreate): Observable<Neutralisation> {
    return this.http.post<Neutralisation>(
      `${this.baseUrl}/${idScenario}/neutralisations`,
      payload
    );
  }

  /** DELETE /scenarios/{id}/neutralisations/{id_neutralisation} */
  delete(idScenario: number, idNeutralisation: number): Observable<void> {
    return this.http.delete<void>(
      `${this.baseUrl}/${idScenario}/neutralisations/${idNeutralisation}`
    );
  }
}
