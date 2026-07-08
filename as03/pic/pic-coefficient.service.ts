import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable, of } from 'rxjs';
import { map } from 'rxjs/operators';
import {
  PicCoefficient,
  PicCoefUpdate,
  PicCoefUpsertResult,
  JourSemaineApi,
  Densite,
  JOUR_TO_API,
  JOUR_FROM_API,
} from '../models/pic-coefficient.model';
import { TrppuContextService } from './trppu-context.service';

/**
 * Base d'API du micro-service TRPPU (YS04).
 * À externaliser dans `environment.ts` le jour du branchement réel.
 */
const API_BASE = '/trppu-api';

/** Identifiant RH de l'utilisateur — placeholder en attendant l'auth front. */
const ID_RH = 'FRONT_TODO';

/** Coefficient tel que renvoyé par YS04 (jours en toutes lettres, flag `modifie`). */
interface ApiPicCoef {
  id_pic_version: number;
  co_produit: string;
  jour_semaine: JourSemaineApi;
  densite: Densite;
  coef: number | null;
  modifie: boolean;
}

/** Réponse de lecture YS04 (DSR-660). */
interface ApiPicScenarioOut {
  id_pic_version_defaut: number | null;
  id_pic_version_scenario: number | null;
  niveau_scenario: string | null;
  coefficients: ApiPicCoef[];
}

/**
 * Service YS04 - paramétrage de rétention en PIC.
 *  - DSR-660 : récupération des coefficients (défaut national + surcharge scénario).
 *  - DSR-661 : enregistrement d'un coefficient modifié.
 *
 * Les appels transmettent `id_session_ihm` (traçabilité, cf. NB des tickets) et,
 * en écriture, `id_rh` (chiffré côté back).
 */
@Injectable({ providedIn: 'root' })
export class PicCoefficientService {

  constructor(
    private http: HttpClient,
    private context: TrppuContextService,
  ) {}

  /**
   * DSR-660 : liste des coefficients de rétention PIC d'un scénario.
   * @param idScenario  id du scénario en cours. `null` => aucune lecture (le service
   *   back exige un id de scénario) : retourne une liste vide.
   */
  getCoefficients(idScenario: number | null): Observable<PicCoefficient[]> {
    if (idScenario == null) {
      return of([]);
    }
    const params = new HttpParams().set('id_session_ihm', this.context.getOrCreateIdSession());
    return this.http
      .get<ApiPicScenarioOut>(`${API_BASE}/scenarios/${idScenario}/pic-coefficients`, { params })
      .pipe(map((resp) => resp.coefficients.map((c) => this.fromApi(c))));
  }

  /**
   * DSR-661 : enregistre un coefficient modifié pour le scénario en cours.
   * Reçoit id_scenario, co_produit, jour_semaine, coef, densite ; le service ajoute
   * `id_rh` et `id_session_ihm`. Retourne l'action réalisée et l'id_pic_version utilisé.
   */
  updateCoefficient(payload: PicCoefUpdate): Observable<PicCoefUpsertResult> {
    const params = new HttpParams().set('id_session_ihm', this.context.getOrCreateIdSession());
    const body = {
      co_produit: payload.co_produit,
      jour_semaine: JOUR_TO_API[payload.jour_semaine],
      densite: payload.densite,
      coef: payload.coef,
      id_rh: ID_RH,
    };
    return this.http.put<PicCoefUpsertResult>(
      `${API_BASE}/scenarios/${payload.id_scenario}/pic-coefficients`,
      body,
      { params },
    );
  }

  /** Mappe un coefficient back (jour en toutes lettres) vers le modèle IHM. */
  private fromApi(c: ApiPicCoef): PicCoefficient {
    return {
      co_produit: c.co_produit,
      jour_semaine: JOUR_FROM_API[c.jour_semaine],
      densite: c.densite,
      coef: c.coef,
      id_pic_version: c.id_pic_version,
      modifie: c.modifie,
    };
  }
}
