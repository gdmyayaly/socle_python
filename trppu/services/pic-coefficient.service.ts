import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { environment } from '../../../../environments/environment';
import {
 PicCoefficient,
 PicCoefUpdate,
 PicCoefUpsertResult,
 PicScenarioOut,
 PicScenarioView,
 ID_PIC_VERSION_DEFAUT,
 JOUR_FRONT_TO_API,
 JOUR_API_TO_FRONT,
} from '../models/pic-coefficient.model';
import { TrppuContextService } from './trppu-context.service';



/**
* Paramétrage de rétention en PIC (YS04 - DSR-660 / DSR-661).
* Lecture fusionnée défaut national + surcharges scénario, et enregistrement
* d'un coefficient modifié.
*/
@Injectable({ providedIn: 'root' })
export class PicCoefficientService {

 readonly apiTrppuUrl = environment.trppuApiUrl;
 private readonly baseUrl = `${this.apiTrppuUrl}/scenarios`;

 constructor(
  private http: HttpClient,
  private context: TrppuContextService,
 ) {}

 /**
 * DSR-660 : paramétrage PIC du scénario (défaut national surchargé par le
 * scénario), converti au format IHM (jours LUN..SAM, coef numérique).
 */
 getPicScenario(idScenario: number): Observable<PicScenarioView> {
  return this.http.get<PicScenarioOut>(
   `${this.baseUrl}/${idScenario}/pic-coefficients`,
   { params: this.sessionParams() },
  ).pipe(map((out) => this.toView(out)));
 }

 /** DSR-660 : uniquement la liste aplatie des coefficients (une cellule chacun). */
 getCoefficients(idScenario: number): Observable<PicCoefficient[]> {
  return this.getPicScenario(idScenario).pipe(map((v) => v.coefficients));
 }

 /**
 * DSR-661 : enregistre un coefficient modifié pour le scénario en cours.
 * `id_scenario` est passé dans l'URL (le backend rejette tout champ en trop
 * dans le body). Retourne l'action réalisée et l'id_pic_version à reporter
 * sur la cellule.
 */
 updateCoefficient(idScenario: number, payload: PicCoefUpdate): Observable<PicCoefUpsertResult> {
  const body = {
   co_produit: payload.co_produit,
   jour_semaine: JOUR_FRONT_TO_API[payload.jour_semaine],
   densite: payload.densite,
   coef: payload.coef,
   id_rh: payload.id_rh,
  };
  return this.http.put<PicCoefUpsertResult>(
   `${this.baseUrl}/${idScenario}/pic-coefficients`,
   body,
   { params: this.sessionParams() },
  );
 }

 private toView(out: PicScenarioOut): PicScenarioView {
  return {
   idPicVersionDefaut: out.id_pic_version_defaut ?? ID_PIC_VERSION_DEFAUT,
   idPicVersionScenario: out.id_pic_version_scenario,
   niveauScenario: out.niveau_scenario,
   coefficients: out.coefficients.map((c) => ({
    co_produit: c.co_produit,
    jour_semaine: JOUR_API_TO_FRONT[c.jour_semaine],
    densite: c.densite,
    coef: c.coef == null ? null : Number(c.coef),
    id_pic_version: c.id_pic_version,
   })),
  };
 }

 /** Paramètre de traçabilité id_session_ihm exigé par YS04 pour les logs. */
 private sessionParams(): HttpParams {
  return new HttpParams().set('id_session_ihm', this.context.getOrCreateIdSession());
 }
}
