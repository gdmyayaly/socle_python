import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, of } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { PeriodeNeutralisee } from '../models/periode-neutralisee.model';
import { VariationTrafic } from '../models/variation-trafic.model';
import { ProduitExclu } from '../models/produit-exclu.model';

@Injectable({ providedIn: 'root' })
export class ParamService {

  constructor(private http: HttpClient) {}

  getPeriodesNeutralisees(siteId: string): Observable<PeriodeNeutralisee[]> {
    return this.http.get<PeriodeNeutralisee[]>(`assets/trppu/periodes-neutralisees-${siteId}.json`)
      .pipe(catchError(() => of([])));
  }

  getVariationsTrafic(siteId: string): Observable<VariationTrafic[]> {
    return this.http.get<VariationTrafic[]>(`assets/trppu/variations-trafic-${siteId}.json`)
      .pipe(catchError(() => of([])));
  }

  getNeutralisationsPeak(siteId: string): Observable<PeriodeNeutralisee[]> {
    return this.http.get<PeriodeNeutralisee[]>(`assets/trppu/neutralisations-peak-${siteId}.json`)
      .pipe(catchError(() => of([])));
  }

  getNeutralisationsSecondaires(siteId: string): Observable<PeriodeNeutralisee[]> {
    return this.http.get<PeriodeNeutralisee[]>(`assets/trppu/neutralisations-secondaires-${siteId}.json`)
      .pipe(catchError(() => of([])));
  }

  getProduitsAExclure(siteId: string): Observable<ProduitExclu[]> {
    return this.http.get<ProduitExclu[]>(`assets/trppu/produits-a-exclure-${siteId}.json`)
      .pipe(catchError(() => of([])));
  }
}
