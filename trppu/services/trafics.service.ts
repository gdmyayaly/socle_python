import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { environment } from '../../../../environments/environment';
import { Produit } from '../models/produit.model';
import { Tmh } from '../models/tmh.model';
import { TmhInput } from '../models/tmh-input.model';
import { TraficCalcule } from '../models/trafic-calcule.model';

export interface GetTraficsParams {
  coRegate: string;
  dateDebut: string;
  dateFin: string;
  limit?: number | null;
}

export interface GetTraficsPivotParams {
  coRegate: string;
  dateDebut: string;
  dateFin: string;
  datePivot: string;
  joursSemaine: number;
  currentTmh: TraficCalcule[];
  limit?: number | null;
}

export interface UpdateTraficExclusionParams {
  scenarioId: number;
  traficId: number;
  excluded: boolean;
}

@Injectable({ providedIn: 'root' })
export class TraficService {
  readonly apiTrppuUrl = environment.trppuApiUrl;
  private readonly baseUrl = `${this.apiTrppuUrl}/trafics`;

  readonly validProduit = ["CO","IP","OO","OS","PPI","PR","TT"];

  constructor(private http: HttpClient) {}

  getTrafics(params: GetTraficsParams): Observable<TraficCalcule[]> {
    const dateDebut = this.toApiDate(params.dateDebut);
    const dateFin = this.toApiDate(params.dateFin);

    let queryParams = new HttpParams()
      .set('co_regate', params.coRegate) // site de test 570800
      .set('date_debut', dateDebut)
      .set('date_fin', dateFin);

    if (params.limit !== undefined && params.limit !== null) {
      queryParams = queryParams.set('limit', String(params.limit));
    }

    return this.http.get<any>(`${this.baseUrl}/get_trafics`, { params: queryParams }).pipe(
      map((response) => this.normalizeTraficsResponse(response))
    );
  }

  getTraficsPivot(params: GetTraficsPivotParams): Observable<{ tmh: TmhInput[]; nbJoursOuvrables: number }> {
    const dateDebut = this.toApiDate(params.dateDebut);
    const dateFin   = this.toApiDate(params.dateFin);
    const datePivot = this.toApiDate(params.datePivot);

    let queryParams = new HttpParams()
      .set('co_regate', params.coRegate) // site de test 570800
      .set('date_debut', dateDebut)
      .set('date_fin',   dateFin)
      .set('date_pivot', datePivot);

    if (params.limit !== undefined && params.limit !== null) {
      queryParams = queryParams.set('limit', String(params.limit));
    }

    return this.http.get<any>(`${this.baseUrl}/get_trafics_pivot`, { params: queryParams }).pipe(
      map((response) => this.normalizeTraficsPivotResponse(response, params.currentTmh, params.joursSemaine))
    );
  }

  private normalizeTraficsPivotResponse(response: any, currentTmh: TraficCalcule[], joursSemaine: number):
    { tmh: TmhInput[]; nbJoursOuvrables: number } {
    const rows = Array.isArray(response?.trafics) ? response?.trafics : [];
    const nbJoursOuvrables = response?.nb_jours?.nbJoursOuvrables ? response?.nb_jours?.nbJoursOuvrables : 0;

    const trafics = [];

    rows.forEach((row: any, index: number) => {
      const produit = row.co_produit; //.length > 2 ? row.co_produit.substring(0,2) : row.co_produit;
      const key = produit;

      if (!trafics[key] && this.validProduit.includes(key)) {
        trafics[key] = {
          co_produit: key,
          volume_realise: row?.trafic_brut ?? 0,
          volume_previsionnel: row?.trafic_previsionnel ?? 0,
          moyenne_journaliere: '0',
          moyenne_hebdo: '0',
          exclusion: false
        };

        const moyenneHebdo = this.calculateMoyenneHebdo(
          trafics[key].volume_realise,
          trafics[key].volume_previsionnel,
          nbJoursOuvrables,
          joursSemaine
        );

        const moyenneJournaliere = this.calculateMoyenneJournaliere(
          trafics[key].volume_realise,
          trafics[key].volume_previsionnel,
          nbJoursOuvrables
        );

        trafics[key].moyenne_hebdo       = moyenneHebdo.toString();
        trafics[key].moyenne_journaliere = moyenneJournaliere.toString();

        if (currentTmh && currentTmh.length > 0) {
          const oldId = currentTmh.find(t => t.produit === key && t.manuel === false);
          if (oldId) {
            trafics[key].id_tmh = oldId.id;
          }
        }
      }
    });

    return { tmh: Object.values(trafics), nbJoursOuvrables };
  }

  public calculateMoyenneJournaliere(volumeRealise: number, volumePrevisionnel: number, nbJoursOuvrables: number): number {
    return nbJoursOuvrables > 0 ? Math.round((volumeRealise + volumePrevisionnel) / nbJoursOuvrables) : 0;
  }

  public calculateMoyenneHebdo(volumeRealise: number, volumePrevisionnel: number, nbJoursOuvrables: number, joursSemaine: number): number {
    return nbJoursOuvrables > 0 ? Math.round(joursSemaine * (volumeRealise + volumePrevisionnel) / nbJoursOuvrables) : 0;
  }

  private normalizeTraficsResponse(response: any): TraficCalcule[] {
    const rows = Array.isArray(response?.data) ? response?.data : [];

    const groupedByProduit = [];

    rows.forEach((row: any, index: number) => {
      const produit = this.extractProduitLabel(row);
      const key = this.normalizeProduitKey(produit);

      if (!groupedByProduit[key]) {
        groupedByProduit[key] = {
          id: key,
          produit,
          exclure: !!(row?.excluded ?? row?.exclure),
          constateBrut: 0,
          previsionnelBrut: 0,
          comptageManuel: 0,
          volumeBrut: 0,       // computed client-side
          traficMoyenHebdo: 0  // computed client-side
        };
      }

      groupedByProduit[key].constateBrut += Number(row?.trafic_constate ?? 0);
      groupedByProduit[key].previsionnelBrut += Number(row?.trafic_prevu ?? 0);
      groupedByProduit[key].comptageManuel += Number(row?.trafic_comptage_manuel ?? row?.comptage_manuel ?? 0);
      groupedByProduit[key].exclure = !!(groupedByProduit[key].exclure || row?.excluded || row?.exclure);
    });

    return Object.values(groupedByProduit);
  }

  private extractProduitLabel(row: any): string {
    return String(row?.lb_type_objet ?? row?.produit ?? row?.libelle_produit ?? '').trim();
  }

  private normalizeProduitKey(produit: string): string {
    if (!produit) {
      return '__empty__';
    }

    // Normalize accents, separators and punctuation so equivalent labels are grouped together.
    return produit
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-zA-Z0-9]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim()
      .toLowerCase();
  }

  private toApiDate(value: string): string {
    return value.replace(/-/g, '').substring(0, 8);
  }
}
