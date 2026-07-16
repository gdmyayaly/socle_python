import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, of } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { Comptage } from '../models/comptage.model';
import { environment } from '../../../../environments/environment';
import { Produit } from '../models/produit.model';

@Injectable({ providedIn: 'root' })
export class ComptageService {
  readonly apiTrppuUrl = environment.trppuApiUrl;
  private readonly baseUrl = `${this.apiTrppuUrl}`;

  constructor(private http: HttpClient) {}

  listProduit(): Observable<Produit[]> {
    return this.http.get<Produit[]>(`${this.baseUrl}/produits`);
  }
}
