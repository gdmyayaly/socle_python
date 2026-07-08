import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { Produit } from '../models/produit.model';

/**
 * Base d'API du micro-service TRPPU (YS04).
 * À externaliser dans `environment.ts` le jour du branchement réel.
 */
const API_BASE = '/trppu-api';

/**
 * Accès à la liste des produits gérés (table trppu_produit).
 * Endpoint back : `GET /trppu-api/produits`.
 */
@Injectable({ providedIn: 'root' })
export class ProduitService {

  constructor(private http: HttpClient) {}

  /**
   * Liste des produits, triée par `co_produit` côté back.
   * @param actifOnly  n'inclut pas les produits désactivés (défaut : true).
   */
  list(actifOnly = true): Observable<Produit[]> {
    const params = new HttpParams()
      .set('actif_only', actifOnly)
      .set('limit', 1000);
    return this.http.get<Produit[]>(`${API_BASE}/produits`, { params });
  }
}
