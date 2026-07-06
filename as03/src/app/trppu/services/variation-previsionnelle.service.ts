import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { VariationPrevisionnelle } from '../models/variation-previsionnelle.model';

/**
 * Base d'API du micro-service TRPPU.
 * À externaliser dans `environment.ts` (ex. `environment.trppuApiUrl`) le jour du branchement réel.
 */
const API_BASE = '/trppu-api';

/** Corps du PUT (DSR-646). `variation_pct = 0` => suppression côté back. */
export interface VariationUpsert {
  variation_pct: number;
  /** Identifiant RH (chiffré côté back, traçabilité). */
  id_rh: string;
}

/** Réponse du PUT (DSR-646). */
export interface VariationUpsertResult {
  co_produit: string;
  variation_pct: number | null;
  action: 'created' | 'updated' | 'deleted' | 'noop';
}

/**
 * Accès aux variations prévisionnelles d'un scénario.
 * Endpoints back : `/trppu-api/scenarios/{id_scenario}/variations`.
 * (Pas de suppression exposée ici : le PUT à 0 % supprime déjà côté back.)
 */
@Injectable({ providedIn: 'root' })
export class VariationPrevisionnelleService {

  constructor(private http: HttpClient) {}

  /**
   * DSR-651 : liste des variations d'un scénario.
   * Les produits à 0 % ne sont PAS renvoyés (défaut non stocké).
   */
  list(idScenario: number): Observable<VariationPrevisionnelle[]> {
    return this.http.get<VariationPrevisionnelle[]>(
      `${API_BASE}/scenarios/${idScenario}/variations`
    );
  }

  /**
   * DSR-646 : ajoute/modifie la variation d'un produit.
   * `variation_pct = 0` supprime la ligne côté back (action = "deleted"/"noop").
   */
  upsert(
    idScenario: number,
    coProduit: string,
    body: VariationUpsert
  ): Observable<VariationUpsertResult> {
    return this.http.put<VariationUpsertResult>(
      `${API_BASE}/scenarios/${idScenario}/variations/${coProduit}`,
      body
    );
  }
}
