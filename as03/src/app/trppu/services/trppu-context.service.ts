import { Injectable } from '@angular/core';

/**
 * Contexte courant de l'utilisateur (scénario "en cours" + site sélectionné),
 * persisté dans le localStorage et partagé entre les onglets de l'IHM TRPPU.
 *
 * - `id_scenario` absent  => aucun scénario en cours (mode lecture seule).
 * - `co_regate`   absent  => aucun site sélectionné.
 */
@Injectable({ providedIn: 'root' })
export class TrppuContextService {

  private static readonly KEY_ID_SCENARIO = 'trppu.id_scenario';
  private static readonly KEY_CO_REGATE = 'trppu.co_regate';

  /** Id du scénario en cours, ou null si aucun. */
  getIdScenario(): number | null {
    const raw = localStorage.getItem(TrppuContextService.KEY_ID_SCENARIO);
    if (!raw || raw.trim() === '') {
      return null;
    }
    const id = Number(raw);
    return Number.isFinite(id) ? id : null;
  }

  /** Code Regate du site sélectionné, ou null si aucun. */
  getCoRegate(): string | null {
    const raw = localStorage.getItem(TrppuContextService.KEY_CO_REGATE);
    return raw && raw.trim() !== '' ? raw : null;
  }

  /** Positionne le scénario en cours (à appeler quand l'utilisateur ouvre un scénario). */
  setIdScenario(id: number | null): void {
    if (id == null) {
      localStorage.removeItem(TrppuContextService.KEY_ID_SCENARIO);
    } else {
      localStorage.setItem(TrppuContextService.KEY_ID_SCENARIO, String(id));
    }
  }

  /** Positionne le site sélectionné. */
  setCoRegate(coRegate: string | null): void {
    if (!coRegate) {
      localStorage.removeItem(TrppuContextService.KEY_CO_REGATE);
    } else {
      localStorage.setItem(TrppuContextService.KEY_CO_REGATE, coRegate);
    }
  }

  /** Réinitialise le contexte (déconnexion / changement de site). */
  clear(): void {
    localStorage.removeItem(TrppuContextService.KEY_ID_SCENARIO);
    localStorage.removeItem(TrppuContextService.KEY_CO_REGATE);
  }
}
