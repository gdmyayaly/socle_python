import { Injectable } from '@angular/core';

/**
 * Contexte courant de l'utilisateur (scénario "en cours" + site sélectionné,
 * + identifiant de session), persisté dans le localStorage et partagé entre
 * les onglets de l'IHM TRPPU.
 *
 * - `id_scenario` absent  => aucun scénario en cours (mode lecture seule).
 * - `co_regate`   absent  => aucun site sélectionné.
 * - `id_session`  absent  => aucune session active (utilisateur non connecté).
 */
@Injectable({ providedIn: 'root' })
export class TrppuContextService {

  private static readonly KEY_ID_SCENARIO = 'trppu.id_scenario';
  private static readonly KEY_CO_REGATE = 'trppu.co_regate';
  private static readonly KEY_ID_SESSION = 'trppu.id_session';

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

  /**
   * Identifiant de la session courante, ou null si aucune session active.
   *
   * Cet identifiant est utilisé pour tracer l'activité de l'utilisateur et
   * accompagner les soumissions de données effectuées durant la session.
   */
  getIdSession(): string | null {
    const raw = localStorage.getItem(TrppuContextService.KEY_ID_SESSION);
    return raw && raw.trim() !== '' ? raw : null;
  }

  /**
   * Génère un nouvel identifiant de session, le persiste et le retourne.
   * À appeler après chaque connexion de l'utilisateur.
   */
  generateIdSession(): string {
    const id = TrppuContextService.newSessionId();
    localStorage.setItem(TrppuContextService.KEY_ID_SESSION, id);
    return id;
  }

  /**
   * Retourne l'identifiant de la session courante s'il existe, sinon en génère
   * un nouveau. Pratique au moment d'une soumission pour garantir la présence
   * d'un identifiant de traçage.
   */
  getOrCreateIdSession(): string {
    return this.getIdSession() ?? this.generateIdSession();
  }

  /** Génère un identifiant de session unique (UUID v4). */
  private static newSessionId(): string {
    const cryptoObj = typeof crypto !== 'undefined' ? crypto : undefined;
    if (cryptoObj?.randomUUID) {
      return cryptoObj.randomUUID();
    }
    // Repli pour les environnements sans crypto.randomUUID.
    if (cryptoObj?.getRandomValues) {
      const bytes = cryptoObj.getRandomValues(new Uint8Array(16));
      bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
      bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant
      const hex = Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('');
      return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
    }
    return `${Date.now().toString(16)}-${Math.random().toString(16).slice(2)}`;
  }

  /** Réinitialise le contexte (déconnexion / changement de site). */
  clear(): void {
    localStorage.removeItem(TrppuContextService.KEY_ID_SCENARIO);
    localStorage.removeItem(TrppuContextService.KEY_CO_REGATE);
    localStorage.removeItem(TrppuContextService.KEY_ID_SESSION);
  }
}
