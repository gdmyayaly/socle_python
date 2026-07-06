import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { delay } from 'rxjs/operators';
import {
  PicCoefficient,
  PicCoefUpdate,
  JourSemaine,
  Densite,
  ID_PIC_VERSION_DEFAUT,
} from '../models/pic-coefficient.model';

/**
 * Service YS04 - paramétrage de rétention en PIC.
 *  - DSR-660 : récupération des coefficients (defaut + spécifiques scénario).
 *  - DSR-661 : enregistrement d'un coefficient modifié.
 *
 * ⚠️ IMPLÉMENTATION SIMULÉE : les données sont en dur et servies via des
 * Observable (delay pour reproduire la latence réseau). Le jour du branchement
 * réel, remplacer le corps des méthodes par des appels HttpClient vers YS04,
 * la signature publique reste identique.
 */
@Injectable({ providedIn: 'root' })
export class PicCoefficientService {

  /** Version PIC attribuée à un coefficient modifié dans un scénario (!= 1). */
  private static readonly ID_PIC_VERSION_SCENARIO = 2;

  /** Clé de persistance (simulation du back) des overrides propres à un scénario. */
  private static readonly KEY_OVERRIDES = 'trppu.pic_overrides';

  private static readonly JOURS: JourSemaine[] = ['LUN', 'MAR', 'MER', 'JEU', 'VEN', 'SAM'];

  /**
   * DSR-660 : liste des coefficients de rétention PIC.
   * @param idScenario  id du scénario en cours (null => valeurs par défaut seules).
   */
  getCoefficients(idScenario: number | null): Observable<PicCoefficient[]> {
    const defauts = this.buildDefauts();

    if (idScenario == null) {
      return of(defauts).pipe(delay(200));
    }

    // Fusion défauts + overrides du scénario (id_pic_version != 1).
    const overrides = this.loadOverrides(idScenario);
    const fusion = defauts.map((c) => {
      const key = this.cellKey(c.co_produit, c.jour_semaine, c.densite);
      const ov = overrides[key];
      return ov != null
        ? { ...c, coef: ov, id_pic_version: PicCoefficientService.ID_PIC_VERSION_SCENARIO }
        : c;
    });
    return of(fusion).pipe(delay(200));
  }

  /**
   * DSR-661 : enregistre un coefficient modifié pour le scénario en cours.
   * Reçoit id_scenario, co_produit, jour_semaine, coef, densite.
   */
  updateCoefficient(payload: PicCoefUpdate): Observable<void> {
    // Simulation de la persistance back : stockage de l'override.
    const overrides = this.loadOverrides(payload.id_scenario);
    overrides[this.cellKey(payload.co_produit, payload.jour_semaine, payload.densite)] = payload.coef;
    this.saveOverrides(payload.id_scenario, overrides);

    // delay => la cellule reste "en cours" (rouge) le temps de l'aller-retour.
    return of(void 0).pipe(delay(500));
  }

  // ---------------------------------------------------------------------------
  // Simulation des overrides (persistés en localStorage pour survivre au reload)
  // ---------------------------------------------------------------------------
  private loadOverrides(idScenario: number): Record<string, number> {
    const raw = localStorage.getItem(this.overridesKey(idScenario));
    if (!raw) {
      return {};
    }
    try {
      return JSON.parse(raw) as Record<string, number>;
    } catch {
      return {};
    }
  }

  private saveOverrides(idScenario: number, overrides: Record<string, number>): void {
    localStorage.setItem(this.overridesKey(idScenario), JSON.stringify(overrides));
  }

  private overridesKey(idScenario: number): string {
    return `${PicCoefficientService.KEY_OVERRIDES}.${idScenario}`;
  }

  private cellKey(coProduit: string, jour: JourSemaine, densite: Densite): string {
    return `${coProduit}|${jour}|${densite}`;
  }

  // ---------------------------------------------------------------------------
  // Jeu de valeurs par défaut en dur (id_pic_version = 1) - cf. maquette "AB".
  // Ordre par jour : [dense, faible1, faible2] ; samedi : [dense] seul.
  // null = cellule vide (pas de densité clairsemée2 le lundi/mardi).
  // ---------------------------------------------------------------------------
  private buildDefauts(): PicCoefficient[] {
    const grille: Record<string, (number | null)[][]> = {
      OO: [[21.7, 1.3, null], [19.8, 0.7, null], [16.1, 0.7, 0.0], [17.9, 0.7, 0.0], [18.1, 0.7, 0.0], [2.4]],
      OS: [[14.4, 2.7, null], [10.5, 1.9, null], [13.7, 2.5, 0.0], [14.2, 2.6, 0.0], [14.5, 2.7, 0.0], [20.1]],
      PR: [[14.4, 5.0, null], [12.9, 5.3, null], [12.2, 5.4, 0.0], [12.9, 5.3, 0.0], [13.6, 5.1, 0.0], [7.9]],
      CO: [[7.3, 7.3, null], [5.7, 5.7, null], [9.7, 9.7, 0.0], [9.7, 9.7, 0.0], [9.3, 9.3, 0.0], [16.7]],
      PP: [[14.8, 1.9, null], [14.8, 1.9, null], [14.8, 1.9, 0.0], [14.8, 1.9, 0.0], [14.8, 1.9, 0.0], [16.5]],
      IP: [[25.0, 0.0, null], [25.0, 0.0, null], [25.0, 0.0, 0.0], [25.0, 0.0, 0.0], [0.0, 0.0, 0.0], [0.0]],
    };

    const coeffs: PicCoefficient[] = [];
    for (const coProduit of Object.keys(grille)) {
      grille[coProduit].forEach((valeursJour, indexJour) => {
        const jour = PicCoefficientService.JOURS[indexJour];
        valeursJour.forEach((coef, densite) => {
          coeffs.push({
            co_produit: coProduit,
            jour_semaine: jour,
            densite: densite as Densite,
            coef,
            id_pic_version: ID_PIC_VERSION_DEFAUT,
          });
        });
      });
    }
    return coeffs;
  }
}
