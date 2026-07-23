import { Injectable } from '@angular/core';
import { forkJoin, of, Observable, Subject } from 'rxjs';
import { map, switchMap, debounceTime, concatMap, catchError, retry, tap } from 'rxjs/operators';

import { Scenario } from '../models/scenario.model';
import { Tmh } from '../models/tmh.model';
import { TmhInput } from '../models/tmh-input.model';
import { ScenarioService } from './scenario.service';
import { NeutralisationService } from './neutralisation.service';
import { TraficService } from './trafics.service';

/**
 * Ajustements applicables au calcul des moyennes TMH d'un scénario :
 * jours neutralisés déduits des jours ouvrables + variations prévisionnelles %.
 */
export interface AjustementsScenario {
  scenario: Scenario;
  /** Nombre de jours par semaine retenu (override appelant ?? scénario ?? 6). */
  joursSemaine: number;
  /** Base de jours avant déduction : nb_jours_ouvres (5 j) ou nb_jours_ouvrables (6 j) — règle DSR-656. */
  nbJoursBase: number;
  /** Somme des nb_jour des périodes neutralisées du scénario. */
  nbJoursNeutralises: number;
  /** max(nbJoursBase − nbJoursNeutralises, 0) : dénominateur effectif des moyennes. */
  joursOuvrablesEffectifs: number;
  /** variation_pct par co_produit (absent => 0). */
  variations: Map<string, number>;
  estFige: boolean;
}

/** Demande de recalcul poussée dans le pipeline interne. */
interface DemandeRecalcul {
  idScenario: number;
  idRh: string;
}

/** Délai de coalescence des demandes de recalcul successives (ms). */
const RECALCUL_DEBOUNCE_MS = 500;
/** Nombre de relances d'un recalcul en échec. */
const RECALCUL_RETRIES = 1;
/** Délai entre deux relances (ms). */
const RECALCUL_RETRY_DELAY_MS = 2_000;

/**
 * Recalcul et persistance des moyennes TMH d'un scénario en appliquant :
 * - la déduction des jours neutralisés du nombre de jours ouvrables,
 * - la variation prévisionnelle % sur le volume prévisionnel des lignes auto.
 * Le volume prévisionnel de BASE (volume_previsionnel) reste brut, jamais
 * modifié : le prévisionnel ajusté est persisté dans
 * volume_previsionnel_recalcule et les moyennes intègrent les ajustements,
 * ce qui rend le recalcul ré-exécutable sans cumul.
 */
@Injectable({ providedIn: 'root' })
export class TmhRecalculService {

  /** Erreurs de recalcul (après relances), pour affichage par les composants. */
  readonly recalculErrors$ = new Subject<any>();

  /** Émet l'idScenario après chaque recalcul persisté : permet aux vues
   *  (ex. trafics calculés) de se rafraîchir dynamiquement. */
  readonly recalculTermine$ = new Subject<number>();

  private readonly demandes$ = new Subject<DemandeRecalcul>();

  constructor(
    private scenarioService: ScenarioService,
    private neutralisationService: NeutralisationService,
    private traficService: TraficService,
  ) {
    // Coalescence des rafales (sliders, ajouts successifs) puis exécution
    // sérialisée : chaque recalcul relit l'état frais, le dernier gagne.
    // Le catchError DANS le concatMap est indispensable : sans lui, une
    // erreur terminerait le pipeline pour toute la session.
    this.demandes$
      .pipe(
        debounceTime(RECALCUL_DEBOUNCE_MS),
        concatMap(demande =>
          this.recalculerEtPersister(demande.idScenario, demande.idRh).pipe(
            tap(() => this.recalculTermine$.next(demande.idScenario)),
            catchError(err => {
              this.recalculErrors$.next(err);
              return of(null);
            })
          )
        )
      )
      .subscribe();
  }

  /** Demande un recalcul (coalescé puis sérialisé par le pipeline interne). */
  demanderRecalcul(idScenario: number, idRh: string): void {
    this.demandes$.next({ idScenario, idRh });
  }

  /**
   * Charge les ajustements courants du scénario (scénario + neutralisations + variations).
   * `joursSemaineOverride` permet de calculer avec une valeur pas encore
   * persistée (changement 5/6 jours : le PATCH du parent part en parallèle).
   * Limite connue (back non modifiable ici) : au passage 5<->6 jours, les
   * nb_jour stockés des neutralisations ont été calculés avec l'ancien
   * nb_jours_semaine — même limite que le back pour nb_jours_scenario.
   */
  getAjustements(idScenario: number, joursSemaineOverride?: number): Observable<AjustementsScenario> {
    return forkJoin({
      scenario: this.scenarioService.getScenario(idScenario),
      neutralisations: this.neutralisationService.list(idScenario),
      variations: this.scenarioService.listVariations(idScenario),
    }).pipe(
      map(({ scenario, neutralisations, variations }) => {
        const joursSemaine = joursSemaineOverride ?? scenario.nb_jours_semaine ?? 6;
        const nbJoursBase = (joursSemaine === 5 ? scenario.nb_jours_ouvres : scenario.nb_jours_ouvrables) ?? 0;
        const nbJoursNeutralises = (neutralisations || [])
          .reduce((total, n) => total + (n.nb_jour ?? 0), 0);

        const variationsMap = new Map<string, number>();
        (variations || []).forEach(v => {
          const pct = Number(v['variation_pct']);
          variationsMap.set(v['co_produit'], isNaN(pct) ? 0 : pct);
        });

        return {
          scenario,
          joursSemaine,
          nbJoursBase,
          nbJoursNeutralises,
          joursOuvrablesEffectifs: Math.max(nbJoursBase - nbJoursNeutralises, 0),
          variations: variationsMap,
          estFige: !!scenario.est_fige,
        };
      })
    );
  }

  /**
   * Applique les ajustements aux lignes TMH (fonction pure) :
   * - lignes manuelles : moyennes recalculées sur le volume réalisé seul
   *   (un comptage mesuré ne reçoit pas de variation prévisionnelle) ;
   * - lignes auto : prévisionnel ajusté de ±pct% avant calcul des moyennes,
   *   et persisté dans volume_previsionnel_recalcule (la base reste intacte).
   * Volumes de base, flags et id_tmh inchangés.
   */
  applyAjustements(rows: TmhInput[], ajustements: AjustementsScenario): TmhInput[] {
    const jEff = ajustements.joursOuvrablesEffectifs;

    return rows.map(row => {
      const volumeRealise = Number(row.volume_realise) || 0;

      if (row.manuel) {
        return {
          ...row,
          // Un comptage mesuré ne reçoit pas de variation : recalculé = base.
          volume_previsionnel_recalcule: Number(row.volume_previsionnel) || 0,
          moyenne_journaliere: this.traficService.calculateMoyenneJournaliere(volumeRealise, 0, jEff),
          moyenne_hebdo: this.traficService.calculateMoyenneHebdo(volumeRealise, 0, jEff, ajustements.joursSemaine),
        };
      }

      const pct = ajustements.variations.get(row.co_produit) ?? 0;
      const volumePrevisionnelAjuste = Math.round((Number(row.volume_previsionnel) || 0) * (1 + pct / 100));

      return {
        ...row,
        // Le prévisionnel ajusté est persisté ; volume_previsionnel reste la base.
        volume_previsionnel_recalcule: volumePrevisionnelAjuste,
        moyenne_journaliere: this.traficService.calculateMoyenneJournaliere(volumeRealise, volumePrevisionnelAjuste, jEff),
        moyenne_hebdo: this.traficService.calculateMoyenneHebdo(volumeRealise, volumePrevisionnelAjuste, jEff, ajustements.joursSemaine),
      };
    });
  }

  /**
   * Chaîne complète : relit l'état frais (scénario, neutralisations,
   * variations, TMH bruts), recalcule les moyennes ajustées et les persiste.
   * Ne fait rien si le scénario est figé.
   */
  recalculerEtPersister(idScenario: number, idRh: string, joursSemaine?: number): Observable<unknown> {
    return forkJoin({
      ajustements: this.getAjustements(idScenario, joursSemaine),
      tmhRaw: this.scenarioService.listTmhRaw(idScenario),
    }).pipe(
      switchMap(({ ajustements, tmhRaw }) => {
        if (ajustements.estFige) {
          return of(null);
        }
        const inputs = (tmhRaw || []).map(t => this.mapTmhToInput(t));
        if (inputs.length === 0) {
          return of(null);
        }
        return this.scenarioService.upsertTmh(idScenario, idRh, this.applyAjustements(inputs, ajustements));
      }),
      retry({ count: RECALCUL_RETRIES, delay: RECALCUL_RETRY_DELAY_MS })
    );
  }

  /** Ligne brute Tmh (GET /tmh) -> TmhInput du PUT batch (exclusion <- bl_exclu, manuel <- bl_manuel). */
  private mapTmhToInput(row: Tmh): TmhInput {
    return {
      id_tmh: row.id_tmh,
      co_produit: row.co_produit,
      volume_realise: row.volume_realise,
      volume_previsionnel: row.volume_previsionnel,
      volume_previsionnel_recalcule: row.volume_previsionnel_recalcule ?? null,
      moyenne_journaliere: Number(row.moyenne_journaliere) || 0,
      moyenne_hebdo: Number(row.moyenne_hebdo) || 0,
      exclusion: row.bl_exclu,
      manuel: row.bl_manuel,
      motif: row.motif,
    };
  }
}
