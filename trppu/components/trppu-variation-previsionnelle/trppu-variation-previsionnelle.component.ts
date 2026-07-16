import { Component, Input, Output, EventEmitter, OnChanges, SimpleChanges, OnInit, OnDestroy } from '@angular/core';
import { forkJoin, of, Subject } from 'rxjs';
import { debounceTime, concatMap, catchError, tap, takeUntil } from 'rxjs/operators';
import { VariationPrevisionnelle } from '../../models/variation-previsionnelle.model';
import { VariationPrevisionnelleService } from '../../services/variation-previsionnelle.service';
import { TmhRecalculService } from '../../services/tmh-recalcul.service';
import { UserService } from '../../../../service/user/user.service';
import { DSRUser } from '../../../../model/user.model';

/** Délai de regroupement des mouvements de slider avant enregistrement (ms). */
const SAVE_DEBOUNCE_MS = 400;

@Component({
 selector: 'app-trppu-variation-previsionnelle',
 templateUrl: './trppu-variation-previsionnelle.component.html',
 styleUrls: ['./trppu-variation-previsionnelle.component.scss']
})
export class TrppuVariationPrevisionnelleComponent implements OnChanges, OnInit, OnDestroy {

 @Input() idScenario: number | null = null;
 @Input() idRh = '';
 /** Lecture seule (scénario figé / archivé). */
 @Input() readonly = false;

 /** Émis à chaque modification de valeur (grille complète). */
 @Output() variationsChange = new EventEmitter<VariationPrevisionnelle[]>();

 readonly min = -100;
 readonly max = 100;
 readonly step = 5;

 variations: VariationPrevisionnelle[] = [];
 loading = false;
 saving = false;
 error: string | null = null;
 private utilisateurConnecter : DSRUser;

 /** Lignes modifiées en attente d'enregistrement (une entrée par produit). */
 private pending = new Map<string, VariationPrevisionnelle>();
 private saveTrigger$ = new Subject<void>();
 private destroy$ = new Subject<void>();

 constructor(
  private variationPrevisionelleService: VariationPrevisionnelleService,
  private tmhRecalcul: TmhRecalculService,
  private userService: UserService
 ) {}

 ngOnInit(): void {
  // this.idRh = this.userService.getUserTrppu().idRh;
  this.userService.getUserTrppuAsync().subscribe(user=>{this.utilisateurConnecter=user;this.idRh=this.utilisateurConnecter.idRh});
  this.chargerVariations();

  // Regroupe les mouvements de slider puis enregistre le lot ; concatMap
  // sérialise les lots pour ne jamais avoir deux écritures concurrentes.
  this.saveTrigger$
   .pipe(
    debounceTime(SAVE_DEBOUNCE_MS),
    concatMap(() => this.flushPending()),
    takeUntil(this.destroy$)
   )
   .subscribe();
 }

 ngOnDestroy(): void {
  this.destroy$.next();
  this.destroy$.complete();
 }

 ngOnChanges(changes: SimpleChanges): void {
  if (changes['idScenario']) {
   this.chargerVariations();
  }
 }

 private chargerVariations(): void {
  this.error = null;

  if (this.idScenario == null) {
   this.variations = [];
   return;
  }

  this.loading = true;
  this.variationPrevisionelleService.list(this.idScenario).subscribe({
   next: (data) => {
    this.appliquerVariations(data);
    this.loading = false;
   },
   error: () => {
    this.error = 'Erreur de chargement des variations.';
    this.loading = false;
   },
  });
 }

 /** Alimente la grille avec les variations renvoyées par le service. */
 private appliquerVariations(stockees: VariationPrevisionnelle[]): void {
  this.variations = stockees.map(v => ({
   ...v,
   variation_pct: Number(v.variation_pct),
  }));
  this.variationsChange.emit([...this.variations]);
 }

 // --------------------------------------------------------------------------
 // Écriture (DSR-646) : upsert d'une ligne (0 % => suppression côté back)
 // --------------------------------------------------------------------------
 onVariationChange(ligne: VariationPrevisionnelle, valeur: number): void {
  ligne.variation_pct = valeur;
  this.variationsChange.emit([...this.variations]);
  this.pending.set(ligne.co_produit, ligne);
  this.saveTrigger$.next();
 }

 /**
  * Enregistre le lot des variations en attente, puis déclenche le recalcul
  * des moyennes TMH (le % est appliqué au volume prévisionnel de chaque produit).
  */
 private flushPending() {
  if (this.idScenario == null || this.readonly || this.pending.size === 0) {
   this.pending.clear();
   return of(null);
  }

  const idScenario = this.idScenario;
  const lignes = Array.from(this.pending.values());
  this.pending.clear();

  this.saving = true;
  return forkJoin(
   lignes.map(ligne =>
    this.variationPrevisionelleService.upsert(idScenario, ligne.co_produit, {
     variation_pct: ligne.variation_pct,
     id_rh: this.idRh,
    })
   )
  ).pipe(
   tap(() => {
    this.saving = false;
    if (this.idRh) {
     this.tmhRecalcul.demanderRecalcul(idScenario, this.idRh);
    }
   }),
   catchError(() => {
    this.error = 'Erreur d\'enregistrement de la variation.';
    this.saving = false;
    return of(null);
   })
  );
 }

 // --------------------------------------------------------------------------
 // Affichage
 // --------------------------------------------------------------------------
 /** Position (0-100 %) du curseur sur la piste, à partir de la valeur signée. */
 toPercent(valeur: number): number {
  return ((valeur - this.min) / (this.max - this.min)) * 100;
 }
}