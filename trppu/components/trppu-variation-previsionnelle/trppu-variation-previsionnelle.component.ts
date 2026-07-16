import { Component, Input, Output, EventEmitter, OnChanges, SimpleChanges, OnInit } from '@angular/core';
import { VariationPrevisionnelle } from '../../models/variation-previsionnelle.model';
import { VariationPrevisionnelleService } from '../../services/variation-previsionnelle.service';
import { UserService } from '../../../../service/user/user.service';
import { DSRUser } from '../../../../model/user.model';

@Component({
 selector: 'app-trppu-variation-previsionnelle',
 templateUrl: './trppu-variation-previsionnelle.component.html',
 styleUrls: ['./trppu-variation-previsionnelle.component.scss']
})
export class TrppuVariationPrevisionnelleComponent implements OnChanges, OnInit {

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
 constructor(
  private variationPrevisionelleService: VariationPrevisionnelleService,
  private userService: UserService
 ) {}

 ngOnInit(): void {
  // this.idRh = this.userService.getUserTrppu().idRh;
  this.userService.getUserTrppuAsync().subscribe(user=>{this.utilisateurConnecter=user;this.idRh=this.utilisateurConnecter.idRh});
  this.chargerVariations();
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
  this.enregistrer(ligne);
 }

 private enregistrer(ligne: VariationPrevisionnelle): void {
  if (this.idScenario == null || this.readonly) {
   return;
  }

  this.saving = true;
  this.variationPrevisionelleService.upsert(this.idScenario, ligne.co_produit, {
   variation_pct: ligne.variation_pct,
   id_rh: this.idRh,
  }).subscribe({
   next: () => { this.saving = false; },
   error: () => {
    this.error = 'Erreur d\'enregistrement de la variation.';
    this.saving = false;
   },
  });
 }

 // --------------------------------------------------------------------------
 // Affichage
 // --------------------------------------------------------------------------
 /** Position (0-100 %) du curseur sur la piste, à partir de la valeur signée. */
 toPercent(valeur: number): number {
  return ((valeur - this.min) / (this.max - this.min)) * 100;
 }
}