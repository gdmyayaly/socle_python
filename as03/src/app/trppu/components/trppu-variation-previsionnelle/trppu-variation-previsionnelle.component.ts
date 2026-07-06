import { Component, Input, Output, EventEmitter, OnChanges, SimpleChanges } from '@angular/core';
import { VariationPrevisionnelle } from '../../models/variation-previsionnelle.model';
import { VariationPrevisionnelleService } from '../../services/variation-previsionnelle.service';

/**
 * Grille complète des produits gérés (ordre d'affichage de la maquette).
 * variation_pct = 0 par défaut : c'est la valeur des nouveaux scénarios
 * (et le 0 % n'est pas stocké côté back).
 * À terme, cette liste peut provenir du référentiel produit (trppu_produit).
 */
const PRODUITS_PAR_DEFAUT: VariationPrevisionnelle[] = [
  { co_produit: 'OO', libelle: 'OO',     variation_pct: 0 },
  { co_produit: 'OS', libelle: 'OS',     variation_pct: 0 },
  { co_produit: 'PR', libelle: 'Presse', variation_pct: 0 },
  { co_produit: 'CO', libelle: 'Colis',  variation_pct: 0 },
  { co_produit: 'PP', libelle: 'PPI',    variation_pct: 0 },
  { co_produit: 'IP', libelle: 'IP',     variation_pct: 0 },
];

@Component({
  selector: 'app-trppu-variation-previsionnelle',
  templateUrl: './trppu-variation-previsionnelle.component.html',
  styleUrls: ['./trppu-variation-previsionnelle.component.css']
})
export class TrppuVariationPrevisionnelleComponent implements OnChanges {

  /** Paramètre de configuration : id du scénario ciblé. */
  @Input() idScenario: number | null = null;
  /** Identifiant RH transmis au PUT (traçabilité). */
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

  constructor(private service: VariationPrevisionnelleService) {
    this.variations = this.clonerDefauts();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['idScenario']) {
      this.chargerVariations();
    }
  }

  // --------------------------------------------------------------------------
  // Lecture (DSR-651)
  // --------------------------------------------------------------------------
  private chargerVariations(): void {
    // On repart toujours de la grille complète à 0 %.
    this.variations = this.clonerDefauts();
    this.error = null;

    if (this.idScenario == null) {
      return;
    }

    // ===== DONNÉES EN DUR (à remplacer par l'appel réseau ci-dessous) =====
    const donneesEnDur: VariationPrevisionnelle[] = [
      { co_produit: 'OS', variation_pct: 25 },
      { co_produit: 'CO', variation_pct: 15 },
      { co_produit: 'PP', variation_pct: -20 },
    ];
    this.appliquerVariations(donneesEnDur);

    // ===== APPEL RÉSEAU (décommenter pour brancher le back DSR-651) =====
    // this.loading = true;
    // this.service.list(this.idScenario).subscribe({
    //   next: (data) => {
    //     this.appliquerVariations(data);
    //     this.loading = false;
    //   },
    //   error: () => {
    //     this.error = 'Erreur de chargement des variations.';
    //     this.loading = false;
    //   },
    // });
  }

  /** Superpose les variations stockées sur la grille par défaut (0 %). */
  private appliquerVariations(stockees: VariationPrevisionnelle[]): void {
    for (const v of stockees) {
      const ligne = this.variations.find(x => x.co_produit === v.co_produit);
      if (ligne) {
        ligne.variation_pct = Number(v.variation_pct);
      }
    }
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

    // ===== EN DUR : la valeur est déjà dans le modèle, rien à faire =====

    // ===== APPEL RÉSEAU (PUT DSR-646, 0 % => suppression côté back) =====
    // this.saving = true;
    // this.service.upsert(this.idScenario, ligne.co_produit, {
    //   variation_pct: ligne.variation_pct,
    //   id_rh: this.idRh,
    // }).subscribe({
    //   next: () => { this.saving = false; },
    //   error: () => {
    //     this.error = 'Erreur d\'enregistrement de la variation.';
    //     this.saving = false;
    //   },
    // });
  }

  // --------------------------------------------------------------------------
  // Affichage
  // --------------------------------------------------------------------------
  /** Position (0-100 %) du curseur sur la piste, à partir de la valeur signée. */
  toPercent(valeur: number): number {
    return ((valeur - this.min) / (this.max - this.min)) * 100;
  }

  private clonerDefauts(): VariationPrevisionnelle[] {
    return PRODUITS_PAR_DEFAUT.map(p => ({ ...p }));
  }
}
