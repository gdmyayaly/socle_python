import { Component, OnInit } from '@angular/core';
import {
 PicCoefficient,
 JourSemaine,
 Densite,
 ID_PIC_VERSION_DEFAUT,
} from '../models/pic-coefficient.model';
import { PicCoefficientService } from '../services/pic-coefficient.service';
import { TrppuContextService } from '../services/trppu-context.service';



/** Cellule du tableau (coefficient + état d'affichage/édition). */
interface CoefCell {
 co_produit: string;
 jour: JourSemaine;
 densite: Densite;
 coef: number | null;
 id_pic_version: number;
 edit: string;   // texte saisi dans l'input (sans le "%")
 pending: boolean; // enregistrement en cours -> gras/rouge
 error: boolean;  // valeur hors plage 0-100
}

interface CoefRow {
 co_produit: string;
 libelle: string;
 cells: CoefCell[];
}

interface ColonneDef {
 jour: JourSemaine;
 densite: Densite;
}

/** Libellés produits (ordre d'affichage de la maquette). */
const PRODUITS: { code: string; libelle: string }[] = [
 { code: 'OO', libelle: 'oo' },
 { code: 'OS', libelle: 'os' },
 { code: 'PR', libelle: 'presse' },
 { code: 'CO', libelle: 'colis' },
 { code: 'PP', libelle: 'ppi' },
 { code: 'IP', libelle: 'ip' },
];

/** Définition des jours et de leurs densités (samedi : une seule densité). */
const JOURS_DEF: { code: JourSemaine; label: string; densites: Densite[] }[] = [
 { code: 'LUN', label: 'lundi', densites: [0, 1, 2] },
 { code: 'MAR', label: 'mardi', densites: [0, 1, 2] },
 { code: 'MER', label: 'mercredi', densites: [0, 1, 2] },
 { code: 'JEU', label: 'jeudi', densites: [0, 1, 2] },
 { code: 'VEN', label: 'vendredi', densites: [0, 1, 2] },
 { code: 'SAM', label: 'samedi', densites: [0] },
];

const DENSITE_LABELS: Record<Densite, string> = {
 0: 'dense',
 1: 'clairsemée',
 2: 'clairsemée2',
};


@Component({
  selector: 'app-configpic',
  templateUrl: './configpic.component.html',
  styleUrls: ['./configpic.component.scss']
})
export class ConfigpicComponent implements OnInit {



 readonly defaultVersion = ID_PIC_VERSION_DEFAUT;



 // Contexte (localStorage)
 idScenario: number | null = null;
 coRegate: string | null = null;
 /** Modifiable uniquement si un scénario est en cours. */
 editable = false;

 // En-têtes
 joursHeader: { label: string; span: number }[] = JOURS_DEF.map(j => ({
  label: j.label,
  span: j.densites.length,
 }));
 densiteHeader: string[] = [];
 columns: ColonneDef[] = [];

 // Corps
 rows: CoefRow[] = [];
 totalRow: (number | null)[] = [];

 loading = false;
 errorMessage: string | null = null;

 constructor(
  private picService: PicCoefficientService,
  private context: TrppuContextService,
 ) {
  this.buildColonnes();
 }

 ngOnInit(): void {
  // Id du scénario en cours et site : récupérés dans le localStorage.
  this.idScenario = this.context.getIdScenario();
  this.coRegate = this.context.getCoRegate();
  this.editable = this.idScenario != null;
  this.chargerCoefficients();
 }

 // ---------------------------------------------------------------------------
 // Construction du squelette de colonnes / en-têtes
 // ---------------------------------------------------------------------------
 private buildColonnes(): void {
  this.columns = [];
  this.densiteHeader = [];
  for (const j of JOURS_DEF) {
   for (const d of j.densites) {
    this.columns.push({ jour: j.code, densite: d });
    // Samedi : densité "non précisée".
    this.densiteHeader.push(j.code === 'SAM' ? '' : DENSITE_LABELS[d]);
   }
  }
 }

 // ---------------------------------------------------------------------------
 // Lecture des coefficients (YS04 - DSR-660)
 // ---------------------------------------------------------------------------
 private chargerCoefficients(): void {
  this.loading = true;
  this.errorMessage = null;
  this.picService.getCoefficients(this.idScenario).subscribe({
   next: (coeffs) => {
    this.buildRows(coeffs);
    this.recomputeTotals();
    this.loading = false;
   },
   error: () => {
    this.errorMessage = 'Erreur de chargement du paramétrage PIC.';
    this.loading = false;
   },
  });
 }

 private buildRows(coeffs: PicCoefficient[]): void {
  const lookup = new Map<string, PicCoefficient>();
  for (const c of coeffs) {
   lookup.set(this.cellKey(c.co_produit, c.jour_semaine, c.densite), c);
  }

  this.rows = PRODUITS.map((p) => ({
   co_produit: p.code,
   libelle: p.libelle,
   cells: this.columns.map((col) => {
    const c = lookup.get(this.cellKey(p.code, col.jour, col.densite));
    const coef = c ? c.coef : null;
    return {
     co_produit: p.code,
     jour: col.jour,
     densite: col.densite,
     coef,
     id_pic_version: c ? c.id_pic_version : ID_PIC_VERSION_DEFAUT,
     edit: coef == null ? '' : this.numToFr(coef),
     pending: false,
     error: false,
    } as CoefCell;
   }),
  }));
 }

 // ---------------------------------------------------------------------------
 // Édition d'une cellule (YS04 - DSR-661), sauvegarde auto à la perte de focus
 // ---------------------------------------------------------------------------
 onBlur(cell: CoefCell): void {
  if (!this.editable || cell.coef === null) {
   return;
  }

  // Nettoyage : suppression du "%" éventuel, virgule -> point.
  const brut = (cell.edit ?? '').replace(/%/g, '').replace(',', '.').trim();

  // Champ vidé -> on restaure la valeur précédente (pas de modification).
  if (brut === '') {
   cell.edit = this.numToFr(cell.coef);
   cell.error = false;
   return;
  }

  const valeur = Number(brut);
  if (!Number.isFinite(valeur) || valeur < 0 || valeur > 100) {
   cell.error = true;
   this.errorMessage = 'Veuillez saisir une valeur décimale comprise entre 0 et 100.';
   return;
  }

  cell.error = false;
  this.errorMessage = null;

  // Aucune modification réelle -> on normalise juste l'affichage.
  if (valeur === cell.coef) {
   cell.edit = this.numToFr(valeur);
   return;
  }

  // Modification en cours : gras + rouge, mise à jour immédiate des totaux.
  cell.coef = valeur;
  cell.edit = this.numToFr(valeur);
  cell.pending = true;
  this.recomputeTotals();

  this.picService.updateCoefficient({
   id_scenario: this.idScenario!,
   co_produit: cell.co_produit,
   jour_semaine: cell.jour,
   coef: valeur,
   densite: cell.densite,
  }).subscribe({
   next: () => {
    // Enregistré : le coefficient devient propre au scénario -> gras + vert.
    cell.pending = false;
    cell.id_pic_version = 2;
   },
   error: () => {
    cell.pending = false;
    this.errorMessage = 'Erreur lors de l\'enregistrement du coefficient.';
   },
  });
 }

 // ---------------------------------------------------------------------------
 // Total trafic par colonne — RG à définir avec le métier.
 // Placeholder : moyenne des coefficients produits non vides de la colonne.
 // ---------------------------------------------------------------------------
 private recomputeTotals(): void {
  this.totalRow = this.columns.map((_, i) => {
   const valeurs = this.rows
    .map((r) => r.cells[i].coef)
    .filter((v): v is number => v != null);
   if (valeurs.length === 0) {
    return null;
   }
   const moyenne = valeurs.reduce((a, b) => a + b, 0) / valeurs.length;
   return Math.round(moyenne * 10) / 10;
  });
 }

 // ---------------------------------------------------------------------------
 // Helpers d'affichage
 // ---------------------------------------------------------------------------
 isScenario(cell: CoefCell): boolean {
  return !cell.pending && cell.coef !== null && cell.id_pic_version !== ID_PIC_VERSION_DEFAUT;
 }

 formatCoef(coef: number | null): string {
  return coef == null ? '' : this.numToFr(coef) + '%';
 }

 private numToFr(n: number): string {
  return n.toString().replace('.', ',');
 }

 private cellKey(coProduit: string, jour: JourSemaine, densite: Densite): string {
  return `${coProduit}|${jour}|${densite}`;
 }
}

