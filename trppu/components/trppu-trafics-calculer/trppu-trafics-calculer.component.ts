import { Component, Input, Output, EventEmitter, OnChanges, SimpleChanges, OnInit, OnDestroy } from '@angular/core';
import { formatNumber } from '@angular/common';
import { MatDialog, MatDialogRef } from '@angular/material/dialog';
import { forkJoin, Subject } from 'rxjs';
import { retry, finalize, switchMap, map, takeUntil } from 'rxjs/operators';

import { TraficCalcule } from '../../models/trafic-calcule.model';
import { TraficPivot } from '../../models/trafic-pivot.model';
import { Tmh } from '../../models/tmh.model';
import { TmhInput } from '../../models/tmh-input.model';
import { Produit } from '../../models/produit.model';
import { TraficService } from '../../services/trafics.service';
import { ScenarioService } from '../../services/scenario.service';
import { ComptageService } from '../../services/comptage.service';
import { TmhRecalculService, AjustementsScenario } from '../../services/tmh-recalcul.service';
import { UserService } from '../../../../service/user/user.service';
import { DSRUser } from '../../../../model/user.model';

import { ConfirmDialogComponent, ConfirmDialogData } from '../../../../shared/dialog/confirm-dialog/confirm-dialog.component';
import { MessageDialogComponent, MessageDialogData } from '../../../../shared/dialog/message-dialog/message-dialog.component';


export type JoursParSemaine = 5 | 6;

// Pour le _ c'est juiste pour le visuel on peut metre 3000 comme 3_000 ce n'est pas grav
/** Délai avant de prévenir l'utilisateur que le traitement est long (ms). | 30s ici  */
const LONG_PROCESS_DELAY_MS = 30_000;
/** Nombre maximum de relances de la requête de calcul en cas d'échec. */
const MAX_PIVOT_RETRIES = 2;
/** Délai entre deux relances (ms). */
const RETRY_DELAY_MS = 3_000;

@Component({
 selector: 'app-trppu-trafics-calculer',
 templateUrl: './trppu-trafics-calculer.component.html',
 styleUrls: ['./trppu-trafics-calculer.component.css']
})
export class TrppuTraficsCalculerComponent implements OnChanges, OnInit, OnDestroy {

 @Input() scenarioId: number | null = null;
 @Input() coRegate: string | null = null;
 @Input() dateDebut: string | null = null;
 @Input() dateFin: string | null = null;
 @Input() datePivot: string | null = null;
 @Input() joursOuvrables: number = 0;
 @Input() joursParSemaine: JoursParSemaine = 6;

 @Output() joursParSemaineChange = new EventEmitter<JoursParSemaine>();

 readonly joursOptions: JoursParSemaine[] = [5, 6];

 joursOuvres = 0;

 hasManuel = false;
 displayedColumns: string[] = [
  'collapse', 'exclure', 'produit', 'constateBrut', 'previsionnelBrut', 'volumeBrut', 'traficMoyenHebdo', 'motif', 'manuel'
 ];
 displayedFooters: string[] = [
  'exclure', 'produit', 'constateBrut', 'manuel'
 ];

 trafics: TraficCalcule[] = [];
 private collapsedProduits = new Set<string>();
 private hoveredManualIconKeys = new Set<string>();
 private rawTrafics: Tmh[] = [];

 /** Ajustements courants du scénario (jours neutralisés + variations %). */
 private ajustements: AjustementsScenario | null = null;

 availableProduits: Produit[] = [];
 manuelsProduits: Produit[] = [];
 newTraficId: string | null = null;
 newValeur: number | null = null;
 newMotif: string | null = null;
 private utilisateurConnecter: DSRUser;

 /** Timer et modal persistant pour le message "traitement long". */
 private longProcessTimeoutId: ReturnType<typeof setTimeout> | null = null;
 private longProcessDialogRef: MatDialogRef<MessageDialogComponent> | null = null;

 /** Résiliation des souscriptions à la destruction du composant. */
 private destroy$ = new Subject<void>();

 constructor(
  private traficService: TraficService,
  private scenarioService: ScenarioService,
  private userService: UserService,
  private comptageService: ComptageService,
  private tmhRecalcul: TmhRecalculService,
  private dialog: MatDialog,
 ) {
  this.resolveProduits();
 }

 ngOnInit(): void {
  this.userService.getUserTrppuAsync().subscribe(user => this.utilisateurConnecter = user);

  // Rafraîchissement dynamique : après chaque recalcul persisté (variation %
  // modifiée depuis /parameters, neutralisations…), on recharge le tableau —
  // refresh() relit aussi les ajustements, donc le % du tooltip reste frais.
  this.tmhRecalcul.recalculTermine$
   .pipe(takeUntil(this.destroy$))
   .subscribe(idScenario => {
    if (idScenario === this.scenarioId) {
     this.refresh();
    }
   });
 }

 ngOnDestroy(): void {
  this.destroy$.next();
  this.destroy$.complete();
 }

 ngOnChanges(changes: SimpleChanges): void {
  if (changes.scenarioId) {
   this.refresh();
  } else if (changes.dateDebut || changes.dateFin || changes.datePivot || changes.joursParSemaine) {
   this.loadTraficsWithPivot();
  }
 }

 refresh(update: boolean = false): void {
  if (this.scenarioId === null) {
   this.trafics = [];
   return;
  }

  // Charge d'abord les ajustements (jours neutralisés + variations) pour que
  // les moyennes des lignes manuelles/comptages utilisent le nombre de jours
  // ouvrables effectif du scénario.
  this.tmhRecalcul.getAjustements(this.scenarioId, this.joursParSemaine).subscribe({
   next: (ajustements) => {
    this.ajustements = ajustements;
    this.joursOuvrables = ajustements.joursOuvrablesEffectifs;
    this.loadTraficsScenario(update);
   },
   // Repli : on relit les TMH avec les valeurs courantes (comportement historique).
   error: () => this.loadTraficsScenario(update)
  });
 }

 onJoursParSemaineChange(value: JoursParSemaine): void {
  this.joursParSemaine = value;
  this.joursParSemaineChange.emit(value);
  //this.recompute();
 }

 onExclureChange(row: TraficCalcule, excluded: boolean): void {
  const previousExcluded = row.exclure;

  if (this.scenarioId === null) {
   return;
  }

  this.scenarioService.updateTmhExclusion(this.scenarioId, row.id, { bl_exclu: excluded }).subscribe({
   next: ()   => this.applyExclusionLocally(row.id, excluded),
   complete: () => {
    this.refresh();
    this.openMessage(`La ligne de produit a été bien ${excluded ? 'exclue' : 'réintégrée'}`, 'success');
   },
   error: (err) => this.openError(err, 'Erreur lors de la mise à jour de l\'exclusion du produit.')
  });
 }

 private updateTraficsScenario(): void {
  if (this.scenarioId === null) {
   return;
  }

  const traficsToUpdate = [];

  this.trafics.forEach(t => {
    if(t.manuelCalc) {
      const payload = {
        id_tmh: t.id,
        co_produit: t.coProduit,
        volume_realise: t.constateBrut,
        volume_previsionnel: t.previsionnelBrut,
        moyenne_journaliere: t.traficMoyenJourna,
        moyenne_hebdo: t.traficMoyenHebdo,
        manuel: t.manuel,
        exclusion: t.exclure,
        motif: t.motif
      };

      traficsToUpdate.push(payload);
    }
  });

  this.scenarioService.upsertTmh(this.scenarioId, this.utilisateurConnecter.idRh, traficsToUpdate).subscribe({
    //next: ()   =>   this.openMessage(`Les moyennes calculées ont été bien mis à jour !`, 'success'),
    complete: () => this.refresh(),
    error: (err) => this.openError(err, `Erreur lors de la mise à jour des moyennes hebdo et journalières !`)
  });
 }

 private loadTraficsScenario(update: boolean = false): void {
  if (this.scenarioId === null) {
   this.trafics = [];
   this.collapsedProduits.clear();
    this.hoveredManualIconKeys.clear();
   return;
  }

  forkJoin({
   tmh: this.scenarioService.listTmh(this.scenarioId.toString(), this.joursOuvrables, this.joursParSemaine)
  }).subscribe({
   next: ({ tmh }) => {
    this.manuelsProduits = [];
    this.collapsedProduits.clear();
    this.hoveredManualIconKeys.clear();
    this.trafics = tmh.map(t => {
     const produitRef = this.availableProduits.find(p => p.co_produit === t.produit);

     return {
      ...t,
      lbProduit: produitRef ? produitRef.lb_produit : t.produit
     };
    });
   },
   complete: () => {
    if (update) {
      this.updateTraficsScenario();
    }

    this.availableProduits.forEach(t => {
      const tmh = this.trafics.find(p => p.coProduit === t.co_produit && p.manuel === false);
      if (tmh === undefined) {
       this.manuelsProduits.push(t);
      }
    });
   },
   error: () => {
    this.trafics = [];
    this.collapsedProduits.clear();
    this.hoveredManualIconKeys.clear();
   }
  });
 }

 /**
  * Old databrick data
  * DEPRECATED : replaced by loadTraficsWithPivot
  * @returns
  */
 private loadTrafics(): void {
  if (this.scenarioId === null || !this.coRegate || !this.dateDebut || !this.dateFin) {
   this.trafics = [];
   this.rawTrafics = [];
   return;
  }

  const coRegate = this.coRegate;
  const dateDebut = this.dateDebut;
  const dateFin = this.dateFin;

  forkJoin({
   trafics: this.traficService.getTrafics({ coRegate, dateDebut, dateFin })
  }).subscribe({
   next: ({ trafics }) => {
    this.trafics = trafics;
    //this.recompute();
   },
   error: () => {
    this.trafics = [];
    this.rawTrafics = [];
   }
  });
 }

 private loadTraficsWithPivot(): void {
  if (this.scenarioId === null || !this.coRegate || !this.dateDebut || !this.dateFin || !this.datePivot) {
   this.trafics = [];
   this.collapsedProduits.clear();
    this.hoveredManualIconKeys.clear();
   this.rawTrafics = [];
   return;
  }

  const scenarioId = this.scenarioId;
  const coRegate = this.coRegate;
  const dateDebut = this.dateDebut;
  const dateFin = this.dateFin;
  const datePivot = this.datePivot;
  const joursSemaine = this.joursParSemaine;

  // Prévient l'utilisateur via un modal si le traitement dépasse le seuil.
  this.startLongProcessTimer();

  forkJoin({
   pivot: this.traficService
    .getTraficsPivot({ coRegate, dateDebut, dateFin, datePivot, joursSemaine, currentTmh: this.trafics })
    .pipe(
     // En cas d'échec (ex. 502 dû à un traitement long), on relance jusqu'à
     // MAX_PIVOT_RETRIES fois, avec un délai entre chaque tentative.
     retry({ count: MAX_PIVOT_RETRIES, delay: RETRY_DELAY_MS })
    ),
   // Jours neutralisés + variations % du scénario, appliqués aux moyennes
   // pour ne pas écraser les ajustements faits depuis /parameters.
   ajustements: this.tmhRecalcul
    .getAjustements(scenarioId, joursSemaine)
    .pipe(retry({ count: 1, delay: RETRY_DELAY_MS }))
  })
   .pipe(
    map(({ pivot, ajustements }) => {
     // Base DSR-656 (ouvrés en 5 j / ouvrables en 6 j) ; repli sur la base
     // pivot si les nb_jours du scénario sont absents.
     const base = ajustements.nbJoursBase > 0 ? ajustements.nbJoursBase : pivot.nbJoursOuvrables;
     const ajustementsEffectifs: AjustementsScenario = {
      ...ajustements,
      nbJoursBase: base,
      joursOuvrablesEffectifs: Math.max(base - ajustements.nbJoursNeutralises, 0)
     };
     this.ajustements = ajustementsEffectifs;
     this.joursOuvrables = ajustementsEffectifs.joursOuvrablesEffectifs;
     return this.tmhRecalcul.applyAjustements(pivot.tmh, ajustementsEffectifs);
    }),
    // Enchaîne sur l'enregistrement des TMH calculés.
    switchMap(tmh =>
     this.scenarioService.upsertTmh(scenarioId, this.utilisateurConnecter.idRh, tmh)
    ),
    // Ferme toujours le modal d'attente, succès comme échec.
    finalize(() => this.clearLongProcessTimer())
   )
   .subscribe({
     next: () => {
      this.trafics  = [];
      this.collapsedProduits.clear();
        this.hoveredManualIconKeys.clear();
      this.rawTrafics = [];
     },
     complete: () => this.refresh(),
     error: (err) => {
      this.clearLongProcessTimer();
      this.trafics = [];
      this.collapsedProduits.clear();
      this.hoveredManualIconKeys.clear();
      this.rawTrafics = [];
      this.openError(err, 'Erreur lors du calcul des trafics après plusieurs tentatives.');
    }
   });
 }

 private resolveProduits(): void {
  this.comptageService.listProduit().subscribe({
   next: (produits) => {
    this.availableProduits = produits && produits.length > 0 ? produits : [];
   },
   error: (error) => {
    console.error('Error fetching produits:', error);
    this.availableProduits = this.traficService.validProduit.map(p => ({
     co_produit: p,
     lb_produit: p,
     dt_desactivation: null,
     motif_desactivation: null,
     dt_creation: new Date().toISOString()
    }));
   }
  });
 }

 get canAddComptage(): boolean {
  return this.newValeur !== null && this.newValeur !== 0;
 }

 get enableAddComptage(): boolean {
  return this.trafics.length > 0;
 }

 onAddComptage(): void {
  if (this.scenarioId === null || this.newTraficId === null || this.newValeur === null) {
   return;
  }

  if (!this.canAddComptage) {
    this.openError(null, 'Merci de saisir une valeur valide du comptage manuel !');
    return;
  }

  const payload = {
   id_tmh: null,
   co_produit: this.newTraficId,
   volume_realise: this.newValeur,
   volume_previsionnel: 0,
   moyenne_journaliere: this.traficService.calculateMoyenneJournaliere(this.newValeur, 0, this.joursOuvrables),
   moyenne_hebdo: this.traficService.calculateMoyenneHebdo(this.newValeur, 0, this.joursOuvrables, this.joursParSemaine),
   manuel: true,
   exclusion: false,
   motif: this.newMotif
  };

  this.scenarioService.upsertTmh(this.scenarioId, this.utilisateurConnecter.idRh, [payload]).subscribe({
   next: (data) => {},
   complete: () => {
    this.refresh(true);
    this.openMessage(`Le comptage manuel a été bien ajouté pour le produit : ${this.newTraficId}`, 'success');
    this.newTraficId = null;
    this.newValeur = null;
    this.newMotif = null;
   },
   error: (err) => {
    console.error('Error adding comptage:', err);
    this.openError(err, 'Erreur lors de l\'ajout du comptage.');
   }
  });
 }

 onAddComptageProduit(trafic: TraficCalcule): void {
  if (this.scenarioId === null || trafic.coProduit === null) {
   return;
  }

  const todayDate = new Date().toLocaleString();

  const payload = {
   id_tmh: null,
   co_produit: trafic.coProduit,
   volume_realise: 0,
   volume_previsionnel: 0,
   moyenne_journaliere: 0,
   moyenne_hebdo: 0,
   manuel: true,
   exclusion: false,
   motif: 'Ajout du ' + todayDate
  };

  this.scenarioService.upsertTmh(this.scenarioId, this.utilisateurConnecter.idRh, [payload]).subscribe({
   next: (data) => {},
   complete: () => {
    this.refresh(true);
    this.openMessage(`Le comptage manuel a été bien ajouté pour le produit : ${trafic.coProduit}`, 'success');
   },
   error: (err) => {
    console.error('Error adding comptage:', err);
    this.openError(err, 'Erreur lors de l\'ajout du comptage produit.');
   }
  });
 }

 updateComptage(trafic: TraficCalcule, event: FocusEvent, type: 'constateBrut' | 'motif'): void {
  if (this.scenarioId === null || trafic.coProduit === null) {
   return;
  }

  const input = event.target as HTMLInputElement;

  let newValue = trafic.constateBrut;
  if (type === 'constateBrut') {
   if(newValue === parseInt(input.value)) return;
   newValue = parseInt(input.value);
  }

  let newMotif = trafic.motif;
  if (type === 'motif') {
   if((newMotif !== null && newMotif === input.value) || (newMotif === null && '' === input.value) ) return;
   newMotif = input.value;
  }

  const payload = {
   id_tmh: trafic.id,
   co_produit: trafic.coProduit,
   volume_realise: newValue,
   volume_previsionnel: 0,
   moyenne_journaliere: this.traficService.calculateMoyenneJournaliere(newValue, 0, this.joursOuvrables),
   moyenne_hebdo: this.traficService.calculateMoyenneHebdo(newValue, 0, this.joursOuvrables, this.joursParSemaine),
   manuel: true,
   exclusion: false,
   motif: newMotif
  };

  this.scenarioService.upsertTmh(this.scenarioId, this.utilisateurConnecter.idRh, [payload]).subscribe({
   next: (data) => this.refresh(true),
   complete: () => this.openMessage(`Le comptage manuel a été bien mis à jour pour le produit : ${trafic.coProduit}`, 'success'),
   error: (err) => {
    console.error('Error updating comptage:', err);
    this.openError(err, 'Erreur lors de la mise à jour du comptage.');
   }
  });
 }

 onRemoveManual(trafic: TraficCalcule): void {
 const data: ConfirmDialogData = {
  message: `Supprimer le comptage manuel pour le produit "${trafic.lbProduit}" ?`,
  type: 'warning',
  yesAction: { label: 'Oui, supprimer' },
  noAction: { label: 'Annuler' },
 };

 this.dialog
  .open(ConfirmDialogComponent, { data, width: '420px' })
  .afterClosed()
  .subscribe(result => {
  if (result?.event === true) {
   this.deleteManual(trafic);
  }
  });
 }

 /** Suppression effective du comptage manuel, appelée uniquement après confirmation. */
 private deleteManual(trafic: TraficCalcule): void {
   this.scenarioService.deleteTmhVolume(this.scenarioId, trafic.id).subscribe({
    next: () => {
     this.trafics = this.trafics.filter(t => t.id !== trafic.id);
    },
    complete: () => {
     this.openMessage('Le comptage manuel a été bien supprimé', 'success');
     this.refresh(true);
    },
  error: (err) => this.openError(err, 'Erreur lors de la suppression du comptage manuel.')
   });
  }

 private applyExclusionLocally(traficId: number, excluded: boolean): void {
  this.trafics = this.trafics.map(t => t.id === traficId ? { ...t, exclure: excluded } : t);
 }

 /** Programme l'affichage d'un modal d'attente si le traitement dépasse le seuil. */
 private startLongProcessTimer(): void {
 this.clearLongProcessTimer();
 this.longProcessTimeoutId = setTimeout(() => {
  this.longProcessDialogRef = this.dialog.open(MessageDialogComponent, {
  data: {
   message: 'Le calcul des trafics est en cours et peut prendre un moment. Merci de patienter…',
   type: 'info'
  } as MessageDialogData,
  width: '420px',
  disableClose: true
  });
 }, LONG_PROCESS_DELAY_MS);
}

 /** Annule le timer d'attente et ferme le modal persistant s'il est ouvert. */
 private clearLongProcessTimer(): void {
  if (this.longProcessTimeoutId !== null) {
   clearTimeout(this.longProcessTimeoutId);
   this.longProcessTimeoutId = null;
  }
  if (this.longProcessDialogRef) {
   this.longProcessDialogRef.close();
   this.longProcessDialogRef = null;
  }
 }

 isParentRow(row: TraficCalcule): boolean {
  return !row.manuel || row.manuelCalc;
 }

 isChildRow(row: TraficCalcule): boolean {
  return row.manuel && !row.manuelCalc;
 }

 hasChildRows(row: TraficCalcule): boolean {
  if (!this.isParentRow(row)) {
   return false;
  }

  const produitKey = this.getProduitKey(row);
  return this.trafics.some(t => this.getProduitKey(t) === produitKey && this.isChildRow(t));
 }

 isProduitCollapsed(row: TraficCalcule): boolean {
  return this.collapsedProduits.has(this.getProduitKey(row));
 }

 toggleProduitRows(row: TraficCalcule): void {
  if (!this.hasChildRows(row)) {
   return;
  }

  const produitKey = this.getProduitKey(row);
  if (this.collapsedProduits.has(produitKey)) {
   this.collapsedProduits.delete(produitKey);
  } else {
   this.collapsedProduits.add(produitKey);
  }
 }

 isRowHidden(row: TraficCalcule): boolean {
  return this.isChildRow(row) && this.collapsedProduits.has(this.getProduitKey(row));
 }

 setManualIconHover(row: TraficCalcule, hovered: boolean): void {
  const rowKey = this.getManualRowKey(row);
  if (hovered) {
   this.hoveredManualIconKeys.add(rowKey);
  } else {
   this.hoveredManualIconKeys.delete(rowKey);
  }
 }

 isManualIconHovered(row: TraficCalcule): boolean {
  return this.hoveredManualIconKeys.has(this.getManualRowKey(row));
 }

 /** Vrai si un prévisionnel recalculé (variation %) distinct de la base doit être mis en avant. */
 hasRecalcule(row: TraficCalcule): boolean {
  return !row.manuel
   && row.previsionnelRecalcule !== null
   && row.previsionnelRecalcule !== undefined
   && Number(row.previsionnelRecalcule) !== Number(row.previsionnelBrut);
 }

 /** Variation % appliquée au produit (0 si absente), depuis les ajustements chargés. */
 getVariationPct(row: TraficCalcule): number {
  return this.ajustements?.variations.get(row.coProduit) ?? 0;
 }

 /** Tooltip de la cellule prévisionnel : valeur de base + variation % appliquée. */
 getRecalculeTooltip(row: TraficCalcule): string {
  const base = formatNumber(row.previsionnelBrut ?? 0, 'fr');
  const pct = this.getVariationPct(row);
  return `Valeur de base : ${base} — Variation appliquée : ${pct > 0 ? '+' : ''}${pct} %`;
 }

 private getProduitKey(row: TraficCalcule): string {
  return row.coProduit ?? row.produit;
 }

 private getManualRowKey(row: TraficCalcule): string {
  return `${row.id ?? 'no-id'}-${this.getProduitKey(row)}`;
 }

 /** Ouvre une MessageDialog d'information (ici : erreurs). */
 private openMessage(message: string, type: string): void {
  const data: MessageDialogData = { message, type };
  this.dialog.open(MessageDialogComponent, { data, width: '420px' });
 }

 /** Ouvre un modal d'erreur en privilégiant le `detail` renvoyé par le back. */
 private openError(err: any, fallback: string): void {
  this.openMessage(err?.error?.detail ?? fallback, 'error');
 }
}
