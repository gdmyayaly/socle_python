import { Component, OnInit, ViewChild } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import { UserService } from '../../../service/user/user.service';
import { Site } from '../../../model/site.model';
import { Scenario } from '../models/scenario.model';
import { Periode } from '../models/periode.model';
import { AmasServiceService } from '../../../service/amas-service/amas-service.service';
import { Comptage } from '../models/comptage.model';
import { JoursParSemaine, TrppuTraficsCalculerComponent } from '../components/trppu-trafics-calculer/trppu-trafics-calculer.component';
import { ScenarioService } from '../services/scenario.service';
import { AuthOidcService } from '@cddng/auth-oidc-apim';
import { ConfirmDialogComponent, ConfirmDialogData } from '../../../shared/dialog/confirm-dialog/confirm-dialog.component';




@Component({
 selector: 'app-calcul-trppu',
 templateUrl: './calcul-trppu.component.html',
 styleUrls: ['./calcul-trppu.component.scss'],
})
export class CalculTrppuComponent implements OnInit {
 @ViewChild(TrppuTraficsCalculerComponent) traficsCalculerRef!: TrppuTraficsCalculerComponent;

 idRh: string | null = null;
 selectedSite: Site | null = null;
 selectedScenario: Scenario | null = null;
 needsRefresh = false;
 periodeHasChanges = false;
 comptageHasChanges = false;
 currentComptages: Comptage[] = [];
 currentPeriode: Periode | null = null;
 currentJoursParSemaine: JoursParSemaine = 6;


 constructor(
  private userService: UserService,
  private amasServiceService: AmasServiceService,
  private scenarioService: ScenarioService,
  private authOidcService: AuthOidcService,
  private dialog: MatDialog,
 ) {}

 ngOnInit(): void {
  // nouvelle bonne implementation du trppu service
  console.log('Chargement des donnée trppu en asych');
  this.userService.getUserTrppuAsync().subscribe(
   user => { this.idRh = user.idRh; console.log(user); }
  );
 }

 onSiteSelected(site: Site | null): void {
  if (this.periodeHasChanges) {
   this.confirmDiscardChanges(
    'Vous avez des modifications non sauvegardées sur la période. Ce changement réinitialisera la période. Continuer ?'
   ).subscribe(confirmed => {
    if (confirmed) this.applySiteSelection(site);
   });
   return;
  }
  this.applySiteSelection(site);
 }

 /** Applique réellement la sélection de site (appelé après confirmation si nécessaire). */
 private applySiteSelection(site: Site | null): void {
  this.selectedSite = site;
  this.selectedScenario = null;
  this.currentPeriode = null;
  this.currentJoursParSemaine = 6;
  this.needsRefresh = false;
  this.periodeHasChanges = false;
 }

 onEditScenario(scenario: Scenario): void {
  const changeScenario =
   this.selectedScenario &&
   this.selectedScenario.id_scenario !== scenario.id_scenario &&
   this.periodeHasChanges;

  if (changeScenario) {
   this.confirmDiscardChanges(
    'Vous avez des modifications non sauvegardées sur la période. Changer de scénario réinitialisera la période. Continuer ?'
   ).subscribe(confirmed => {
    if (confirmed) this.applyEditScenario(scenario);
   });
   return;
  }
  this.applyEditScenario(scenario);
 }

 /** Applique réellement l'édition de scénario (appelé après confirmation si nécessaire). */
 private applyEditScenario(scenario: Scenario): void {
  this.selectedScenario = scenario;
  this.currentPeriode = this.extractPeriode(scenario);
  this.currentJoursParSemaine = this.normalizeJoursParSemaine(scenario.nb_jours_semaine);
  this.needsRefresh = false;
  this.periodeHasChanges = false;
 }

 onRemoveScenario(scenario: Scenario): void {
  if (this.selectedScenario?.id_scenario === scenario.id_scenario) {
   this.selectedScenario = null;
  }
 }

 onPeriodeValidated(periode: Periode): void {
  this.currentPeriode = periode;
 }

 onRefreshTraficsRequested(): void {
  this.needsRefresh = false;
  this.traficsCalculerRef?.refresh();
 }

 onPeriodeChange(periode: Periode): void {
  this.currentPeriode = periode;
 }

 onAddScenario(): void {
  console.log('Nouveau scénario ajouté');
 }

 onComptagesChange(comptages: Comptage[]): void {
  this.currentComptages = comptages;
  console.log('[CalculComponent] comptages reçus du fils:', comptages);
 }

 onRefreshNeeded(): void {
  this.needsRefresh = true;
 }

 onJoursParSemaineChange(jours: JoursParSemaine): void {
  this.currentJoursParSemaine = jours;
  if (!this.selectedScenario) {
   return;
  }

  const scenarioId = this.selectedScenario.id_scenario ?? (this.selectedScenario as any).id;
  if (scenarioId === undefined || scenarioId === null) {
   return;
  }

  this.scenarioService.updateNbJoursSemaine(scenarioId, { nb_jours_semaine: jours }).subscribe({
   next: (updatedScenario) => {
    this.selectedScenario = {
     ...this.selectedScenario,
     ...updatedScenario,
     nb_jours_semaine: (updatedScenario as any).nb_jours_semaine ?? jours
    };
   },
   error: () => {
    // Keep UI value selected; persistence error is non-blocking for local recompute.
   }
  });
 }

 /**
 * Ouvre une ConfirmDialog pour valider l'abandon des modifications non sauvegardées.
 * Renvoie un Observable<boolean> : true si l'utilisateur confirme, false sinon.
 */
 private confirmDiscardChanges(message: string): Observable<boolean> {
  const data: ConfirmDialogData = {
   message,
   type: 'warning',
   yesAction: { label: 'Continuer' },
   noAction: { label: 'Annuler' },
  };
  return this.dialog
   .open(ConfirmDialogComponent, { data, width: '460px' })
   .afterClosed()
   .pipe(map(result => result?.event === true));
 }

 private normalizeJoursParSemaine(value?: number): JoursParSemaine {
  return value === 5 ? 5 : 6;
 }

 private extractPeriode(scenario: Scenario): Periode | null {
  if (scenario.periode?.dateDebut && scenario.periode?.dateFin && scenario.periode?.datePivot) {
   return scenario.periode;
  }
  const debut = (scenario as any).periode_debut ?? null;
  const fin  = (scenario as any).periode_fin ?? null;
  const pivot = (scenario as any).dt_mise_en_oeuvre ?? null;

  if (debut && fin && pivot) {
   return { dateDebut: debut, dateFin: fin, datePivot: pivot };
  }
  return null;
 }
}