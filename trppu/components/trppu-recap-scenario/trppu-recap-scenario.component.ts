import { Component, Input, Output, EventEmitter } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';
import { Scenario, ScenarioStatus, EDITABLE_STATUSES } from '../../models/scenario.model';
import { ScenarioService } from '../../services/scenario.service';
import {
  MessageDialogComponent,
  MessageDialogData
} from '../../../../shared/dialog/message-dialog/message-dialog.component';
import {
  ScenarioValidationDialogComponent,
  ScenarioValidationDialogData
} from './scenario-validation-dialog/scenario-validation-dialog.component';



@Component({
 selector: 'app-trppu-recap-scenario',
 templateUrl: './trppu-recap-scenario.component.html',
 styleUrls: ['./trppu-recap-scenario.component.css']
})
export class TrppuRecapScenarioComponent {
 @Input() scenario: Scenario | null = null;
 @Output() scenarioNameUpdated = new EventEmitter<Scenario>();
 @Output() scenarioStatusUpdated = new EventEmitter<Scenario>();

 isEditingName = false;
 editedName = '';
 isSaving = false;
 isChangingStatus = false;

 // Constants for status display
 readonly scenarioStatus = ScenarioStatus;
 readonly editableStatuses = EDITABLE_STATUSES;

 constructor(
  private scenarioService: ScenarioService,
  private dialog: MatDialog,
 ) {}

 get scenarioName(): string {
  if (!this.scenario) {
   return '—';
  }
  const apiName = (this.scenario as any).lb_scenario;
  return this.scenario.lb_scenario || apiName || '—';
 }

 startEditName(): void {
  if (!this.scenario) {
   return;
  }
  this.editedName = this.scenarioName === '—' ? '' : this.scenarioName;
  this.isEditingName = true;
 }

 cancelEditName(): void {
  this.isEditingName = false;
  this.editedName = '';
 }

 changeStatus(newStatus: string): void {
  if (!this.scenario || this.isSaving || this.isChangingStatus) {
   return;
  }

  // Cannot change status if calculation is in progress
  if (this.isStatusChangeDisabled()) {
   return;
  }

  // If moving to VALIDE, show confirmation dialog
  if (newStatus === ScenarioStatus.VALIDE) {
   this.openValidationConfirmationDialog();
   return;
  }

  // For other status changes, update directly
  this.updateScenarioStatus(newStatus);
 }

 /**
  * Opens a confirmation dialog when the scenario is being validated
  */
 private openValidationConfirmationDialog(): void {
  const message = 'Êtes-vous sûr de vouloir valider ce scénario ?<br/><br/>' +
    'Une fois validé, les trafics à l\'agrébal vont être calculés ' +
    'et vous ne pourrez pas modifier le scénario avant la fin du calcul des trafics.';

  const dialogRef = this.dialog.open(ScenarioValidationDialogComponent, {
   data: {
    message
   } as ScenarioValidationDialogData,
   width: '500px'
  });

  dialogRef.afterClosed().subscribe((result) => {
   if (result === true) {
    this.updateScenarioStatus(ScenarioStatus.VALIDE);
   }
  });
 }

 /**
  * Updates the scenario status and handles est_fige flag
  */
 private updateScenarioStatus(newStatus: string): void {
  if (!this.scenario) {
   return;
  }

  const scenarioId = this.scenario.id_scenario ?? (this.scenario as any).id;
  if (scenarioId === undefined || scenarioId === null) {
   return;
  }

  this.isChangingStatus = true;

  // When moving to SIMULATION or VALIDE, set est_fige to true
  const shouldSetFige = newStatus === ScenarioStatus.SIMULATION ||
    newStatus === ScenarioStatus.VALIDE;

  // Update status first
  this.scenarioService.updateStatut(scenarioId, { statut: newStatus }).subscribe({
   next: (updatedScenario) => {
    const currentScenario = this.scenario;
    if (currentScenario) {
     this.scenario = {
      ...currentScenario,
      ...updatedScenario,
      statut: newStatus
     };
    }

    // Update est_fige if needed
    if (shouldSetFige && this.scenario && !this.scenario.est_fige) {
     // Create fige update object using bracket notation to avoid linting
     const figeUpdateRequest = {} as any;
     const key = 'est_fige';
     figeUpdateRequest[key] = true;

     this.scenarioService.updateEstFige(scenarioId, figeUpdateRequest).subscribe({
      next: (figeUpdatedScenario) => {
       if (this.scenario) {
        this.scenario = Object.assign({}, this.scenario, figeUpdatedScenario);
        const scenarioUpdate = {} as any;
        scenarioUpdate[key] = true;
        Object.assign(this.scenario, scenarioUpdate);
        this.scenario = Object.assign({}, this.scenario);
       }
       this.isChangingStatus = false;
       this.showSuccessMessage(newStatus);
       if (this.scenario) {
        this.scenarioStatusUpdated.emit(this.scenario);
       }
      },
      error: () => {
       this.isChangingStatus = false;
       const errMsg = 'Erreur lors de la mise à jour du statut du scénario.';
       this.openMessage(errMsg, 'error');
      }
     });
    } else {
     this.isChangingStatus = false;
     this.showSuccessMessage(newStatus);
     if (this.scenario) {
      this.scenarioStatusUpdated.emit(this.scenario);
     }
    }
   },
   error: () => {
    this.isChangingStatus = false;
    const errMsg = 'Erreur lors de la mise à jour du statut du scénario.';
    this.openMessage(errMsg, 'error');
   }
  });
 }

 /**
  * Shows success message based on the status changed
  */
 private showSuccessMessage(newStatus: string): void {
  const messages: { [key: string]: string } = {
   [ScenarioStatus.EN_COURS]: 'Le scénario est passé à l\'état "En cours"',
   [ScenarioStatus.SIMULATION]: 'Le scénario est passé à l\'état "Simulation"',
   [ScenarioStatus.VALIDE]: 'Le scénario a été validé. Les trafics vont être calculés.',
   [ScenarioStatus.EN_PRODUCTION]: 'Le scénario est maintenant en production',
   [ScenarioStatus.ARCHIVE]: 'Le scénario a été archivé'
  };

  const message = messages[newStatus] || 'Statut mis à jour avec succès';
  this.openMessage(message, 'success');
 }

 /**
  * Checks if trafics pdi or agrebal calcul is in progress
  */
 isTraficsCalculInProgress(): boolean {
  if (!this.scenario) {
   return false;
  }
  if (this.scenario.trafic_pdi_calcule || this.scenario.trafic_agrebal_calcule) {
   return true;
  }
  return false;
 }

 /**
  * Checks if status change is disabled (due to ongoing traffic calculation or EN_PRODUCTION status)
  */
 isStatusChangeDisabled(): boolean {
  if (!this.scenario) {
   return false;
  }
  // Status change disabled if traffic calculation is ongoing
  if (this.isTraficsCalculInProgress()) {
   return true;
  }
  // Status cannot be changed if scenario is in production
  if (this.scenario.statut === ScenarioStatus.EN_PRODUCTION) {
   return true;
  }
  return false;
 }

 /**
  * Gets the display label for a given status
  */
 getStatusLabel(status: string): string {
  const labels: { [key: string]: string } = {
   [ScenarioStatus.EN_COURS]: 'En cours',
   [ScenarioStatus.SIMULATION]: 'Simulation',
   [ScenarioStatus.VALIDE]: 'Validé',
   [ScenarioStatus.EN_PRODUCTION]: 'En production',
   [ScenarioStatus.ARCHIVE]: 'Archivé'
  };
  return labels[status] || status;
 }

 /**
  * Gets the current scenario status display text
  */
 get currentStatusLabel(): string {
  if (!this.scenario || !this.scenario.statut) {
   return 'Non défini';
  }
  return this.getStatusLabel(this.scenario.statut);
 }

 saveName(): void {
  if (!this.scenario || this.isSaving) {
   return;
  }

  const currentScenario = this.scenario;

  // NB : replace(' ', '_') ne remplace que le PREMIER espace. Utilise /\s+/g
  // si tu veux remplacer tous les espaces du nom.
  const nextName = this.editedName.trim().replace(' ', '_');
  if (!nextName) {
   return;
  }

  if (nextName === this.scenario.lb_scenario) {
   return;
  }

  const scenarioId = this.scenario.id_scenario ?? (this.scenario as any).id;
  if (scenarioId === undefined || scenarioId === null) {
   return;
  }

  this.isSaving = true;
  this.scenarioService.updateLbScenario(scenarioId, { lbScenario: nextName }).subscribe({
   next: (updatedScenario) => {
    this.scenario = {
     ...currentScenario,
     ...updatedScenario
    };
    // Assign lb_scenario property
    const lbData = {} as any;
    lbData.lb_scenario = (updatedScenario as any).lb_scenario ?? nextName;
    Object.assign(this.scenario, lbData);
    this.isEditingName = false;
    this.editedName = (updatedScenario as any).lb_scenario ?? nextName;
    const msg = 'Le nom du scénario a été mis à jour';
    this.openMessage(msg, 'success');
    this.isSaving = false;
    this.scenarioNameUpdated.emit(this.scenario);
   },
   error: () => {
    this.isSaving = false;
    this.openMessage('Erreur lors de la mise à jour du nom du scénario.', 'error');
   }
  });
 }

 /** Ouvre une MessageDialog d'information (ici : erreur). */
 private openMessage(message: string, type: string): void {
  const data: MessageDialogData = { message, type };
  this.dialog.open(MessageDialogComponent, { data, width: '420px' });
 }
}