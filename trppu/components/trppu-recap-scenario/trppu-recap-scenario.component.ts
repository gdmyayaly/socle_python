import { Component, Input, Output, EventEmitter } from '@angular/core';
import { MatSnackBar } from '@angular/material/snack-bar';
import { MatDialog } from '@angular/material/dialog';

import { Scenario } from '../../models/scenario.model';
import { ScenarioService } from '../../services/scenario.service';
import { MessageDialogComponent, MessageDialogData } from '../../../../shared/dialog/message-dialog/message-dialog.component';



@Component({
 selector: 'app-trppu-recap-scenario',
 templateUrl: './trppu-recap-scenario.component.html',
 styleUrls: ['./trppu-recap-scenario.component.css']
})
export class TrppuRecapScenarioComponent {
 @Input() scenario: Scenario | null = null;
 @Output() scenarioNameUpdated = new EventEmitter<Scenario>();

 isEditingName = false;
 editedName = '';
 isSaving = false;

 constructor(
  private scenarioService: ScenarioService,
  private snackBar: MatSnackBar,
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
  if (!this.scenario || this.isSaving) {
   return;
  }
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
     ...updatedScenario,
     lb_scenario: (updatedScenario as any).lb_scenario ?? nextName
    };
    (this.scenario as any).lb_scenario = (updatedScenario as any).lb_scenario ?? nextName;
    this.isEditingName = false;
    this.editedName = (updatedScenario as any).lb_scenario ?? nextName;
    this.snackBar.open('Le nom du scénario a été mis à jour', 'Fermer', { duration: 3000 });
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