import { Component, Input, Output, EventEmitter, OnChanges, SimpleChanges, OnInit } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';

import { Scenario } from '../../models/scenario.model';
import { ScenarioService } from '../../services/scenario.service';
import { Site } from '../../../../model/site.model';
import { UserService } from '../../../../service/user/user.service';
import { TrppuContextService } from '../../services/trppu-context.service';
import { DSRUser } from '../../../../model/user.model';
import { ConfirmDialogComponent, ConfirmDialogData } from '../../../../shared/dialog/confirm-dialog/confirm-dialog.component';
import { MessageDialogComponent, MessageDialogData } from '../../../../shared/dialog/message-dialog/message-dialog.component';



@Component({
 selector: 'app-trppu-scenario-list',
 templateUrl: './trppu-scenario-list.component.html',
 styleUrls: ['./trppu-scenario-list.component.css']
})
export class TrppuScenarioListComponent implements OnChanges, OnInit {

 @Input() siteId: string | null = null;
 @Input() site: Site | null = null;
 @Input() scenario: Scenario | null = null;

 @Output() editScenario = new EventEmitter<Scenario>();
 @Output() removeScenario = new EventEmitter<Scenario>();
 @Output() addScenario = new EventEmitter<void>();

 scenarios: Scenario[] = [];
 displayedColumns: string[] = [
  'nom', 'dateCreation', 'statut', 'dateValidation', 'dateMo', 'dateProduction', 'actions'
 ];
 private utilisateurConnecter: DSRUser;

 constructor(
  private scenarioService: ScenarioService,
  private userService: UserService,
  private trppuContextService: TrppuContextService,
  private dialog: MatDialog,
 ) {}

 ngOnChanges(changes: SimpleChanges): void {
  if (changes['siteId'] || changes['site']) {
   this.loadScenarios();
  }
 }

 ngOnInit(): void {
  this.userService.getUserTrppuAsync().subscribe(user => this.utilisateurConnecter = user);
 }

 refreshScenarios(): void {
  this.loadScenarios();
 }

 private loadScenarios(): void {
  const codeRegate = this.site?.codeRegate ?? this.siteId;
  if (!codeRegate) {
   this.scenarios = [];
   return;
  }

  this.scenarioService.listScenarios({ co_regate: codeRegate }).subscribe({
   next: (data) => this.scenarios = data,
   complete: () => {
    if (this.trppuContextService.getIdScenario()) {
     const selectedScenarioId = this.trppuContextService.getIdScenario();
     const foundScenario = this.scenarios.find(s => s.id_scenario === selectedScenarioId);
     if (foundScenario) {
      this.onEdit(foundScenario);
     }
    }
   },
   error: () => this.scenarios = []
  });
 }

 onEdit(scenario: Scenario): void {
  this.editScenario.emit(scenario);
  this.trppuContextService.setIdScenario(scenario.id_scenario);
 }

 onRemove(scenario: Scenario): void {
  const data: ConfirmDialogData = {
   message: `Supprimer le scénario "${scenario.lb_scenario}" ?`,
   type: 'warning',
   yesAction: { label: 'Oui, supprimer' },
   noAction: { label: 'Annuler' },
  };

  this.dialog
   .open(ConfirmDialogComponent, { data, width: '420px' })
   .afterClosed()
   .subscribe(result => {
    if (result?.event === true) {
     this.deleteScenario(scenario);
    }
   });
 }

 /** Suppression effective côté serveur, appelée uniquement après confirmation. */
 private deleteScenario(scenario: Scenario): void {
  this.scenarioService.deleteScenario(scenario.id_scenario).subscribe({
   next: (deleted) => {
    this.scenarios = this.scenarios.filter(s => s.id_scenario !== scenario.id_scenario);
    this.removeScenario.emit(deleted);
   },
   complete: () => {
    this.editScenario.emit(null);
    this.trppuContextService.setIdScenario(null); // pour faire la suppression
   },
   error: () => this.openMessage('Erreur lors de la suppression du scénario.', 'error')
  });
 }

 onDuplicate(scenario: Scenario): void {
  const data: ConfirmDialogData = {
   message: `Dupliquer le scénario "${scenario.lb_scenario}" ?`,
   type: 'info',
   yesAction: { label: 'Oui, dupliquer' },
   noAction: { label: 'Annuler' },
  };

  this.dialog
   .open(ConfirmDialogComponent, { data, width: '420px' })
   .afterClosed()
   .subscribe(result => {
    if (result?.event === true) {
     this.duplicateScenario(scenario);
    }
   });
 }

 /** Duplication effective côté serveur, appelée uniquement après confirmation. */
 private duplicateScenario(scenario: Scenario): void {
  const newLabel = 'Copie de ' + scenario.lb_scenario;
  const payload = {
   lb_scenario: newLabel.substring(0, 20) // Limite à 20 caractères pour le label du scénario,
  };
  this.scenarioService.duplicateScenario(scenario.id_scenario, payload).subscribe({
   next: (created) => {
    this.scenarios = [...this.scenarios, created];
    this.addScenario.emit();
    this.editScenario.emit(created);
    this.trppuContextService.setIdScenario(created.id_scenario);
   },
   error: () => this.openMessage('Erreur lors de la duplication du scénario.', 'error')
  });
 }

 onAdd(): void {
  if (!this.site) {
   this.openMessage('Aucun site sélectionné.', 'error');
   return;
  }

  const today  = new Date();
  const todayStr = String(today.getDate()).padStart(2, '0')
         + String(today.getMonth() + 1).padStart(2, '0')
         + today.getFullYear().toString();
  const nomStr  = this.site.libelleCourt ? this.site.libelleCourt : this.site.denomination ?? this.site.libelle;
  const nom   = nomStr.replace(' ', '_').substring(0, 11) + '-' + todayStr;

  const payload = {
   lb_scenario: nom.trim(),
   co_roc:  this.site.codeRoc,
   co_regate: this.site.codeRegate,
   lb_regate: this.site.libelle,
   type_site: this.site.typelibelleCourt,
   id_rh:   this.utilisateurConnecter.idRh
  };

  this.scenarioService.createScenario(payload).subscribe({
   next: (created) => {
    this.scenarios = [...this.scenarios, created];
    this.addScenario.emit();
    this.editScenario.emit(created);
    this.trppuContextService.setIdScenario(created.id_scenario);
   },
   error: () => this.openMessage('Erreur lors de la création du scénario.', 'error')
  });
 }

 /** Ouvre une MessageDialog d'information (succès / erreur). */
 private openMessage(message: string, type: string): void {
  const data: MessageDialogData = { message, type };
  this.dialog.open(MessageDialogComponent, { data, width: '420px' });
 }
}