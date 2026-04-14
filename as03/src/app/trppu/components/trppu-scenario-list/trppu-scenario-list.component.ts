import { Component, Input, Output, EventEmitter, OnChanges, SimpleChanges } from '@angular/core';
import { Scenario } from '../../models/scenario.model';
import { ScenarioService } from '../../services/scenario.service';

@Component({
  selector: 'app-trppu-scenario-list',
  templateUrl: './trppu-scenario-list.component.html',
  styleUrls: ['./trppu-scenario-list.component.css']
})
export class TrppuScenarioListComponent implements OnChanges {

  @Input() siteId: number | null = null;

  @Output() editScenario = new EventEmitter<Scenario>();
  @Output() removeScenario = new EventEmitter<Scenario>();

  scenarios: Scenario[] = [];
  displayedColumns: string[] = [
    'nom', 'dateCreation', 'statut', 'dateValidation', 'dateMo', 'dateProduction', 'actions'
  ];

  constructor(private scenarioService: ScenarioService) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['siteId']) {
      this.loadScenarios();
    }
  }

  private loadScenarios(): void {
    if (this.siteId === null) {
      this.scenarios = [];
      return;
    }
    this.scenarioService.getScenariosBySiteId(this.siteId).subscribe({
      next: (data) => this.scenarios = data,
      error: () => this.scenarios = []
    });
  }

  onEdit(scenario: Scenario): void {
    this.editScenario.emit(scenario);
  }

  onRemove(scenario: Scenario): void {
    const confirmed = confirm(`Supprimer le scénario "${scenario.nom}" ?`);
    if (confirmed) {
      this.scenarios = this.scenarios.filter(s => s.id !== scenario.id);
      this.removeScenario.emit(scenario);
    }
  }
}
