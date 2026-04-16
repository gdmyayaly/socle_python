import { Component, Input, Output, EventEmitter, OnChanges, SimpleChanges } from '@angular/core';
import { Scenario } from '../../models/scenario.model';
import { ScenarioService } from '../../services/scenario.service';

@Component({
  selector: 'app-trppu-scenario-list',
  templateUrl: './trppu-scenario-list.component.html',
  styleUrls: ['./trppu-scenario-list.component.css']
})
export class TrppuScenarioListComponent implements OnChanges {

  @Input() siteId: string | null = null;

  @Output() editScenario = new EventEmitter<Scenario>();
  @Output() removeScenario = new EventEmitter<Scenario>();
  @Output() addScenario = new EventEmitter<void>();

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

  onAdd(): void {
    const nom = prompt('Nom du nouveau scénario :');
    if (!nom || !nom.trim()) return;

    const today = new Date().toISOString().slice(0, 10);
    const maxId = this.scenarios.reduce((max, s) => Math.max(max, s.id), 0);

    const newScenario: Scenario = {
      id: maxId + 1,
      nom: nom.trim(),
      dateCreation: today,
      statut: 'Brouillon',
      dateValidation: null,
      dateMo: null,
      dateProduction: null,
      periode: null
    };

    this.scenarios = [...this.scenarios, newScenario];
    this.addScenario.emit();
  }
}
