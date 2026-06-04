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

  @Output() selectScenario = new EventEmitter<Scenario | null>();
  @Output() removeScenario = new EventEmitter<Scenario>();
  @Output() addScenario = new EventEmitter<Scenario>();

  scenarios: Scenario[] = [];
  selectedId: number | null = null;

  displayedColumns: string[] = [
    'select', 'lb_scenario', 'statut', 'version_scenario',
    'dt_creation', 'dt_validation', 'dt_mise_en_prod', 'periode', 'actions'
  ];

  // État de la modale de suppression
  scenarioToDelete: Scenario | null = null;

  constructor(private scenarioService: ScenarioService) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['siteId']) {
      this.selectedId = null;
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

  // --- Sélection ---

  onSelect(scenario: Scenario): void {
    this.selectedId = scenario.id_scenario;
    this.selectScenario.emit(scenario);
  }

  isSelected(scenario: Scenario): boolean {
    return this.selectedId === scenario.id_scenario;
  }

  // --- Suppression (modale de confirmation) ---

  askRemove(scenario: Scenario, event: Event): void {
    event.stopPropagation();
    this.scenarioToDelete = scenario;
  }

  cancelRemove(): void {
    this.scenarioToDelete = null;
  }

  confirmRemove(): void {
    const target = this.scenarioToDelete;
    if (!target) return;

    this.scenarios = this.scenarios.filter(s => s.id_scenario !== target.id_scenario);

    if (this.selectedId === target.id_scenario) {
      this.selectedId = null;
      this.selectScenario.emit(null);
    }

    this.removeScenario.emit(target);
    this.scenarioToDelete = null;
  }

  // --- Ajout (création directe avec un nom par défaut) ---

  onAdd(): void {
    const maxId = this.scenarios.reduce((max, s) => Math.max(max, s.id_scenario), 0);
    const now = new Date().toISOString().slice(0, 19);

    const newScenario: Scenario = {
      id_scenario: maxId + 1,
      co_regate: '',
      lb_scenario: this.defaultName(),
      co_roc: this.siteId ?? '',
      statut: 'EN COURS',
      dt_creation: now,
      dt_validation: null,
      dt_mise_en_prod: null,
      periode_debut: null,
      periode_fin: null,
      periode_realise_debut: null,
      periode_realise_fin: null,
      periode_prev_debut: null,
      periode_prev_fin: null,
      nb_jours_semaine: 5,
      id_pic_version: 1,
      version_scenario: 1,
      est_fige: false
    };

    this.scenarios = [...this.scenarios, newScenario];
    this.addScenario.emit(newScenario);
    this.onSelect(newScenario);
  }

  /** Génère un nom par défaut unique : « Nouveau scénario », puis « Nouveau scénario 2 », etc. */
  private defaultName(): string {
    const base = 'Nouveau scénario';
    const existing = new Set(this.scenarios.map(s => s.lb_scenario));
    if (!existing.has(base)) return base;
    let i = 2;
    while (existing.has(`${base} ${i}`)) i++;
    return `${base} ${i}`;
  }
}
