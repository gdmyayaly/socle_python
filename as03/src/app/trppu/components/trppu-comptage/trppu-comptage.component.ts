import { Component, Input, Output, EventEmitter, OnChanges, SimpleChanges } from '@angular/core';
import { Trafic } from '../../models/trafic.model';
import { Comptage } from '../../models/comptage.model';
import { ComptageService } from '../../services/comptage.service';

@Component({
  selector: 'app-trppu-comptage',
  templateUrl: './trppu-comptage.component.html',
  styleUrls: ['./trppu-comptage.component.css']
})
export class TrppuComptageComponent implements OnChanges {

  @Input() scenarioId: number | null = null;
  @Output() comptagesChange = new EventEmitter<Comptage[]>();
  @Output() dirtyChange = new EventEmitter<boolean>();

  trafics: Trafic[] = [];
  comptages: Comptage[] = [];
  displayedColumns: string[] = ['trafic', 'valeur', 'actions'];

  /** Champs de la ligne d'ajout inline */
  newTraficId: number | null = null;
  newValeur: number | null = null;

  isDirty = false;

  private nextId = 1;
  private originalSnapshot = '';

  constructor(private comptageService: ComptageService) {
    this.comptageService.getTrafics().subscribe({
      next: (data) => this.trafics = data
    });
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['scenarioId']) {
      this.loadComptages();
    }
  }

  private loadComptages(): void {
    if (this.scenarioId === null) {
      this.comptages = [];
      this.saveSnapshot();
      return;
    }
    this.comptageService.getComptagesByScenarioId(this.scenarioId).subscribe({
      next: (data) => {
        this.comptages = data;
        this.nextId = data.reduce((max, c) => Math.max(max, c.id), 0) + 1;
        this.saveSnapshot();
        this.checkDirty();
      }
    });
  }

  get availableTrafics(): Trafic[] {
    const usedIds = new Set(this.comptages.map(c => c.trafic.id));
    return this.trafics.filter(t => !usedIds.has(t.id));
  }

  get canAdd(): boolean {
    return this.newTraficId !== null && this.newValeur !== null && this.newValeur >= 0;
  }

  onAdd(): void {
    if (!this.canAdd) return;

    const trafic = this.trafics.find(t => t.id === this.newTraficId);
    if (!trafic) return;

    const comptage: Comptage = {
      id: this.nextId++,
      trafic,
      valeur: this.newValeur!
    };

    this.comptages = [...this.comptages, comptage];
    this.newTraficId = null;
    this.newValeur = null;
    this.emitChange();
  }

  onRemove(comptage: Comptage): void {
    this.comptages = this.comptages.filter(c => c.id !== comptage.id);
    this.emitChange();
  }

  onValeurChange(comptage: Comptage, value: number): void {
    comptage.valeur = value;
    this.emitChange();
  }

  onPrecharger(): void {
    this.loadComptages();
  }

  private emitChange(): void {
    this.comptagesChange.emit(this.comptages);
    this.checkDirty();
  }

  private saveSnapshot(): void {
    this.originalSnapshot = JSON.stringify(this.comptages.map(c => ({ t: c.trafic.id, v: c.valeur })));
    this.isDirty = false;
    this.dirtyChange.emit(false);
  }

  private checkDirty(): void {
    const current = JSON.stringify(this.comptages.map(c => ({ t: c.trafic.id, v: c.valeur })));
    const dirty = current !== this.originalSnapshot;
    if (dirty !== this.isDirty) {
      this.isDirty = dirty;
      this.dirtyChange.emit(dirty);
    }
  }
}
