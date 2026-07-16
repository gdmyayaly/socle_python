import { Component, Input, Output, EventEmitter, OnChanges, SimpleChanges } from '@angular/core';
import { Produit } from '../../models/produit.model';
import { Comptage } from '../../models/comptage.model';
import { ScenarioService } from '../../services/scenario.service';
import { ComptageService } from '../../services/comptage.service';

@Component({
  selector: 'app-trppu-comptage',
  templateUrl: './trppu-comptage.component.html',
  styleUrls: ['./trppu-comptage.component.css']
})
export class TrppuComptageComponent implements OnChanges {

  @Input() scenarioId: number | null = null;
  @Input() idRh:       string | null = null;
  @Input() produits: Produit[] | null = null;

  @Output() comptagesChange = new EventEmitter<Comptage[]>();
  @Output() dirtyChange = new EventEmitter<boolean>();

  resolvedProduits: Produit[] = [];
  comptages: Comptage[] = [];
  displayedColumns: string[] = ['produit', 'valeur', 'actions'];

  newTraficId: string | null = null;
  newValeur: number | null = null;

  isDirty = false;

  private originalSnapshot = '';

  constructor(
    private scenarioService: ScenarioService,
    private comptageService: ComptageService
  ) {
    this.resolveProduits();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['scenarioId']) {
      this.loadComptages();
    }
  }

  private resolveProduits(): void {
    this.comptageService.listProduit().subscribe({
      next: (produits) => {
        this.resolvedProduits = produits && produits.length > 0 ? produits : [];
      }
    });
  }

  private loadComptages(): void {
    if (this.scenarioId === null) {
      this.comptages = [];
      this.saveSnapshot();
      return;
    }
    this.scenarioService.listComptages(this.scenarioId).subscribe({
      next: (data) => {
        this.comptages = data;
        this.saveSnapshot();
        this.checkDirty();
      }
    });
  }

  get availableProduits(): Produit[] {
    //const usedIds = new Set(this.comptages.map(c => c.co_produit));
    return this.resolvedProduits; //.filter(p => !usedIds.has(p.co_produit));
  }

  get canAdd(): boolean {
    return this.newValeur !== null && this.newValeur >= 0;
  }

  onAdd(): void {
    if (!this.canAdd) return;

    const payload = {
      co_produit: this.newTraficId!,
      dt_comptage: this.toDateStr(new Date().getTime()),
      nb_produit: this.newValeur!,
      id_rh: this.idRh || ''
    };

    this.scenarioService.addComptage(this.scenarioId, payload).subscribe({
      next: (data) => {
        this.saveSnapshot();
        this.checkDirty();
      },
      complete: () => {
        this.loadComptages();
            this.newValeur = null;

      },
      error: (error) => {
        console.error('Error adding comptage:', error);
      }
    });

    this.emitChange();
  }

  onRemove(comptage: Comptage): void {
    this.comptages = this.comptages.filter(c => c.co_produit !== comptage.co_produit);
    this.emitChange();
  }

  onValeurInput(comptage: Comptage, value: number): void {
    comptage.nb_produit = value;
  }

  onValeurBlur(): void {
    this.emitChange();
  }

  onDraftChange(): void {
    this.emitChange();
  }

  private emitChange(): void {
    this.comptagesChange.emit(this.comptages);
    this.checkDirty();
  }

  private saveSnapshot(): void {
    this.originalSnapshot = JSON.stringify(this.comptages.map(c => ({ t: c.co_produit, v: c.nb_produit })));
    this.isDirty = false;
    this.dirtyChange.emit(false);
  }

  private checkDirty(): void {
    const current = JSON.stringify(this.comptages.map(c => ({ t: c.co_produit, v: c.nb_produit })));
    const dirty = current !== this.originalSnapshot;
    if (dirty !== this.isDirty) {
      this.isDirty = dirty;
      this.dirtyChange.emit(dirty);
    }
  }

  private toDateStr(ts: number): string {
    const d = new Date(ts);
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }
}
