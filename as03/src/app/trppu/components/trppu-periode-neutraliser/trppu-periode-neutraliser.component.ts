import { Component, Input, Output, EventEmitter, OnChanges, SimpleChanges } from '@angular/core';
import { PeriodeNeutralisee } from '../../models/periode-neutralisee.model';
import { ParamService } from '../../services/param.service';

@Component({
  selector: 'app-trppu-periode-neutraliser',
  templateUrl: './trppu-periode-neutraliser.component.html',
  styleUrls: ['./trppu-periode-neutraliser.component.css']
})
export class TrppuPeriodeNeutraliserComponent implements OnChanges {

  @Input() siteId: string | null = null;
  @Output() periodesChange = new EventEmitter<PeriodeNeutralisee[]>();

  periodes: PeriodeNeutralisee[] = [];
  displayedColumns: string[] = ['dateDebut', 'dateFin', 'motif', 'actions'];

  newDateDebut = '';
  newDateFin = '';
  newMotif = '';

  private nextId = 1;

  constructor(private paramService: ParamService) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['siteId']) {
      this.loadPeriodes();
    }
  }

  private loadPeriodes(): void {
    if (!this.siteId) {
      this.periodes = [];
      return;
    }
    this.paramService.getPeriodesNeutralisees(this.siteId).subscribe(data => {
      this.periodes = data;
      this.nextId = data.reduce((max, p) => Math.max(max, p.id), 0) + 1;
    });
  }

  get canAdd(): boolean {
    return this.newDateDebut !== '' && this.newDateFin !== '';
  }

  onAdd(): void {
    if (!this.canAdd) return;
    const entry: PeriodeNeutralisee = {
      id: this.nextId++,
      dateDebut: this.newDateDebut,
      dateFin: this.newDateFin,
      motif: this.newMotif || undefined
    };
    this.periodes = [...this.periodes, entry];
    this.newDateDebut = '';
    this.newDateFin = '';
    this.newMotif = '';
    this.periodesChange.emit(this.periodes);
  }

  onRemove(entry: PeriodeNeutralisee): void {
    this.periodes = this.periodes.filter(p => p.id !== entry.id);
    this.periodesChange.emit(this.periodes);
  }
}
