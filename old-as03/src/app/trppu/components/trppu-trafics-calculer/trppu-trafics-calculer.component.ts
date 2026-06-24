import { Component, Input, Output, EventEmitter, OnChanges, SimpleChanges } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { TraficCalcule } from '../../models/trafic-calcule.model';

export type JoursParSemaine = 6 | 7;

@Component({
  selector: 'app-trppu-trafics-calculer',
  templateUrl: './trppu-trafics-calculer.component.html',
  styleUrls: ['./trppu-trafics-calculer.component.css']
})
export class TrppuTraficsCalculerComponent implements OnChanges {

  @Input() scenarioId: number | null = null;

  @Output() joursParSemaineChange = new EventEmitter<JoursParSemaine>();

  readonly joursOptions: JoursParSemaine[] = [6, 7];
  joursParSemaine: JoursParSemaine = 7;

  trafics: TraficCalcule[] = [];
  displayedColumns: string[] = [
    'produit', 'volumeBrut', 'constateBrut', 'previsionnelBrut', 'traficMoyenHebdo'
  ];

  constructor(private http: HttpClient) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['scenarioId']) {
      this.loadTrafics();
    }
  }

  onJoursParSemaineChange(value: JoursParSemaine): void {
    this.joursParSemaine = value;
    console.log('[TrppuTraficsCalculerComponent] jours/semaine changé:', value);
    this.joursParSemaineChange.emit(value);
  }

  private loadTrafics(): void {
    if (this.scenarioId === null) {
      this.trafics = [];
      return;
    }
    this.http.get<TraficCalcule[]>('assets/trppu/trafics-calcules.json').subscribe({
      next: (data) => this.trafics = data,
      error: () => this.trafics = []
    });
  }
}
