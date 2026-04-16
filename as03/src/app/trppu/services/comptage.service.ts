import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, of } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { Trafic } from '../models/trafic.model';
import { Comptage } from '../models/comptage.model';

@Injectable({ providedIn: 'root' })
export class ComptageService {

  constructor(private http: HttpClient) {}

  getTrafics(): Observable<Trafic[]> {
    return this.http.get<Trafic[]>('assets/trppu/trafics.json');
  }

  getComptagesByScenarioId(scenarioId: number): Observable<Comptage[]> {
    return this.http.get<Comptage[]>(`assets/trppu/comptages-scenario-${scenarioId}.json`).pipe(
      catchError(() => of([]))
    );
  }
}
