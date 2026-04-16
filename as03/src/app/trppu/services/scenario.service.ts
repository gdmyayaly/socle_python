import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { Scenario } from '../models/scenario.model';

@Injectable({ providedIn: 'root' })
export class ScenarioService {

  constructor(private http: HttpClient) {}

  getScenariosBySiteId(siteId: string): Observable<Scenario[]> {
    return this.http.get<Scenario[]>(`assets/scenarios-site-${siteId}.json`);
  }
}
