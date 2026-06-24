import { Component } from '@angular/core';
import { Site } from '../models/site.model';
import { Scenario } from '../models/scenario.model';
import { Periode } from '../models/periode.model';
import { Comptage } from '../models/comptage.model';
import { JoursParSemaine } from '../components/trppu-trafics-calculer/trppu-trafics-calculer.component';

@Component({
  selector: 'app-calcul',
  templateUrl: './calcul.component.html',
  styleUrls: ['./calcul.component.css']
})
export class CalculComponent {

  selectedSite: Site | null = null;
  selectedScenario: Scenario | null = null;
  needsRefresh = false;
  periodeHasChanges = false;
  comptageHasChanges = false;
  currentComptages: Comptage[] = [];
  currentPeriode: Periode | null = null;
  currentJoursParSemaine: JoursParSemaine = 7;

  onSiteSelected(site: Site | null): void {
    if (this.periodeHasChanges) {
      const ok = confirm(
        'Vous avez des modifications non sauvegardées sur la période. Ce changement réinitialisera la période. Continuer ?'
      );
      if (!ok) return;
    }
    this.selectedSite = site;
    this.selectedScenario = null;
    this.needsRefresh = false;
    this.periodeHasChanges = false;
  }

  onEditScenario(scenario: Scenario): void {
    if (this.selectedScenario && this.selectedScenario.id !== scenario.id && this.periodeHasChanges) {
      const ok = confirm(
        'Vous avez des modifications non sauvegardées sur la période. Changer de scénario réinitialisera la période. Continuer ?'
      );
      if (!ok) return;
    }
    this.selectedScenario = scenario;
    this.needsRefresh = false;
    this.periodeHasChanges = false;
  }

  onRemoveScenario(scenario: Scenario): void {
    if (this.selectedScenario?.id === scenario.id) {
      this.selectedScenario = null;
    }
  }

  onPeriodeValidated(periode: Periode): void {
    this.currentPeriode = periode;
  }

  onAddScenario(): void {
    console.log('Nouveau scénario ajouté');
  }

  onComptagesChange(comptages: Comptage[]): void {
    this.currentComptages = comptages;
    console.log('[CalculComponent] comptages reçus du fils:', comptages);
  }

  onRefreshNeeded(): void {
    this.needsRefresh = true;
  }

  onJoursParSemaineChange(jours: JoursParSemaine): void {
    this.currentJoursParSemaine = jours;
    console.log('[CalculComponent] jours/semaine reçus du fils:', jours);
  }
}
