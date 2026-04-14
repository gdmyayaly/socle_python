import { Component } from '@angular/core';
import { Site } from './models/site.model';
import { Scenario } from './models/scenario.model';
import { Periode } from './models/periode.model';

@Component({
  selector: 'app-trppu',
  templateUrl: './trppu.component.html',
  styleUrls: ['./trppu.component.css']
})
export class TrppuComponent {

  selectedSite: Site | null = null;
  selectedScenario: Scenario | null = null;
  needsRefresh = false;
  periodeHasChanges = false;

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
    console.log('Période validée:', periode);
  }

  onRefreshNeeded(): void {
    this.needsRefresh = true;
  }
}
