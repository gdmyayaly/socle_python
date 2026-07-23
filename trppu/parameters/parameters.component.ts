import { Component, OnInit } from '@angular/core';
import { Site } from '../../../model/site.model';
import { Scenario } from '../models/scenario.model';
import { PeriodeNeutralisee } from '../models/periode-neutralisee.model';
import { VariationTrafic } from '../models/variation-trafic.model';
import { ProduitExclu } from '../models/produit-exclu.model';
import { ScenarioService } from '../services/scenario.service';
import { Neutralisation } from '../models/neutralisation.model';
import { TrppuContextService } from '../services/trppu-context.service';

export interface ParamPayload {
  site: Site | null;
  periodesNeutralisees: PeriodeNeutralisee[];
  variationsTrafic: VariationTrafic[];
  neutralisationsPeak: PeriodeNeutralisee[];
  neutralisationsSecondaires: PeriodeNeutralisee[];
  produitsExclus: ProduitExclu[];
}
@Component({
  selector: 'app-parameters',
  templateUrl: './parameters.component.html',
  styleUrls: ['./parameters.component.scss']
})
export class ParametersComponent implements OnInit {
  selectedSite: Site | null = null;
  selectedScenario: Scenario | null = null;

  currentPeriodesNeutralisees: PeriodeNeutralisee[] = [];
  currentVariationsTrafic: VariationTrafic[] = [];
  currentNeutralisationsPeak: PeriodeNeutralisee[] = [];
  currentNeutralisationsSecondaires: PeriodeNeutralisee[] = [];
  currentProduitsExclus: ProduitExclu[] = [];
  currentNeutralisations : Neutralisation[] = [];
  userIdrh : string ="";
  constructor(private scenarioService: ScenarioService,private trppuContextService : TrppuContextService) { }

  ngOnInit(): void {
    // if(sessionStorage.getItem('selectedScenario')) {
    //   const selectedScenarioId = sessionStorage.getItem('selectedScenario');
    if(this.trppuContextService.getIdScenario()) {
      const selectedScenarioId = this.trppuContextService.getIdScenario();

      this.scenarioService.getScenario(selectedScenarioId).subscribe({
        next: (data) => this.selectedScenario = data,
        error: () => this.selectedScenario = null
      });
    }
  }

  onSiteSelected(site: Site | null): void {
    this.selectedSite = site;
    this.resetChildrenState();
  }



  onVariationsChange(variations: VariationTrafic[]): void {
    this.currentVariationsTrafic = variations;
  }

  onNeutralisationsPeakChange(periodes: PeriodeNeutralisee[]): void {
    this.currentNeutralisationsPeak = periodes;
  }

  onNeutralisationsSecondairesChange(periodes: PeriodeNeutralisee[]): void {
    this.currentNeutralisationsSecondaires = periodes;
  }

  onProduitsChange(produits: ProduitExclu[]): void {
    this.currentProduitsExclus = produits;
  }

  private resetChildrenState(): void {
    this.currentPeriodesNeutralisees = [];
    this.currentVariationsTrafic = [];
    this.currentNeutralisationsPeak = [];
    this.currentNeutralisationsSecondaires = [];
    this.currentProduitsExclus = [];
  }
   onPeriodesNeutraliseesChange(list: Neutralisation[]): void {
  this.currentNeutralisations = list;
  console.log('[CalculComponent] neutralisations reçues du fils:', list);
 }
}
