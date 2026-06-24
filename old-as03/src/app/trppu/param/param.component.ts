import { Component } from '@angular/core';
import { Site } from '../models/site.model';
import { ProduitExclu } from '../models/produit-exclu.model';
import { PeriodeNeutralisee } from '../models/periode-neutralisee.model';
import { VariationTrafic } from '../models/variation-trafic.model';

export interface ParamPayload {
  site: Site | null;
  periodesNeutralisees: PeriodeNeutralisee[];
  variationsTrafic: VariationTrafic[];
  neutralisationsPeak: PeriodeNeutralisee[];
  neutralisationsSecondaires: PeriodeNeutralisee[];
  produitsExclus: ProduitExclu[];
}

@Component({
  selector: 'app-param',
  templateUrl: './param.component.html',
  styleUrls: ['./param.component.css']
})
export class ParamComponent {

  selectedSite: Site | null = null;

  currentPeriodesNeutralisees: PeriodeNeutralisee[] = [];
  currentVariationsTrafic: VariationTrafic[] = [];
  currentNeutralisationsPeak: PeriodeNeutralisee[] = [];
  currentNeutralisationsSecondaires: PeriodeNeutralisee[] = [];
  currentProduitsExclus: ProduitExclu[] = [];

  lastValidatedPayload: ParamPayload | null = null;

  onSiteSelected(site: Site | null): void {
    this.selectedSite = site;
    this.resetChildrenState();
  }

  onPeriodesNeutraliseesChange(periodes: PeriodeNeutralisee[]): void {
    this.currentPeriodesNeutralisees = periodes;
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

  onValidate(): void {
    const payload: ParamPayload = {
      site: this.selectedSite,
      periodesNeutralisees: this.currentPeriodesNeutralisees,
      variationsTrafic: this.currentVariationsTrafic,
      neutralisationsPeak: this.currentNeutralisationsPeak,
      neutralisationsSecondaires: this.currentNeutralisationsSecondaires,
      produitsExclus: this.currentProduitsExclus
    };
    this.lastValidatedPayload = payload;
    console.log('[ParamComponent] Payload prêt pour soumission:', payload);
  }

  private resetChildrenState(): void {
    this.currentPeriodesNeutralisees = [];
    this.currentVariationsTrafic = [];
    this.currentNeutralisationsPeak = [];
    this.currentNeutralisationsSecondaires = [];
    this.currentProduitsExclus = [];
    this.lastValidatedPayload = null;
  }
}
