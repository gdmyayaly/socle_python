import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { ExternalModule } from '../../external.module';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatAutocompleteModule } from '@angular/material/autocomplete';
import { NgxSliderModule } from '@angular-slider/ngx-slider';

import { TrppuRoutingModule } from './trppu-routing.module';
import { TrppComponent } from './trpp.component';
import { GeoperimetreComponent } from './geoperimetre/geoperimetre.component';
import { ParametersComponent } from './parameters/parameters.component';
import { ConfigpicComponent } from './configpic/configpic.component';
import { CalculTrppuComponent } from './calcul-trppu/calcul-trppu.component';
import { TrppuSelectSiteComponent } from './components/trppu-select-site/trppu-select-site.component';
import { TrppuScenarioListComponent } from './components/trppu-scenario-list/trppu-scenario-list.component';
import { TrppuRecapScenarioComponent } from './components/trppu-recap-scenario/trppu-recap-scenario.component';
import { TrppuPeriodeScenarioComponent } from './components/trppu-periode-scenario/trppu-periode-scenario.component';
import { TrppuComptageComponent } from './components/trppu-comptage/trppu-comptage.component';
import { TrppuTraficsCalculerComponent } from './components/trppu-trafics-calculer/trppu-trafics-calculer.component';
import { TrppuPeriodeNeutraliserComponent } from './components/trppu-periode-neutraliser/trppu-periode-neutraliser.component';
import { TrppuVariationTraficComponent } from './components/trppu-variation-trafic/trppu-variation-trafic.component';
import { TrppuNeutralisationPeakComponent } from './components/trppu-neutralisation-peak/trppu-neutralisation-peak.component';
import { TrppuNeutralisationSecondaireComponent } from './components/trppu-neutralisation-secondaire/trppu-neutralisation-secondaire.component';
import { TrppuProduitAExclureComponent } from './components/trppu-produit-a-exclure/trppu-produit-a-exclure.component';
// import { TrppuLoadderComponent } from './components/trppu-loadder/trppu-loadder.component';
import { HTTP_INTERCEPTORS } from '@angular/common/http';
import { LoaderInterceptor } from './components/trppu-loadder/loader.interceptor';
import { SessionIhmInterceptor } from './services/session-ihm.interceptor';
import { TrppuVariationPrevisionnelleComponent } from './components/trppu-variation-previsionnelle/trppu-variation-previsionnelle.component';
@NgModule({
  declarations: [
    TrppComponent,
    GeoperimetreComponent,
    ParametersComponent,
    ConfigpicComponent,
    CalculTrppuComponent,
    TrppuSelectSiteComponent,
    TrppuScenarioListComponent,
    TrppuRecapScenarioComponent,
    TrppuPeriodeScenarioComponent,
    TrppuComptageComponent,
    TrppuTraficsCalculerComponent,
    TrppuPeriodeNeutraliserComponent,
    TrppuVariationTraficComponent,
    TrppuNeutralisationPeakComponent,
    TrppuNeutralisationSecondaireComponent,
    TrppuProduitAExclureComponent,
    // TrppuLoadderComponent,
    TrppuVariationPrevisionnelleComponent
  ],
  imports: [
    CommonModule,
    RouterModule,
    ExternalModule,
    MatExpansionModule,
    MatAutocompleteModule,
    NgxSliderModule,
    TrppuRoutingModule
  ],
  exports: [
    CommonModule,
    ExternalModule,
    TrppuRoutingModule
  ],
  providers:[
    {
      provide:HTTP_INTERCEPTORS,
      useClass : LoaderInterceptor,
      multi:true // pour le multi interceptor
    },
    {
      provide:HTTP_INTERCEPTORS,
      useClass : SessionIhmInterceptor,
      multi:true // id_session_ihm sur tous les appels /trppu-api (traçabilité Kibana)
    }
  ]
})
export class TrppuModule { }
