import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RouterModule, Routes } from '@angular/router';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatInputModule } from '@angular/material/input';
import { MatTableModule } from '@angular/material/table';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatSliderModule } from '@angular/material/slider';
import { TrppuComponent } from './trppu.component';
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
import { CalculComponent } from './calcul/calcul.component';
import { ParamComponent } from './param/param.component';
import { ConfigComponent } from './config/config.component';

const routes: Routes = [
  {
    path: '',
    component: TrppuComponent,
    children: [
      { path: '', redirectTo: 'calcul', pathMatch: 'full' },
      { path: 'calcul', component: CalculComponent },
      { path: 'param', component: ParamComponent },
      { path: 'config', component: ConfigComponent },
    ]
  }
];

@NgModule({
  declarations: [
    TrppuComponent,
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
    CalculComponent,
    ParamComponent,
    ConfigComponent
  ],
  imports: [
    CommonModule,
    FormsModule,
    RouterModule.forChild(routes),
    MatFormFieldModule,
    MatSelectModule,
    MatButtonModule,
    MatIconModule,
    MatInputModule,
    MatTableModule,
    MatCheckboxModule,
    MatSliderModule
  ]
})
export class TrppuModule { }
