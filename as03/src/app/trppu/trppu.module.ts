import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule, Routes } from '@angular/router';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatTableModule } from '@angular/material/table';
import { TrppuComponent } from './trppu.component';
import { TrppuSelectSiteComponent } from './components/trppu-select-site/trppu-select-site.component';
import { TrppuScenarioListComponent } from './components/trppu-scenario-list/trppu-scenario-list.component';
import { TrppuRecapScenarioComponent } from './components/trppu-recap-scenario/trppu-recap-scenario.component';
import { TrppuPeriodeScenarioComponent } from './components/trppu-periode-scenario/trppu-periode-scenario.component';
import { CalculComponent } from './calcul/calcul.component';
import { ParamComponent } from './param/param.component';
import { ConfigComponent } from './config/config.component';

const routes: Routes = [
  {
    path: '',
    component: TrppuComponent,
    children: [
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
    CalculComponent,
    ParamComponent,
    ConfigComponent
  ],
  imports: [
    CommonModule,
    RouterModule.forChild(routes),
    MatFormFieldModule,
    MatSelectModule,
    MatButtonModule,
    MatIconModule,
    MatTableModule
  ]
})
export class TrppuModule { }
