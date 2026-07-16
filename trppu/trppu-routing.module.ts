import { NgModule } from '@angular/core';
import { RouterModule } from '@angular/router';
import { TrppComponent } from './trpp.component';
import { ParametersComponent } from './parameters/parameters.component';
import { ConfigpicComponent } from './configpic/configpic.component';
import { GeoperimetreComponent } from './geoperimetre/geoperimetre.component';
import { CalculTrppuComponent } from './calcul-trppu/calcul-trppu.component';

@NgModule({
 imports: [
  RouterModule.forChild([
   {
    path: '',
    component: TrppComponent,
    children: [
     {
      path: 'calculate',
      component: CalculTrppuComponent,
      data: {
       breadcrumb: {
        path: 'calculate',
        label: 'Calcul du TRPPU',
        icon: 'calculate'
       }
      }
     },
     {
      path: 'parameters',
      component: ParametersComponent,
      data: {
       breadcrumb: {
        label: 'Paramètres',
        icon: 'calendar_month'
       }
      }
     },
     {
      path: 'config-pic',
      component: ConfigpicComponent,
      data: {
       breadcrumb: {
        label: 'Configuration PIC',
        icon: 'settings'
       }
      }
     },
     {
      path: 'geoperimetre',
      component: GeoperimetreComponent,
      data: {
       breadcrumb: {
        label: 'Géopérimètre',
        icon: 'user'
       }
      }
     },
     { path: '', redirectTo: 'calculate', pathMatch: 'full' }
    ]
   }
  ])
 ],
 exports: [RouterModule]
})
export class TrppuRoutingModule {}