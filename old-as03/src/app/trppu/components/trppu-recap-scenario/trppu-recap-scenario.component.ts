import { Component, Input } from '@angular/core';
import { Scenario } from '../../models/scenario.model';

@Component({
  selector: 'app-trppu-recap-scenario',
  templateUrl: './trppu-recap-scenario.component.html',
  styleUrls: ['./trppu-recap-scenario.component.css']
})
export class TrppuRecapScenarioComponent {
  @Input() scenario: Scenario | null = null;
}
