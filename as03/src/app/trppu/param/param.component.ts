import { Component } from '@angular/core';
import { Site } from '../models/site.model';

@Component({
  selector: 'app-param',
  templateUrl: './param.component.html',
  styleUrls: ['./param.component.css']
})
export class ParamComponent {

  selectedSite: Site | null = null;

  onSiteSelected(site: Site | null): void {
    this.selectedSite = site;
  }
}
