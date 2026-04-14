import { Component, EventEmitter, Output } from '@angular/core';
import { Site } from '../../models/site.model';

@Component({
  selector: 'app-trppu-select-site',
  templateUrl: './trppu-select-site.component.html',
  styleUrls: ['./trppu-select-site.component.css']
})
export class TrppuSelectSiteComponent {

  sites: Site[] = [
    { id: 1, nom: 'Site A' },
    { id: 2, nom: 'Site B' },
    { id: 3, nom: 'Site C' },
  ];

  selectedSite: Site | null = null;

  @Output() siteSelected = new EventEmitter<Site | null>();

  onSelectionChange(site: Site | null): void {
    this.selectedSite = site;
    this.siteSelected.emit(site);
  }

  clear(): void {
    this.selectedSite = null;
    this.siteSelected.emit(null);
  }
}
