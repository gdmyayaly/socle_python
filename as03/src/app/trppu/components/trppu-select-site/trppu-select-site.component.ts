import { Component, EventEmitter, OnInit, Output } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Site } from '../../models/site.model';

@Component({
  selector: 'app-trppu-select-site',
  templateUrl: './trppu-select-site.component.html',
  styleUrls: ['./trppu-select-site.component.css']
})
export class TrppuSelectSiteComponent implements OnInit {

  sites: Site[] = [];
  selectedSite: Site | null = null;

  @Output() siteSelected = new EventEmitter<Site | null>();

  constructor(private http: HttpClient) {}

  ngOnInit(): void {
    this.http.get<Site[]>('assets/sites.json').subscribe(data => {
      this.sites = data;
    });
  }

  onSelectionChange(site: Site): void {
    this.selectedSite = site;
    this.siteSelected.emit(site);
  }

  clear(): void {
    this.selectedSite = null;
    this.siteSelected.emit(null);
  }
}
