import { Component, EventEmitter, OnInit, Input, Output } from '@angular/core';
import { FormControl } from '@angular/forms';
import { Observable } from 'rxjs';
import { map, startWith } from 'rxjs/operators';
import { Site } from '../../../../model/site.model';
import { UserService } from '../../../../service/user/user.service';
import { Scenario } from '../../models/scenario.model';
import { TrppuContextService } from '../../services/trppu-context.service';
import { DSRUser } from '../../../../model/user.model';

export interface SitesGroup {
  dex: string;
  list: Site[];
}

@Component({
  selector: 'app-trppu-select-site',
  templateUrl: './trppu-select-site.component.html',
  styleUrls: ['./trppu-select-site.component.css']
})
export class TrppuSelectSiteComponent implements OnInit {

  sites: Site[] = [];
  sitesGroups: SitesGroup[] = [];
  selectedSite: Site | null = null;
  myControl = new FormControl();
  filteredSites: Observable<Site[]>;
  filteredGroups: Observable<SitesGroup[]>;

  @Input()  scenario: Scenario | null = null;
  @Output() siteSelected = new EventEmitter<Site | null>();
  private utilisateurConnecter : DSRUser;
  constructor(private userService: UserService,private trppuContextService:TrppuContextService) {}

  ngOnInit(): void {
    this.userService.getUserTrppuAsync().subscribe(user=>{
      this.utilisateurConnecter=user

      // this.sites = this.userService.getUserTrppu().geoPerimetre.sort((a,b) => a.libelle.toLowerCase().localeCompare(b.libelle.toLowerCase()));
      // this.sitesGroups = this.userService.getUserTrppu().geoPerimetreGroups;
      this.sites = this.utilisateurConnecter.geoPerimetre.sort((a,b) => a.libelle.toLowerCase().localeCompare(b.libelle.toLowerCase()));
      this.sitesGroups = this.utilisateurConnecter.geoPerimetreGroups;
      this.filteredSites = this.myControl.valueChanges.pipe(
        startWith(''),
        map(value => this.filterSite(value))
      );
      this.filteredGroups = this.myControl.valueChanges.pipe(
        startWith(''),
        map(value => this.filterGroup(value))
      );
      // if(sessionStorage.getItem('selectedSite')) {
      //   const selectedSiteCode = sessionStorage.getItem('selectedSite');
      if(this.trppuContextService.getCoRegate()) {
        const selectedSiteCode = this.trppuContextService.getCoRegate();
        const site = this.userService.getSiteFromGeoPerimetre(selectedSiteCode);
        if(site) {
          this.onSelectionChange(site);
        }
      };
      // Rajout de test
    });
  }

  private filterGroup(value: string): SitesGroup[] {
    if (value) {
      return this.sitesGroups
        .map(group => ({dex: group.dex, list: this.filterGroupSite(group.list, value)}) )
        .filter(group => group.list.length > 0);
    }

    return this.sitesGroups;
  }

  private filterSite(value: string): Site[] {
    const filterValue = value?.toLowerCase();

    return this.sites.filter(site =>
      site.codeRegate.toLowerCase().includes(filterValue) ||
      site.codeRoc.toLowerCase().includes(filterValue) ||
      site.libelle.toLowerCase().includes(filterValue) ||
      site.denomination.toLowerCase().includes(filterValue)
    );
  }

  private filterGroupSite(opt: Site[], value: string): Site[] {
    console.log("[Debug select-site-toLowerCase] ")
    console.log(value);
    console.log(typeof(value))
    const filterValue = value?.toLowerCase();

    return opt.filter(site =>
      site.codeRegate.toLowerCase().includes(filterValue) ||
      site.codeRoc.toLowerCase().includes(filterValue) ||
      site.libelle.toLowerCase().includes(filterValue) ||
      site.denomination.toLowerCase().includes(filterValue)
    );
  }

  displaySite(site: Site): string {
    return site ? `${site.libelleCourt ? site.libelleCourt : site.denomination ? site.denomination : site.libelle}` : '';
  }

  onSelectionChange(site: Site): void {
    this.userService.getSiteDataByRegate(site.codeRegate).subscribe({
      next: (siteData) => {
        const enrichedSite: Site = {
          ...site,
          codeType: siteData?.codeType ?? site.codeType,
          typelibelleCourt: siteData?.typelibelleCourt ?? siteData?.typeLibelleCourt ?? site.typelibelleCourt,
          dex: siteData?.libelleAnimationFonctNationale ?? site.dex
        };
        this.selectedSite = enrichedSite;
        this.siteSelected.emit(enrichedSite);
        this.trppuContextService.setCoRegate(site.codeRegate)
        // sessionStorage.setItem('selectedSite', site.codeRegate);
      },
      error: () => {
        this.selectedSite = site;
        this.siteSelected.emit(site);
      }
    });
  }

  clear(): void {
    // sessionStorage.setItem('selectedSite', null);
    // sessionStorage.removeItem('selectedScenario');
    this.trppuContextService.setCoRegate(null); // si ce dernier est null alors je fait la suppression
    // rajout pour le netoiement du siteselectionner (ICI le coderegate)
    this.trppuContextService.setIdScenario(null);
    this.selectedSite = null;
    this.siteSelected.emit(null);
  }
}
