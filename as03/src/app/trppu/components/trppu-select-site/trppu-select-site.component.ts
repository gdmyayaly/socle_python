import { Component, EventEmitter, OnDestroy, OnInit, Output } from '@angular/core';
import { FormControl } from '@angular/forms';
import { BehaviorSubject, combineLatest, of, Observable, Subject } from 'rxjs';
import { catchError, map, startWith, takeUntil } from 'rxjs/operators';
import { Site } from '../../../../model/site.model';
import { UserService } from '../../../../service/user/user.service';

interface SiteGroup {
  type: string;
  sites: Site[];
}

@Component({
  selector: 'app-trppu-select-site',
  templateUrl: './trppu-select-site.component.html',
  styleUrls: ['./trppu-select-site.component.css']
})
export class TrppuSelectSiteComponent implements OnInit, OnDestroy {
  selectedSite: Site | null = null;
  myControl = new FormControl();
  filteredGroups: Observable<SiteGroup[]>;
  loading = false;

  @Output() siteSelected = new EventEmitter<Site | null>();

  // Source de vérité de la liste : ré-émet à chaque site enrichi
  private sitesSubject = new BehaviorSubject<Site[]>([]);
  private destroy$ = new Subject<void>();

  // Pense à namespacer par utilisateur si plusieurs comptes partagent le navigateur
  private readonly CACHE_KEY = 'trppu-enriched-sites';
  private readonly CACHE_TTL_MS = 30 * 60 * 1000; // 30 min

  constructor(private userService: UserService) {}

  ngOnInit(): void {
    const baseSites: Site[] = this.userService.getUser().geoPerimetre
      .sort((a, b) => a.libelle.toLowerCase().localeCompare(b.libelle.toLowerCase()));

    // Flux d'affichage : se recalcule quand le TEXTE ou la LISTE change
    this.filteredGroups = combineLatest([
      this.myControl.valueChanges.pipe(startWith('')),
      this.sitesSubject.asObservable()
    ]).pipe(
      map(([value, sites]) => {
        const filterValue = typeof value === 'string' ? value : '';
        return this.filterAndGroup(filterValue, sites);
      })
    );

    // 1) Cache valide → on l'utilise, zéro appel réseau
    const cached = this.readCache();
    if (cached && cached.length > 0) {
      this.sitesSubject.next(cached);
      return;
    }

    // 2) Pas de cache → on affiche immédiatement la liste de base...
    if (baseSites.length === 0) {
      this.sitesSubject.next([]);
      return;
    }
    this.sitesSubject.next(baseSites);

    // ...puis on enrichit au fil de l'eau
    this.loading = true;
    let completed = 0;

    baseSites.forEach(site => {
      this.userService.getSiteDataByRegate(site.codeRegate).pipe(
        map(data => this.enrichSite(site, data)),
        catchError(() => of(site)),       // échec d'un appel → on garde le site de base
        takeUntil(this.destroy$)
      ).subscribe(enriched => {
        // On remplace le site concerné et on ré-émet → la liste se rafraîchit
        const updated = this.sitesSubject.value.map(s =>
          s.codeRegate === enriched.codeRegate ? enriched : s
        );
        this.sitesSubject.next(updated);

        completed++;
        if (completed === baseSites.length) {
          this.loading = false;
          this.writeCache(this.sitesSubject.value); // on persiste une fois tout enrichi
        }
      });
    });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  // --- Enrichissement ---
  private enrichSite(site: Site, siteData: any): Site {
    return {
      ...site,
      codeType: siteData?.codeType ?? site.codeType,
      typelibelleCourt: siteData?.typelibelleCourt ?? siteData?.typeLibelleCourt ?? site.typelibelleCourt,
      dex: siteData?.libelleAnimationFonctNationale ?? site.dex
    };
  }

  // --- Filtre + regroupement ---
  private filterAndGroup(value: string, sites: Site[]): SiteGroup[] {
    const filterValue = value?.toLowerCase() ?? '';

    const filtered = sites.filter(site =>
      site.codeRegate.toLowerCase().includes(filterValue) ||
      site.codeRoc.toLowerCase().includes(filterValue) ||
      site.libelle.toLowerCase().includes(filterValue) ||
      site.denomination.toLowerCase().includes(filterValue)
    );

    return this.groupSites(filtered);
  }

  private groupSites(sites: Site[]): SiteGroup[] {
    const groups = new Map<string, Site[]>();
    sites.forEach(site => {
      const key = site.typelibelleCourt || 'Autres'; // <-- critère de regroupement
      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key)!.push(site);
    });

    return Array.from(groups.entries())
      .map(([type, sitesGroup]) => ({ type, sites: sitesGroup }))
      .sort((a, b) => a.type.toLowerCase().localeCompare(b.type.toLowerCase()));
  }

  // --- Tooltip : toutes les infos du site ---
  tooltipFor(site: Site): string {
    const lines = [
      site.libelleCourt ? `Libellé court : ${site.libelleCourt}` : null,
      site.denomination ? `Dénomination : ${site.denomination}` : null,
      site.libelle ? `Libellé : ${site.libelle}` : null,
      site.codeRegate ? `Code Regate : ${site.codeRegate}` : null,
      site.codeRoc ? `Code Roc : ${site.codeRoc}` : null,
      site.typelibelleCourt ? `Type : ${site.typelibelleCourt}` : null,
      site.codeType ? `Code type : ${site.codeType}` : null,
      site.dex ? `DEX : ${site.dex}` : null
    ].filter(Boolean);
    return lines.join('\n');
  }

  // --- Cache sessionStorage ---
  private readCache(): Site[] | null {
    try {
      const raw = sessionStorage.getItem(this.CACHE_KEY);
      if (!raw) {
        return null;
      }
      const parsed = JSON.parse(raw) as { timestamp: number; sites: Site[] };
      if (Date.now() - parsed.timestamp > this.CACHE_TTL_MS) {
        sessionStorage.removeItem(this.CACHE_KEY); // expiré
        return null;
      }
      return parsed.sites;
    } catch {
      return null;
    }
  }

  private writeCache(sites: Site[]): void {
    try {
      sessionStorage.setItem(this.CACHE_KEY, JSON.stringify({ timestamp: Date.now(), sites }));
    } catch {
      // quota dépassé / storage indisponible → on ignore silencieusement
    }
  }

  // --- Optionnel : forcer le rechargement ---
  refresh(): void {
    sessionStorage.removeItem(this.CACHE_KEY);
    this.ngOnInit();
  }

  // --- Affichage / sélection ---
  displaySite(site: Site): string {
    return site
      ? `${site.libelleCourt ? site.libelleCourt : site.denomination ? site.denomination : site.libelle}`
      : '';
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