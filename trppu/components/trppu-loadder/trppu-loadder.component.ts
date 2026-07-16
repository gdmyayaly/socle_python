import { Component, OnInit } from '@angular/core';
import { Router, NavigationEnd } from '@angular/router';
import { combineLatest, Observable } from 'rxjs';
import { filter, map, startWith } from 'rxjs/operators';
import { LoaderService } from './loader.service';

@Component({
  selector: 'app-trppu-loadder',
  templateUrl: './trppu-loadder.component.html',
  styleUrls: ['./trppu-loadder.component.scss']
})
export class TrppuLoadderComponent implements OnInit {

loading$: Observable<boolean>;

 constructor(
  private loaderService: LoaderService,
  private router: Router
 ) {
  // URL courante, normalisée comme dans l'intercepteur
  const currentUrl$ = this.router.events.pipe(
   filter((e): e is NavigationEnd => e instanceof NavigationEnd),
   map((e) => e.urlAfterRedirects.split('?')[0].split('#')[0]),
   startWith(this.router.url.split('?')[0].split('#')[0])
  );

  this.loading$ = combineLatest([
   currentUrl$,
   this.loaderService.loadingRoutes$,
  ]).pipe(
   map(([url, loadingRoutes]) => loadingRoutes.has(url))
  );
 }
 ngOnInit(): void {
   
 }
}
