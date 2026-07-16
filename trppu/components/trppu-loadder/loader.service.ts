// loader.service.ts
import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';
import { map } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class LoaderService {
 // Nombre de requêtes en cours, par route
 private requestCounts = new Map<string, number>();

 // Ensemble des routes actuellement en chargement
 private loadingRoutesSubject = new BehaviorSubject<Set<string>>(new Set());
 public readonly loadingRoutes$: Observable<Set<string>> =
  this.loadingRoutesSubject.asObservable();

 show(routeKey: string): void {
  const current = this.requestCounts.get(routeKey) ?? 0;
  this.requestCounts.set(routeKey, current + 1);
  this.emit();
 }

 hide(routeKey: string): void {
  const current = this.requestCounts.get(routeKey) ?? 0;
  if (current <= 1) {
   this.requestCounts.delete(routeKey); // on nettoie la Map
  } else {
   this.requestCounts.set(routeKey, current - 1);
  }
  this.emit();
 }

 /** État booléen pour une route précise */
 isLoadingForRoute$(routeKey: string): Observable<boolean> {
  return this.loadingRoutes$.pipe(map((set) => set.has(routeKey)));
 }

 private emit(): void {
  // nouvelle référence Set => déclenche bien l'émission
  this.loadingRoutesSubject.next(new Set(this.requestCounts.keys()));
 }
}