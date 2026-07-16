import {
 Component, Input, Output, EventEmitter,
 OnChanges, SimpleChanges, ElementRef, ViewChild, AfterViewInit
} from '@angular/core';
import { MatSnackBar } from '@angular/material/snack-bar';
import { Periode } from '../../models/periode.model';
import { ScenarioService } from '../../services/scenario.service';


/** Pas d'écart du slider en millisecondes (1 jour par défaut) */
const STEP_MS = 1 * 24 * 60 * 60 * 1000;

/** Fenêtre affichée autour de la période sélectionnée */
const FLEX_RANGE_MONTHS = 6; // pour les périodes >= 1 an
const FLEX_RANGE_WEEKS = 6; // pour les périodes < 1 an
const FLEX_RANGE_DAYS = 10; // pour les périodes < 1 mois
const FLEX_RANGE_DAYS_W = 5; // pour les périodes < 1 mois

/** Écart maximum autorisé entre début et fin (en années) */
const MAX_RANGE_YEARS = 2;
const MAX_RANGE_YEAR = 365 - 2; // -2 pour éviter les problèmes de mois bissextiles
const MAX_RANGE_DAYS = MAX_RANGE_YEARS * 365 - 2; // -2 pour éviter les problèmes de mois bissextiles

interface Notification {
 message: string;
 type: 'info' | 'warn' | 'success';
}

export type JoursParSemaine = 5 | 6;

@Component({
 selector: 'app-trppu-periode-scenario',
 templateUrl: './trppu-periode-scenario.component.html',
 styleUrls: ['./trppu-periode-scenario.component.css']
})
export class TrppuPeriodeScenarioComponent implements OnChanges, AfterViewInit {

 @Input() dateMin = '2025-01-01';
 @Input() dateMax = '2030-12-31';
 @Input() dateMinLimit = '2015-01-01';
 @Input() dateMaxLimit = '2030-12-31';
 @Input() datePivot: string | null = null;
 @Input() periode: Periode | null = null;
 @Input() joursParSemaine: JoursParSemaine = 6;
 @Input() scenarioId: number | null = null;
 @Input() idRh: string | null = null;

 @Output() periodeValidated = new EventEmitter<Periode>();
 @Output() refreshNeeded = new EventEmitter<void>();
 @Output() dirtyChange = new EventEmitter<boolean>();

 @ViewChild('track') trackRef!: ElementRef<HTMLDivElement>;

 minTs = 0;
 maxTs = 0;
 todayTs = 0;
 startTs = 0;
 dmoTs = 0;
 endTs = 0;

 startDateStr = '';
 dmoDateStr = '';
 endDateStr = '';

 editingStart = false;
 editingDmo = false;
 editingEnd = false;
 editStartValue = '';
 editDmoValue = '';
 editEndValue = '';
 private backupStartStr = '';
 private backupDmoStr = '';
 private backupEndStr = '';
 isDirty = false;
 isValidated = false;
 notifications: Notification[] = [];
 dragging: 'start' | 'dmo' | 'end' | null = null;

 /** Notifications en attente pendant le drag, émises au relâchement */
 private pendingNotifications: Notification[] = [];

 private trackLeft = 0;
 private trackWidth = 0;

 private boundMouseMove = this.onMouseMove.bind(this);
 private boundMouseUp = this.onMouseUp.bind(this);
 private boundTouchMove = this.onTouchMove.bind(this);
 private boundTouchEnd = this.onTouchEnd.bind(this);

 constructor(
  private scenarioService: ScenarioService,
  private snackBar: MatSnackBar
 ) {}

 ngOnChanges(changes: SimpleChanges): void {
  if (changes.dateMin || changes.dateMax || changes.dateMinLimit || changes.dateMaxLimit || changes.periode) {
   this.init();
  }
 }

 ngAfterViewInit(): void {
  this.updateTrackDimensions();
 }

 private init(): void {
  const minLimitTs = this.getMinLimitTs();
  const maxLimitTs = this.getMaxLimitTs();

  this.todayTs = this.getTodayTs();

  if (this.periode) {
   this.startTs = this.clampToLimits(this.toTs(this.periode.dateDebut));
   this.endTs = this.clampToLimits(this.toTs(this.periode.dateFin));
   this.dmoTs = this.clampToLimits(this.toTs(this.periode.datePivot));
  } else {
   // Défaut : intervalle incluant aujourd'hui (début = aujourd'hui, fin = aujourd'hui + MAX_RANGE_YEARS - un jour)
   const now = new Date();
   this.dmoTs = this.getTodayTs();

   const defaultStart = new Date();
   defaultStart.setDate(now.getDate() - MAX_RANGE_YEAR);
   this.startTs = this.snapFromMin(this.clampWithBounds(defaultStart.getTime(), minLimitTs, maxLimitTs), minLimitTs);

   const defaultEnd = new Date();
   defaultEnd.setDate(now.getDate() + MAX_RANGE_YEAR);
   this.endTs = this.snapFromMin(this.clampWithBounds(defaultEnd.getTime(), minLimitTs, maxLimitTs), minLimitTs);

   this.setDefaultDmo();
  }

  if (this.startTs > this.endTs) {
   this.endTs = this.startTs;
  }

  this.updateFlexibleBounds();
  this.enforceMaxRange('end');
  this.syncStrings();
  this.isDirty = false;
  this.isValidated = false;
  this.notifications = [];
 }

 // ── Positions en pourcentage ──

 get startPercent(): number {
  return this.toPercent(this.startTs);
 }

 get dmoPercent(): number {
  return this.toPercent(this.dmoTs);
 }

 get endPercent(): number {
  return this.toPercent(this.endTs);
 }

 get rangePercent(): number {
  return this.endPercent - this.startPercent;
 }

 /** Position du marqueur "Aujourd'hui" : suit la borne début si avant, ou la borne fin si après */
 get todayPercent(): number {
  const todayTs = this.getTodayTs();
  if (todayTs < this.startTs) return this.toPercent(this.startTs);
  if (todayTs > this.endTs) return this.toPercent(this.endTs);
  return this.toPercent(todayTs);
 }

 /** true si aujourd'hui est avant la période sélectionnée */
 get todayBeforePeriode(): boolean {
  return this.getTodayTs() < this.startTs;
 }

 /** true si aujourd'hui est après la période sélectionnée */
 get todayAfterPeriode(): boolean {
  return this.getTodayTs() > this.endTs;
 }

 get showMiddleThumb(): boolean {
  return this.getPeriodeTiming() !== 'future';
 }

 private getTodayTs(): number {
  const now = new Date();
  return new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
 }

 get todayStr(): string {
  const now = new Date();
  return this.toDateStr(now.getTime());
 }

 get selectedDays(): number {
  return Math.round((this.endTs - this.startTs) / (24 * 60 * 60 * 1000));
 }

 /** Durée lisible : ex "2 ans 3 mois 15 jours" */
 get durationLabel(): string {
  const start = new Date(this.startTs);
  const end = new Date(this.endTs);

  let years = end.getFullYear() - start.getFullYear();
  let months = end.getMonth() - start.getMonth();
  let days = end.getDate() - start.getDate();

  if (days < 0) {
   months--;
   const prevMonth = new Date(end.getFullYear(), end.getMonth(), 0);
   days += prevMonth.getDate();
  }
  if (months < 0) {
   years--;
   months += 12;
  }

  const parts: string[] = [];
  if (years > 0) parts.push(`${years} an${years > 1 ? 's' : ''}`);
  if (months > 0) parts.push(`${months} mois`);
  if (days > 0) parts.push(`${days} jour${days > 1 ? 's' : ''}`);

  return parts.length > 0 ? parts.join(' ') : '0 jour';
 }

 // ── Drag ──

 onThumbMouseDown(event: MouseEvent, which: 'start' | 'dmo' | 'end'): void {
  event.preventDefault();
  this.dragging = which;
  this.updateTrackDimensions();
  document.addEventListener('mousemove', this.boundMouseMove);
  document.addEventListener('mouseup', this.boundMouseUp);
 }

 onThumbTouchStart(event: TouchEvent, which: 'start' | 'dmo' | 'end'): void {
  this.dragging = which;
  this.updateTrackDimensions();
  document.addEventListener('touchmove', this.boundTouchMove, { passive: false });
  document.addEventListener('touchend', this.boundTouchEnd);
 }

 private onMouseMove(event: MouseEvent): void {
  if (!this.dragging) return;
  this.handleDrag(event.clientX);
 }

 private onTouchMove(event: TouchEvent): void {
  if (!this.dragging) return;
  event.preventDefault();
  this.handleDrag(event.touches[0].clientX);
 }

 private onMouseUp(): void {
  this.stopDrag();
 }

 private onTouchEnd(): void {
  this.stopDrag();
 }

 private stopDrag(): void {
  this.dragging = null;
  document.removeEventListener('mousemove', this.boundMouseMove);
  document.removeEventListener('mouseup', this.boundMouseUp);
  document.removeEventListener('touchmove', this.boundTouchMove);
  document.removeEventListener('touchend', this.boundTouchEnd);

  // Émettre les notifications accumulées pendant le drag
  for (const n of this.pendingNotifications) {
   this.addNotification(n.message, n.type);
  }
  this.pendingNotifications = [];
 }

 private handleDrag(clientX: number): void {
  const ratio = (clientX - this.trackLeft) / this.trackWidth;
  const rawTs = this.minTs + ratio * (this.maxTs - this.minTs);
  const snapped = this.snap(this.clamp(rawTs));
  let hasPeriodeChanged = false;

  if (this.dragging === 'start') {
   this.startTs = Math.min(snapped, this.endTs);
   this.enforceMaxRange('start');
   hasPeriodeChanged = true;
  }
  if (this.dragging === 'end') {
   this.endTs = Math.max(snapped, this.startTs);
   this.enforceMaxRange('end');
   hasPeriodeChanged = true;
  }
  if (this.dragging === 'dmo') {
   this.dmoTs = Math.min(this.endTs, Math.max(this.startTs, snapped));
   hasPeriodeChanged = true;
  }

  if (hasPeriodeChanged) {
   this.updateFlexibleBounds();
   this.syncMiddlePointerOnPeriodChange();
   this.syncStrings();
   this.markDirty();
  }
 }

 // ── Édition directe ──

 openEditStart(): void {
  this.backupStartStr = this.startDateStr;
  this.editStartValue = this.startDateStr;
  this.editingStart = true;
 }

 openEditDmo(): void {
  this.backupDmoStr = this.dmoDateStr;
  this.editDmoValue = this.dmoDateStr;
  this.editingDmo = true;
 }

 openEditEnd(): void {
  this.backupEndStr = this.endDateStr;
  this.editEndValue = this.endDateStr;
  this.editingEnd = true;
 }

 confirmStartEdit(): void {
  this.applyStartDate(this.editStartValue);
 }

 cancelStartEdit(): void {
  this.editingStart = false;
  this.editStartValue = '';
 }

 confirmDmoEdit(): void {
  this.applyDmoDate(this.editDmoValue);
 }

 cancelDmoEdit(): void {
  this.editingDmo = false;
  this.editDmoValue = '';
 }

 confirmEndEdit(): void {
  this.applyEndDate(this.editEndValue);
 }

 cancelEndEdit(): void {
  this.editingEnd = false;
  this.editEndValue = '';
 }

 private applyDmoDate(value: string): void {
  const ts = this.toTs(value);
  if (isNaN(ts)) return;

  this.dmoTs = this.snap(Math.min(this.endTs, Math.max(this.startTs, ts)));
  this.editingDmo = false;
  this.editDmoValue = '';

  this.syncMiddlePointerOnPeriodChange();
  this.syncStrings();
  this.markDirty();
 }

 private applyStartDate(value: string): void {
  const ts = this.toTs(value);
  if (isNaN(ts)) return;

  let adjusted = false;
  let newTs = ts;

  if (newTs < this.minTs) {
   newTs = this.minTs;
   adjusted = true;
  }
  if (newTs > this.endTs) {
   newTs = this.endTs;
   adjusted = true;
  }

  this.startTs = this.snap(newTs);
  this.enforceMaxRange('start');
  this.updateFlexibleBounds();
  this.syncMiddlePointerOnPeriodChange();
  this.syncStrings();
  this.editingStart = false;
  this.editStartValue = '';
  this.markDirty();

  if (adjusted) {
   this.addNotification('La date de début a été ajustée pour rester dans les limites.', 'warn');
  }
 }

 private applyEndDate(value: string): void {
  const ts = this.toTs(value);
  if (isNaN(ts)) return;

  let adjusted = false;
  let newTs = ts;

  if (newTs > this.maxTs) {
   newTs = this.maxTs;
   adjusted = true;
  }
  if (newTs < this.startTs) {
   newTs = this.startTs;
   adjusted = true;
  }

  this.endTs = this.snap(newTs);
  this.enforceMaxRange('end');
  this.updateFlexibleBounds();
  this.syncMiddlePointerOnPeriodChange();
  this.syncStrings();
  this.editingEnd = false;
  this.editEndValue = '';
  this.markDirty();

  if (adjusted) {
   this.addNotification('La date de fin a été ajustée pour rester dans les limites.', 'warn');
  }
 }

 // ── Validation ──

 onValidate(): void {
  // Garde en tête : rien à faire sans scénario (évite d'afficher un feedback inutile).
  if (this.scenarioId === null) {
   this.addNotification('Aucun scénario sélectionné.', 'warn');
   return;
  }

  const p: Periode = {
   dateDebut: this.startDateStr,
   dateFin: this.endDateStr,
   datePivot: this.dmoDateStr
  };

  const payload = {
   periode_debut: p.dateDebut,
   periode_fin: p.dateFin,
   dt_mise_en_oeuvre: p.datePivot,
   nb_jours_semaine: this.joursParSemaine,
   id_rh:    this.idRh
  };

  this.snackBar.open('Validation de la période...', 'Fermer', { duration: 3000 });

  this.scenarioService.updateScenario(this.scenarioId, payload).subscribe({
   next: () => {
    this.periodeValidated.emit(p);
    this.isValidated = true;
    this.isDirty = false;
    this.dirtyChange.emit(false);
    this.addNotification('Période validée avec succès !', 'success');
   },
   error: (err) => {
    this.addNotification(
     err?.error?.detail ?? 'Erreur lors de la mise à jour de la période.',
     'warn'
    );
   }
  });
 }

 // ── Helpers ──

 private markDirty(): void {
  if (this.isValidated) {
   this.addNotification(
    'La période a été modifiée. Veuillez rafraîchir les données pour que le calcul se refasse.',
    'info'
   );
   this.refreshNeeded.emit();
  }
  this.isDirty = true;
  this.isValidated = false;
  this.dirtyChange.emit(true);
 }

 private addNotification(message: string, type: 'info' | 'warn' | 'success'): void {
  const notif: Notification = { message, type };
  this.notifications = [notif, ...this.notifications.slice(0, 1)];

  setTimeout(() => {
   this.notifications = this.notifications.filter(n => n !== notif);
  }, 5000);
 }

 /**
 * Vérifie que l'écart entre début et fin ne dépasse pas MAX_RANGE_YEARS.
 * Ajuste l'autre borne (celle qui n'a PAS été déplacée) et notifie.
 * @param moved 'start' si c'est le début qui vient d'être modifié, 'end' sinon
 */
 private enforceMaxRange(moved: 'start' | 'end'): void {
  const startDate = new Date(this.startTs);
  const endDate = new Date(this.endTs);

  const maxEndDate = new Date(startDate.getFullYear() + MAX_RANGE_YEARS, startDate.getMonth(), startDate.getDate());
  const minStartDate = new Date(endDate.getFullYear() - MAX_RANGE_YEARS, endDate.getMonth(), endDate.getDate());

  if (moved === 'start') {
   if (this.endTs > maxEndDate.getTime()) {
    this.endTs = this.snap(this.clamp(maxEndDate.getTime()));
    this.queueNotification(
     `L'écart maximum est de ${MAX_RANGE_YEARS} ans. La date de fin a été ajustée automatiquement.`,
     'warn'
    );
   }
  } else {
   if (this.startTs < minStartDate.getTime()) {
    this.startTs = this.snap(this.clamp(minStartDate.getTime()));
    this.queueNotification(
     `L'écart maximum est de ${MAX_RANGE_YEARS} ans. La date de début a été ajustée automatiquement.`,
     'warn'
    );
   }
  }
 }

 /**
 * Si on est en train de drag, accumule la notification pour l'afficher au relâchement.
 * Sinon (édition directe), l'affiche immédiatement.
 */
 private queueNotification(message: string, type: 'info' | 'warn' | 'success'): void {
  if (this.dragging) {
   // Éviter les doublons pendant un même drag
   if (!this.pendingNotifications.some(n => n.message === message)) {
    this.pendingNotifications.push({ message, type });
   }
  } else {
   this.addNotification(message, type);
  }
 }

 /** Arrondi au pas le plus proche */
 private snap(ts: number): number {
  return this.snapFromMin(ts, this.minTs);
 }

 private snapFromMin(ts: number, minTs: number): number {
  return Math.round((ts - minTs) / STEP_MS) * STEP_MS + minTs;
 }

 private syncStrings(): void {
  this.startDateStr = this.toDateStr(this.startTs);
  this.dmoDateStr = this.toDateStr(this.dmoTs);
  this.endDateStr = this.toDateStr(this.endTs);
 }

 private toPercent(ts: number): number {
  if (this.maxTs === this.minTs) return 0;
  return ((ts - this.minTs) / (this.maxTs - this.minTs)) * 100;
 }

 private clamp(ts: number): number {
  return Math.max(this.minTs, Math.min(this.maxTs, ts));
 }

 private clampToLimits(ts: number): number {
  return this.clampWithBounds(ts, this.getMinLimitTs(), this.getMaxLimitTs());
 }

 private clampWithBounds(ts: number, minTs: number, maxTs: number): number {
  return Math.max(minTs, Math.min(maxTs, ts));
 }

 private toTs(dateStr: string): number {
  return new Date(dateStr).getTime();
 }

 private toDateStr(ts: number): string {
  const d = new Date(ts);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
 }

 private updateTrackDimensions(): void {
  if (this.trackRef) {
   const rect = this.trackRef.nativeElement.getBoundingClientRect();
   this.trackLeft = rect.left;
   this.trackWidth = rect.width;
  }
 }

 private setDefaultDmo(): void {
  this.setDefaultDmoForCurrentPeriode();
 }

 private clampMoWithinPeriode(): void {
  this.dmoTs = this.snap(Math.min(this.endTs, Math.max(this.startTs, this.dmoTs)));
 }

 private syncMiddlePointerOnPeriodChange(): void {
  if(this.dmoTs < this.startTs) {
   this.dmoTs = this.startTs;
  }
  if(this.dmoTs > this.endTs) {
   this.dmoTs = this.endTs;
  }
  if(this.getTodayTs() < this.startTs) {
   this.dmoTs = this.startTs;
  }
 }

 private setDefaultDmoForCurrentPeriode(): void {
  const timing = this.getPeriodeTiming();

  if (timing === 'past') {
   this.dmoTs = this.startTs + (this.endTs - this.startTs) / 2;
  } else if (timing === 'current') {
   this.dmoTs = this.getTodayTs();
  } else {
   this.dmoTs = this.startTs;
  }

  this.clampMoWithinPeriode();
 }

 private getPeriodeTiming(): 'past' | 'current' | 'future' {
  const todayTs = this.getTodayTs();
  if (this.endTs < todayTs) {
   return 'past';
  }
  if (this.startTs > todayTs) {
   return 'future';
  }
  return 'current';
 }

 private updateFlexibleBounds(): void {
  const periodDurationMs = this.endTs - this.startTs;

  // Déterminer le type de flexibilité selon la durée de la période
  const { amount, unit } = this.getFlexibilityRange(periodDurationMs);

  const nextMinTs = this.clampToLimits(this.shiftByUnit(this.startTs, -amount, unit));
  const nextMaxTs = this.clampToLimits(this.shiftByUnit(this.endTs, amount, unit));

  this.minTs = nextMinTs;
  this.maxTs = nextMaxTs;
  this.dateMin = this.toDateStr(nextMinTs);
  this.dateMax = this.toDateStr(nextMaxTs);
 }

 /**
 * Détermine la plage de flexibilité en fonction de la durée de la période
 * @param durationMs Durée en millisecondes
 * @returns Objet avec amount et unit
 */
 private getFlexibilityRange(durationMs: number): { amount: number; unit: 'months' | 'weeks' | 'days' } {
  const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;
  const ONE_MONTH_MS = 30 * 24 * 60 * 60 * 1000;
  const ONE_YEAR_MS = 365 * 24 * 60 * 60 * 1000;

  if (durationMs < ONE_WEEK_MS) {
   return { amount: FLEX_RANGE_DAYS_W, unit: 'days' };
  } else if (durationMs < ONE_MONTH_MS) {
   return { amount: FLEX_RANGE_DAYS, unit: 'days' };
  } else if (durationMs < ONE_YEAR_MS) {
   return { amount: FLEX_RANGE_WEEKS, unit: 'weeks' };
  } else {
   return { amount: FLEX_RANGE_MONTHS, unit: 'months' };
  }
 }

 /**
 * Décale une date/timestamp selon une unité (mois, semaines ou jours)
 * @param ts Timestamp à décaler
 * @param delta Nombre d'unités (positif ou négatif)
 * @param unit Unité de décalage
 * @returns Nouveau timestamp
 */
 private shiftByUnit(ts: number, delta: number, unit: 'months' | 'weeks' | 'days'): number {
  const date = new Date(ts);

  if (unit === 'months') {
   return new Date(date.getFullYear(), date.getMonth() + delta, date.getDate()).getTime();
  } else if (unit === 'weeks') {
   date.setDate(date.getDate() + delta * 7);
   return date.getTime();
  } else { // days
   date.setDate(date.getDate() + delta);
   return date.getTime();
  }
 }

 private getMinLimitTs(): number {
  return this.toTs(this.dateMinLimit || this.dateMin);
 }

 private getMaxLimitTs(): number {
  return this.toTs(this.dateMaxLimit || this.dateMax);
 }

 formatDateFr(dateStr: string): string {
  const parts = dateStr.split('-');
  if (parts.length !== 3) return dateStr;
  return `${parts[2]}/${parts[1]}/${parts[0]}`;
 }
}