import {
  Component, Input, Output, EventEmitter,
  OnChanges, SimpleChanges, ElementRef, ViewChild, AfterViewInit
} from '@angular/core';
import { Periode } from '../../models/periode.model';

/** Pas d'écart du slider en millisecondes (1 jour par défaut) */
const STEP_MS = 1 * 24 * 60 * 60 * 1000;

/** Écart maximum autorisé entre début et fin (en années) */
const MAX_RANGE_YEARS = 2;

interface Notification {
  message: string;
  type: 'info' | 'warn' | 'success';
}

@Component({
  selector: 'app-trppu-periode-scenario',
  templateUrl: './trppu-periode-scenario.component.html',
  styleUrls: ['./trppu-periode-scenario.component.css']
})
export class TrppuPeriodeScenarioComponent implements OnChanges, AfterViewInit {

  @Input() dateMin = '2025-01-01';
  @Input() dateMax = '2030-12-31';
  @Input() periode: Periode | null = null;

  @Output() periodeValidated = new EventEmitter<Periode>();
  @Output() refreshNeeded = new EventEmitter<void>();
  @Output() dirtyChange = new EventEmitter<boolean>();

  @ViewChild('track') trackRef!: ElementRef<HTMLDivElement>;

  minTs = 0;
  maxTs = 0;
  startTs = 0;
  endTs = 0;

  startDateStr = '';
  endDateStr = '';

  editingStart = false;
  editingEnd = false;
  isDirty = false;
  isValidated = false;
  notifications: Notification[] = [];
  dragging: 'start' | 'end' | null = null;

  /** Notifications en attente pendant le drag, émises au relâchement */
  private pendingNotifications: Notification[] = [];

  private trackLeft = 0;
  private trackWidth = 0;

  private boundMouseMove = this.onMouseMove.bind(this);
  private boundMouseUp = this.onMouseUp.bind(this);
  private boundTouchMove = this.onTouchMove.bind(this);
  private boundTouchEnd = this.onTouchEnd.bind(this);

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['dateMin'] || changes['dateMax'] || changes['periode']) {
      this.init();
    }
  }

  ngAfterViewInit(): void {
    this.updateTrackDimensions();
  }

  private init(): void {
    this.minTs = this.toTs(this.dateMin);
    this.maxTs = this.toTs(this.dateMax);

    if (this.periode) {
      this.startTs = this.clamp(this.toTs(this.periode.dateDebut));
      this.endTs = this.clamp(this.toTs(this.periode.dateFin));
      this.enforceMaxRange('end');
    } else {
      // Défaut : début au min, fin à min + MAX_RANGE_YEARS
      this.startTs = this.minTs;
      const defaultEnd = new Date(new Date(this.minTs).getFullYear() + MAX_RANGE_YEARS,
        new Date(this.minTs).getMonth(), new Date(this.minTs).getDate());
      this.endTs = this.snap(this.clamp(defaultEnd.getTime()));
    }

    this.syncStrings();
    this.isDirty = false;
    this.isValidated = false;
    this.notifications = [];
  }

  // ── Positions en pourcentage ──

  get startPercent(): number {
    return this.toPercent(this.startTs);
  }

  get endPercent(): number {
    return this.toPercent(this.endTs);
  }

  get rangePercent(): number {
    return this.endPercent - this.startPercent;
  }

  /** Position du marqueur "Aujourd'hui" en % (-1 si hors plage) */
  get todayPercent(): number {
    const now = new Date();
    const todayTs = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
    if (todayTs < this.minTs || todayTs > this.maxTs) return -1;
    return this.toPercent(todayTs);
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

  onThumbMouseDown(event: MouseEvent, which: 'start' | 'end'): void {
    event.preventDefault();
    this.dragging = which;
    this.updateTrackDimensions();
    document.addEventListener('mousemove', this.boundMouseMove);
    document.addEventListener('mouseup', this.boundMouseUp);
  }

  onThumbTouchStart(event: TouchEvent, which: 'start' | 'end'): void {
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

    if (this.dragging === 'start') {
      this.startTs = Math.min(snapped, this.endTs);
      this.enforceMaxRange('start');
    } else {
      this.endTs = Math.max(snapped, this.startTs);
      this.enforceMaxRange('end');
    }

    this.syncStrings();
    this.markDirty();
  }

  // ── Édition directe ──

  onStartDateEdit(value: string): void {
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
    this.syncStrings();
    this.editingStart = false;
    this.markDirty();

    if (adjusted) {
      this.addNotification('La date de début a été ajustée pour rester dans les limites.', 'warn');
    }
  }

  onEndDateEdit(value: string): void {
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
    this.syncStrings();
    this.editingEnd = false;
    this.markDirty();

    if (adjusted) {
      this.addNotification('La date de fin a été ajustée pour rester dans les limites.', 'warn');
    }
  }

  // ── Validation ──

  onValidate(): void {
    const p: Periode = {
      dateDebut: this.startDateStr,
      dateFin: this.endDateStr
    };
    this.periodeValidated.emit(p);
    this.isValidated = true;
    this.isDirty = false;
    this.dirtyChange.emit(false);
    this.addNotification('Période validée avec succès !', 'success');
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
    return Math.round((ts - this.minTs) / STEP_MS) * STEP_MS + this.minTs;
  }

  private syncStrings(): void {
    this.startDateStr = this.toDateStr(this.startTs);
    this.endDateStr = this.toDateStr(this.endTs);
  }

  private toPercent(ts: number): number {
    if (this.maxTs === this.minTs) return 0;
    return ((ts - this.minTs) / (this.maxTs - this.minTs)) * 100;
  }

  private clamp(ts: number): number {
    return Math.max(this.minTs, Math.min(this.maxTs, ts));
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

  formatDateFr(dateStr: string): string {
    const parts = dateStr.split('-');
    if (parts.length !== 3) return dateStr;
    return `${parts[2]}/${parts[1]}/${parts[0]}`;
  }
}
