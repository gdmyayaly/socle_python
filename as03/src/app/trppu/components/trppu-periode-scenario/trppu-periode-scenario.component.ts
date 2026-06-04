import {
  Component, Input, Output, EventEmitter,
  OnChanges, SimpleChanges, ElementRef, ViewChild, AfterViewInit
} from '@angular/core';
import { Periode } from '../../models/periode.model';

/** Écart maximum autorisé entre début et fin (en années) */
const MAX_RANGE_YEARS = 2;

const DAY_MS = 24 * 60 * 60 * 1000;

/** Granularité d'accroche du slider */
type Granularity = 'jour' | 'semaine' | 'mois' | 'annee';

interface GranularityOption {
  value: Granularity;
  label: string;
}

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

  /** Curseur "date du jour" déplaçable, frontière réalisé / prévisionnel */
  cursorTs = 0;

  startDateStr = '';
  endDateStr = '';
  cursorDateStr = '';

  /** Granularité d'accroche courante */
  granularity: Granularity = 'jour';
  readonly granularities: GranularityOption[] = [
    { value: 'jour', label: 'Jours' },
    { value: 'semaine', label: 'Semaines' },
    { value: 'mois', label: 'Mois' },
    { value: 'annee', label: 'Année' }
  ];

  editingStart = false;
  editingEnd = false;
  editStartValue = '';
  editEndValue = '';
  private backupStartStr = '';
  private backupEndStr = '';
  isDirty = false;
  isValidated = false;
  notifications: Notification[] = [];
  dragging: 'start' | 'end' | 'cursor' | null = null;

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
      // Défaut : intervalle incluant aujourd'hui (début = aujourd'hui, fin = aujourd'hui + MAX_RANGE_YEARS)
      const now = new Date();
      const todayTs = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
      this.startTs = this.snap(this.clamp(todayTs));
      const defaultEnd = new Date(now.getFullYear() + MAX_RANGE_YEARS, now.getMonth(), now.getDate());
      this.endTs = this.snap(this.clamp(defaultEnd.getTime()));
      this.enforceMaxRange('end');
    }

    // Curseur = aujourd'hui, ramené dans la fourchette [début, fin]
    this.cursorTs = this.clamp(this.snap(this.getTodayTs()));
    this.cursorTs = Math.min(Math.max(this.cursorTs, this.startTs), this.endTs);

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

  /** true quand les bulles début/fin sont trop proches pour ne pas se chevaucher */
  get tooltipsClose(): boolean {
    if (this.trackWidth > 0) {
      const gapPx = (this.rangePercent / 100) * this.trackWidth;
      return gapPx < 150;
    }
    return this.rangePercent < 22;
  }

  // ── Curseur "date du jour" ──

  get cursorPercent(): number {
    return this.toPercent(this.cursorTs);
  }

  /** Largeur du segment réalisé (début → curseur) */
  get realiseWidthPercent(): number {
    return this.cursorPercent - this.startPercent;
  }

  /** Largeur du segment prévisionnel (curseur → fin) */
  get prevWidthPercent(): number {
    return this.endPercent - this.cursorPercent;
  }

  /** Durée du réalisé (partie grise) exprimée dans l'unité de l'écart courant */
  get realiseSpanLabel(): string {
    return this.formatSpan(this.startTs, this.cursorTs);
  }

  /** Durée du prévisionnel exprimée dans l'unité de l'écart courant */
  get prevSpanLabel(): string {
    return this.formatSpan(this.cursorTs, this.endTs);
  }

  /** Formate une durée selon la granularité (jour / semaine / mois / année) */
  private formatSpan(fromTs: number, toTs: number): string {
    const from = new Date(fromTs);
    const to = new Date(toTs);

    switch (this.granularity) {
      case 'semaine': {
        const w = Math.round((toTs - fromTs) / (7 * DAY_MS));
        return `${w} sem.`;
      }
      case 'mois': {
        const m = (to.getFullYear() - from.getFullYear()) * 12 + (to.getMonth() - from.getMonth());
        return `${m} mois`;
      }
      case 'annee': {
        const y = to.getFullYear() - from.getFullYear();
        return `${y} an${y > 1 ? 's' : ''}`;
      }
      case 'jour':
      default: {
        const d = Math.round((toTs - fromTs) / DAY_MS);
        return `${d} j`;
      }
    }
  }

  private getTodayTs(): number {
    const now = new Date();
    return new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
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

  onThumbMouseDown(event: MouseEvent, which: 'start' | 'end' | 'cursor'): void {
    event.preventDefault();
    this.dragging = which;
    this.updateTrackDimensions();
    document.addEventListener('mousemove', this.boundMouseMove);
    document.addEventListener('mouseup', this.boundMouseUp);
  }

  onThumbTouchStart(event: TouchEvent, which: 'start' | 'end' | 'cursor'): void {
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
    const snapped = this.clamp(this.snap(rawTs));

    if (this.dragging === 'start') {
      this.startTs = Math.min(snapped, this.endTs);
      this.enforceMaxRange('start');
      this.cursorTs = Math.max(this.cursorTs, this.startTs);
    } else if (this.dragging === 'end') {
      this.endTs = Math.max(snapped, this.startTs);
      this.enforceMaxRange('end');
      this.cursorTs = Math.min(this.cursorTs, this.endTs);
    } else {
      this.dragCursor(snapped);
    }

    this.syncStrings();
    this.markDirty();
  }

  /**
   * Déplace le curseur dans la fourchette. S'il atteint une borne, il la pousse,
   * sans toutefois dépasser l'écart maximum autorisé.
   */
  private dragCursor(snapped: number): void {
    let t = snapped;

    if (t > this.endTs) {
      const maxEnd = this.clamp(this.addYears(this.startTs, MAX_RANGE_YEARS));
      t = Math.min(t, maxEnd);
      this.endTs = t;
    } else if (t < this.startTs) {
      const minStart = this.clamp(this.addYears(this.endTs, -MAX_RANGE_YEARS));
      t = Math.max(t, minStart);
      this.startTs = t;
    }

    this.cursorTs = t;
  }

  // ── Édition directe ──

  openEditStart(): void {
    this.backupStartStr = this.startDateStr;
    this.editStartValue = this.startDateStr;
    this.editingStart = true;
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

  confirmEndEdit(): void {
    this.applyEndDate(this.editEndValue);
  }

  cancelEndEdit(): void {
    this.editingEnd = false;
    this.editEndValue = '';
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
    this.cursorTs = Math.max(this.cursorTs, this.startTs);
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
    this.cursorTs = Math.min(this.cursorTs, this.endTs);
    this.syncStrings();
    this.editingEnd = false;
    this.editEndValue = '';
    this.markDirty();

    if (adjusted) {
      this.addNotification('La date de fin a été ajustée pour rester dans les limites.', 'warn');
    }
  }

  // ── Granularité ──

  setGranularity(g: Granularity): void {
    if (this.granularity === g) return;
    this.granularity = g;

    const prevStart = this.startTs;
    const prevEnd = this.endTs;
    const prevCursor = this.cursorTs;

    this.startTs = this.clamp(this.snap(this.startTs));
    this.endTs = this.clamp(this.snap(this.endTs));
    this.cursorTs = this.clamp(this.snap(this.cursorTs));
    this.cursorTs = Math.min(Math.max(this.cursorTs, this.startTs), this.endTs);

    this.syncStrings();

    if (prevStart !== this.startTs || prevEnd !== this.endTs || prevCursor !== this.cursorTs) {
      this.markDirty();
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

  /** Accroche un timestamp à la granularité courante (jour / semaine / mois / année). */
  private snap(ts: number): number {
    const d = new Date(ts);

    switch (this.granularity) {
      case 'semaine': {
        const base = new Date(d.getFullYear(), d.getMonth(), d.getDate());
        const dow = (base.getDay() + 6) % 7; // lundi = 0
        const monday = new Date(base);
        monday.setDate(base.getDate() - dow);
        const nextMonday = new Date(monday);
        nextMonday.setDate(monday.getDate() + 7);
        return (ts - monday.getTime() <= nextMonday.getTime() - ts)
          ? monday.getTime()
          : nextMonday.getTime();
      }
      case 'mois': {
        const first = new Date(d.getFullYear(), d.getMonth(), 1);
        const nextFirst = new Date(d.getFullYear(), d.getMonth() + 1, 1);
        return (ts - first.getTime() <= nextFirst.getTime() - ts)
          ? first.getTime()
          : nextFirst.getTime();
      }
      case 'annee': {
        const first = new Date(d.getFullYear(), 0, 1);
        const nextFirst = new Date(d.getFullYear() + 1, 0, 1);
        return (ts - first.getTime() <= nextFirst.getTime() - ts)
          ? first.getTime()
          : nextFirst.getTime();
      }
      case 'jour':
      default:
        return new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
    }
  }

  private addYears(ts: number, years: number): number {
    const d = new Date(ts);
    return new Date(d.getFullYear() + years, d.getMonth(), d.getDate()).getTime();
  }

  private syncStrings(): void {
    this.startDateStr = this.toDateStr(this.startTs);
    this.endDateStr = this.toDateStr(this.endTs);
    this.cursorDateStr = this.toDateStr(this.cursorTs);
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
