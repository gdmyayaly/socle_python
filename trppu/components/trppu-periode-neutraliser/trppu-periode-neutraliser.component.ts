import {
  Component,
  Input,
  Output,
  EventEmitter,
  OnChanges,
  SimpleChanges,
  OnInit,
  ViewChild,
  ElementRef,
} from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { MatDialog } from '@angular/material/dialog';

import { Scenario } from '../../models/scenario.model';
import { Neutralisation } from '../../models/neutralisation.model';
import { UserService } from '../../../../service/user/user.service';
import { environment } from '../../../../../environments/environment';
import { ScenarioService } from '../../services/scenario.service';
import apiPaths from '../../../../literals/api-paths.literal';
import { DSRUser } from '../../../../model/user.model';
import {
  ConfirmDialogComponent,
  ConfirmDialogData,
} from '../../../../shared/dialog/confirm-dialog/confirm-dialog.component';
import {
  MessageDialogComponent,
  MessageDialogData,
} from '../../../../shared/dialog/message-dialog/message-dialog.component';

/** Réponse d'un item de l'API jours fériés. */
interface JourFerie {
  date: string; // DD-MM-YYYY
  ymd_date: string; // YYYY-MM-DD
  jour: string;
  libelle: string;
  timestamp: number;
  action: string;
}

@Component({
  selector: 'app-trppu-periode-neutraliser',
  templateUrl: './trppu-periode-neutraliser.component.html',
  styleUrls: ['./trppu-periode-neutraliser.component.css'],
})
export class TrppuPeriodeNeutraliserComponent implements OnChanges, OnInit {
  @Input() scenario: Scenario | null = null;
  @Output() periodesChange = new EventEmitter<Neutralisation[]>();

  // Références sur les inputs pour l'enchaînement du focus.
  @ViewChild('debutInput') debutInput?: ElementRef<HTMLInputElement>;
  @ViewChild('finInput') finInput?: ElementRef<HTMLInputElement>;
  @ViewChild('motifInput') motifInput?: ElementRef<HTMLInputElement>;

  rows: Neutralisation[] = [];
  displayedColumns: string[] = [
    'dateDebut',
    'dateFin',
    'motif',
    'nbJour',
    'actions',
  ];

  /** Nombre de jours ouvrés par semaine (5 = lun-ven, 6 = lun-sam). Déduit du scénario. */
  nbJoursSemaine = 6;

  /** Bornes du sélecteur, dérivées de la période du scénario. */
  dateMin: Date | null = null;
  dateMax: Date | null = null;

  /** Jours fériés (clés YYYY-MM-DD) à exclure de la sélection et du décompte. */
  private holidaySet = new Set<string>();

  /** Ligne de saisie. */
  newDebut: Date | null = null;
  newFin: Date | null = null;
  newMotif = '';

  /** Vrai lorsque la ligne de saisie est ouverte. */
  isAdding = false;
  /** Empêche l'enregistrement sur blur quand le blur est provoqué par le bouton Annuler. */
  private cancelling = false;

  saving = false;
  errorMessage = '';
  private utilisateurConnecter: DSRUser;
  readonly apiTrppuUrl = environment.trppuApiUrl;
  private readonly NEUTRALISATIONS_API = `${this.apiTrppuUrl}/scenarios`;

  constructor(
    private http: HttpClient,
    private userService: UserService,
    private scenarioService: ScenarioService,
    private dialog: MatDialog,
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['scenario']) {
      this.reset();
      if (this.scenario) {
        this.nbJoursSemaine = this.scenario.nb_jours_semaine ?? 6;
        this.initBornes();
        this.loadHolidays();
        this.loadNeutralisations();
      }
    }
  }

  ngOnInit(): void {
    this.userService
      .getUserTrppuAsync()
      .subscribe((user) => (this.utilisateurConnecter = user));
  }

  // ── Identifiant scénario ──

  private get idScenario(): number | null {
    if (!this.scenario) return null;
    return this.scenario.id_scenario ?? this.scenario.id ?? null;
  }

  // ── Initialisation ──

  private reset(): void {
    this.rows = [];
    this.holidaySet = new Set<string>();
    this.newDebut = null;
    this.newFin = null;
    this.newMotif = '';
    this.isAdding = false;
    this.cancelling = false;
    this.errorMessage = '';
    this.dateMin = null;
    this.dateMax = null;
  }

  private initBornes(): void {
    const debut =
      this.scenario?.periode_debut ?? this.scenario?.periode?.dateDebut ?? null;
    const fin =
      this.scenario?.periode_fin ?? this.scenario?.periode?.dateFin ?? null;
    this.dateMin = debut ? this.parseYmd(debut) : null;
    this.dateMax = fin ? this.parseYmd(fin) : null;
  }

  private loadHolidays(): void {
    const dataApi = {
      url: apiPaths.getFerierDays,
      method: 'GET',
    };
    this.http.post<JourFerie[]>(environment.orgateProxyUrl, dataApi).subscribe({
      next: (data) => {
        const set = new Set<string>();
        for (const f of data || []) {
          if (f?.ymd_date) set.add(f.ymd_date);
        }
        this.holidaySet = set;
      },
      error: () => {
        // Pas de blocage si l'API fériés échoue : on garde la gestion des week-ends.
        this.holidaySet = new Set<string>();
      },
    });
  }

  private loadNeutralisations(): void {
    const id = this.idScenario;
    if (id == null) return;
    this.http
      .get<
        Neutralisation[]
      >(`${this.NEUTRALISATIONS_API}/${id}/neutralisations`)
      .subscribe({
        next: (data) => {
          this.rows = (data || []).map((n) => ({
            id: n.id,
            dt_debut: n.dt_debut,
            dt_fin: n.dt_fin,
            motif: n.motif,
            nb_jour: n.nb_jour,
          }));
          this.periodesChange.emit(this.rows);
          this.ensureDraftIfEmpty();
        },
        error: () => {
          this.rows = [];
          this.periodesChange.emit(this.rows);
          this.ensureDraftIfEmpty();
        },
      });
  }

  /** Aucune période -> on ouvre d'office la ligne de saisie. */
  private ensureDraftIfEmpty(): void {
    if (this.rows.length === 0) {
      this.onAjouter();
    }
  }

  // ── Filtre du sélecteur de dates (jours ouvrés non fériés) ──

  dateFilter = (d: Date | null): boolean => {
    if (!d) return false;
    const day = d.getDay(); // 0 = dimanche … 6 = samedi
    if (day === 0) return false; // dimanche toujours exclu
    if (day === 6 && this.nbJoursSemaine === 5) return false; // samedi exclu en semaine de 5 jours
    if (this.holidaySet.has(this.ymd(d))) return false; // jour férié
    return true;
  };

  /** Nombre de jours ouvrés (hors week-ends/fériés) dans [debut, fin] inclus. */
  computeNbJour(debut: Date | null, fin: Date | null): number {
    if (!debut || !fin || debut > fin) return 0;
    let count = 0;
    const cur = new Date(
      debut.getFullYear(),
      debut.getMonth(),
      debut.getDate(),
    );
    const end = new Date(fin.getFullYear(), fin.getMonth(), fin.getDate());
    while (cur <= end) {
      if (this.dateFilter(cur)) count++;
      cur.setDate(cur.getDate() + 1);
    }
    return count;
  }

  // ── Saisie / enchaînement du focus ──

  /** Ouvre une nouvelle ligne de saisie et place le focus sur la date de début. */
  onAjouter(): void {
    this.isAdding = true;
    this.errorMessage = '';
    this.newDebut = null;
    this.newFin = null;
    this.newMotif = '';
    setTimeout(() => this.debutInput?.nativeElement.focus(), 0);
  }

  /** Annule la saisie en cours. Sans période existante, on garde la ligne ouverte. */
  onAnnuler(): void {
    this.newDebut = null;
    this.newFin = null;
    this.newMotif = '';
    this.errorMessage = '';
    if (this.rows.length === 0) {
      setTimeout(() => this.debutInput?.nativeElement.focus(), 0);
      return;
    }
    this.isAdding = false;
  }

  /** Marque une annulation imminente pour neutraliser l'enregistrement au blur du motif. */
  onCancelMouseDown(): void {
    this.cancelling = true;
  }

  /** Après une date de début valide, on passe à la date de fin. */
  onDebutChange(): void {
    if (this.isSelectableDate(this.newDebut)) {
      setTimeout(() => this.finInput?.nativeElement.focus(), 0);
    }
  }

  /** Après une date de fin valide (>= début), on passe au motif. */
  onFinChange(): void {
    if (
      this.isSelectableDate(this.newFin) &&
      this.newDebut &&
      this.newFin! >= this.newDebut
    ) {
      setTimeout(() => this.motifInput?.nativeElement.focus(), 0);
    }
  }

  /** Perte de focus du motif : enregistrement automatique si la saisie est complète. */
  onMotifBlur(): void {
    if (this.cancelling) {
      this.cancelling = false;
      return;
    }
    if (this.isAdding && this.canSave) {
      this.persist();
    }
  }

  get canSave(): boolean {
    return (
      !this.saving &&
      this.isSelectableDate(this.newDebut) &&
      this.isSelectableDate(this.newFin) &&
      this.newDebut! <= this.newFin! &&
      this.newMotif.trim().length > 0
    );
  }

  /**
   * Vrai si `d` est une date saisissable : Date valide, jour ouvré non férié
   * (cf. dateFilter), et dans les bornes de la période du scénario.
   */
  private isSelectableDate(d: Date | null): boolean {
    if (!(d instanceof Date) || isNaN(d.getTime())) return false;
    if (!this.dateFilter(d)) return false;
    if (this.dateMin && d < this.dateMin) return false;
    if (this.dateMax && d > this.dateMax) return false;
    return true;
  }

  /** Vrai si `row` est la dernière ligne persistée (pour y afficher « Ajouter »). */
  isLastRow(row: Neutralisation): boolean {
    return this.rows.length > 0 && this.rows[this.rows.length - 1] === row;
  }

  /** Enregistrement effectif côté serveur (déclenché au blur du motif). */
  private persist(): void {
    const id = this.idScenario;
    if (id == null) {
      this.errorMessage = 'Scénario invalide.';
      return;
    }
    const payload = {
      dt_debut: this.ymd(this.newDebut!),
      dt_fin: this.ymd(this.newFin!),
      motif: this.newMotif.trim(),
      id_rh: this.utilisateurConnecter.idRh,
    };
    this.saving = true;
    this.errorMessage = '';
    this.http
      .post<Neutralisation>(
        `${this.NEUTRALISATIONS_API}/${id}/neutralisations`,
        payload,
      )
      .subscribe({
        next: (created) => {
          this.rows = [
            ...this.rows,
            {
              id: created.id,
              dt_debut: created.dt_debut ?? payload.dt_debut,
              dt_fin: created.dt_fin ?? payload.dt_fin,
              motif: created.motif ?? payload.motif,
              nb_jour: created.nb_jour,
            },
          ];
          // Referme la ligne de saisie ; « Ajouter » réapparaît sur la dernière ligne.
          this.newDebut = null;
          this.newFin = null;
          this.newMotif = '';
          this.saving = false;
          this.isAdding = false;
          this.periodesChange.emit(this.rows);

          this.openMessage(
            'Neutralisation enregistrée avec succès.',
            'success',
          );
        },
        error: (err) => {
          this.saving = false;
          // On garde la ligne ouverte pour correction.
          if (err?.status === 409) {
            this.errorMessage =
              'Cette période est déjà neutralisée pour ce scénario.';
          } else if (err?.status === 422) {
            this.errorMessage = 'La période ne contient aucun jour ouvré.';
          } else {
            this.errorMessage =
              "Erreur lors de l'enregistrement de la neutralisation.";
          }
        },
      });
  }

  onRemove(row: Neutralisation): void {
    const id = this.idScenario;

    // Ligne non persistée : simple retrait local.
    if (id == null || row.id == null) {
      this.rows = this.rows.filter((r) => r !== row);
      this.periodesChange.emit(this.rows);
      this.ensureDraftIfEmpty();
      return;
    }

    const data: ConfirmDialogData = {
      message: `Voulez-vous vraiment supprimer la neutralisation du ${this.formatDateFr(row.dt_debut)} au ${this.formatDateFr(row.dt_fin)} ?`,
      type: 'warning',
      yesAction: { label: 'Oui, supprimer' },
      noAction: { label: 'Annuler' },
    };

    this.dialog
      .open(ConfirmDialogComponent, { data, width: '420px' })
      .afterClosed()
      .subscribe((result) => {
        if (result?.event === true) {
          this.deleteNeutralisation(id, row);
        }
      });
  }

  /** Suppression effective côté serveur, appelée uniquement après confirmation. */
  private deleteNeutralisation(idScenario: number, row: Neutralisation): void {
    this.http
      .delete(
        `${this.NEUTRALISATIONS_API}/${idScenario}/neutralisations/${row.id}`,
      )
      .subscribe({
        next: () => {
          this.rows = this.rows.filter((r) => r.id !== row.id);
          this.periodesChange.emit(this.rows);
          this.openMessage('Neutralisation supprimée avec succès.', 'success');
          this.ensureDraftIfEmpty();
        },
        error: () => {
          this.errorMessage =
            'Erreur lors de la suppression de la neutralisation.';
          this.openMessage(
            'Erreur lors de la suppression de la neutralisation.',
            'error',
          );
        },
      });
  }

  /** Ouvre une MessageDialog d'information (succès / erreur). */
  private openMessage(message: string, type: string): void {
    const data: MessageDialogData = { message, type };
    this.dialog.open(MessageDialogComponent, { data, width: '420px' });
  }

  // ── Helpers de dates ──

  /** Date -> 'YYYY-MM-DD' (local, sans décalage de fuseau). */
  ymd(d: Date): string {
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }

  /** 'YYYY-MM-DD' -> Date locale (minuit). */
  parseYmd(s: string): Date {
    const [y, m, d] = s.split('-').map(Number);
    return new Date(y, (m || 1) - 1, d || 1);
  }

  /** 'YYYY-MM-DD' -> 'DD/MM/YYYY' pour l'affichage. */
  formatDateFr(dateStr: string): string {
    const parts = dateStr.split('-');
    if (parts.length !== 3) return dateStr;
    return `${parts[2]}/${parts[1]}/${parts[0]}`;
  }
}
