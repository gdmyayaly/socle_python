import { Component, Input, Output, EventEmitter, OnChanges, SimpleChanges } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Scenario } from '../../models/scenario.model';
import { Neutralisation } from '../../models/neutralisation.model';

/**
 * URLs « variabilisées » (sans environment, tout reste dans le ts).
 * - HOLIDAYS_API_URL : ressource HTTPS des jours fériés (à renseigner).
 * - NEUTRALISATIONS_API : base des routes backend des scénarios (chemin relatif → proxy).
 * - ID_RH : identifiant RH de l'utilisateur. Placeholder en attendant une vraie auth front.
 */
const HOLIDAYS_API_URL = '<A_RENSEIGNER>'; // ex: https://.../jours-feries
const NEUTRALISATIONS_API = '/trppu-api/scenarios';
const ID_RH = 'FRONT_TODO';

/** Réponse d'un item de l'API jours fériés. */
interface JourFerie {
  date: string;       // DD-MM-YYYY
  ymd_date: string;   // YYYY-MM-DD
  jour: string;
  libelle: string;
  timestamp: number;
  action: string;
}

@Component({
  selector: 'app-trppu-periode-neutraliser',
  templateUrl: './trppu-periode-neutraliser.component.html',
  styleUrls: ['./trppu-periode-neutraliser.component.css']
})
export class TrppuPeriodeNeutraliserComponent implements OnChanges {

  @Input() scenario: Scenario | null = null;
  @Output() periodesChange = new EventEmitter<Neutralisation[]>();

  rows: Neutralisation[] = [];
  displayedColumns: string[] = ['dateDebut', 'dateFin', 'motif', 'nbJour', 'actions'];

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

  saving = false;
  errorMessage = '';

  constructor(private http: HttpClient) {}

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
    this.errorMessage = '';
    this.dateMin = null;
    this.dateMax = null;
  }

  private initBornes(): void {
    const debut = this.scenario?.periode_debut ?? this.scenario?.periode?.dateDebut ?? null;
    const fin = this.scenario?.periode_fin ?? this.scenario?.periode?.dateFin ?? null;
    this.dateMin = debut ? this.parseYmd(debut) : null;
    this.dateMax = fin ? this.parseYmd(fin) : null;
  }

  private loadHolidays(): void {
    if (!HOLIDAYS_API_URL || HOLIDAYS_API_URL.startsWith('<')) {
      // URL non renseignée : on n'exclut aucun férié (week-ends seuls restent gérés).
      return;
    }
    this.http.get<JourFerie[]>(HOLIDAYS_API_URL).subscribe({
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
      }
    });
  }

  private loadNeutralisations(): void {
    const id = this.idScenario;
    if (id == null) return;
    this.http.get<Neutralisation[]>(`${NEUTRALISATIONS_API}/${id}/neutralisations`).subscribe({
      next: (data) => {
        this.rows = (data || []).map(n => ({
          id: n.id,
          dt_debut: n.dt_debut,
          dt_fin: n.dt_fin,
          motif: n.motif,
          nb_jour: n.nb_jour
        }));
        this.periodesChange.emit(this.rows);
      },
      error: () => {
        this.rows = [];
        this.periodesChange.emit(this.rows);
      }
    });
  }

  // ── Filtre du sélecteur de dates (jours ouvrés non fériés) ──

  dateFilter = (d: Date | null): boolean => {
    if (!d) return false;
    const day = d.getDay(); // 0 = dimanche … 6 = samedi
    if (day === 0) return false;                              // dimanche toujours exclu
    if (day === 6 && this.nbJoursSemaine === 5) return false; // samedi exclu en semaine de 5 jours
    if (this.holidaySet.has(this.ymd(d))) return false;       // jour férié
    return true;
  };

  /** Nombre de jours ouvrés (hors week-ends/fériés) dans [debut, fin] inclus. */
  computeNbJour(debut: Date | null, fin: Date | null): number {
    if (!debut || !fin || debut > fin) return 0;
    let count = 0;
    const cur = new Date(debut.getFullYear(), debut.getMonth(), debut.getDate());
    const end = new Date(fin.getFullYear(), fin.getMonth(), fin.getDate());
    while (cur <= end) {
      if (this.dateFilter(cur)) count++;
      cur.setDate(cur.getDate() + 1);
    }
    return count;
  }

  // ── Saisie ──

  get canSave(): boolean {
    return (
      !this.saving &&
      !!this.newDebut &&
      !!this.newFin &&
      this.newDebut <= this.newFin &&
      this.newMotif.trim().length > 0
    );
  }

  onEnregistrer(): void {
    if (!this.canSave) return;
    const id = this.idScenario;
    if (id == null) {
      this.errorMessage = 'Scénario invalide.';
      return;
    }
    const payload = {
      dt_debut: this.ymd(this.newDebut!),
      dt_fin: this.ymd(this.newFin!),
      motif: this.newMotif.trim(),
      id_rh: ID_RH
    };
    this.saving = true;
    this.errorMessage = '';
    this.http.post<Neutralisation>(`${NEUTRALISATIONS_API}/${id}/neutralisations`, payload).subscribe({
      next: (created) => {
        this.rows = [
          ...this.rows,
          {
            id: created.id,
            dt_debut: created.dt_debut ?? payload.dt_debut,
            dt_fin: created.dt_fin ?? payload.dt_fin,
            motif: created.motif ?? payload.motif,
            nb_jour: created.nb_jour
          }
        ];
        // Réinitialise la ligne vide pour une nouvelle saisie.
        this.newDebut = null;
        this.newFin = null;
        this.newMotif = '';
        this.saving = false;
        this.periodesChange.emit(this.rows);
      },
      error: (err) => {
        this.saving = false;
        if (err?.status === 409) {
          this.errorMessage = 'Cette période est déjà neutralisée pour ce scénario.';
        } else if (err?.status === 422) {
          this.errorMessage = 'La période ne contient aucun jour ouvré.';
        } else {
          this.errorMessage = "Erreur lors de l'enregistrement de la neutralisation.";
        }
      }
    });
  }

  onRemove(row: Neutralisation): void {
    const id = this.idScenario;
    if (id == null || row.id == null) {
      // Ligne non persistée : simple retrait local.
      this.rows = this.rows.filter(r => r !== row);
      this.periodesChange.emit(this.rows);
      return;
    }
    this.http.delete(`${NEUTRALISATIONS_API}/${id}/neutralisations/${row.id}`).subscribe({
      next: () => {
        this.rows = this.rows.filter(r => r.id !== row.id);
        this.periodesChange.emit(this.rows);
      },
      error: () => {
        this.errorMessage = 'Erreur lors de la suppression de la neutralisation.';
      }
    });
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
