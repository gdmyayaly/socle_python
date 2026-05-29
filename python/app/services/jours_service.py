"""Service de calcul des nombres de jours (ouvrés / ouvrables / neutralisés).

Logique **pure** (testable sans base) + accès DB à la table `trppu_jours_feries`.

Définitions (cf. DSR-613) :
- jours ouvrés    = lundi -> vendredi (5/semaine)
- jours ouvrables = lundi -> samedi   (6/semaine)
- fériés hors week-end : déduits des ouvrés ET des ouvrables
- férié tombant un samedi : déduit des ouvrables uniquement
- férié tombant un dimanche : non déduit (déjà exclu)

La même mécanique sert au calcul du `nb_jour` neutralisé (cf. DSR-645) :
- semaine 5 jours : nb_jour = (ouvrés de la période)
- semaine 6 jours : nb_jour = (ouvrables de la période)

Python : weekday() -> lundi=0 ... samedi=5, dimanche=6.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from app.db.mysql import db_read

# --- Jours fériés français (logique pure, utilisée pour le seed et les tests) ---


def _easter_sunday(year: int) -> date:
    """Dimanche de Pâques (algorithme de Computus grégorien anonyme)."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def compute_french_holidays(year: int) -> dict[date, str]:
    """Jours fériés nationaux français pour une année (fixes + liés à Pâques)."""
    easter = _easter_sunday(year)
    feries: dict[date, str] = {
        date(year, 1, 1): "Jour de l'an",
        date(year, 5, 1): "Fête du Travail",
        date(year, 5, 8): "Victoire 1945",
        date(year, 7, 14): "Fête nationale",
        date(year, 8, 15): "Assomption",
        date(year, 11, 1): "Toussaint",
        date(year, 11, 11): "Armistice 1918",
        date(year, 12, 25): "Noël",
        easter + timedelta(days=1): "Lundi de Pâques",
        easter + timedelta(days=39): "Ascension",
        easter + timedelta(days=50): "Lundi de Pentecôte",
    }
    return feries


def french_holidays_between(debut: date, fin: date) -> set[date]:
    """Ensemble des fériés FR (national) entre deux dates incluses."""
    feries: set[date] = set()
    for year in range(debut.year, fin.year + 1):
        for d in compute_french_holidays(year):
            if debut <= d <= fin:
                feries.add(d)
    return feries


# --- Comptage des jours (logique pure) ---


@dataclass(frozen=True)
class NbJours:
    nb_jours_total: int
    nb_jours_ouvres_bruts: int
    nb_jours_ouvrables_bruts: int
    nb_feries_hors_weekend: int
    nb_feries_samedi: int
    nb_jours_ouvres: int       # ouvrés - fériés hors week-end
    nb_jours_ouvrables: int    # ouvrables - fériés hors week-end - fériés samedi


def count_jours(debut: date, fin: date, feries: set[date]) -> NbJours:
    """Calcule les comptages bruts et nets de jours sur [debut, fin] (inclus)."""
    if fin < debut:
        raise ValueError("fin doit être >= debut")

    total = (fin - debut).days + 1
    ouvres_bruts = 0
    ouvrables_bruts = 0
    d = debut
    while d <= fin:
        wd = d.weekday()
        if wd <= 4:            # lundi..vendredi
            ouvres_bruts += 1
            ouvrables_bruts += 1
        elif wd == 5:          # samedi
            ouvrables_bruts += 1
        d += timedelta(days=1)

    feries_in = {f for f in feries if debut <= f <= fin}
    feries_hors_we = sum(1 for f in feries_in if f.weekday() <= 4)
    feries_samedi = sum(1 for f in feries_in if f.weekday() == 5)

    return NbJours(
        nb_jours_total=total,
        nb_jours_ouvres_bruts=ouvres_bruts,
        nb_jours_ouvrables_bruts=ouvrables_bruts,
        nb_feries_hors_weekend=feries_hors_we,
        nb_feries_samedi=feries_samedi,
        nb_jours_ouvres=ouvres_bruts - feries_hors_we,
        nb_jours_ouvrables=ouvrables_bruts - feries_hors_we - feries_samedi,
    )


def compute_nb_jour_neutralise(
    debut: date, fin: date, nb_jours_semaine: int, feries: set[date]
) -> int:
    """Nombre de jours réellement déduits pour une période neutralisée (DSR-645).

    - semaine 5 jours : jours ouvrés de la période
    - semaine 6 jours : jours ouvrables de la période
    """
    c = count_jours(debut, fin, feries)
    if nb_jours_semaine == 5:
        return c.nb_jours_ouvres
    return c.nb_jours_ouvrables


# --- Accès base : table trppu_jours_feries ---


async def load_feries(debut: date, fin: date) -> set[date]:
    """Charge les fériés nationaux de la table `trppu_jours_feries` sur [debut, fin]."""
    rows = await db_read.fetch_all(
        "SELECT dt FROM trppu_jours_feries "
        "WHERE est_national = 1 AND dt BETWEEN %s AND %s",
        (debut, fin),
    )
    result: set[date] = set()
    for r in rows:
        dt = r["dt"]
        result.add(dt if isinstance(dt, date) else date.fromisoformat(str(dt)))
    return result


async def compute_nb_jours(debut: date, fin: date) -> NbJours:
    """Wrapper async : charge les fériés en base puis calcule les comptages."""
    feries = await load_feries(debut, fin)
    return count_jours(debut, fin, feries)


async def compute_nb_jour_neutralise_db(
    debut: date, fin: date, nb_jours_semaine: int
) -> int:
    """Wrapper async : charge les fériés en base puis calcule le nb_jour neutralisé."""
    feries = await load_feries(debut, fin)
    return compute_nb_jour_neutralise(debut, fin, nb_jours_semaine, feries)
