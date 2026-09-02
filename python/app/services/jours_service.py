"""Service de calcul des nombres de jours (ouvrés / ouvrables / neutralisés).

Logique **pure** (testable sans dépendance) + récupération des jours fériés via
l'API jours fermés (cf. `app.services.jours_fermes_client`).

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

import logging

from dataclasses import dataclass
from datetime import date, timedelta

from app.log_utils import ctx
from app.services.jours_fermes_client import fetch_feries_between

logger = logging.getLogger(__name__)


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


# --- Récupération des fériés : API jours fermés ---


async def load_feries(debut: date, fin: date) -> set[date]:
    """Récupère les jours fermés via l'API jours-fermes sur [debut, fin].

    Lève `JoursFermesAPIError` si l'API est indisponible (cf. jours_fermes_client).
    """
    return await fetch_feries_between(debut, fin)


async def compute_nb_jours(debut: date, fin: date) -> NbJours:
    """Wrapper async : charge les fériés en base puis calcule les comptages."""
    feries = await load_feries(debut, fin)
    nbj = count_jours(debut, fin, feries)
    # Ces comptages sont écrits tels quels dans trppu_scenario : les tracer permet
    # de rejouer le calcul si un scénario affiche un nombre de jours contesté.
    logger.debug(
        "Nombres de jours calculés %s",
        ctx(
            debut=debut,
            fin=fin,
            nb_feries=len(feries),
            nb_jours_ouvres=nbj.nb_jours_ouvres,
            nb_jours_ouvrables=nbj.nb_jours_ouvrables,
        ),
    )
    return nbj


async def compute_nb_jour_neutralise_db(
    debut: date, fin: date, nb_jours_semaine: int
) -> int:
    """Wrapper async : charge les fériés en base puis calcule le nb_jour neutralisé."""
    feries = await load_feries(debut, fin)
    nb_jour = compute_nb_jour_neutralise(debut, fin, nb_jours_semaine, feries)
    logger.debug(
        "Nombre de jours neutralisés calculé %s",
        ctx(
            debut=debut,
            fin=fin,
            nb_jours_semaine=nb_jours_semaine,
            nb_feries=len(feries),
            nb_jour=nb_jour,
        ),
    )
    return nb_jour
