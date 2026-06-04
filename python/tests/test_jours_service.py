"""Tests de la logique pure du service de jours (sans base).

Vérifie les exemples chiffrés des tickets DSR-613 et DSR-645.
"""

from datetime import date

from app.services.jours_service import (
    compute_nb_jour_neutralise,
    count_jours,
)

# Fériés FR du 01/03/2025 -> 31/03/2026 (cf. DSR-613). Utilisés comme set littéral
# depuis que la source des fériés est l'API jours-fermes (plus de calcul local).
FERIES_DSR613 = {
    date(2025, 4, 21),   # Lundi de Pâques
    date(2025, 5, 1),    # Fête du Travail
    date(2025, 5, 8),    # Victoire 1945
    date(2025, 5, 29),   # Ascension
    date(2025, 6, 9),    # Lundi de Pentecôte
    date(2025, 7, 14),   # Fête nationale
    date(2025, 8, 15),   # Assomption
    date(2025, 11, 1),   # Toussaint (samedi)
    date(2025, 11, 11),  # Armistice 1918
    date(2025, 12, 25),  # Noël
    date(2026, 1, 1),    # Jour de l'an
}


def test_feries_2025_2026_exemple_dsr613():
    """DSR-613 : 11 fériés sur 01/03/2025 -> 31/03/2026, dont 10 hors WE + 1 samedi."""
    feries = FERIES_DSR613
    assert len(feries) == 11
    hors_we = sum(1 for f in feries if f.weekday() <= 4)
    samedi = sum(1 for f in feries if f.weekday() == 5)
    dimanche = sum(1 for f in feries if f.weekday() == 6)
    assert (hors_we, samedi, dimanche) == (10, 1, 0)


def test_count_jours_exemple_dsr613():
    """DSR-613 — valeurs DÉFINITIONNELLEMENT correctes sur 01/03/2025 -> 31/03/2026.

    NB : l'exemple du ticket annonce 272 ouvrés bruts / 262 nets, ce qui est
    arithmétiquement faux : ouvrables(339) - ouvrés = nb de samedis ; 339-272=67
    samedis est impossible sur 396 jours (il y en a 57). Le bon compte est donc
    282 ouvrés bruts -> 272 nets. Le côté ouvrables (339 -> 328) du ticket est, lui,
    correct. Discrepance à confirmer avec le PO (cf. README_incomprehensions.md).
    """
    debut, fin = date(2025, 3, 1), date(2026, 3, 31)
    feries = FERIES_DSR613
    c = count_jours(debut, fin, feries)
    assert c.nb_jours_total == 396
    assert c.nb_jours_ouvres_bruts == 282
    assert c.nb_jours_ouvrables_bruts == 339
    assert c.nb_feries_hors_weekend == 10
    assert c.nb_feries_samedi == 1
    assert c.nb_jours_ouvres == 272       # ticket : 262 (erroné)
    assert c.nb_jours_ouvrables == 328    # ticket : 328 (correct)


def test_nb_jour_neutralise_peak_dsr645():
    """DSR-645 PEAK 10/11/2026 -> 19/12/2026 (40 j, férié mer. 11/11).

    Valeurs correctes : 6 samedis + 5 dimanches sur 40 jours.
    - 5j : 40 - 6(sam) - 5(dim) - 1(férié hors WE) = 28
    - 6j : 40 - 5(dim) - 1(férié hors WE) = 34
    NB : l'exemple du ticket (18 / 28) repose sur « 10 samedis + 11 dimanches »,
    impossible sur 40 jours. Discrepance à confirmer avec le PO.
    """
    debut, fin = date(2026, 11, 10), date(2026, 12, 19)
    feries = {date(2026, 11, 11)}  # Armistice (mercredi)
    assert compute_nb_jour_neutralise(debut, fin, 5, feries) == 28
    assert compute_nb_jour_neutralise(debut, fin, 6, feries) == 34


def test_nb_jour_neutralise_saison_dsr645():
    """DSR-645 SAISON 08/08/2026 -> 23/08/2026 (16 j, férié samedi 15/08) : 10 (5j) / 12 (6j)."""
    debut, fin = date(2026, 8, 8), date(2026, 8, 23)
    feries = {date(2026, 8, 15)}  # Assomption (samedi)
    assert compute_nb_jour_neutralise(debut, fin, 5, feries) == 10
    assert compute_nb_jour_neutralise(debut, fin, 6, feries) == 12
