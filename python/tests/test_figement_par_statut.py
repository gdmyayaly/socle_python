"""Tests de garde du figement par statut (DSR-669).

Règle métier : `trppu_scenario.est_fige` ne vaut 1 **qu'en production**. Un
scénario VALIDE ou SIMULATION reste modifiable ; le figer hors production le
rendait non éditable (409 via `assert_editable`) — c'est l'anomalie corrigée ici.
"""

import pytest
from fastapi import HTTPException

from app.routes.trppu_scenario.statuts import (
    FIGE_PAR_STATUT,
    resolve_fige_from_statut,
)


def test_seule_la_production_fige():
    """Aucun statut hors EN PRODUCTION ne doit mapper vers True."""
    figes = {statut for statut, fige in FIGE_PAR_STATUT.items() if fige}
    assert figes == {"EN PRODUCTION"}


@pytest.mark.parametrize(
    "statut",
    ["en production", "EN PRODUCTION", "En Production", "  en   production  "],
)
def test_production_fige_quelle_que_soit_la_casse(statut):
    assert resolve_fige_from_statut(statut) is True


@pytest.mark.parametrize(
    "statut",
    ["validé", "VALIDE", "Validé", "simulation", "SIMULATION", "en cours", "EN COURS"],
)
def test_hors_production_ne_fige_pas(statut):
    """Régression DSR-669 : validé/simulation figeaient à tort le scénario."""
    assert resolve_fige_from_statut(statut) is False


@pytest.mark.parametrize("statut", ["archive", "toto", "figé", ""])
def test_statut_inconnu_rejete_en_422(statut):
    with pytest.raises(HTTPException) as exc:
        resolve_fige_from_statut(statut)
    assert exc.value.status_code == 422
    assert "inconnu" in exc.value.detail
