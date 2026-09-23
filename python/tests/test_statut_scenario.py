"""Tests de la machine à états exposée par PATCH /trppu-api/scenarios/{id}/statut.

Le figement suit le statut : VALIDE et EN PRODUCTION figent (est_fige = 1),
EN COURS / SIMULATION défigent. EN PRODUCTION n'est atteignable que depuis VALIDE,
avec les contrôles DSR-707 C4 (trafics calculés) et C5 (un seul scénario en
production par site) repris de l'ancienne route POST /mise-en-prod. L'archivage
n'est plus accessible.
"""

import asyncio

import pytest
from fastapi import HTTPException

from app.routes.trppu_scenario import routes as scenario_routes
from app.routes.trppu_scenario.schemas import StatutUpdate
from app.routes.trppu_scenario.statuts import (
    apply_transition_side_effects,
    assert_transition_allowed,
)


class FakeTx:
    """Curseur transactionnel : journalise les requêtes, simule C5 au besoin."""

    def __init__(self, autre_en_prod=None):
        self._autre = autre_en_prod
        self.executed: list[tuple[str, tuple]] = []

    async def fetch_one(self, sql, params=None):
        self.executed.append((sql, params))
        if "EN PRODUCTION" in sql and "FOR UPDATE" in sql:
            return {"id_scenario": self._autre} if self._autre else None
        # increment_version relit la version après l'UPDATE.
        return {"version_scenario": 3}

    async def execute(self, sql, params=None):
        self.executed.append((sql, params))
        return 1


class FakeWrite:
    def __init__(self, tx):
        self.tx = tx

    def transaction(self):
        tx = self.tx

        class _Cm:
            async def __aenter__(self):
                return tx

            async def __aexit__(self, *exc):
                return False

        return _Cm()


def _scenario(statut="VALIDE", pdi=1, agrebal=1, en_cours=0):
    return {
        "id_scenario": 125,
        "co_regate": "123456",
        "lb_scenario": "Scénario Avril",
        "statut": statut,
        "est_fige": 0,
        "trafic_pdi_calcule": pdi,
        "trafic_agrebal_calcule": agrebal,
        "calcul_trafic_en_cours": en_cours,
    }


def _patch(scenario, cible, *, monkeypatch, tx=None):
    tx = tx or FakeTx()

    async def _fetch(id_scenario):
        return scenario

    async def _noop(**kwargs):
        return None

    monkeypatch.setattr(scenario_routes, "fetch_scenario_or_404", _fetch)
    monkeypatch.setattr(scenario_routes, "enregistrer_appel", _noop)
    monkeypatch.setattr(scenario_routes, "db_write", FakeWrite(tx))
    asyncio.run(scenario_routes.update_statut(125, StatutUpdate(statut=cible)))
    return tx


def _update_sql(target: str, depuis: str) -> str:
    tx = FakeTx()
    asyncio.run(
        apply_transition_side_effects(tx, {"id_scenario": 7, "statut": depuis}, target)
    )
    assert len(tx.executed) == 1
    return tx.executed[0][0]


# --- Machine à états ------------------------------------------------------------


@pytest.mark.parametrize(
    "depuis, vers",
    [
        ("EN COURS", "SIMULATION"),
        ("EN COURS", "VALIDE"),
        ("SIMULATION", "EN COURS"),
        ("SIMULATION", "VALIDE"),
        ("VALIDE", "EN COURS"),
        ("VALIDE", "SIMULATION"),
        ("VALIDE", "EN PRODUCTION"),
    ],
)
def test_transitions_autorisees(depuis, vers):
    assert_transition_allowed(depuis, vers)


@pytest.mark.parametrize("depuis", ["EN COURS", "SIMULATION", "VALIDE", "EN PRODUCTION"])
def test_archivage_interdit(depuis):
    with pytest.raises(HTTPException) as exc:
        assert_transition_allowed(depuis, "ARCHIVE")
    assert exc.value.status_code == 422


@pytest.mark.parametrize("depuis", ["EN COURS", "SIMULATION"])
def test_mise_en_production_uniquement_depuis_valide(depuis):
    with pytest.raises(HTTPException) as exc:
        assert_transition_allowed(depuis, "EN PRODUCTION")
    assert exc.value.status_code == 422


@pytest.mark.parametrize("vers", ["EN COURS", "SIMULATION", "VALIDE"])
def test_un_scenario_en_production_ne_change_plus_de_statut(vers):
    with pytest.raises(HTTPException) as exc:
        assert_transition_allowed("EN PRODUCTION", vers)
    assert exc.value.status_code == 422


# --- Effets de bord -------------------------------------------------------------


def test_la_validation_fige_le_scenario():
    sql = _update_sql("VALIDE", "EN COURS")
    assert "est_fige = 1" in sql
    assert "dt_validation = COALESCE(dt_validation, NOW())" in sql


@pytest.mark.parametrize("vers", ["EN COURS", "SIMULATION"])
def test_le_retour_en_statut_de_travail_defige_le_scenario(vers):
    assert "est_fige = 0" in _update_sql(vers, "VALIDE")


def test_la_mise_en_production_fige_et_date():
    sql = _update_sql("EN PRODUCTION", "VALIDE")
    assert "est_fige = 1" in sql
    assert "dt_mise_en_prod = NOW()" in sql
    assert "dt_validation = COALESCE(dt_validation, NOW())" in sql


# --- Mise en production via la route (DSR-707 C4/C5) -----------------------------


def test_c4_refuse_un_scenario_non_calcule(monkeypatch):
    with pytest.raises(HTTPException) as exc:
        _patch(_scenario(agrebal=0), "EN PRODUCTION", monkeypatch=monkeypatch)
    assert exc.value.status_code == 409
    assert exc.value.detail == (
        "Les trafics du scénario 125 ne sont pas complètement calculés"
    )


def test_c5_refuse_un_second_scenario_en_production(monkeypatch):
    tx = FakeTx(autre_en_prod=130)
    with pytest.raises(HTTPException) as exc:
        _patch(_scenario(), "EN PRODUCTION", monkeypatch=monkeypatch, tx=tx)
    assert exc.value.status_code == 409
    assert exc.value.detail == "Un scénario est déjà en production pour ce site"
    assert not any(s.startswith("UPDATE") for s, _ in tx.executed)


def test_mise_en_production_nominale(monkeypatch):
    tx = _patch(_scenario(), "EN PRODUCTION", monkeypatch=monkeypatch)
    # C5 verrouille avant toute écriture.
    assert "FOR UPDATE" in tx.executed[0][0]
    assert any("dt_mise_en_prod = NOW()" in s for s, _ in tx.executed)


def test_c4_c5_ne_bloquent_pas_une_simple_validation(monkeypatch):
    """Les contrôles de production ne s'appliquent qu'à la cible EN PRODUCTION."""
    tx = _patch(
        _scenario(statut="EN COURS", pdi=0, agrebal=0),
        "VALIDE",
        monkeypatch=monkeypatch,
        tx=FakeTx(autre_en_prod=130),
    )
    assert not any("FOR UPDATE" in s for s, _ in tx.executed)


# --- Routes retirées --------------------------------------------------------------


def test_routes_archive_et_mise_en_prod_retirees():
    chemins = {
        (m, r.path) for r in scenario_routes.router.routes for m in getattr(r, "methods", ())
    }
    assert ("POST", "/trppu-api/scenarios/{id_scenario}/archive") not in chemins
    assert ("POST", "/trppu-api/scenarios/{id_scenario}/mise-en-prod") not in chemins
    assert ("PATCH", "/trppu-api/scenarios/{id_scenario}/statut") in chemins
