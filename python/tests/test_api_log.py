"""Tests de la persistance des appels d'écriture (`app/services/api_log.py`).

La table `trppu_api_log` existe au schéma depuis l'origine mais n'était alimentée
par aucune route (IMP-5). Ces tests fixent les trois garanties attendues :
bornage des colonnes, aucune donnée sensible, et surtout **l'audit ne casse
jamais le métier**.
"""

import asyncio
import json
import logging

import pytest

from app.log_utils import reset_id_session_ihm, set_id_session_ihm
from app.services import api_log


class FauxDb:
    """Capture les paramètres d'INSERT ; peut simuler une base indisponible."""

    def __init__(self, exception=None):
        self.calls: list[tuple[str, tuple]] = []
        self._exception = exception

    async def execute(self, sql, params=None, retries=None):
        if self._exception is not None:
            raise self._exception
        self.calls.append((sql, params))
        return 1


@pytest.fixture
def faux_db(monkeypatch):
    db = FauxDb()
    monkeypatch.setattr(api_log, "db_write", db)
    return db


@pytest.fixture(autouse=True)
def _session_propre():
    token = set_id_session_ihm(None)
    yield
    reset_id_session_ihm(token)


def _params_de(db):
    """(api_name, id_scenario, regate, caller, params_json) du dernier INSERT."""
    return db.calls[-1][1]


def test_insert_avec_les_bonnes_colonnes(faux_db):
    asyncio.run(
        api_log.enregistrer_appel(
            api_name=api_log.ACTION_CREATION_SCENARIO,
            id_scenario=52,
            regate="012345",
            params={"co_regate": "012345"},
        )
    )
    sql, params = faux_db.calls[0]
    assert "INSERT INTO trppu_api_log" in sql
    api_name, id_scenario, regate, _caller, params_json = params
    assert api_name == "CREATION_SCENARIO"
    assert id_scenario == 52
    assert regate == "012345"
    assert json.loads(params_json)["co_regate"] == "012345"


def test_caller_alimente_par_l_id_session_ihm(faux_db):
    """Les routes ne sont pas authentifiées : la session IHM est le seul appelant connu."""
    set_id_session_ihm("11111111-2222-3333-4444-555555555555")
    asyncio.run(api_log.enregistrer_appel(api_name="X", id_scenario=1))
    assert _params_de(faux_db)[3] == "11111111-2222-3333-4444-555555555555"


def test_colonnes_bornees(faux_db):
    asyncio.run(
        api_log.enregistrer_appel(api_name="A" * 200, id_scenario=1, regate="0123456789")
    )
    api_name, _id, regate, _caller, _p = _params_de(faux_db)
    assert len(api_name) == api_log.API_NAME_MAX_LEN
    assert len(regate) == api_log.REGATE_LEN


def test_id_rh_jamais_persiste(faux_db):
    """Même règle que pour les logs fichiers : aucun id_rh en clair en base."""
    asyncio.run(
        api_log.enregistrer_appel(
            api_name="X", id_scenario=1, params={"id_rh": "A123456", "garde": 1}
        )
    )
    params_json = _params_de(faux_db)[4]
    assert "A123456" not in params_json
    assert json.loads(params_json)["garde"] == 1


def test_id_scenario_recopie_dans_params(faux_db):
    """La FK est détachée à la suppression du scénario : la copie garde la trace."""
    asyncio.run(api_log.enregistrer_appel(api_name="X", id_scenario=52))
    assert json.loads(_params_de(faux_db)[4])["id_scenario"] == 52


def test_params_volumineux_tronques(faux_db):
    asyncio.run(
        api_log.enregistrer_appel(
            api_name="X", id_scenario=1, params={"gros": "x" * 50_000}
        )
    )
    params_json = _params_de(faux_db)[4]
    assert len(params_json) < 50_000
    assert json.loads(params_json)["tronque"] is True


def test_params_non_serialisables_absorbes(faux_db):
    """Un objet exotique dans le payload ne doit pas faire échouer l'appel métier."""
    asyncio.run(
        api_log.enregistrer_appel(api_name="X", id_scenario=1, params={"o": object()})
    )
    assert json.loads(_params_de(faux_db)[4])  # JSON valide produit malgré tout


def test_echec_d_ecriture_ne_remonte_pas(monkeypatch, caplog):
    """L'audit est best-effort : une base indisponible ne doit pas casser la requête."""
    monkeypatch.setattr(api_log, "db_write", FauxDb(exception=RuntimeError("base KO")))
    with caplog.at_level(logging.WARNING, logger="app.services.api_log"):
        asyncio.run(api_log.enregistrer_appel(api_name="X", id_scenario=1))
    assert "trppu_api_log" in caplog.text
