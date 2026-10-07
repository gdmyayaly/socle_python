"""Site (`co_regate`) et ROC (`co_roc`) sur chaque ligne de log JSON (ContextVar, comme
`id_scenario`) : posés par le mode ALL ou `charger_scenario`, remis à zéro par l'appelant."""

import argparse
import asyncio
import json
import logging

import pytest

from app import main
from app.json_formatter import JsonFormatter
from app.log_utils import get_site, reset_site, set_site
from app.traitements import orchestrateur
from app.traitements import scenario as scn
from app.traitements.rapport import SUCCES, Rapport
from tests.conftest import FausseBase


def _format() -> dict:
    record = logging.LogRecord("yb05", logging.INFO, "test.py", 1, "message", (), None)
    return json.loads(JsonFormatter().format(record))


@pytest.fixture(autouse=True)
def _contexte_propre():
    token = set_site(None, None)
    yield
    reset_site(token)


class CaptureJson(logging.Handler):
    """Formate à l'émission, comme le handler réel : contexte de la tâche qui journalise."""

    def __init__(self):
        super().__init__(logging.DEBUG)
        self.lignes: list[dict] = []
        self.setFormatter(JsonFormatter())

    def emit(self, record):
        self.lignes.append(json.loads(self.format(record)))


@pytest.fixture
def capture():
    handler = CaptureJson()
    racine = logging.getLogger("app")
    niveau = racine.level
    racine.addHandler(handler)
    racine.setLevel(logging.DEBUG)
    yield handler
    racine.removeHandler(handler)
    racine.setLevel(niveau)


# --- Formateur ---------------------------------------------------------------------------------


def test_champs_presents_et_nuls_hors_traitement():
    """Clés toujours posées : le mapping Kibana reste stable."""
    ligne = _format()
    assert ligne["co_regate"] is None and ligne["co_roc"] is None


def test_site_et_roc_repris_dans_le_log():
    set_site("ZT0001", "ZR0001")
    ligne = _format()
    assert (ligne["co_regate"], ligne["co_roc"]) == ("ZT0001", "ZR0001")


@pytest.mark.parametrize(
    ("brut", "attendu"), [(" 750100 ", "750100"), ("", None), ("   ", None), (750100, "750100")]
)
def test_codes_normalises(brut, attendu):
    set_site(brut, None)
    assert get_site() == (attendu, None)


def test_reset_efface_le_site():
    token = set_site("ZT0001", "ZR0001")
    reset_site(token)
    assert _format()["co_regate"] is None


# --- charger_scenario --------------------------------------------------------------------------


def test_charger_scenario_pose_le_site():
    base = FausseBase(
        {"FROM trppu_scenario WHERE id_scenario": {"id_scenario": 7, "co_regate": "750100",
                                                    "co_roc": "75010"}},
        lecture_seule=True,
    )

    async def lire():
        # Même tâche que l'appelant : c'est ainsi que les traitements voient le site.
        await scn.charger_scenario(base, 7)
        return get_site()

    assert asyncio.run(lire()) == ("750100", "75010")


def test_scenario_absent_ne_change_pas_le_site():
    base = FausseBase({"FROM trppu_scenario WHERE id_scenario": None}, lecture_seule=True)

    async def lire():
        await scn.charger_scenario(base, 7)
        return get_site()

    assert asyncio.run(lire()) == (None, None)


# --- Mode ALL ----------------------------------------------------------------------------------


def _rapport(statut=SUCCES):
    return Rapport(titre="doublure", id_scenario=0, statut=statut)


def _all(monkeypatch, eligibilite, *, nb_workers=1, id_scenario=None):
    async def reussi(id_scenario, **_):
        await asyncio.sleep(0)
        return _rapport()

    monkeypatch.setattr(orchestrateur, "controle_eligibilite", eligibilite)
    monkeypatch.setattr(orchestrateur, "calcul_trafic_pdi", reussi)
    monkeypatch.setattr(orchestrateur, "calcul_trafic_agrebal", reussi)
    base = FausseBase(
        {
            "AND trafic_pdi_calcule = 0": [
                {"id_scenario": 1, "co_regate": "ZT0001", "co_roc": "ZR0001"},
                {"id_scenario": 2, "co_regate": "ZT0002", "co_roc": "ZR0002"},
                {"id_scenario": 3, "co_regate": "ZT0003", "co_roc": "ZR0003"},
            ],
            "AND trafic_pdi_calcule = 1": [],
        }
    )
    return asyncio.run(
        orchestrateur.executer_tout(
            id_scenario, nb_workers=nb_workers, db_lecture=base, db_ecriture=FausseBase({})
        )
    )


def test_all_chaque_ligne_porte_le_site_de_son_scenario(monkeypatch, capture):
    """Workers entrelacés : chaque ligne garde le site de SON scénario, dès « Début »."""

    async def eligibilite(id_scenario, **_):
        await asyncio.sleep(0)  # laisse les autres workers s'intercaler
        return _rapport("ELIGIBLE")

    _all(monkeypatch, eligibilite, nb_workers=3)

    par_scenario = [
        ligne for ligne in capture.lignes if ligne["app_message"].startswith(
            ("Début traitement scénario", "Fin traitement scénario")
        )
    ]
    assert len(par_scenario) == 6
    for ligne in par_scenario:
        numero = ligne["id_scenario"]
        assert ligne["co_regate"] == f"ZT{numero:04d}"
        assert ligne["co_roc"] == f"ZR{numero:04d}"
    assert get_site() == (None, None), "le site a fuité hors du worker"


def test_all_lignes_hors_scenario_sans_site(monkeypatch, capture):
    async def eligibilite(id_scenario, **_):
        return _rapport("ELIGIBLE")

    _all(monkeypatch, eligibilite)

    fin = next(ligne for ligne in capture.lignes if ligne["app_message"].startswith("Fin mode ALL"))
    assert fin["co_regate"] is None and fin["id_scenario"] is None


def test_all_un_seul_scenario_site_pose_a_la_lecture(monkeypatch):
    """Identifiant explicite : pas de liste, `charger_scenario` pose le site."""
    vus = []

    async def eligibilite(id_scenario, **_):
        vus.append(get_site())
        base = FausseBase(
            {"FROM trppu_scenario WHERE id_scenario": {"co_regate": "ZT0009", "co_roc": "ZR0009"}}
        )
        await scn.charger_scenario(base, id_scenario)
        vus.append(get_site())
        return _rapport("ELIGIBLE")

    _all(monkeypatch, eligibilite, id_scenario=9)

    assert vus == [(None, None), ("ZT0009", "ZR0009")]
    assert get_site() == (None, None)


# --- Commandes mono-scénario -------------------------------------------------------------------


def test_commande_mono_scenario_efface_le_site_en_sortie(capsys):
    async def traitement(id_scenario):
        set_site("ZT0005", "ZR0005")  # ce que fait charger_scenario
        assert get_site() == ("ZT0005", "ZR0005")
        return _rapport()

    args = argparse.Namespace(id_scenario=5, commande="calcul-trafic-pdi", json=False)
    asyncio.run(main._executer_traitement(traitement, args))

    assert get_site() == (None, None)
