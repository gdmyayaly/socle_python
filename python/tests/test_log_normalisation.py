"""Normalisation des logs (CONVENTION-LOGS) et traçabilité Kibana (DSR-716).

Quatre volets :

1. le champ racine `co_regate` du JSON : toujours présent, posé dès que le site est
   connu, jamais propagé d'une requête à l'autre ;
2. la ligne d'appel du middleware porte les paramètres de la requête, sans les champs
   sensibles ni `id_session_ihm` (déjà champ racine) ;
3. les cas que le ticket cite : site au chargement d'un scénario, « aucun trafic »
   explicite, rejets 4xx tracés ;
4. un garde-fou sur tout `app/` : la grammaire Début / Fin / Rejet / Erreur et le bloc
   `ctx()` ne doivent plus pouvoir régresser.
"""

import ast
import asyncio
import json
import logging
import re
from pathlib import Path

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.json_formatter import JsonFormatter
from app.log_utils import (
    CO_REGATE_MAX_LEN,
    get_co_regate,
    reset_co_regate,
    set_co_regate,
)


class CaptureJson(logging.Handler):
    """Formate AU MOMENT de l'émission, comme le handler réel : le contexte lu est celui
    de la requête qui journalise, pas celui du test au moment de l'assertion."""

    def __init__(self):
        super().__init__(logging.DEBUG)
        self.setFormatter(JsonFormatter())
        self.lignes: list[dict] = []

    def emit(self, record):
        self.lignes.append(json.loads(self.format(record)))

    def messages(self, debut: str) -> list[dict]:
        return [l for l in self.lignes if l["app_message"].startswith(debut)]


@pytest.fixture
def capture():
    handler = CaptureJson()
    loggers = [logging.getLogger(n) for n in ("trppu", "app")]
    niveaux = [lg.level for lg in loggers]
    for lg in loggers:
        lg.addHandler(handler)
        lg.setLevel(logging.DEBUG)
    yield handler
    for lg, niveau in zip(loggers, niveaux):
        lg.removeHandler(handler)
        lg.setLevel(niveau)


@pytest.fixture(autouse=True)
def _contexte_propre():
    token = set_co_regate(None)
    yield
    reset_co_regate(token)


def _format() -> dict:
    record = logging.LogRecord("trppu", logging.INFO, "t.py", 1, "message", (), None)
    return json.loads(JsonFormatter().format(record))


# --- 1. Champ racine co_regate ---------------------------------------------------------


def test_co_regate_present_et_nul_hors_requete():
    ligne = _format()
    assert "co_regate" in ligne and ligne["co_regate"] is None


def test_co_regate_repris_dans_le_log():
    set_co_regate("123456")
    assert _format()["co_regate"] == "123456"


@pytest.mark.parametrize(
    ("brut", "attendu"),
    [(" 123456 ", "123456"), ("", None), ("   ", None), (123456, "123456"),
     ("x" * 40, "x" * CO_REGATE_MAX_LEN)],
)
def test_co_regate_normalise(brut, attendu):
    set_co_regate(brut)
    assert get_co_regate() == attendu


def test_reset_ne_laisse_pas_fuiter_le_site():
    token = set_co_regate("123456")
    reset_co_regate(token)
    assert _format()["co_regate"] is None


def test_fetch_scenario_pose_le_site(monkeypatch):
    from app.routes.trppu_scenario import helpers

    class Base:
        async def fetch_one(self, sql, params=None):
            return {"id_scenario": 131, "co_regate": "654321"}

    monkeypatch.setattr(helpers, "db_read", Base())

    async def lire():
        await helpers.fetch_scenario_or_404(131)
        return get_co_regate()

    assert asyncio.run(lire()) == "654321"


def test_fetch_site_pose_le_site_et_trace_le_404(monkeypatch, capture):
    from app.routes.trppu_site import helpers

    class Base:
        def __init__(self, ligne):
            self.ligne = ligne

        async def fetch_one(self, sql, params=None):
            return self.ligne

    async def lire():
        await helpers.fetch_site_or_404("123456")
        return get_co_regate()

    monkeypatch.setattr(helpers, "db_read", Base({"co_regate": "123456"}))
    assert asyncio.run(lire()) == "123456"

    monkeypatch.setattr(helpers, "db_read", Base(None))
    with pytest.raises(HTTPException) as exc:
        asyncio.run(helpers.fetch_site_or_404("999999"))
    assert exc.value.status_code == 404
    (rejet,) = capture.messages("Rejet accès site")
    assert "http=404" in rejet["app_message"] and rejet["severity_label"] == "WARNING"


# --- 2. Middleware : paramètres de la requête -------------------------------------------


def _appel(capture, url):
    from app.main import app

    TestClient(app).get(url)  # sans `with` : le lifespan ne se lance pas
    (ligne,) = [l for l in capture.messages(">>> GET /")]
    return ligne


def test_middleware_ajoute_les_parametres(capture):
    ligne = _appel(
        capture,
        "/?co_regate=123456&date_debut=2026-01-01&produits=OO&produits=PPI"
        "&id_session_ihm=UUID-1&id_rh=SECRET",
    )
    message = ligne["app_message"]
    assert "co_regate=123456" in message
    assert "date_debut=2026-01-01" in message
    assert "produits=OO,PPI" in message
    # Déjà champ racine : pas de doublon dans le message.
    assert "id_session_ihm" not in message and ligne["id_session_ihm"] == "UUID-1"
    # Champ sensible : jamais journalisé.
    assert "SECRET" not in message and "id_rh" not in message
    # Le site du query param devient le champ racine de toutes les lignes.
    assert ligne["co_regate"] == "123456"


def test_middleware_sans_parametre_garde_la_forme_courte(capture):
    ligne = _appel(capture, "/")
    assert ligne["app_message"] == ">>> GET /"
    assert ligne["co_regate"] is None


# --- 3. Cas cités par DSR-716 -------------------------------------------------------------


def test_lecture_tmh_vide_dit_aucun_trafic(monkeypatch, capture):
    from app.routes.trppu_tmh import routes

    async def scenario(id_scenario):
        return {"id_scenario": id_scenario, "co_regate": "123456"}

    async def aucun(db, id_scenario):
        return []

    monkeypatch.setattr(routes, "fetch_scenario_or_404", scenario)
    monkeypatch.setattr(routes, "fetch_tmh", aucun)
    asyncio.run(routes.list_tmh(131))

    (fin,) = capture.messages("Fin lecture TMH")
    assert "constat=aucun trafic TMH pour ce scénario" in fin["app_message"]


def test_lecture_scenario_donne_le_site(monkeypatch, capture):
    from app.routes.trppu_scenario import routes

    async def scenario(id_scenario):
        return {"id_scenario": id_scenario, "co_regate": "123456", "statut": "VALIDE",
                "lb_scenario": "Test"}

    monkeypatch.setattr(routes, "fetch_scenario_or_404", scenario)
    asyncio.run(routes.get_scenario(131))

    (fin,) = capture.messages("Fin lecture scénario")
    assert "co_regate=123456" in fin["app_message"]


@pytest.mark.parametrize(
    ("debut", "fin", "motif"),
    [("2026-02-01", "2026-01-01", "postérieure"), ("2020-01-01", "2026-01-01", "écart")],
)
def test_nb_jours_rejets_400_traces(capture, debut, fin, motif):
    from app.routes import calcl_nbr_jours

    with pytest.raises(HTTPException) as exc:
        asyncio.run(calcl_nbr_jours.get_nb_jours(date_debut=debut, date_fin=fin))
    assert exc.value.status_code == 400
    (rejet,) = capture.messages("Rejet calcul nb_jours")
    assert "http=400" in rejet["app_message"] and motif in rejet["app_message"]


# --- 4. Garde-fou sur tout app/ -------------------------------------------------------------

APP = Path(__file__).resolve().parent.parent / "app"
NIVEAUX = {"debug", "info", "warning", "error", "exception", "critical"}


def _appels_de_log():
    for fichier in sorted(APP.rglob("*.py")):
        source = fichier.read_text(encoding="utf-8")
        for noeud in ast.walk(ast.parse(source)):
            if (
                isinstance(noeud, ast.Call)
                and isinstance(noeud.func, ast.Attribute)
                and noeud.func.attr in NIVEAUX
                and isinstance(noeud.func.value, ast.Name)
                and noeud.func.value.id in ("logger", "log")
                and noeud.args
                and isinstance(noeud.args[0], ast.Constant)
                and isinstance(noeud.args[0].value, str)
            ):
                yield f"{fichier.relative_to(APP).as_posix()}:{noeud.lineno}", noeud


APPELS = list(_appels_de_log())


def test_le_garde_fou_voit_bien_les_logs():
    assert len(APPELS) > 200


def test_erreurs_techniques_prefixees_erreur():
    """`logger.exception` / `logger.error` : toujours « Erreur <action> »."""
    fautifs = [
        f"{ou} {n.args[0].value!r}"
        for ou, n in APPELS
        if n.func.attr in ("exception", "error")
        and not n.args[0].value.startswith(("Erreur ", "<<<"))  # <<< : middleware
    ]
    assert not fautifs, fautifs


def test_contexte_toujours_par_ctx():
    """Hors middleware (>>> / <<<), un message paramétré prend un seul argument : ctx()."""
    fautifs = []
    for ou, n in APPELS:
        message = n.args[0].value
        if message.startswith((">>>", "<<<")) or len(n.args) == 1:
            continue
        unique_ctx = (
            len(n.args) == 2
            and message.count("%") == 1
            and message.endswith("%s")
            and isinstance(n.args[1], ast.Call)
            and getattr(n.args[1].func, "id", None) == "ctx"
        )
        if not unique_ctx:
            fautifs.append(f"{ou} {message!r}")
    assert not fautifs, fautifs


def test_plus_aucun_message_echec():
    """« Échec … » n'est pas regroupable avec les autres erreurs dans Kibana."""
    fautifs = [f"{ou}" for ou, n in APPELS if re.match(r"\s*Échec", n.args[0].value)]
    assert not fautifs, fautifs
