"""Tests de la propagation de `id_session_ihm` dans les logs JSON (DSR-660/661).

L'IHM pose ce query param sur tous les appels /trppu-api (session-ihm.interceptor.ts).
Le middleware HTTP le publie dans un ContextVar et `JsonFormatter` doit l'ajouter comme
champ racine à *chaque* ligne de log, pour permettre le regroupement par session dans
Kibana — y compris sur les lignes qu'aucun endpoint n'émet (middleware, exceptions).
"""

import json
import logging

import pytest

from app.json_formatter import JsonFormatter
from app.log_utils import (
    ID_SESSION_IHM_MAX_LEN,
    get_id_session_ihm,
    reset_id_session_ihm,
    set_id_session_ihm,
)


def _format(message: str = "message de test") -> dict:
    """Formate un enregistrement de log et retourne le JSON décodé."""
    record = logging.LogRecord(
        name="trppu",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )
    return json.loads(JsonFormatter().format(record))


@pytest.fixture(autouse=True)
def _contexte_propre():
    """Isole chaque test : le contexte est remis à None avant et après."""
    token = set_id_session_ihm(None)
    yield
    reset_id_session_ihm(token)


def test_champ_present_meme_hors_requete():
    """La clé est toujours posée (à null) pour que le mapping Kibana reste stable."""
    log_record = _format()
    assert "id_session_ihm" in log_record
    assert log_record["id_session_ihm"] is None


def test_valeur_reprise_dans_le_log():
    set_id_session_ihm("11111111-2222-3333-4444-555555555555")
    assert _format()["id_session_ihm"] == "11111111-2222-3333-4444-555555555555"


def test_reset_ne_laisse_pas_fuiter_la_session():
    """Une session ne doit pas déborder sur la requête suivante."""
    token = set_id_session_ihm("UUID-REQUETE-1")
    assert _format()["id_session_ihm"] == "UUID-REQUETE-1"
    reset_id_session_ihm(token)
    assert _format()["id_session_ihm"] is None


@pytest.mark.parametrize("valeur", [None, "", "   "])
def test_valeurs_vides_normalisees_en_null(valeur):
    """Un param absent ou vide ne doit pas produire une chaîne vide en base Kibana."""
    set_id_session_ihm(valeur)
    assert get_id_session_ihm() is None
    assert _format()["id_session_ihm"] is None


def test_valeur_bornee_en_longueur():
    """Un query param arbitraire ne doit pas gonfler chaque ligne de log."""
    set_id_session_ihm("x" * (ID_SESSION_IHM_MAX_LEN + 50))
    assert len(_format()["id_session_ihm"]) == ID_SESSION_IHM_MAX_LEN


def test_valeur_nettoyee_des_espaces():
    set_id_session_ihm("  UUID-ESPACES  ")
    assert _format()["id_session_ihm"] == "UUID-ESPACES"


def test_champ_present_sur_un_log_d_exception():
    """Les traces d'erreur doivent aussi porter la session (chemin `log.exception`)."""
    set_id_session_ihm("UUID-ERREUR")
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = logging.LogRecord(
            name="trppu",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="échec",
            args=(),
            exc_info=sys.exc_info(),
        )
        log_record = json.loads(JsonFormatter().format(record))

    assert log_record["id_session_ihm"] == "UUID-ERREUR"
    assert "exc_info" in log_record
