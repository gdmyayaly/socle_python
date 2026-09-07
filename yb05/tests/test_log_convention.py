"""Tests de la convention de log (`app/log_utils.py` + `app/json_formatter.py`).

Deux briques : le rendu du bloc de contexte, et l'identifiant de corrélation
`id_scenario` que `JsonFormatter` pose sur chaque ligne. En mode ALL les workers
s'entrelacent — c'est ce champ qui permet de reconstituer la trace d'un scénario.

Cf. `yb05/docs/CONVENTION-LOGS.md`.
"""

import asyncio
import json
import logging

import pytest

from app.json_formatter import JsonFormatter
from app.log_utils import (
    CTX_VALEUR_MAX_LEN,
    ctx,
    get_id_scenario,
    reset_id_scenario,
    safe_preview,
    set_id_scenario,
)


def _format(message: str = "message de test") -> dict:
    """Formate un enregistrement de log et retourne le JSON décodé."""
    record = logging.LogRecord(
        name="yb05",
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
    token = set_id_scenario(None)
    yield
    reset_id_scenario(token)


# --- ctx() : rendu du bloc (cle=valeur, ...) --------------------------------


def test_ordre_des_cles_preserve():
    """Identifiants d'abord, duration_ms en dernier : l'ordre d'appel fait foi."""
    rendu = str(ctx(id_scenario=12345, lignes=1240, duration_ms=8421.04))
    assert rendu == "(id_scenario=12345, lignes=1240, duration_ms=8421.0)"


def test_valeurs_none_omises():
    assert str(ctx(id_scenario=12345, motif=None)) == "(id_scenario=12345)"


def test_contexte_vide():
    assert str(ctx()) == "()"


def test_chaine_non_quotee():
    """Un verdict doit se lire tel quel, sans guillemets parasites."""
    assert str(ctx(verdict="NON_ELIGIBLE")) == "(verdict=NON_ELIGIBLE)"


def test_valeur_longue_tronquee():
    rendu = str(ctx(motif="x" * (CTX_VALEUR_MAX_LEN + 100)))
    assert "tronqué 100 car." in rendu
    assert len(rendu) < CTX_VALEUR_MAX_LEN + 100


def test_rendu_paresseux(caplog):
    """Le contexte ne doit pas être rendu quand le niveau est désactivé.

    C'est la raison d'être de l'objet retourné par `ctx` : les `logger.debug` des
    étapes de calcul ne sont visibles qu'avec `-v` et ne doivent rien coûter sinon.
    """
    rendus: list[int] = []

    class Espion:
        def __repr__(self):
            rendus.append(1)
            return "valeur"

    logger = logging.getLogger("test.paresse")
    logger.setLevel(logging.INFO)
    logger.debug("message %s", ctx(champ=Espion()))
    assert rendus == [], "le contexte a été rendu alors que DEBUG est désactivé"

    # « au moins un » et non « exactement un » : un handler peut formater le même
    # enregistrement plusieurs fois.
    with caplog.at_level(logging.INFO, logger="test.paresse"):
        logger.info("message %s", ctx(champ=Espion()))
    assert rendus, "le contexte n'a pas été rendu alors que INFO est actif"


def test_rendu_ne_leve_jamais():
    """Un __repr__ défaillant ne doit pas casser le traitement qu'il journalise."""

    class Explosif:
        def __repr__(self):
            raise RuntimeError("boom")

    rendu = str(ctx(id_scenario=12345, champ=Explosif()))
    assert "id_scenario=12345" in rendu
    assert "boom" not in rendu


def test_safe_preview_borne_et_ne_leve_pas():
    assert safe_preview("x" * 1000, max_len=50).startswith("'" + "x" * 20)
    assert "truncated" in safe_preview("x" * 1000, max_len=50)


# --- id_scenario : l'identifiant de corrélation ----------------------------


def test_champ_present_meme_hors_traitement():
    """La clé est toujours posée (à null) pour que le mapping Kibana reste stable."""
    log_record = _format()
    assert "id_scenario" in log_record
    assert log_record["id_scenario"] is None


def test_valeur_reprise_dans_le_log():
    set_id_scenario(12345)
    assert _format()["id_scenario"] == 12345


def test_reset_ne_laisse_pas_fuiter_le_scenario():
    """Un scénario ne doit pas déborder sur le suivant dans la file du worker."""
    token = set_id_scenario(111)
    assert _format()["id_scenario"] == 111
    reset_id_scenario(token)
    assert _format()["id_scenario"] is None


@pytest.mark.parametrize("valeur", [None, "pas un entier", object()])
def test_valeurs_inexploitables_normalisees_en_null(valeur):
    """Annoter un log ne doit jamais faire échouer le traitement annoté."""
    set_id_scenario(valeur)
    assert get_id_scenario() is None
    assert _format()["id_scenario"] is None


def test_valeur_texte_convertie_en_entier():
    set_id_scenario("12345")
    assert get_id_scenario() == 12345


def test_champ_present_sur_un_log_d_exception():
    """Les traces d'erreur doivent aussi porter le scénario (chemin `logger.exception`)."""
    set_id_scenario(12345)
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = logging.LogRecord(
            name="yb05",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="échec",
            args=(),
            exc_info=sys.exc_info(),
        )
        log_record = json.loads(JsonFormatter().format(record))

    assert log_record["id_scenario"] == 12345
    assert "exc_info" in log_record


def test_isolation_entre_taches_asyncio():
    """Le mode ALL fait tourner NB_WORKER scénarios en parallèle.

    Chaque tâche doit voir son propre scénario : sans cette isolation, les lignes
    de log seraient attribuées au mauvais scénario.
    """
    vus: dict[int, int | None] = {}

    async def worker(numero: int, id_scenario: int) -> None:
        set_id_scenario(id_scenario)
        await asyncio.sleep(0)  # laisse la main aux autres tâches
        vus[numero] = get_id_scenario()

    async def scenario_principal() -> None:
        await asyncio.gather(worker(1, 111), worker(2, 222), worker(3, 333))

    asyncio.run(scenario_principal())
    assert vus == {1: 111, 2: 222, 3: 333}
