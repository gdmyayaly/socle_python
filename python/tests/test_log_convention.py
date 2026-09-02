"""Tests de la convention de log partagée (`app/log_utils.py`).

Couvre les trois briques sur lesquelles reposent tous les messages normalisés :
le rendu du bloc de contexte, l'expurgation des champs sensibles et le calcul du
delta avant/après. Voir `api_docs/CONVENTION-LOGS.md`.
"""

import logging

import pytest
from pydantic import BaseModel

from app.log_utils import (
    CHAMPS_SENSIBLES,
    CTX_VALEUR_MAX_LEN,
    ctx,
    diff_champs,
    params_loggables,
)


# --- ctx() : rendu du bloc (cle=valeur, ...) --------------------------------


def test_ordre_des_cles_preserve():
    """Identifiants d'abord, duration_ms en dernier : l'ordre d'appel fait foi."""
    rendu = str(ctx(id_scenario=52, co_regate="012345", duration_ms=84.2))
    assert rendu == "(id_scenario=52, co_regate=012345, duration_ms=84.2)"


def test_valeurs_none_omises():
    """Un champ absent ne doit pas polluer la ligne avec 'cle=None'."""
    assert str(ctx(id_scenario=52, motif=None)) == "(id_scenario=52)"


def test_contexte_vide():
    assert str(ctx()) == "()"


def test_flottant_formate_a_une_decimale():
    """Une durée à la microseconde près n'apporte rien et casse le regroupement."""
    assert str(ctx(duration_ms=84.2394)) == "(duration_ms=84.2)"


def test_chaine_non_quotee():
    """Un co_regate doit se lire tel quel, sans guillemets parasites."""
    assert str(ctx(co_regate="012345")) == "(co_regate=012345)"


def test_valeur_longue_tronquee():
    rendu = str(ctx(motif="x" * (CTX_VALEUR_MAX_LEN + 100)))
    assert "tronqué 100 car." in rendu
    assert len(rendu) < CTX_VALEUR_MAX_LEN + 100


def test_rendu_paresseux(caplog):
    """Le contexte ne doit pas être rendu quand le niveau est désactivé.

    C'est la raison d'être de l'objet retourné par `ctx` : les `logger.debug`
    des étapes intermédiaires ne doivent rien coûter en production.
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

    # On compte « au moins un » et non « exactement un » : un handler peut
    # formater le même enregistrement plusieurs fois.
    with caplog.at_level(logging.INFO, logger="test.paresse"):
        logger.info("message %s", ctx(champ=Espion()))
    assert rendus, "le contexte n'a pas été rendu alors que INFO est actif"


def test_rendu_ne_leve_jamais():
    """Un __repr__ défaillant ne doit pas casser la requête qu'il journalise.

    Le repli est appliqué champ par champ (via `safe_preview`) : les autres
    valeurs du contexte restent lisibles.
    """

    class Explosif:
        def __repr__(self):
            raise RuntimeError("boom")

    rendu = str(ctx(id_scenario=52, champ=Explosif()))
    assert "id_scenario=52" in rendu
    assert "boom" not in rendu


# --- params_loggables() : la règle « jamais d'id_rh en clair » --------------


class _Payload(BaseModel):
    co_regate: str
    id_rh: str
    nb_jours_semaine: int


def test_id_rh_absent_d_un_modele_pydantic():
    resultat = params_loggables(
        _Payload(co_regate="012345", id_rh="A123456", nb_jours_semaine=5)
    )
    assert "id_rh" not in resultat
    assert "A123456" not in str(resultat)
    assert resultat == {"co_regate": "012345", "nb_jours_semaine": 5}


def test_id_rh_absent_d_un_dict():
    assert params_loggables({"co_regate": "012345", "id_rh": "A123456"}) == {
        "co_regate": "012345"
    }


@pytest.mark.parametrize("champ", sorted(CHAMPS_SENSIBLES))
def test_tous_les_champs_sensibles_sont_retires(champ):
    assert params_loggables({champ: "SECRET", "garde": 1}) == {"garde": 1}


def test_objet_non_serialisable_borne():
    """Un objet quelconque ne doit ni lever, ni déverser sa représentation entière."""
    resultat = params_loggables(object())
    assert set(resultat) == {"payload"}


# --- diff_champs() : brique de reconstitution des UPDATE --------------------


def test_seuls_les_champs_modifies_remontent():
    avant = {"statut": "EN COURS", "nb_jours_scenario": 250, "lb_scenario": "S1"}
    apres = {"statut": "VALIDE", "nb_jours_scenario": 250, "lb_scenario": "S1"}
    assert diff_champs(avant, apres) == {"statut": ["EN COURS", "VALIDE"]}


def test_aucun_changement_donne_un_delta_vide():
    etat = {"statut": "EN COURS"}
    assert diff_champs(etat, dict(etat)) == {}


def test_colonnes_absentes_de_apres_ignorees():
    """Un UPDATE partiel ne doit pas faire apparaître les colonnes qu'il ne touche pas."""
    avant = {"statut": "EN COURS", "lb_scenario": "S1"}
    apres = {"statut": "VALIDE"}
    assert diff_champs(avant, apres) == {"statut": ["EN COURS", "VALIDE"]}


def test_champ_sensible_jamais_dans_le_delta():
    delta = diff_champs({"id_rh": "AVANT"}, {"id_rh": "APRES", "statut": "VALIDE"})
    assert "id_rh" not in delta
    assert delta == {"statut": [None, "VALIDE"]}
