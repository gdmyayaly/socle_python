"""DSR-701 — les douze règles d'éligibilité, une par une.

Chaque règle est testée dans son cas bloquant : c'est là qu'une inversion de condition se voit.
Le cas nominal, lui, vérifie surtout que les douze règles sont bien toutes évaluées et
affichées — un contrôle oublié passerait sinon inaperçu, le verdict restant vert.
"""

import asyncio

import pytest

from app.traitements.eligibilite import controle_eligibilite
from app.traitements.rapport import ELIGIBLE, NON_ELIGIBLE
from tests.conftest import FausseBase, reponses_eligibles


def _controler(reponses, *, lecture_seule=True):
    base = FausseBase(reponses, lecture_seule=lecture_seule)
    rapport = asyncio.run(controle_eligibilite(12345, db_lecture=base))
    return rapport, base


def _scenario(**surcharges):
    """Réponses d'un site complet, avec un scénario modifié sur les champs donnés."""
    reponses = reponses_eligibles()
    reponses["FROM trppu_scenario WHERE id_scenario"].update(surcharges)
    return reponses


# ---------------------------------------------------------------------------
# Cas nominal
# ---------------------------------------------------------------------------


def test_scenario_complet_est_eligible():
    rapport, _ = _controler(reponses_eligibles())

    assert rapport.statut == ELIGIBLE
    assert rapport.reussi
    assert rapport.motifs == []


def test_les_douze_regles_sont_toutes_evaluees():
    """Le mode contrôle doit rendre la liste complète, pas s'arrêter au premier refus."""
    rapport, _ = _controler(reponses_eligibles())

    assert len(rapport.controles) == 12


def test_le_controle_n_ecrit_rien():
    """CA-05 : le traitement ELIGIBILITE n'insère, ne modifie et ne met à jour rien.

    La doublure lève sur toute écriture : si le traitement en tentait une, ce test échouerait.
    """
    rapport, base = _controler(reponses_eligibles())

    assert rapport.reussi
    assert base.ecritures() == []


# ---------------------------------------------------------------------------
# Règle 1 — existence
# ---------------------------------------------------------------------------


def test_regle_1_scenario_inexistant():
    reponses = reponses_eligibles()
    reponses["FROM trppu_scenario WHERE id_scenario"] = None

    rapport, _ = _controler(reponses)

    assert rapport.statut == NON_ELIGIBLE
    assert rapport.motifs == ["Scénario inexistant"]
    # Court-circuit : sans scénario, aucune autre règle n'est évaluable.
    assert len(rapport.controles) == 1


# ---------------------------------------------------------------------------
# Règles 2 à 7 — état du scénario
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "surcharges, motif",
    [
        ({"statut": "EN COURS"}, "Le scénario n'est pas validé"),
        ({"est_fige": 0}, "Le scénario n'est pas figé"),
        ({"calcul_trafic_en_cours": 1}, "Un calcul de trafic est déjà en cours"),
        ({"trafic_pdi_calcule": 1}, "Les trafics PDI sont déjà calculés"),
        ({"trafic_agrebal_calcule": 1}, "Les trafics Agrébal sont déjà calculés"),
        ({"id_pic_version": 0}, "Aucune version PIC associée au scénario"),
    ],
    ids=["statut", "fige", "calcul_en_cours", "pdi_calcule", "agrebal_calcule", "pic_version"],
)
def test_regles_2_a_7(surcharges, motif):
    rapport, _ = _controler(_scenario(**surcharges))

    assert rapport.statut == NON_ELIGIBLE
    assert motif in rapport.motifs
    # Les autres règles restent évaluées : un seul motif remonte.
    assert len(rapport.motifs) == 1
    assert len(rapport.controles) == 12


# ---------------------------------------------------------------------------
# Règles 8 à 12 — données du site
# ---------------------------------------------------------------------------


def test_regle_8_aucun_coefficient_de_retention():
    reponses = reponses_eligibles()
    reponses["COUNT(*) AS nb FROM trppu_pic_coefficients"] = {"nb": 0}

    rapport, _ = _controler(reponses)

    assert rapport.motifs == [
        "Aucun coefficient de rétention trouvé pour la version PIC du scénario"
    ]


def test_regle_9_aucune_version_de_cles_active():
    reponses = reponses_eligibles()
    reponses["FROM trppu_version_cle"] = None

    rapport, _ = _controler(reponses)

    assert rapport.motifs == ["Aucune version de clés active disponible"]


def test_regle_10_aucun_referentiel():
    reponses = reponses_eligibles()
    reponses["FROM trppu_referentiel"] = None

    rapport, _ = _controler(reponses)

    assert rapport.motifs == ["Aucun référentiel actif disponible"]


def test_regle_10_referentiel_de_la_version_perime():
    """Le ticket exige que les deux requêtes rendent le même identifiant.

    Un écart signifie que la version de clés active repose sur un référentiel dépassé :
    calculer produirait des trafics à partir de clés périmées.
    """
    reponses = reponses_eligibles()
    reponses["FROM trppu_referentiel"] = {"id_referentiel": 9}

    rapport, _ = _controler(reponses)

    assert rapport.statut == NON_ELIGIBLE
    assert "n'est pas le dernier référentiel du site (9)" in rapport.motifs[0]


def test_regle_11_aucun_agrebal_sur_le_site():
    """La règle est lue sur `trppu_agrebal_pdi`, pas sur la table des trafics calculés."""
    reponses = reponses_eligibles()
    reponses["FROM trppu_agrebal_pdi"] = []

    rapport, _ = _controler(reponses)

    assert "Aucun Agrébal trouvé sur le site" in rapport.motifs
    assert "Aucun PDI rattaché aux Agrébals du site" in rapport.motifs


def test_regle_12_agrebal_sans_pdi():
    reponses = reponses_eligibles()
    reponses["FROM trppu_agrebal_pdi"] = [
        {
            "agrebal_id": 2404,
            "agrebal_uuid": "uuid-2404",
            "agrebal_pdiQuantity": 0,
            "agrebal_pdiList": "[]",
        }
    ]

    rapport, _ = _controler(reponses)

    assert rapport.motifs == ["Aucun PDI rattaché aux Agrébals du site"]


def test_liste_de_pdi_illisible_ne_fait_pas_planter_le_controle():
    """Un JSON corrompu doit produire un motif métier, pas une exception."""
    reponses = reponses_eligibles()
    reponses["FROM trppu_agrebal_pdi"] = [
        {
            "agrebal_id": 2404,
            "agrebal_uuid": "uuid-2404",
            "agrebal_pdiQuantity": 3,
            "agrebal_pdiList": "{ceci n'est pas du JSON",
        }
    ]

    rapport, _ = _controler(reponses)

    assert rapport.motifs == ["Aucun PDI rattaché aux Agrébals du site"]


# ---------------------------------------------------------------------------
# Cumul
# ---------------------------------------------------------------------------


def test_plusieurs_motifs_sont_cumules():
    """Cas bloquant du ticket : le rapport liste tous les motifs d'un coup."""
    reponses = _scenario(est_fige=0)
    reponses["COUNT(*) AS nb FROM trppu_pic_coefficients"] = {"nb": 0}

    rapport, _ = _controler(reponses)

    assert rapport.statut == NON_ELIGIBLE
    assert len(rapport.motifs) == 2
    assert "Le scénario n'est pas figé" in rapport.motifs
