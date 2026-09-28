"""Tests du script de nettoyage (`scripts/nettoyer_csv_cles.py`).

Fichiers réels dans le dossier temporaire de pytest : ce qui est vérifié, ce sont les
quatre fichiers produits.
"""

from __future__ import annotations

import csv

import pytest

from app.traitements.cles_repartition import COLONNES_CSV
from scripts import nettoyer_csv_cles as script
from tests.test_chargement_cles_repartition import (
    LIGNE_MAL_FORMEE,
    LIGNE_PLEINE,
    LIGNE_TROUEE,
    csv_de,
)

# Même PDI que LIGNE_PLEINE, trafic différent : doublon en conflit.
LIGNE_CONFLIT = LIGNE_PLEINE.replace("1.0196987815888434", "2.5", 1)
# `nature` sur 4 caractères : passe la conversion, refusé par char(3) en base.
LIGNE_TROP_LONGUE = LIGNE_TROUEE.replace(";CLO;", ";CLOS;", 1).replace(
    "100011320;100011320", "100099999;100099999", 1
)


def lire(chemin) -> list[list[str]]:
    with open(chemin, encoding="utf-8", newline="") as flux:
        return list(csv.reader(flux, delimiter=";"))


@pytest.fixture
def source(tmp_path):
    def poser(*lignes: str, nom: str = "cles.csv"):
        chemin = tmp_path / nom
        chemin.write_text(csv_de(*lignes), encoding="utf-8")
        return chemin

    return poser


def test_fichier_conforme_rendu_a_l_identique(source):
    chemin = source(LIGNE_PLEINE, LIGNE_TROUEE)

    bilan = script.nettoyer(chemin)

    assert (bilan.lues, bilan.conservees, bilan.erreurs, bilan.doublons) == (2, 2, 0, 0)
    assert bilan.referentiel == 1
    propre = lire(bilan.sorties["propre"])
    assert propre[0] == list(COLONNES_CSV)
    assert propre[1:] == [LIGNE_PLEINE.split(";"), LIGNE_TROUEE.split(";")]
    assert lire(bilan.sorties["erreurs"]) == [["numero_ligne", "motif", *COLONNES_CSV]]


def test_tri_erreurs_doublons_identiques_et_conflits(source):
    chemin = source(LIGNE_PLEINE, LIGNE_MAL_FORMEE, LIGNE_PLEINE, LIGNE_CONFLIT, LIGNE_TROUEE)

    bilan = script.nettoyer(chemin)

    assert bilan.conservees == 2
    assert bilan.erreurs == 1
    assert (bilan.doublons_identiques, bilan.doublons_conflits) == (1, 1)

    # LIGNE_MAL_FORMEE porte le même PDI mais est invalide : c'est une erreur, pas un doublon.
    erreurs = lire(bilan.sorties["erreurs"])[1:]
    assert erreurs[0][0] == "3"
    assert "trafic_colis" in erreurs[0][1]

    doublons = lire(bilan.sorties["doublons"])[1:]
    assert [d[:3] for d in doublons] == [["4", "2", "IDENTIQUE"], ["5", "2", "CONFLIT"]]


def test_contraintes_mysql_verifiees(source):
    """`nature` sur 4 caractères passerait la conversion, pas le char(3) de la table."""
    bilan = script.nettoyer(source(LIGNE_PLEINE, LIGNE_TROP_LONGUE))

    assert bilan.erreurs == 1
    assert "'nature' fait 4 caractères, 3 au maximum" in bilan.exemples_erreurs[0]


def test_referentiel_impose_ecarte_les_autres(source):
    bilan = script.nettoyer(source(LIGNE_PLEINE), referentiel=2)

    assert bilan.conservees == 0
    assert "référentiel 1" in bilan.exemples_erreurs[0]


def test_nombre_de_colonnes_incorrect(source):
    bilan = script.nettoyer(source(LIGNE_PLEINE, "1;2;3"))

    assert bilan.erreurs == 1
    assert "3 colonne(s) au lieu de 18" in bilan.exemples_erreurs[0]


def test_lignes_vides_ignorees(tmp_path):
    chemin = tmp_path / "cles.csv"
    chemin.write_text(csv_de(LIGNE_PLEINE) + "\n;;\n", encoding="utf-8")

    bilan = script.nettoyer(chemin)

    assert (bilan.lues, bilan.vides, bilan.erreurs) == (1, 2, 0)


def test_entete_faux_refuse(tmp_path):
    chemin = tmp_path / "cles.csv"
    chemin.write_text("a;b\n1;2\n", encoding="utf-8")

    assert script.main([str(chemin)]) == 1


def test_rapport_texte_72_colonnes_sans_accents(source, capsys):
    chemin = source(LIGNE_PLEINE, LIGNE_MAL_FORMEE, LIGNE_CONFLIT)

    assert script.main([str(chemin), "--sortie", str(chemin.parent / "sortie")]) == 0

    rapport = (chemin.parent / "sortie" / "cles_rapport.txt").read_text(encoding="utf-8")
    # Seuls les chemins et la commande (lignes indentées) peuvent dépasser : coupés, ils
    # ne se copieraient plus.
    assert all(len(l) <= 72 for l in rapport.splitlines() if not l.startswith("  "))
    commande = next(l for l in rapport.splitlines() if "charger-cles-repartition-local" in l)
    assert commande.rstrip().endswith("cles_propre.csv")
    assert rapport.isascii()
    assert "Lignes en erreur" in rapport
    assert "en conflit" in rapport
    assert "charger-cles-repartition-local 1" in rapport


def test_categories_regroupent_sans_valeur_ni_numero():
    motif = "Ligne 42 : 'trafic_colis' = 'abc' n'est pas un décimal."
    assert script.categorie(motif) == "'trafic_colis' n'est pas un décimal."


def test_noms_de_sortie(tmp_path):
    sorties = script.chemins_de_sortie(tmp_path / "livraison.csv.gz", None)
    assert sorties["propre"].name == "livraison_propre.csv"
    assert sorties["rapport"].name == "livraison_rapport.txt"
