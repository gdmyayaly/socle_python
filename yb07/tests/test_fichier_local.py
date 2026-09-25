"""Tests de l'accès aux fichiers locaux (`app/services/fichier_local.py`).

Fichiers réels écrits dans le dossier temporaire de pytest : rien n'est simulé, c'est le
comportement du disque qui est vérifié.
"""

from __future__ import annotations

import gzip

import pytest

from app.erreurs import TraitementImpossible
from app.services import fichier_local


def test_presence_rend_la_taille(tmp_path):
    fichier = tmp_path / "cles.csv"
    fichier.write_bytes(b"a;b\n1;2\n")

    assert fichier_local.verifier_presence(str(fichier)) == 8


def test_fichier_absent_refuse(tmp_path):
    with pytest.raises(TraitementImpossible) as erreur:
        fichier_local.verifier_presence(str(tmp_path / "absent.csv"))
    assert "introuvable" in str(erreur.value)


def test_dossier_refuse(tmp_path):
    with pytest.raises(TraitementImpossible) as erreur:
        fichier_local.verifier_presence(str(tmp_path))
    assert "n'est pas un fichier" in str(erreur.value)


def test_lecture_texte_bom_absorbe(tmp_path):
    fichier = tmp_path / "cles.csv"
    fichier.write_bytes("﻿a;b\r\n1;é\r\n".encode("utf-8"))

    with fichier_local.ouvrir(str(fichier), encodage="utf-8-sig") as flux:
        contenu = flux.read()

    # newline="" : les \r\n sont laissés au module csv.
    assert contenu == "a;b\r\n1;é\r\n"


def test_lecture_gz_decompressee_a_la_volee(tmp_path):
    fichier = tmp_path / "cles.csv.gz"
    fichier.write_bytes(gzip.compress(b"a;b\n1;2\n"))

    with fichier_local.ouvrir(str(fichier), encodage="utf-8") as flux:
        assert flux.read() == "a;b\n1;2\n"


def test_ouverture_impossible_traduite(tmp_path):
    with pytest.raises(TraitementImpossible):
        with fichier_local.ouvrir(str(tmp_path / "absent.csv"), encodage="utf-8"):
            pass
