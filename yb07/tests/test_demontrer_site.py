"""Tests de `scripts/demontrer_site.py` : extrait du site et démonstration pas à pas."""

from __future__ import annotations

import csv

from app.traitements.cles_repartition import COLONNES_CSV
from scripts import demontrer_site as script


def _ligne(pdi, site, colis="1.0", ip="1", fin="", dex="750558"):
    valeurs = dict.fromkeys(COLONNES_CSV, "")
    valeurs.update(
        id=pdi, pdi_rattache=pdi, trafic_colis=colis, trafic_oo="1.5", trafic_3s="0.2",
        nature="BPF", regate_site=site, type="PDC1", libelle_site="SORIGNY PDC1",
        regate_dex=dex, libelle_dex="PARIS CENTRE", potentielip=ip, id_referentiel="1",
        date_debut_validite="2026-07-21", date_fin_validite=fin,
    )
    return ";".join(valeurs[c] for c in COLONNES_CSV)


def _source(tmp_path, *lignes):
    source = tmp_path / "livraison.csv"
    source.write_text("\n".join([";".join(COLONNES_CSV), *lignes]) + "\n", encoding="utf-8")
    return source


def test_demonstration_d_un_potentiel_ip_vide(tmp_path):
    source = _source(
        tmp_path,
        _ligne("1", "111111"),
        _ligne("2", "122200", ip=""),
        _ligne("3", "122200", ip=""),
        _ligne("4", "122200", ip="5", fin="2026-08-01"),  # inactif : porte l'IP
        _ligne("5", "122200", ip="2", dex=""),            # non conforme : exclu du total
    )

    assert script.main([str(source), "122200", "--sortie", str(tmp_path / "demo")]) == 0

    with open(tmp_path / "demo" / "livraison_site_122200.csv", encoding="utf-8") as flux:
        lignes = list(csv.reader(flux, delimiter=";"))
    assert lignes[0][:3] == ["numero_ligne", "actif", "conforme"]
    assert [(l[0], l[1], l[2]) for l in lignes[1:]] == [
        ("3", "O", "O"), ("4", "O", "O"), ("5", "N", "O"), ("6", "O", "N"),
    ]

    texte = (tmp_path / "demo" / "livraison_site_122200_demonstration.txt").read_text(
        encoding="utf-8"
    )
    assert "5. POTENTIEL IP" in texte
    assert "(vide) + (vide)" in texte and "= 0" in texte
    assert "= 0 / 0" in texte
    assert "1 PDI INACTIF(S) portent 5" in texte
    assert "refusee(s) au" in texte  # la ligne sans DEX est signalée, pas comptée
    assert "Total = 3.0" in texte  # colis : sain
    assert texte.isascii()
    assert all(len(l) <= 72 for l in texte.splitlines() if not l.startswith("Fichier"))


def test_site_sans_total_nul(tmp_path):
    source = _source(tmp_path, _ligne("1", "111111"))

    assert script.main([str(source), "111111"]) == 0

    texte = (tmp_path / "livraison_site_111111_demonstration.txt").read_text(encoding="utf-8")
    assert "Aucun total nul pour le site 111111" in texte


def test_site_absent_du_fichier(tmp_path):
    source = _source(tmp_path, _ligne("1", "111111"))

    assert script.main([str(source), "999999"]) == 1
