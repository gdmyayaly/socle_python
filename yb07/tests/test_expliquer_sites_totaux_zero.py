"""Tests de `scripts/expliquer_sites_totaux_zero.py` : une cause par site et par total nul."""

from __future__ import annotations

import csv

from app.traitements.cles_repartition import COLONNES_CSV
from scripts import expliquer_sites_totaux_zero as script


def _ligne(site, total, actif, colis="1", ip="1"):
    valeurs = dict.fromkeys(COLONNES_CSV, "")
    valeurs.update(regate_site=site, trafic_colis=colis, trafic_oo="1", trafic_3s="1",
                   potentielip=ip, libelle_site=f"Site {site}", libelle_dex="DEX")
    return [total, actif, *(valeurs[c] for c in COLONNES_CSV)]


def test_les_quatre_causes(tmp_path, capsys):
    source = tmp_path / "cles_sites_totaux_zero.csv"
    with open(source, "w", encoding="utf-8", newline="") as flux:
        ecrivain = csv.writer(flux, delimiter=";")
        ecrivain.writerow(["totaux_a_zero", "actif", *COLONNES_CSV])
        ecrivain.writerows([
            _ligne("1", "potentielip", "O", ip=""),            # vide partout
            _ligne("2", "potentielip", "O", ip="0"),           # 0 partout
            _ligne("3", "potentielip", "O", ip=""),            # mixte
            _ligne("3", "potentielip", "O", ip="0"),
            _ligne("4", "colis", "O", colis="0"),               # porté par un inactif
            _ligne("4", "colis", "N", colis="4.5"),
        ])

    assert script.main([str(source)]) == 0

    with open(tmp_path / "cles_sites_totaux_zero_explication.csv", encoding="utf-8") as flux:
        lignes = list(csv.DictReader(flux, delimiter=";"))
    causes = {l["regate_site"]: l["cause"] for l in lignes}
    assert causes == {
        "1": script.VIDE,
        "2": script.ZERO,
        "3": script.MIXTE,
        "4": script.PORTE_PAR_INACTIFS,
    }
    assert lignes[3]["somme_inactifs"] == "4.5"
    assert "4 site(s)" in capsys.readouterr().out
