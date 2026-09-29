"""Tests de `scripts/extraire_sites_totaux_zero.py` : trois fichiers, mêmes règles que le
chargement et que l'étape `agregats`."""

from __future__ import annotations

import csv

from app.traitements.cles_repartition import COLONNES_CSV
from scripts import extraire_sites_totaux_zero as script


def _ligne(pdi, site, colis, ip, fin="", dex="750558"):
    valeurs = dict.fromkeys(COLONNES_CSV, "")
    valeurs.update(
        id=pdi, pdi_rattache=pdi, trafic_colis=colis, trafic_oo="1.5", trafic_3s="0.2",
        nature="BPF", regate_site=site, type="PDC1", libelle_site="X", regate_dex=dex,
        libelle_dex="D", potentielip=ip, id_referentiel="1",
        date_debut_validite="2026-07-21", date_fin_validite=fin,
    )
    return ";".join(valeurs[c] for c in COLONNES_CSV)


def _lire(chemin):
    with open(chemin, encoding="utf-8", newline="") as flux:
        return list(csv.reader(flux, delimiter=";"))


def _source(tmp_path, *lignes):
    source = tmp_path / "livraison.csv"
    source.write_text("\n".join([";".join(COLONNES_CSV), *lignes]) + "\n", encoding="utf-8")
    return source


def test_trois_fichiers_nommes_d_apres_la_source_dans_le_dossier_demande(tmp_path):
    source = _source(tmp_path, _ligne("1", "111111", "1.0", "2"))
    dossier = tmp_path / "tri"

    assert script.main([str(source), "--sortie", str(dossier)]) == 0

    assert sorted(p.name for p in dossier.iterdir()) == [
        "livraison_bon.csv",
        "livraison_lignes_incompletes.csv",
        "livraison_sites_totaux_zero.csv",
    ]


def test_tri_des_lignes(tmp_path):
    source = _source(
        tmp_path,
        _ligne("1", "111111", "1.0", "2"),                   # 2 : site sain -> bon
        _ligne("2", "111111", "abc", "1"),                   # 3 : incomplète
        _ligne("3", "222222", "1.0", ""),                    # 4 : potentiel IP vide partout
        _ligne("4", "222222", "2.0", ""),                    # 5
        _ligne("5", "333333", "0.0", "3"),                   # 6 : colis porté par un inactif
        _ligne("6", "333333", "4.0", "1", fin="2026-08-01"), # 7
        _ligne("7", "444444", "1.0", "1", dex=""),           # 8 : incomplète (DEX vide)
    )

    bilan = script.trier(source)

    incompletes = _lire(bilan["sorties"]["incompletes"])
    assert [l[0] for l in incompletes[1:]] == ["3", "8"]
    assert "trafic_colis" in incompletes[1][1]

    zero = _lire(bilan["sorties"]["totaux_zero"])
    assert zero[0][:2] == ["totaux_a_zero", "actif"]
    rang_id = 2 + COLONNES_CSV.index("id")
    assert {(l[0], l[1], l[rang_id]) for l in zero[1:]} == {
        ("potentielip", "O", "3"),
        ("potentielip", "O", "4"),
        ("colis", "O", "5"),
        ("colis", "N", "6"),
    }

    bon = _lire(bilan["sorties"]["bon"])
    assert bon[0] == list(COLONNES_CSV)  # rechargeable tel quel : en-tête d'origine
    assert [l[0] for l in bon[1:]] == ["1"]


def test_les_totaux_ignorent_les_lignes_incompletes(tmp_path):
    """Le chargement les refuserait : elles n'entrent pas dans les totaux de la base."""
    source = _source(
        tmp_path,
        _ligne("1", "555555", "0.0", "1"),
        _ligne("2", "555555", "5.0", "1", dex=""),  # incomplète : ses 5 colis ne comptent pas
    )

    bilan = script.trier(source)

    assert bilan["sites_totaux_zero"] == {"555555": "colis"}


def test_le_fichier_bon_se_recharge(tmp_path):
    """Il repasse la validation du chargement sans aucune ligne incomplète ni site nul."""
    source = _source(
        tmp_path,
        _ligne("1", "111111", "1.0", "2"),
        _ligne("2", "222222", "1.0", ""),
        _ligne("3", "111111", "x", "2"),
    )
    bon = script.trier(source)["sorties"]["bon"]

    rebilan = script.trier(bon, dossier_sortie=tmp_path / "second")

    assert rebilan["incompletes"] == 0
    assert rebilan["sites_totaux_zero"] == {}
    assert rebilan["lignes_bonnes"] == 1


def test_sortie_qui_n_est_pas_un_dossier_refusee(tmp_path):
    source = _source(tmp_path, _ligne("1", "111111", "1.0", "2"))
    fichier = tmp_path / "pas_un_dossier.txt"
    fichier.write_text("x", encoding="utf-8")

    assert script.main([str(source), "--sortie", str(fichier)]) == 1
