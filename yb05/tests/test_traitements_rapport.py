"""Rendu des rapports et branchement de la CLI.

Le format de sortie est un livrable en soi : les tickets le décrivent au caractère près et
l'exploitation le lit. Il est donc verrouillé ici, sur les deux cas des tickets — nominal et
bloquant — plutôt que relu à chaque évolution.
"""

import json

import pytest

from app import main as cli
from app.traitements.rapport import ECHEC, ELIGIBLE, NON_ELIGIBLE, Rapport


def _rapport_nominal() -> Rapport:
    rapport = Rapport(titre="Contrôle d'éligibilité YB05", id_scenario=12345)
    rapport.ok("Scénario trouvé")
    rapport.ok("Statut VALIDE")
    rapport.statut = ELIGIBLE
    return rapport


def _rapport_bloquant() -> Rapport:
    rapport = Rapport(titre="Contrôle d'éligibilité YB05", id_scenario=12345)
    rapport.ok("Scénario trouvé")
    rapport.ko("Le scénario n'est pas figé", libelle="Scénario figé")
    rapport.ko("Aucun coefficient de rétention trouvé")
    rapport.statut = NON_ELIGIBLE
    return rapport


# ---------------------------------------------------------------------------
# Rendu texte
# ---------------------------------------------------------------------------


def test_rendu_nominal():
    lignes = _rapport_nominal().texte().splitlines()

    assert lignes[0] == "-" * 50
    assert lignes[1] == "Contrôle d'éligibilité YB05"
    assert lignes[2] == "Scénario : 12345"
    assert "[OK] Scénario trouvé" in lignes
    assert lignes[-1] == f"RESULTAT : {ELIGIBLE}"


def test_rendu_bloquant_affiche_le_motif_et_non_le_libelle():
    """Convention des tickets : en cas d'échec, c'est le message bloquant qui s'affiche."""
    texte = _rapport_bloquant().texte()

    assert "[KO] Le scénario n'est pas figé" in texte
    assert "[OK] Scénario figé" not in texte
    assert "Motifs :" in texte
    assert "  - Aucun coefficient de rétention trouvé" in texte


def test_rendu_des_etats_et_de_l_erreur():
    rapport = Rapport(titre="Calcul des trafics PDI", id_scenario=12345)
    rapport.ok("Contrôle d'éligibilité")
    rapport.erreur = "Aucune clé de répartition trouvée pour la version 12"
    rapport.etats["TRAFIC_PDI_CALCULE"] = 0
    rapport.statut = ECHEC

    texte = rapport.texte()

    assert "[ERREUR]" in texte
    assert "Aucune clé de répartition trouvée pour la version 12" in texte
    assert "TRAFIC_PDI_CALCULE = 0" in texte
    assert texte.rstrip().endswith("RESULTAT : ECHEC")


# ---------------------------------------------------------------------------
# Rendu JSON et statut
# ---------------------------------------------------------------------------


def test_rendu_json():
    charge = json.loads(json.dumps(_rapport_bloquant().to_dict(), ensure_ascii=False))

    assert charge["statut"] == NON_ELIGIBLE
    assert charge["reussi"] is False
    assert len(charge["controles"]) == 3
    assert charge["motifs"] == [
        "Le scénario n'est pas figé",
        "Aucun coefficient de rétention trouvé",
    ]


def test_reussi_tient_compte_de_l_erreur():
    """Un rapport sans contrôle en échec mais porteur d'une erreur n'est pas un succès."""
    rapport = _rapport_nominal()
    assert rapport.reussi

    rapport.erreur = "base injoignable"
    assert not rapport.reussi


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "argv, attendu",
    [
        (["--traitement=ELIGIBILITE", "--scenario=12345"], ["ELIGIBILITE", "12345"]),
        (["--traitement", "ELIGIBILITE", "--scenario", "999"], ["ELIGIBILITE", "999"]),
        (["eligibilite", "12345", "--json"], ["eligibilite", "12345", "--json"]),
        (["--json", "db-check"], ["--json", "db-check"]),
    ],
    ids=["forme=", "forme espace", "sous-commande", "inchangé"],
)
def test_normalisation_de_la_ligne_de_commande(argv, attendu):
    """Les deux formes documentées par les tickets mènent à la même sous-commande."""
    assert cli.normaliser_argv(argv) == attendu


@pytest.mark.parametrize(
    "commande",
    ["eligibilite", "ELIGIBILITE", "calcul-trafic-pdi", "CALCUL_TRAFIC_PDI",
     "calcul-trafic-agrebal", "CALCUL_TRAFIC_AGREBAL"],
)
def test_les_commandes_et_leurs_alias_sont_declarees(commande):
    args = cli.build_parser().parse_args([commande, "12345"])

    assert args.id_scenario == 12345
    assert callable(args.handler)


def test_option_json_acceptee_apres_la_sous_commande():
    """`--json` doit fonctionner avant comme après la sous-commande."""
    assert cli.build_parser().parse_args(["eligibilite", "1", "--json"]).json is True
    assert cli.build_parser().parse_args(["--json", "eligibilite", "1"]).json is True


def test_la_cli_rend_1_et_reste_lisible_quand_la_base_est_injoignable(monkeypatch, capsys):
    """Aucune stacktrace en sortie : l'exploitant lit un [KO] et un RESULTAT."""

    async def base_injoignable(id_scenario):
        raise RuntimeError("MySQL injoignable")

    monkeypatch.setattr(cli, "controle_eligibilite", base_injoignable)

    code = cli.main(["eligibilite", "12345"])
    sortie = capsys.readouterr().out

    assert code == cli.EXIT_KO
    assert "[KO] Traitement interrompu : MySQL injoignable" in sortie
    assert "RESULTAT : ECHEC" in sortie
    assert "Traceback" not in sortie


def test_la_cli_rend_0_sur_un_traitement_reussi(monkeypatch, capsys):
    async def traitement_ok(id_scenario):
        return _rapport_nominal()

    monkeypatch.setattr(cli, "controle_eligibilite", traitement_ok)

    code = cli.main(["eligibilite", "12345", "--json"])
    charge = json.loads(capsys.readouterr().out)

    assert code == cli.EXIT_OK
    assert charge["statut"] == ELIGIBLE
