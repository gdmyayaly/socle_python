"""Tests du chargement des clés de répartition (`app/traitements/cles_repartition.py`).

Ni base MySQL ni réseau : `FausseBase` remplace `Database` et le flux S3 est remplacé par
un `io.StringIO`. Ce qui est vérifié ici, ce sont les règles de gestion reprises du
chargement historique — purge avant insertion (RG6), vides convertis en NULL (RG3),
concordance du référentiel (RG1) — et le refus net de tout ce qui est douteux.
"""

from __future__ import annotations

import asyncio
import io
from contextlib import contextmanager
from datetime import date
from decimal import Decimal

import pytest

from app.traitements import cles_repartition as module
from app.erreurs import TraitementImpossible
from app.traitements.rapport import ECHEC, SUCCES
from tests.conftest import FausseBase

EN_TETE = ";".join(module.COLONNES_CSV)

# Une ligne complète, telle qu'elle sort du fichier métier.
LIGNE_PLEINE = (
    "100005554;6105149;1.0196987815888434;0.0;0.6444216483320571;BPF;372920;PDC1;"
    "SORIGNY PDC1;372900;CHARGE AMBOISE PPDC;750558;PARIS CENTRE VAL DE LOIRE DEXC;"
    "1;0;1;2026-07-21;"
)
# La même, sans établissement ni nb_pre ni potentielip : les quatre champs de la RG3.
LIGNE_TROUEE = (
    "100011320;100011320;3.02;19.25;0.70;CLO;833280;PPDC;BRIGNOLES PROVENCE VERTE PPDC;"
    ";;750569;PARIS PACA CORSE LPM DEXC;;;1;2026-07-21;"
)


def csv_de(*lignes: str) -> str:
    return "\n".join((EN_TETE, *lignes)) + "\n"


def base_nominale(**surcharges) -> FausseBase:
    """`FausseBase` répondant à toutes les requêtes du traitement."""
    reponses = {
        "SELECT COUNT(*) AS nb FROM trppu_cles_repartition": {"nb": 0},
        "nb_pdi_distincts": {
            "nb_lignes": 1,
            "nb_pdi_distincts": 1,
            "nb_actives": 1,
            "debut_validite_min": date(2026, 7, 21),
        },
    }
    reponses.update(surcharges)
    return FausseBase(reponses)


@pytest.fixture
def s3_bouchonne(monkeypatch):
    """Remplace l'accès S3 par un flux en mémoire. Retourne un poseur de contenu."""

    def poser(contenu: str) -> None:
        @contextmanager
        def _ouvrir(cle, *, encodage):
            yield io.StringIO(contenu)

        monkeypatch.setattr(module.s3, "ouvrir_objet", _ouvrir)
        monkeypatch.setattr(module.s3, "verifier_presence", lambda cle: len(contenu))
        monkeypatch.setattr(module.s3, "chemin_objet", lambda fichier: fichier)

    return poser


@pytest.fixture
def brancher_base(monkeypatch):
    """Branche une `FausseBase` sur les deux instances globales du traitement."""

    def poser(base: FausseBase) -> FausseBase:
        monkeypatch.setattr(module, "db_read", base)
        monkeypatch.setattr(module, "db_write", base)
        return base

    return poser


def charger(**kwargs):
    return asyncio.run(module.charger_cles_repartition(**kwargs))


# --- Conversion d'une ligne -------------------------------------------------


def test_la_colonne_id_du_fichier_alimente_id_pdi():
    """`id` en base est auto-incrémenté : le `id` du CSV est le PDI."""
    ligne = dict(zip(module.COLONNES_CSV, LIGNE_PLEINE.split(";")))
    valeurs = module.convertir(ligne, 2, id_referentiel=1)
    assert valeurs[0] == 100005554
    assert valeurs[1] == 6105149


def test_conversion_complete():
    ligne = dict(zip(module.COLONNES_CSV, LIGNE_PLEINE.split(";")))
    valeurs = module.convertir(ligne, 2, id_referentiel=1)
    assert valeurs == (
        100005554,
        6105149,
        Decimal("1.0196987815888434"),
        Decimal("0.0"),
        Decimal("0.6444216483320571"),
        "BPF",
        "372920",
        "PDC1",
        "SORIGNY PDC1",
        "372900",
        "CHARGE AMBOISE PPDC",
        "750558",
        "PARIS CENTRE VAL DE LOIRE DEXC",
        1,
        0,
        1,
        date(2026, 7, 21),
        None,
    )


def test_rg3_les_quatre_champs_vides_deviennent_null():
    ligne = dict(zip(module.COLONNES_CSV, LIGNE_TROUEE.split(";")))
    valeurs = module.convertir(ligne, 3, id_referentiel=1)
    assert valeurs[9] is None  # co_regate_etablissement
    assert valeurs[10] is None  # lb_etablissement
    assert valeurs[13] is None  # nb_pre
    assert valeurs[14] is None  # potentielip


def test_potentielip_vide_ne_vaut_pas_zero():
    """0 et « inconnu » ne se confondent pas : un potentiel IP absent reste NULL."""
    ligne = dict(zip(module.COLONNES_CSV, LIGNE_TROUEE.split(";")))
    assert module.convertir(ligne, 3, id_referentiel=1)[14] is None

    ligne_avec_zero = dict(zip(module.COLONNES_CSV, LIGNE_PLEINE.split(";")))
    assert module.convertir(ligne_avec_zero, 2, id_referentiel=1)[14] == 0


def test_date_fin_validite_vide_devient_null():
    ligne = dict(zip(module.COLONNES_CSV, LIGNE_PLEINE.split(";")))
    assert module.convertir(ligne, 2, id_referentiel=1)[17] is None


def test_colonne_obligatoire_vide_echoue_en_citant_la_ligne():
    """Transposition du sql_mode strict : pas de conversion silencieuse."""
    ligne = dict(zip(module.COLONNES_CSV, LIGNE_PLEINE.split(";")))
    ligne["nature"] = ""
    with pytest.raises(TraitementImpossible) as erreur:
        module.convertir(ligne, 42, id_referentiel=1)
    assert "Ligne 42" in str(erreur.value)
    assert "nature" in str(erreur.value)


def test_referentiel_discordant_refuse():
    ligne = dict(zip(module.COLONNES_CSV, LIGNE_PLEINE.split(";")))
    with pytest.raises(TraitementImpossible) as erreur:
        module.convertir(ligne, 2, id_referentiel=7)
    assert "référentiel 1" in str(erreur.value)
    assert "référentiel 7" in str(erreur.value)


def test_entete_inattendu_refuse():
    with pytest.raises(TraitementImpossible) as erreur:
        module.verifier_entete(["id", "pdi_rattache"])
    assert "En-tête du fichier inattendu" in str(erreur.value)


def test_entete_conforme_accepte():
    assert module.verifier_entete(list(module.COLONNES_CSV)) is None


# --- Déroulé du traitement --------------------------------------------------


def test_purge_avant_insertion(s3_bouchonne, brancher_base):
    """RG6 : la purge du référentiel précède toute insertion."""
    s3_bouchonne(csv_de(LIGNE_PLEINE))
    base = brancher_base(base_nominale())

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert rapport.statut == SUCCES, rapport.motifs
    ecritures = base.ecritures()
    assert "DELETE FROM trppu_cles_repartition" in ecritures[0]
    assert "INSERT INTO trppu_cles_repartition" in ecritures[1]


def test_trppu_referentiel_jamais_interrogee(s3_bouchonne, brancher_base):
    """La table est vouée à disparaître : le chargement ne doit pas en dépendre.

    `FausseBase` lève sur toute requête sans réponse déclarée ; la vérification explicite
    ci-dessous garde la trace de l'intention.
    """
    s3_bouchonne(csv_de(LIGNE_PLEINE))
    base = brancher_base(base_nominale())

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert rapport.statut == SUCCES, rapport.motifs
    assert all("trppu_referentiel" not in sql for _, sql, _ in base.journal)


def test_fichier_absent_rejette_avant_la_purge(monkeypatch, s3_bouchonne, brancher_base):
    """Purger 22 M de lignes puis découvrir que le fichier n'existe pas coûterait cher."""
    s3_bouchonne(csv_de(LIGNE_PLEINE))

    def absent(cle):
        raise TraitementImpossible(f"Fichier '{cle}' absent du bucket 'trppu'.")

    monkeypatch.setattr(module.s3, "verifier_presence", absent)
    base = brancher_base(base_nominale())

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert rapport.statut == ECHEC
    assert base.ecritures() == []


def test_decoupage_en_lots(monkeypatch, s3_bouchonne, brancher_base):
    monkeypatch.setattr(module, "CHARGEMENT_TAILLE_LOT", 2)
    s3_bouchonne(csv_de(*[LIGNE_PLEINE] * 5))
    base = brancher_base(
        base_nominale(
            **{
                "nb_pdi_distincts": {
                    "nb_lignes": 5,
                    "nb_pdi_distincts": 5,
                    "nb_actives": 5,
                    "debut_validite_min": date(2026, 7, 21),
                }
            }
        )
    )

    rapport = charger(id_referentiel=1, fichier="f.csv")

    insertions = [sql for sql in base.ecritures() if "INSERT" in sql]
    assert len(insertions) == 3  # 2 + 2 + 1
    assert rapport.etats["LIGNES_CHARGEES"] == 5


def test_numero_de_ligne_correct_sur_le_deuxieme_lot(
    monkeypatch, s3_bouchonne, brancher_base
):
    """Le numéro cité doit être celui du fichier, en-tête compris, pas celui du lot."""
    monkeypatch.setattr(module, "CHARGEMENT_TAILLE_LOT", 2)
    mauvaise = LIGNE_PLEINE.replace(";BPF;", ";;")
    s3_bouchonne(csv_de(LIGNE_PLEINE, LIGNE_PLEINE, LIGNE_PLEINE, mauvaise))
    brancher_base(base_nominale())

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert rapport.statut == ECHEC
    assert "Ligne 5" in " ".join(rapport.motifs)


def test_volumetrie_incoherente_signalee(s3_bouchonne, brancher_base):
    """Le contrôle final relit la table : un écart doit se voir dans le rapport."""
    s3_bouchonne(csv_de(LIGNE_PLEINE))
    brancher_base(
        base_nominale(
            **{
                "nb_pdi_distincts": {
                    "nb_lignes": 3,
                    "nb_pdi_distincts": 3,
                    "nb_actives": 3,
                    "debut_validite_min": date(2026, 7, 21),
                }
            }
        )
    )

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert rapport.statut == ECHEC
    assert "Volumétrie incohérente" in " ".join(rapport.motifs)


def test_doublon_retraduit_en_message_de_dedoublonnage():
    erreur = module._traduire_erreur_insertion(
        Exception("(1062, \"Duplicate entry '100005554-1' for key 'uk_pdi_ref'\")"), 1500
    )
    assert isinstance(erreur, TraitementImpossible)
    assert "Ligne 1500" in str(erreur) or "ligne 1500" in str(erreur)
    assert "dédoublonnage" in str(erreur)


def test_aucun_fichier_configure_rejette(monkeypatch, brancher_base):
    monkeypatch.setattr(module, "CSV_CLES_REPARTITION", "")
    brancher_base(FausseBase({}, lecture_seule=True))
    rapport = charger(id_referentiel=1, fichier=None)
    assert rapport.statut == ECHEC
    assert "CSV_CLES_REPARTITION" in " ".join(rapport.motifs)


def test_le_traitement_ne_leve_jamais(monkeypatch, s3_bouchonne, brancher_base):
    """Une panne inattendue devient un rapport d'échec, pas une exception."""
    s3_bouchonne(csv_de(LIGNE_PLEINE))
    base = brancher_base(base_nominale())

    async def explose(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(base, "execute", explose)

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert rapport.statut == ECHEC
    assert "boom" in (rapport.erreur or "")


# --- Source locale ----------------------------------------------------------


def test_chargement_depuis_un_fichier_local(monkeypatch, tmp_path, brancher_base):
    """Fichier réel sur disque : S3 ne doit jamais être sollicité."""

    def interdit(*args, **kwargs):
        raise AssertionError("S3 sollicité pour un chargement local")

    monkeypatch.setattr(module.s3, "verifier_presence", interdit)
    monkeypatch.setattr(module.s3, "ouvrir_objet", interdit)
    fichier = tmp_path / "cles.csv"
    fichier.write_text(csv_de(LIGNE_PLEINE), encoding="utf-8")
    base = brancher_base(base_nominale())

    rapport = charger(id_referentiel=1, chemin_local=str(fichier))

    assert rapport.statut == SUCCES, rapport.motifs
    assert rapport.etats["LIGNES_CHARGEES"] == 1
    assert any("présent en local" in c.libelle for c in rapport.controles)
    ecritures = base.ecritures()
    assert "DELETE FROM trppu_cles_repartition" in ecritures[0]
    assert "INSERT INTO trppu_cles_repartition" in ecritures[1]


def test_fichier_local_absent_rejette_avant_la_purge(tmp_path, brancher_base):
    base = brancher_base(base_nominale())

    rapport = charger(id_referentiel=1, chemin_local=str(tmp_path / "absent.csv"))

    assert rapport.statut == ECHEC
    assert "introuvable" in " ".join(rapport.motifs)
    assert base.ecritures() == []


def test_commande_locale_analyse_ses_arguments():
    from app.main import build_parser, cmd_charger_cles_repartition_local

    args = build_parser().parse_args(
        ["charger-cles-repartition-local", "3", "data/cles.csv"]
    )

    assert args.id_traitement == 3
    assert args.chemin == "data/cles.csv"
    assert args.handler is cmd_charger_cles_repartition_local


def test_commande_locale_transmet_le_chemin(monkeypatch):
    import app.main as main

    recu = {}

    async def _faux(id_referentiel, fichier=None, *, chemin_local=None):
        recu.update(id_referentiel=id_referentiel, chemin_local=chemin_local)
        return module.Rapport(titre="T", id_traitement=id_referentiel)

    monkeypatch.setattr(main, "charger_cles_repartition", _faux)
    args = main.build_parser().parse_args(["charger-cles-repartition-local", "2", "x.csv"])

    asyncio.run(main.cmd_charger_cles_repartition_local(args))

    assert recu == {"id_referentiel": 2, "chemin_local": "x.csv"}
