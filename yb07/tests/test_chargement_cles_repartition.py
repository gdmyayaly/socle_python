"""Tests du chargement des clés de répartition (`app/traitements/cles_repartition.py`).

Ni base MySQL ni réseau : `FausseBase` remplace `Database` et le flux S3 est remplacé par
un `io.StringIO`. Ce qui est vérifié ici, ce sont les règles de gestion reprises du
chargement historique — purge avant insertion (RG6), vides convertis en NULL (RG3),
concordance du référentiel (RG1) — et le refus net de tout ce qui est douteux.
"""

from __future__ import annotations

import asyncio
import io
from contextlib import asynccontextmanager, contextmanager
from datetime import date
from decimal import Decimal

import pymysql
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


#: DDL d'un chargement nominal, dans l'ordre (labels des scripts sur connexion dédiée).
SCRIPTS_NOMINAUX = [
    "chargement/purge",
    "chargement/index-retrait",
    "chargement/index-construction",
    "chargement/index-unique",
]


def csv_de(*lignes: str) -> str:
    return "\n".join((EN_TETE, *lignes)) + "\n"


def base_nominale(**surcharges) -> FausseBase:
    """`FausseBase` répondant à toutes les requêtes du traitement."""
    reponses = {
        "SELECT COUNT(*) AS nb FROM trppu_cles_repartition": {"nb": 0},
        # Garde-fou du TRUNCATE : aucun autre référentiel dans la table.
        "WHERE id_referentiel <> %s LIMIT 1": None,
        # Index présents avant le chargement : les deux index secondaires canoniques.
        "FROM information_schema.STATISTICS": [
            {"nom": "PRIMARY"},
            {"nom": "uk_pdi_ref"},
            {"nom": "idx_cr_ref_actif"},
        ],
        # Recherche des doublons avant de recréer l'index unique : aucun.
        "HAVING COUNT(*) > 1": [],
        # Sites à total nul (--skip-errors seulement) : aucun.
        "HAVING SUM(trafic_colis) = 0": [],
        "AS nb_actives": {
            "nb_lignes": 1,
            "nb_actives": 1,
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
        # Le DDL (purge, index) passe par `index_chargement`, qui a sa propre référence :
        # sans ce remplacement, le test tenterait une connexion réelle.
        monkeypatch.setattr(module.index_chargement, "db_write", base)
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
    """RG6 : la purge (TRUNCATE) précède toute insertion."""
    s3_bouchonne(csv_de(LIGNE_PLEINE))
    base = brancher_base(base_nominale())

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert rapport.statut == SUCCES, rapport.motifs
    assert base.scripts_joues() == SCRIPTS_NOMINAUX
    assert "TRUNCATE TABLE trppu_cles_repartition" in base.texte_du_script("purge")
    assert "INSERT INTO trppu_cles_repartition" in base.ecritures()[0]


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
                "AS nb_actives": {
                    "nb_lignes": 5,
                    "nb_actives": 5,
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
                "AS nb_actives": {
                    "nb_lignes": 3,
                    "nb_actives": 3,
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

    def explose(*args, **kwargs):
        raise RuntimeError("boom")

    # Panne au moment d'insérer, index retirés : l'erreur est rendue ET la table remise
    # en état, sans qu'aucune exception ne remonte.
    monkeypatch.setattr(base, "transaction", explose)

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert rapport.statut == ECHEC
    assert "boom" in (rapport.erreur or "")
    assert base.scripts_joues()[-1] == "chargement/purge"


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
    assert base.scripts_joues() == SCRIPTS_NOMINAUX
    assert "INSERT INTO trppu_cles_repartition" in base.ecritures()[0]


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

    async def _faux(id_referentiel, fichier=None, *, chemin_local=None, ignorer_erreurs=False):
        recu.update(id_referentiel=id_referentiel, chemin_local=chemin_local)
        return module.Rapport(titre="T", id_traitement=id_referentiel)

    monkeypatch.setattr(main, "charger_cles_repartition", _faux)
    args = main.build_parser().parse_args(["charger-cles-repartition-local", "2", "x.csv"])

    asyncio.run(main.cmd_charger_cles_repartition_local(args))

    assert recu == {"id_referentiel": 2, "chemin_local": "x.csv"}


# --- --skip-errors ----------------------------------------------------------

# Même PDI que LIGNE_PLEINE, trafic différent : doublon « en conflit ».
LIGNE_CONFLIT_CHARGEMENT = LIGNE_PLEINE.replace("1.0196987815888434", "2.5", 1)
# PDI_1 invalide (trafic non numérique) : non conforme à la conversion.
LIGNE_MAL_FORMEE = LIGNE_PLEINE.replace("1.0196987815888434", "abc", 1)


class BaseAvecDoublon(FausseBase):
    """Refuse en doublon (1062) tout PDI déjà inséré, comme `uk_pdi_ref`."""

    def __init__(self, *args, code=1062, **kwargs):
        super().__init__(*args, **kwargs)
        self.code = code
        self.pdi_inseres: set[int] = set()

    def _refuser_si_doublon(self, ligne):
        if ligne[0] in self.pdi_inseres:
            raise pymysql.err.IntegrityError(self.code, f"Duplicate entry '{ligne[0]}-1'")

    @asynccontextmanager
    async def transaction(self):
        base = self

        class Curseur:
            async def execute_many(self, query, lignes):
                lignes = list(lignes)
                for ligne in lignes:
                    base._refuser_si_doublon(ligne)
                if len({ligne[0] for ligne in lignes}) != len(lignes):
                    raise pymysql.err.IntegrityError(base.code, "Duplicate entry")
                base.pdi_inseres.update(ligne[0] for ligne in lignes)
                return base._enregistrer("execute_many", query, lignes)

            async def execute(self, query, ligne):
                base._refuser_si_doublon(ligne)
                base.pdi_inseres.add(ligne[0])
                return base._enregistrer("execute", query, ligne)

        yield Curseur()


def test_skip_errors_ecarte_la_ligne_mal_formee(s3_bouchonne, brancher_base):
    s3_bouchonne(csv_de(LIGNE_PLEINE, LIGNE_MAL_FORMEE))
    brancher_base(base_nominale())

    rapport = charger(id_referentiel=1, fichier="f.csv", ignorer_erreurs=True)

    assert rapport.statut == SUCCES, rapport.motifs
    assert rapport.etats["LIGNES_CHARGEES"] == 1
    assert rapport.etats["LIGNES_IGNOREES"] == 1
    assert rapport.avertissements[0].startswith("Ligne 3 :")
    assert "Avertissements :" in rapport.texte()


def test_sans_skip_errors_la_meme_ligne_fait_echouer(s3_bouchonne, brancher_base):
    s3_bouchonne(csv_de(LIGNE_PLEINE, LIGNE_MAL_FORMEE))
    brancher_base(base_nominale())

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert rapport.statut == ECHEC
    assert "LIGNES_IGNOREES" not in rapport.etats


def test_index_conserves_sans_droit_alter_doublon_rejoue_ligne_a_ligne(
    s3_bouchonne, brancher_base
):
    """Sans droit ALTER, le chargement se fait index en place : plus lent, mais correct."""
    s3_bouchonne(csv_de(LIGNE_PLEINE, LIGNE_PLEINE))
    base = brancher_base(
        BaseAvecDoublon(
            base_nominale().reponses,
            echecs_scripts={
                "index-retrait": pymysql.err.OperationalError(1142, "ALTER command denied")
            },
        )
    )

    rapport = charger(id_referentiel=1, fichier="f.csv", ignorer_erreurs=True)

    assert rapport.statut == SUCCES, rapport.motifs
    assert rapport.etats["LIGNES_CHARGEES"] == 1
    assert rapport.etats["LIGNES_IGNOREES"] == 1
    assert any("droit ALTER manquant" in a for a in rapport.avertissements)
    doublon = next(a for a in rapport.avertissements if "doublon de PDI" in a)
    assert "Ligne 3" in doublon
    assert base.pdi_inseres == {100005554}
    assert "chargement/index-construction" not in base.scripts_joues()


def test_skip_errors_ne_masque_pas_une_panne_technique(s3_bouchonne, brancher_base):
    """Une connexion perdue (2013) n'est pas un défaut de ligne : le chargement s'arrête."""
    s3_bouchonne(csv_de(LIGNE_PLEINE, LIGNE_PLEINE))
    brancher_base(BaseAvecDoublon(base_nominale().reponses, code=2013))

    rapport = charger(id_referentiel=1, fichier="f.csv", ignorer_erreurs=True)

    assert rapport.statut == ECHEC


def test_skip_errors_aucune_ligne_conforme_echoue(s3_bouchonne, brancher_base):
    s3_bouchonne(csv_de(LIGNE_MAL_FORMEE, LIGNE_MAL_FORMEE))
    brancher_base(base_nominale())

    rapport = charger(id_referentiel=1, fichier="f.csv", ignorer_erreurs=True)

    assert rapport.statut == ECHEC
    assert "Aucune ligne conforme" in " ".join(rapport.motifs)


def test_skip_errors_detail_plafonne(monkeypatch, s3_bouchonne, brancher_base):
    monkeypatch.setattr(module, "CHARGEMENT_MAX_REJETS_DETAILLES", 1)
    s3_bouchonne(csv_de(LIGNE_PLEINE, LIGNE_MAL_FORMEE, LIGNE_MAL_FORMEE, LIGNE_MAL_FORMEE))
    brancher_base(base_nominale())

    rapport = charger(id_referentiel=1, fichier="f.csv", ignorer_erreurs=True)

    assert rapport.etats["LIGNES_IGNOREES"] == 3
    assert len(rapport.avertissements) == 2
    assert "2 autre(s)" in rapport.avertissements[1]


def test_avertissements_sans_effet_sur_le_verdict():
    rapport = module.Rapport(titre="T", id_traitement=1)
    rapport.avertissements.append("Ligne 3 : ignorée")

    assert rapport.reussi
    assert rapport.to_dict()["avertissements"] == ["Ligne 3 : ignorée"]


@pytest.mark.parametrize(
    "arguments",
    [
        ["charger-cles-repartition", "1", "--skip-errors"],
        ["charger-cles-repartition-local", "1", "f.csv", "--skip-errors"],
        ["init", "1", "--skip-errors"],
    ],
)
def test_option_skip_errors_sur_les_trois_commandes(arguments):
    from app.main import build_parser

    assert build_parser().parse_args(arguments).ignorer_erreurs is True
    assert build_parser().parse_args(arguments[:-1]).ignorer_erreurs is False


# --- Purge par TRUNCATE -----------------------------------------------------


def test_purge_refusee_si_un_autre_referentiel_est_present(s3_bouchonne, brancher_base):
    """Un TRUNCATE ne se rattrape pas : il ne doit jamais emporter un autre référentiel."""
    s3_bouchonne(csv_de(LIGNE_PLEINE))
    base = base_nominale()
    base.reponses["WHERE id_referentiel <> %s LIMIT 1"] = {"id_referentiel": 2}
    base.lecture_seule = True
    brancher_base(base)

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert rapport.statut == ECHEC
    assert "référentiel 2" in " ".join(rapport.motifs)
    assert base.ecritures() == []


def test_purge_sans_droit_drop_message_actionnable(s3_bouchonne, brancher_base):
    s3_bouchonne(csv_de(LIGNE_PLEINE))
    base = base_nominale()
    base.echecs_scripts = {"purge": pymysql.err.OperationalError(1142, "DROP command denied")}
    brancher_base(base)

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert rapport.statut == ECHEC
    motifs = " ".join(rapport.motifs)
    assert "droit manquant" in motifs and "DROP" in motifs


def test_plus_aucun_delete(s3_bouchonne, brancher_base):
    s3_bouchonne(csv_de(LIGNE_PLEINE))
    base = brancher_base(base_nominale())

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert not base.a_ecrit("DELETE FROM trppu_cles_repartition")
    assert any("Purge (TRUNCATE)" in c.libelle for c in rapport.controles)


def test_option_truncate_retiree():
    from app.main import build_parser

    with pytest.raises(SystemExit):
        build_parser().parse_args(["init", "1", "--truncate"])


# --- Index retirés pendant le chargement, doublons traités avant l'index unique -------


def _ligne_en_base(id_, trafic="1.0196987815888434"):
    """Ligne telle que relue en base par la recherche des doublons."""
    return {
        "id": id_,
        "id_pdi": 100005554,
        "pdi_rattache": 6105149,
        "trafic_colis": Decimal(trafic),
        "trafic_oo": Decimal("0.0"),
        "trafic_3s": Decimal("0.6444216483320571"),
        "nature": "BPF",
        "co_regate_site": "372920",
        "type_site": "PDC1",
        "lb_regate": "SORIGNY PDC1",
        "co_regate_etablissement": "372900",
        "lb_etablissement": "CHARGE AMBOISE PPDC",
        "co_regate_dex": "750558",
        "lb_dex": "PARIS CENTRE VAL DE LOIRE DEXC",
        "nb_pre": 1,
        "potentielip": 0,
        "id_referentiel": 1,
        "date_debut_validite": date(2026, 7, 21),
        "date_fin_validite": None,
    }


def base_avec_doublon_en_base(*lignes_en_base) -> FausseBase:
    return base_nominale(
        **{
            # Même clé que `base_nominale` : la doublure rend le premier fragment trouvé.
            "HAVING COUNT(*) > 1": [
                {"id_pdi": 100005554, "nb_occurrences": len(lignes_en_base)}
            ],
            "WHERE id_pdi IN (": list(lignes_en_base),
        }
    )


def test_skip_errors_doublon_ecarte_avant_l_index_unique(s3_bouchonne, brancher_base):
    s3_bouchonne(csv_de(LIGNE_PLEINE, LIGNE_CONFLIT_CHARGEMENT))
    base = brancher_base(
        base_avec_doublon_en_base(_ligne_en_base(1), _ligne_en_base(2, trafic="2.5"))
    )

    rapport = charger(id_referentiel=1, fichier="f.csv", ignorer_erreurs=True)

    assert rapport.statut == SUCCES, rapport.motifs
    assert base.scripts_joues() == SCRIPTS_NOMINAUX
    # La seconde occurrence (id 2) est supprimée, la première conservée.
    assert base.parametres_de("DELETE FROM trppu_cles_repartition WHERE id IN") == (2,)
    assert rapport.etats["LIGNES_CHARGEES"] == 1
    assert rapport.etats["LIGNES_IGNOREES"] == 1
    assert any("PDI 100005554 : doublon en CONFLIT" in a for a in rapport.avertissements)
    assert any("1 doublon(s) de PDI écarté(s)" in c.libelle for c in rapport.controles)


def test_doublon_identique_distingue_du_conflit(s3_bouchonne, brancher_base):
    s3_bouchonne(csv_de(LIGNE_PLEINE, LIGNE_PLEINE))
    brancher_base(base_avec_doublon_en_base(_ligne_en_base(1), _ligne_en_base(2)))

    rapport = charger(id_referentiel=1, fichier="f.csv", ignorer_erreurs=True)

    assert any("doublon identique" in a for a in rapport.avertissements)


def test_mode_strict_doublon_echoue_et_remet_la_table_en_etat(s3_bouchonne, brancher_base):
    """Sans --skip-errors : échec, table vidée et index recréés — jamais laissée sans index."""
    s3_bouchonne(csv_de(LIGNE_PLEINE, LIGNE_CONFLIT_CHARGEMENT))
    base = brancher_base(
        base_avec_doublon_en_base(_ligne_en_base(1), _ligne_en_base(2, trafic="2.5"))
    )

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert rapport.statut == ECHEC
    assert "1 PDI en doublon" in " ".join(rapport.motifs)
    assert not base.a_ecrit("DELETE FROM trppu_cles_repartition WHERE id IN")
    # Retrait, construction, puis purge de remise en état ; jamais d'index unique posé.
    assert base.scripts_joues() == [
        "chargement/purge",
        "chargement/index-retrait",
        "chargement/index-construction",
        "chargement/purge",
    ]
    assert rapport.etats["LIGNES_CHARGEES"] == 0
    assert any("table a été vidée" in a for a in rapport.avertissements)


def test_erreur_en_cours_de_chargement_remet_la_table_en_etat(s3_bouchonne, brancher_base):
    s3_bouchonne(csv_de(LIGNE_PLEINE, LIGNE_MAL_FORMEE))
    # Index absents à la relecture : la remise en état doit les recréer.
    base = brancher_base(
        base_nominale(**{"FROM information_schema.STATISTICS": [{"nom": "PRIMARY"}]})
    )

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert rapport.statut == ECHEC
    assert base.scripts_joues() == [
        "chargement/purge",
        "chargement/purge",
        "chargement/remise-en-etat",
    ]
    remise = base.texte_du_script("remise-en-etat")
    assert "ADD UNIQUE KEY `uk_pdi_ref`" in remise


def test_verrou_non_obtenu_echoue_vite_avec_message(s3_bouchonne, brancher_base):
    """Le TRUNCATE ne doit jamais bloquer l'API : attente bornée, puis échec lisible."""
    s3_bouchonne(csv_de(LIGNE_PLEINE))
    base = base_nominale()
    base.echecs_scripts = {
        "purge": pymysql.err.OperationalError(1205, "Lock wait timeout exceeded")
    }
    brancher_base(base)

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert rapport.statut == ECHEC
    assert "verrou non obtenu" in " ".join(rapport.motifs)


def test_ddl_borne_l_attente_de_verrou(s3_bouchonne, brancher_base):
    s3_bouchonne(csv_de(LIGNE_PLEINE))
    base = brancher_base(base_nominale())

    charger(id_referentiel=1, fichier="f.csv")

    for label in SCRIPTS_NOMINAUX:
        texte = base.texte_du_script(label.split("/")[1])
        assert texte.startswith("SET SESSION lock_wait_timeout = ")


def test_index_deja_absents_apres_un_crash(s3_bouchonne, brancher_base):
    """Un chargement tué à mi-course laisse la table sans index : le suivant les recrée."""
    s3_bouchonne(csv_de(LIGNE_PLEINE))
    base = brancher_base(
        base_nominale(**{"FROM information_schema.STATISTICS": [{"nom": "PRIMARY"}]})
    )

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert rapport.statut == SUCCES, rapport.motifs
    assert "chargement/index-retrait" not in base.scripts_joues()
    assert "chargement/index-unique" in base.scripts_joues()


def test_index_inconnu_laisse_en_place(s3_bouchonne, brancher_base):
    s3_bouchonne(csv_de(LIGNE_PLEINE))
    base = brancher_base(
        base_nominale(
            **{
                "FROM information_schema.STATISTICS": [
                    {"nom": "PRIMARY"},
                    {"nom": "uk_pdi_ref"},
                    {"nom": "idx_ajoute_par_le_dba"},
                ]
            }
        )
    )

    charger(id_referentiel=1, fichier="f.csv")

    retrait = base.texte_du_script("index-retrait")
    assert "DROP INDEX `uk_pdi_ref`" in retrait
    assert "idx_ajoute_par_le_dba" not in retrait


# --- Conversions ------------------------------------------------------------


@pytest.mark.parametrize("valeur", ["NaN", "Infinity", "-inf"])
def test_decimal_non_fini_refuse(valeur):
    ligne = dict(zip(module.COLONNES_CSV, LIGNE_PLEINE.split(";")))
    ligne["trafic_oo"] = valeur
    with pytest.raises(TraitementImpossible):
        module.convertir(ligne, 2, id_referentiel=1)


@pytest.mark.parametrize(
    ("valeur", "attendu"),
    [("2026-07-21", date(2026, 7, 21)), ("2026-7-21", date(2026, 7, 21))],
)
def test_dates_chemin_rapide_et_ancien_format(valeur, attendu):
    ligne = dict(zip(module.COLONNES_CSV, LIGNE_PLEINE.split(";")))
    ligne["date_debut_validite"] = valeur
    assert module.convertir(ligne, 2, id_referentiel=1)[16] == attendu


@pytest.mark.parametrize("valeur", ["2026-13-01", "21/07/2026", "20260721"])
def test_dates_invalides_refusees(valeur):
    ligne = dict(zip(module.COLONNES_CSV, LIGNE_PLEINE.split(";")))
    ligne["date_debut_validite"] = valeur
    with pytest.raises(TraitementImpossible):
        module.convertir(ligne, 2, id_referentiel=1)


# --- Contrôles finaux -------------------------------------------------------


def test_aucune_lecture_sur_l_instance_de_lecture(monkeypatch, s3_bouchonne, brancher_base):
    """Garde-fous et contrôles finaux sur l'instance d'écriture : une réplique en retard sur
    22 M d'insertions et deux ALTER attendrait, ou compterait une table incomplète."""
    s3_bouchonne(csv_de(LIGNE_PLEINE))
    brancher_base(base_nominale())
    # Toute requête sur la lecture lève KeyError (aucune réponse déclarée).
    monkeypatch.setattr(module, "db_read", FausseBase({}, lecture_seule=True))

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert rapport.statut == SUCCES, (rapport.motifs, rapport.erreur)


def test_date_debut_min_calculee_a_la_lecture(s3_bouchonne, brancher_base):
    plus_ancienne = LIGNE_TROUEE.replace("2026-07-21", "2025-01-01", 1)
    s3_bouchonne(csv_de(LIGNE_PLEINE, plus_ancienne))
    brancher_base(
        base_nominale(**{"AS nb_actives": {"nb_lignes": 2, "nb_actives": 2}})
    )

    rapport = charger(id_referentiel=1, fichier="f.csv")

    assert rapport.statut == SUCCES, rapport.motifs
    assert rapport.etats["DATE_DEBUT_VALIDITE_MIN"] == date(2025, 1, 1)


def test_controle_final_sans_comptage_distinct_ni_lecture_de_table():
    """`COUNT(DISTINCT)` sur 22 M de lignes = table temporaire géante ; `MIN(date_debut)`
    = lecture de toute la table. Ni l'un ni l'autre ne doit revenir."""
    sql = " ".join(module.CONTROLE_FINAL_SQL.split()).upper()
    assert "DISTINCT" not in sql
    assert "DATE_DEBUT_VALIDITE" not in sql



# --- Sites à total nul écartés par --skip-errors ------------------------------


def _site_nul(code="122200", nb=2, **nuls):
    ligne = {"co_regate_site": code, "nb_pdi_actifs": nb}
    ligne.update({"colis": 0, "oo": 0, "t3s": 0, "potentielip": 0, **nuls})
    return ligne


def test_skip_errors_ecarte_les_sites_a_total_nul(s3_bouchonne, brancher_base):
    s3_bouchonne(csv_de(LIGNE_PLEINE, LIGNE_TROUEE))
    base = brancher_base(
        base_nominale(
            **{
                "HAVING SUM(trafic_colis) = 0": [_site_nul("833280", nb=1, potentielip=1)],
                "AS nb_actives": {"nb_lignes": 1, "nb_actives": 1},
            }
        )
    )
    # Le PDI actif du site est supprimé ; il n'a pas de PDI inactif.
    base.rowcounts = {
        "date_fin_validite IS NULL AND co_regate_site IN": 1,
        "date_fin_validite IS NOT NULL AND co_regate_site IN": 0,
    }

    rapport = charger(id_referentiel=1, fichier="f.csv", ignorer_erreurs=True)

    assert rapport.statut == SUCCES, rapport.motifs
    # Actifs puis inactifs du site, dans une même transaction, paramétrés.
    suppressions = [sql for sql in base.ecritures() if sql.startswith("DELETE")]
    assert len(suppressions) == 2
    assert "date_fin_validite IS NULL" in suppressions[0]
    assert "date_fin_validite IS NOT NULL" in suppressions[1]
    assert base.parametres_de("date_fin_validite IS NULL AND co_regate_site IN") == (1, "833280")
    assert rapport.etats["SITES_TOTAL_NUL_ECARTES"] == 1
    assert rapport.etats["LIGNES_SITES_TOTAL_NUL"] == 1
    assert rapport.etats["LIGNES_CHARGEES"] == 1  # 2 insérées - 1 retirée
    assert any(
        "Site 833280 écarté : total potentielip nul" in a for a in rapport.avertissements
    )


def test_sans_skip_errors_les_sites_a_total_nul_ne_sont_pas_touches(
    s3_bouchonne, brancher_base
):
    """En mode strict, c'est le calcul des clés qui bloque, avec sa question métier."""
    s3_bouchonne(csv_de(LIGNE_PLEINE))
    base = brancher_base(base_nominale())

    charger(id_referentiel=1, fichier="f.csv")

    lues = [sql for genre, sql, _ in base.journal if genre == "fetch"]
    assert not any("HAVING SUM(trafic_colis) = 0" in sql for sql in lues)
    assert not base.a_ecrit("co_regate_site IN")


def test_aucun_site_a_total_nul(s3_bouchonne, brancher_base):
    s3_bouchonne(csv_de(LIGNE_PLEINE))
    base = brancher_base(base_nominale())

    rapport = charger(id_referentiel=1, fichier="f.csv", ignorer_erreurs=True)

    assert rapport.statut == SUCCES, rapport.motifs
    assert any("Aucun site à total de trafic nul" in c.libelle for c in rapport.controles)
    assert not base.a_ecrit("co_regate_site IN")


def test_recherche_des_sites_nuls_en_lecture_sequentielle():
    """Conditions minimales : un seul parcours séquentiel, pas 22 M lectures aléatoires."""
    sql = " ".join(module.SITES_TOTAL_NUL_SQL.split())
    assert "IGNORE INDEX (idx_cr_ref_actif)" in sql
    assert "date_fin_validite IS NULL" in sql
