"""Orchestration de `init` : ordre, mode d'exécution, reprise, garde-fous avant l'étape
irréversible `cles`. Les scripts eux-mêmes sont couverts par `test_scripts_dsr.py`."""

from __future__ import annotations

import asyncio
import json
import logging

import pytest

from app.db.sql_script import SqlScriptError
from app.erreurs import TraitementImpossible
from app.traitements import initialisation
from app.traitements.initialisation import ETAPES, initialiser_cles_repartition
from app.traitements.rapport import SUCCES, Rapport
from tests.conftest import EcritureInterdite, FausseBase

SITES = ["000001", "000002", "000003"]

#: Lignes rendues par chaque instruction d'écriture des scripts, repérées par leur aperçu.
ROWCOUNTS = {
    "INSERT INTO trppu_trafic_site": len(SITES),
    "INSERT INTO trppu_version_cle": 1,
    "INSERT INTO trppu_cles_repartition_calcule": 120,
}


def _lectures(**surcharges) -> FausseBase:
    """Lectures d'un référentiel sain à trois sites ; une surcharge casse un seul point."""
    reponses = {
        # Prérequis. « Chargement finalisé » en premier : sa requête contient aussi le
        # fragment des index présents, la doublure rend la première réponse qui correspond.
        "AS chargement_uk": {"chargement_uk": 2, "chargement_tmp": 0},
        "SELECT 1 AS ok FROM trppu_cles_repartition": {"ok": 1},
        "FROM information_schema.STATISTICS": [
            {"nom": "uq_site_trafic"},
            {"nom": "idx_cr_ref_actif"},
            {"nom": "uq_crc_version_pdi"},
            {"nom": "idx_regate_actif"},
        ],
        "COLUMN_NAME = 'date_creation'": {"nb": 1},
        "COLUMN_NAME IN ('trafic_colis_total'": [
            {"colonne": "trafic_colis_total", "nb_chiffres": 35, "nb_decimales": 19},
            {"colonne": "trafic_oo_total", "nb_chiffres": 35, "nb_decimales": 19},
            {"colonne": "trafic_3s_total", "nb_chiffres": 35, "nb_decimales": 19},
        ],
        # Agrégats
        "GROUP BY id_referentiel": [{"id_referentiel": 1, "nb": len(SITES)}],
        "SELECT COUNT(*) AS nb FROM trppu_trafic_site WHERE": {"nb": len(SITES)},
        "GROUP BY co_regate_site HAVING COUNT(*) > 1": {"nb": 0},
        "AS chiffres_max": {"chiffres_max": 9},
        # Versions
        "SELECT co_regate_site FROM trppu_trafic_site": [
            {"co_regate_site": site} for site in SITES
        ],
        "AS nb_versions": {"nb_versions": len(SITES), "nb_sites": len(SITES)},
        "GROUP BY co_regate HAVING COUNT(*) > 1": {"nb": 0},
        # Clés
        "trafic_colis_total = 0": [],
        "%s AND EXISTS": {"nb": 0},
        "v.actif <> 'O'": {"nb": 0},
        "AND date_fin_validite IS NULL": {"nb": 120},
        "AS somme_colis": [],
    }
    reponses.update(surcharges)
    return FausseBase(reponses)


#: Étendue des `id` de `trppu_cles_repartition` : 120 lignes, soit un seul lot de 1000.
BORNES = {"MIN(id) AS id_min": {"id_min": 1, "id_max": 120}}
INDEX_CLES_PRESENT = {"INDEX_NAME = 'uq_crc_version_pdi'": {"nb": 1}}


def _ecritures(reponses: dict | None = None, **options) -> FausseBase:
    """Instance d'écriture : index `uq_crc_version_pdi` en place, 120 lignes à calculer."""
    options.setdefault("rowcounts_scripts", ROWCOUNTS)
    return FausseBase({**INDEX_CLES_PRESENT, **BORNES, **(reponses or {})}, **options)


def _rapport_chargement(lignes: int = 120) -> Rapport:
    rapport = Rapport(titre="CHARGEMENT", id_traitement=1, libelle_identifiant="Référentiel")
    rapport.ok(f"{lignes} ligne(s) insérée(s)")
    rapport.statut = SUCCES
    rapport.etats["LIGNES_CHARGEES"] = lignes
    rapport.etats["LIGNES_ACTIVES"] = lignes
    return rapport


@pytest.fixture
def chargement_reussi(monkeypatch):
    """Neutralise l'étape 1, qui a sa propre couverture (`test_chargement_cles_repartition`)."""
    appels = []

    async def _faux(
        id_referentiel, fichier=None, *, ignorer_erreurs=False
    ):
        appels.append((id_referentiel, fichier))
        return _rapport_chargement()

    monkeypatch.setattr(initialisation, "charger_cles_repartition", _faux)
    return appels


def _lancer(**kwargs) -> Rapport:
    kwargs.setdefault("db_lecture", _lectures())
    kwargs.setdefault("db_ecriture", _ecritures())
    return asyncio.run(initialiser_cles_repartition(kwargs.pop("id_referentiel", 1), **kwargs))


def _etapes_des_scripts(ecritures: FausseBase) -> list[str]:
    """Nom de fichier de chaque script joué, sans le suffixe de site."""
    return [label.split("@")[0] for label in ecritures.scripts_joues()]


# ---------------------------------------------------------------------------
# Enchaînement
# ---------------------------------------------------------------------------


def test_la_chaine_joue_les_six_etapes_dans_l_ordre(chargement_reussi):
    ecritures = _ecritures()
    rapport = _lancer(db_ecriture=ecritures)

    assert rapport.reussi, rapport.motifs
    assert rapport.etats["ETAPES_JOUEES"] == ",".join(ETAPES)
    assert chargement_reussi == [(1, None)]

    joues = _etapes_des_scripts(ecritures)
    # Le script des versions est joué une fois par site : première occurrence seulement.
    ordre = [nom for i, nom in enumerate(joues) if nom not in joues[:i]]
    assert ordre == [
        "db/DSR-696-699_migration.sql",
        "db/fix_error.sql",
        "db/DSR-696_site_trafic.sql",
        "db/DSR-698_version_cle.sql",
        "db/DSR-699_cles_calculees.sql",
    ]


def test_la_migration_et_le_correctif_sont_joues_hors_transaction(chargement_reussi):
    """Leur DDL provoque un COMMIT implicite : ouvrir une transaction ne protégerait rien."""
    ecritures = _ecritures()
    _lancer(db_ecriture=ecritures)

    assert ecritures.script_joue("migration.sql")["transactional"] is False
    assert ecritures.script_joue("fix_error.sql")["transactional"] is False


def test_les_scripts_de_donnees_sont_joues_en_transaction(chargement_reussi):
    ecritures = _ecritures()
    _lancer(db_ecriture=ecritures)

    for fichier in ("DSR-696_site_trafic", "DSR-698_version_cle"):
        assert ecritures.script_joue(fichier)["transactional"] is True, fichier
    # Les clés : un commit par lot (autocommit), pour borner la taille des transactions.
    assert ecritures.script_joue("DSR-699_cles_calculees")["transactional"] is False


def test_le_chargement_en_echec_arrete_la_chaine(monkeypatch):
    async def _echec(
        id_referentiel, fichier=None, *, ignorer_erreurs=False
    ):
        rapport = Rapport(titre="CHARGEMENT", id_traitement=id_referentiel)
        rapport.ko("Fichier introuvable sur S3.")
        return rapport

    monkeypatch.setattr(initialisation, "charger_cles_repartition", _echec)
    ecritures = _ecritures()
    rapport = _lancer(db_ecriture=ecritures)

    assert not rapport.reussi
    assert ecritures.scripts_joues() == []
    assert rapport.etats["REPRENDRE_A"] == "chargement"


def test_un_script_absent_echoue_avant_toute_ecriture(monkeypatch, tmp_path):
    """Les scripts sont lus avant la première connexion : rien ne doit avoir été touché."""
    monkeypatch.setattr(initialisation, "REPERTOIRE_SQL", tmp_path)
    ecritures = _ecritures(lecture_seule=True)

    rapport = _lancer(db_ecriture=ecritures, etape="migration")

    assert not rapport.reussi
    assert ecritures.scripts_joues() == []
    assert "Script illisible" in " ".join(rapport.motifs)


# ---------------------------------------------------------------------------
# Reprise
# ---------------------------------------------------------------------------


def test_depuis_versions_ne_rejoue_pas_les_etapes_amont():
    ecritures = _ecritures()
    rapport = _lancer(db_ecriture=ecritures, depuis="versions")

    assert rapport.reussi, rapport.motifs
    assert rapport.etats["ETAPES_JOUEES"] == "versions,cles"
    joues = _etapes_des_scripts(ecritures)
    assert "db/DSR-696-699_migration.sql" not in joues
    assert "db/DSR-696_site_trafic.sql" not in joues


def test_etape_cles_ne_joue_que_le_script_des_cles():
    ecritures = _ecritures()
    rapport = _lancer(db_ecriture=ecritures, etape="cles")

    assert rapport.reussi, rapport.motifs
    assert _etapes_des_scripts(ecritures) == ["db/DSR-699_cles_calculees.sql"]


def test_depuis_et_etape_s_excluent():
    rapport = _lancer(depuis="versions", etape="cles")
    assert not rapport.reussi
    assert "s'excluent" in " ".join(rapport.motifs)


def test_une_etape_inconnue_est_refusee():
    rapport = _lancer(etape="calcul")
    assert not rapport.reussi
    assert "Étape inconnue" in " ".join(rapport.motifs)


def test_depuis_agregats_refuse_si_la_migration_manque():
    lectures = _lectures(**{"FROM information_schema.STATISTICS": [{"nom": "uq_site_trafic"}]})
    ecritures = _ecritures(lecture_seule=True)

    rapport = _lancer(db_lecture=lectures, db_ecriture=ecritures, depuis="agregats")

    assert not rapport.reussi
    assert "migration" in " ".join(rapport.motifs)
    assert ecritures.scripts_joues() == []


def test_depuis_agregats_refuse_si_les_totaux_sont_trop_etroits():
    """Sans le correctif, l'agrégation échoue en ERROR 1264 — après avoir purgé."""
    lectures = _lectures(
        **{
            "COLUMN_NAME IN ('trafic_colis_total'": [
                {"colonne": "trafic_colis_total", "nb_chiffres": 24, "nb_decimales": 18},
            ]
        }
    )
    ecritures = _ecritures(lecture_seule=True)

    rapport = _lancer(db_lecture=lectures, db_ecriture=ecritures, depuis="agregats")

    assert not rapport.reussi
    assert "correctif" in " ".join(rapport.motifs)
    assert ecritures.scripts_joues() == []


def test_depuis_cles_refuse_si_les_versions_ne_couvrent_pas_tous_les_sites():
    """Un site sans version est écarté silencieusement par DSR-699 — et figé par le CA4."""
    lectures = _lectures(**{"AS nb_versions": {"nb_versions": 1, "nb_sites": 1}})
    ecritures = _ecritures(lecture_seule=True)

    rapport = _lancer(db_lecture=lectures, db_ecriture=ecritures, etape="cles")

    assert not rapport.reussi
    motifs = " ".join(rapport.motifs)
    assert "3 site(s) agrégé(s)" in motifs and "1 version(s) active(s)" in motifs
    assert ecritures.scripts_joues() == []


def test_un_garde_fou_en_echec_n_ecrit_rien():
    """`lecture_seule=True` lève sur toute écriture : la preuve est l'absence d'exception."""
    lectures = _lectures(**{"SELECT 1 AS ok FROM trppu_cles_repartition": None})
    ecritures = _ecritures(lecture_seule=True)

    rapport = _lancer(db_lecture=lectures, db_ecriture=ecritures, depuis="migration")

    assert not rapport.reussi
    assert "aucune ligne" in " ".join(rapport.motifs)


# ---------------------------------------------------------------------------
# Boucle site par site (DSR-698)
# ---------------------------------------------------------------------------


def test_la_liste_des_sites_vient_des_agregats_et_non_des_cles_de_repartition():
    """Lire les sites dans `trppu_cles_repartition` balaierait 24 M lignes pour rien."""
    lectures = _lectures()
    _lancer(db_lecture=lectures, depuis="versions")

    lues = [sql for genre, sql, _ in lectures.journal if genre == "fetch"]
    assert any("SELECT co_regate_site FROM trppu_trafic_site" in sql for sql in lues)
    assert not any("DISTINCT co_regate_site FROM trppu_cles_repartition" in sql for sql in lues)


def test_un_script_est_joue_par_site_avec_son_co_regate():
    ecritures = _ecritures()
    _lancer(db_ecriture=ecritures, etape="versions")

    labels = ecritures.scripts_joues()
    assert len(labels) == len(SITES)
    for site in SITES:
        assert any(label.endswith(f"@{site}") for label in labels)
        assert f"SET @co_regate := '{site}'" in ecritures.texte_du_script(f"@{site}")


def test_le_commentaire_est_injecte_quote():
    ecritures = _ecritures()
    _lancer(db_ecriture=ecritures, etape="versions", commentaire="L'Haÿ-les-Roses")

    texte = ecritures.texte_du_script("@000001")
    assert "SET @commentaire := 'L''Haÿ-les-Roses'" in texte


def test_le_commentaire_par_defaut_nomme_le_referentiel():
    ecritures = _ecritures()
    _lancer(id_referentiel=7, db_ecriture=ecritures, etape="versions")

    assert "SET @commentaire := 'Initialisation référentiel 7'" in ecritures.texte_du_script(
        "@000001"
    )


def test_un_site_en_echec_n_arrete_pas_la_boucle():
    """Un site n'explique pas le suivant : arrêter au premier gâcherait le passage entier."""
    echec = SqlScriptError(
        "boum",
        source="db/DSR-698_version_cle.sql",
        index=9,
        statement="INSERT INTO trppu_version_cle (…) SELECT …",
        original=RuntimeError("1062 Duplicate entry"),
    )
    ecritures = _ecritures(echecs_scripts={"@000002": echec})

    rapport = _lancer(db_ecriture=ecritures, etape="versions")

    # Le lot échoue sur le site 2 et est annulé, puis rejoué site par site : chaque site est
    # tenté, et seul le fautif est écarté.
    assert ecritures.lots_annules == 1
    rejoues = [s["label"] for s in ecritures.scripts if not s.get("lot")]
    assert [label.rsplit("@", 1)[1] for label in rejoues] == SITES
    assert not rapport.reussi
    assert rapport.etats["SITES_VERSIONS_KO"] == 1
    assert rapport.etats["VERSIONS_CREEES"] == len(SITES) - 1


def test_l_etape_cles_n_est_pas_jouee_si_un_site_a_echoue():
    """Protège l'irréversible : des clés sur une couverture partielle seraient figées par le CA4."""
    echec = SqlScriptError(
        "boum",
        source="db/DSR-698_version_cle.sql",
        index=9,
        statement="INSERT …",
        original=RuntimeError("1062"),
    )
    ecritures = _ecritures(echecs_scripts={"@000002": echec})

    rapport = _lancer(db_ecriture=ecritures, depuis="versions")

    assert "DSR-699_cles_calculees.sql" not in " ".join(ecritures.scripts_joues())
    assert rapport.etats["REPRENDRE_A"] == "versions"
    assert "CA4" in " ".join(rapport.motifs)


def test_le_rapport_ne_porte_pas_une_ligne_par_site(monkeypatch):
    """Le rapport agrège les sites (le détail va dans les logs) : soixante suffisent à le voir."""
    sites = [f"{n:06d}" for n in range(1, 61)]
    lectures = _lectures(
        **{
            "SELECT co_regate_site FROM trppu_trafic_site": [
                {"co_regate_site": site} for site in sites
            ],
            "AS nb_versions": {"nb_versions": len(sites), "nb_sites": len(sites)},
            "SELECT COUNT(*) AS nb FROM trppu_trafic_site WHERE": {"nb": len(sites)},
        }
    )
    rapport = _lancer(db_lecture=lectures, etape="versions")

    assert rapport.reussi, rapport.motifs
    assert len(rapport.controles) < 20
    assert rapport.etats["SITES_TRAITES"] == len(sites)


def test_un_avancement_est_journalise_pendant_la_boucle(monkeypatch, caplog):
    monkeypatch.setattr(initialisation, "INIT_LOG_TOUS_LES_SITES", 2)
    monkeypatch.setattr(initialisation, "INIT_VERSIONS_TAILLE_LOT", 2)
    with caplog.at_level(logging.INFO, logger="app.traitements.initialisation"):
        _lancer(etape="versions")

    avancements = [r for r in caplog.records if r.getMessage().startswith("Avancement")]
    assert avancements
    assert "sites=2" in avancements[0].getMessage()


# ---------------------------------------------------------------------------
# Critères d'acceptation du calcul des clés
# ---------------------------------------------------------------------------


def test_un_total_nul_ne_bloque_plus_et_remonte_au_rapport():
    """Total de site nul : clé à 0, calcul joué, site porté au rapport avec son nombre de PDI."""
    lectures = _lectures(
        **{
            "trafic_colis_total = 0": [
                {
                    "co_regate_site": "000002",
                    "trafic_colis_total": 5,
                    "trafic_oo_total": 3,
                    "trafic_3s_total": 1,
                    "potentielip_total": 0,
                }
            ],
        }
    )
    # En tête : la requête contient aussi le fragment du comptage des PDI actifs (CA1), et
    # la doublure rend la première réponse qui correspond.
    lectures.reponses = {
        "co_regate_site IN (": [{"co_regate_site": "000002", "nb": 57}],
        **lectures.reponses,
    }
    ecritures = _ecritures()

    rapport = _lancer(db_lecture=lectures, db_ecriture=ecritures, etape="cles")

    assert rapport.reussi, rapport.motifs
    assert "db/DSR-699_cles_calculees.sql" in _etapes_des_scripts(ecritures)
    assert rapport.etats["SITES_CLE_A_ZERO"] == 1
    assert rapport.etats["PDI_CLE_A_ZERO"] == 57
    assert any(
        "Site 000002 : total potentielip nul — clé(s) potentielip enregistrée(s) à 0 "
        "pour ses 57 PDI actif(s)" in a
        for a in rapport.avertissements
    )


def test_le_calcul_ecrit_zero_au_lieu_de_diviser_par_zero():
    from pathlib import Path

    script = (Path(__file__).resolve().parent.parent / "db" / "DSR-699_cles_calculees.sql")
    texte = " ".join(script.read_text(encoding="utf-8-sig").split())
    for total in ("trafic_colis_total", "trafic_oo_total", "trafic_3s_total",
                  "potentielip_total"):
        assert f"IF(s.{total} = 0, 0," in texte


def test_controle_des_sommes_attend_zero_pour_un_total_nul():
    """Sans ce cas, les sites à clé 0 seraient déclarés hors tolérance à tort (CA3)."""
    from app.traitements import controles_init

    sql = " ".join(controles_init.SOMMES_HORS_TOLERANCE_SQL.split())
    assert "IF(MAX(s.potentielip_total) = 0, 0, 1)" in sql
    assert "JOIN trppu_trafic_site s" in sql


CLES_PRESENTES = "FROM trppu_cles_repartition_calcule WHERE id_referentiel"


def _deja_calcule(cles_presentes: int) -> FausseBase:
    """Lectures d'un référentiel dont des versions portent déjà `cles_presentes` clés."""
    lectures = _lectures(**{"%s AND EXISTS": {"nb": 2}})
    # En tête : la doublure rend la première réponse qui correspond.
    lectures.reponses = {CLES_PRESENTES: {"nb": cles_presentes}, **lectures.reponses}
    return lectures


def test_un_perimetre_entierement_calcule_bloque_avant_le_calcul():
    """Le relancer parcourrait tous les lots sans rien écrire : un faux `[OK]`."""
    ecritures = _ecritures(lecture_seule=True)

    rapport = _lancer(db_lecture=_deja_calcule(120), db_ecriture=ecritures, etape="cles")

    assert not rapport.reussi
    assert "CA4" in " ".join(rapport.motifs)
    assert ecritures.scripts_joues() == []


def test_le_garde_fou_du_ca4_joue_aussi_sur_une_chaine_complete(chargement_reussi):
    """Le chargement purge `trppu_cles_repartition`, jamais les clés déjà calculées."""
    rapport = _lancer(db_lecture=_deja_calcule(120))

    assert not rapport.reussi
    assert rapport.etats["REPRENDRE_A"] == "cles"


def test_un_calcul_interrompu_est_repris_et_complete():
    """Reprise CA4 après des lots déjà commités : seules les clés absentes sont écrites."""
    ecritures = _ecritures(
        rowcounts_scripts={**ROWCOUNTS, "INSERT INTO trppu_cles_repartition_calcule": 70}
    )

    rapport = _lancer(db_lecture=_deja_calcule(50), db_ecriture=ecritures, etape="cles")

    assert rapport.reussi, rapport.motifs
    assert rapport.etats["CLES_CALCULEES"] == 70
    libelles = " ".join(c.libelle for c in rapport.controles)
    assert "Reprise du calcul : 50 clé(s) déjà présente(s) pour 120 PDI actif(s)" in libelles
    assert "CA1 — 120 clé(s) pour 120 PDI actif(s)" in libelles


# ---------------------------------------------------------------------------
# Étape « cles » : lots de CHARGEMENT_TAILLE_LOT lignes, un commit par lot
# ---------------------------------------------------------------------------


def test_les_cles_sont_calculees_par_tranches_d_id(monkeypatch):
    """2 500 lignes, lots de 1000 : trois tranches ]0;1000] ]1000;2000] ]2000;2500]."""
    monkeypatch.setattr(initialisation, "CHARGEMENT_TAILLE_LOT", 1000)
    ecritures = _ecritures({"MIN(id) AS id_min": {"id_min": 1, "id_max": 2500}})

    _lancer(db_ecriture=ecritures, etape="cles")

    assert ecritures.scripts_joues() == [
        "db/DSR-699_cles_calculees.sql@1-1000",
        "db/DSR-699_cles_calculees.sql@1001-2000",
        "db/DSR-699_cles_calculees.sql@2001-2500",
    ]
    texte = ecritures.texte_du_script("@1001-2000")
    assert "SET @id_debut := 1000" in texte and "SET @id_fin := 2000" in texte


def test_chaque_lot_est_commite_seul():
    """Autocommit : chaque INSERT de lot est sa propre transaction (erreur 3231 évitée)."""
    ecritures = _ecritures()

    _lancer(db_ecriture=ecritures, etape="cles")

    lots = [s for s in ecritures.scripts if "DSR-699" in s["label"]]
    assert lots and all(s["transactional"] is False for s in lots)


def test_les_lots_partagent_une_connexion_par_paquet(monkeypatch):
    monkeypatch.setattr(initialisation, "CHARGEMENT_TAILLE_LOT", 10)
    monkeypatch.setattr(initialisation, "LOTS_CLES_PAR_CONNEXION", 4)
    ecritures = _ecritures({"MIN(id) AS id_min": {"id_min": 1, "id_max": 100}})

    _lancer(db_ecriture=ecritures, etape="cles")

    # 10 lots, par paquets de 4 : trois appels (4 + 4 + 2).
    assert ecritures.lots_commites == 3
    assert len([s for s in ecritures.scripts if "DSR-699" in s["label"]]) == 10


def test_les_cles_ecrites_sont_la_somme_des_lots(monkeypatch):
    monkeypatch.setattr(initialisation, "CHARGEMENT_TAILLE_LOT", 50)
    ecritures = _ecritures(
        {"MIN(id) AS id_min": {"id_min": 1, "id_max": 120}},
        rowcounts_scripts={**ROWCOUNTS, "INSERT INTO trppu_cles_repartition_calcule": 40},
    )

    rapport = _lancer(db_ecriture=ecritures, etape="cles")

    assert rapport.reussi, rapport.motifs
    assert rapport.etats["CLES_CALCULEES"] == 120  # 3 lots de 40


def test_un_lot_en_echec_laisse_les_precedents_et_dit_comment_reprendre(monkeypatch):
    monkeypatch.setattr(initialisation, "CHARGEMENT_TAILLE_LOT", 50)
    monkeypatch.setattr(initialisation, "LOTS_CLES_PAR_CONNEXION", 1)
    echec = SqlScriptError(
        "boum",
        source="db/DSR-699_cles_calculees.sql@101-120",
        index=7,
        statement="INSERT INTO trppu_cles_repartition_calcule …",
        original=RuntimeError("3231 writeset"),
    )
    ecritures = _ecritures(
        {"MIN(id) AS id_min": {"id_min": 1, "id_max": 120}},
        echecs_scripts={"@101-120": echec},
    )

    rapport = _lancer(db_ecriture=ecritures, etape="cles")

    assert not rapport.reussi
    assert ecritures.lots_commites == 2  # ]0;50] et ]50;100] restent écrits
    assert any("Relancer « --etape cles »" in a for a in rapport.avertissements)


def test_table_source_vide_refusee():
    ecritures = _ecritures({"MIN(id) AS id_min": {"id_min": None, "id_max": None}})

    rapport = _lancer(db_ecriture=ecritures, etape="cles")

    assert not rapport.reussi
    assert "trppu_cles_repartition est vide" in " ".join(rapport.motifs)


def test_zero_cle_ecrite_fait_echouer_l_etape():
    ecritures = _ecritures(
        rowcounts_scripts={**ROWCOUNTS, "INSERT INTO trppu_cles_repartition_calcule": 0}
    )
    rapport = _lancer(db_ecriture=ecritures, etape="cles")

    assert not rapport.reussi
    assert "Aucune clé écrite" in " ".join(rapport.motifs)


def test_une_somme_de_cles_hors_tolerance_fait_echouer_l_etape():
    lectures = _lectures(
        **{
            "AS somme_colis": [
                {
                    "co_regate_site": "000002",
                    "somme_colis": "0.87",
                    "somme_oo": "1.0",
                    "somme_3s": "1.0",
                    "somme_potentielip": "1.0",
                }
            ]
        }
    )
    rapport = _lancer(db_lecture=lectures, etape="cles")

    assert not rapport.reussi
    assert "CA3" in " ".join(rapport.motifs)
    assert rapport.etats["SITES_HORS_TOLERANCE"] == 1


def test_une_somme_hors_tolerance_est_journalisee_en_warning(caplog):
    """DSR-699 demande une alerte dans les logs ; le SQL ne sait pas journaliser."""
    lectures = _lectures(
        **{
            "AS somme_colis": [
                {
                    "co_regate_site": "000002",
                    "somme_colis": "0.87",
                    "somme_oo": "1.0",
                    "somme_3s": "1.0",
                    "somme_potentielip": "1.0",
                }
            ]
        }
    )
    with caplog.at_level(logging.WARNING, logger="app.traitements.controles_init"):
        _lancer(db_lecture=lectures, etape="cles")

    alertes = [
        r for r in caplog.records if r.getMessage().startswith("Rejet contrôle somme des clés")
    ]
    assert alertes
    assert "co_regate=000002" in alertes[0].getMessage()


def test_le_nombre_d_anomalies_journalisees_est_borne(monkeypatch, caplog):
    monkeypatch.setattr(initialisation.controles_init, "INIT_MAX_ANOMALIES_LOGUEES", 3)
    anomalies = [
        {
            "co_regate_site": f"{n:06d}",
            "somme_colis": "0.5",
            "somme_oo": "1.0",
            "somme_3s": "1.0",
            "somme_potentielip": "1.0",
        }
        for n in range(10)
    ]
    lectures = _lectures(**{"AS somme_colis": anomalies})

    with caplog.at_level(logging.WARNING, logger="app.traitements.controles_init"):
        _lancer(db_lecture=lectures, etape="cles")

    alertes = [
        r for r in caplog.records if r.getMessage().startswith("Rejet contrôle somme des clés")
    ]
    # Trois sites nommés, plus une ligne de synthèse pour les sept autres.
    assert len(alertes) == 4
    assert "anomalies_non_journalisees=7" in alertes[-1].getMessage()


def test_le_ca1_reutilise_les_lignes_actives_du_chargement(chargement_reussi):
    """Recompter les PDI actifs balaierait 24 M entrées d'index pour une valeur déjà connue."""
    lectures = _lectures()
    rapport = _lancer(db_lecture=lectures)

    assert rapport.reussi, rapport.motifs
    lues = [sql for genre, sql, _ in lectures.journal if genre == "fetch"]
    assert not any("AND date_fin_validite IS NULL" in sql for sql in lues)


def test_sans_controles_longs_ne_joue_pas_la_somme_des_cles():
    lectures = _lectures()
    rapport = _lancer(db_lecture=lectures, etape="cles", controles_longs=False)

    assert rapport.reussi, rapport.motifs
    lues = [sql for genre, sql, _ in lectures.journal if genre == "fetch"]
    assert not any("AS somme_colis" in sql for sql in lues)
    assert "non joué" in " ".join(c.libelle for c in rapport.controles)


# ---------------------------------------------------------------------------
# Robustesse
# ---------------------------------------------------------------------------


def test_le_traitement_ne_leve_jamais():
    ecritures = _ecritures(echecs_scripts={"DSR-699": RuntimeError("connexion perdue")})
    rapport = _lancer(db_ecriture=ecritures, etape="cles")

    assert not rapport.reussi
    assert "connexion perdue" in (rapport.erreur or "")


def test_le_sql_complet_n_apparait_pas_dans_le_rapport():
    """Le rapport part en JSON vers une supervision : on s'en tient à l'aperçu tronqué."""
    secret = "INSERT INTO trppu_version_cle (co_regate) VALUES ('CONFIDENTIEL-" + "x" * 300
    echec = SqlScriptError(
        "boum",
        source="db/DSR-699_cles_calculees.sql",
        index=7,
        statement=secret,
        original=RuntimeError("1064"),
    )
    ecritures = _ecritures(echecs_scripts={"DSR-699": echec})

    rapport = _lancer(db_ecriture=ecritures, etape="cles")
    rendu = rapport.texte()

    assert secret not in rendu
    # L'aperçu du socle tronque à 120 caractères : aucune tranche plus longue ne doit passer.
    assert secret[:150] not in rendu


def test_le_statut_est_toujours_renseigne(chargement_reussi):
    """Sans lui, le rapport afficherait `RESULTAT :` vide — `statut` ne se calcule pas seul."""
    assert _lancer().statut == SUCCES
    assert _lancer(etape="calcul").statut == "ECHEC"


def test_le_rapport_est_serialisable_en_json(chargement_reussi):
    rapport = _lancer()
    assert json.loads(json.dumps(rapport.to_dict(), default=str))["reussi"] is True


def test_les_durees_par_etape_sont_rendues(chargement_reussi):
    """Ce sont elles qui diront si la chaîne entière tient dans la fenêtre d'exploitation."""
    rapport = _lancer()
    for etape in ETAPES:
        assert f"DUREE_MS_{etape.upper()}" in rapport.etats


# ---------------------------------------------------------------------------
# Marche à blanc
# ---------------------------------------------------------------------------


def test_dry_run_n_ecrit_rien(chargement_reussi):
    """`lecture_seule=True` laisse passer le dry_run et lève sur toute écriture réelle."""
    ecritures = _ecritures(lecture_seule=True)
    rapport = _lancer(db_ecriture=ecritures, dry_run=True)

    assert rapport.reussi, rapport.motifs
    assert all(script["dry_run"] for script in ecritures.scripts)
    assert chargement_reussi == [], "le chargement n'a pas de mode à blanc, il doit être sauté"


def test_dry_run_ne_lit_pas_la_base(chargement_reussi):
    """Base de lecture vide : toute lecture lèverait `KeyError`."""
    rapport = _lancer(
        db_lecture=FausseBase({}), db_ecriture=_ecritures(lecture_seule=True), dry_run=True
    )

    assert rapport.reussi, rapport.motifs


def test_dry_run_decoupe_bien_les_cinq_scripts(chargement_reussi):
    ecritures = _ecritures(lecture_seule=True)
    _lancer(db_ecriture=ecritures, dry_run=True)

    instructions = {
        script["label"]: script["resultat"].total_count for script in ecritures.scripts
    }
    # + 1 pour migration et correctif : `SET SESSION lock_wait_timeout` injecté en tête.
    assert instructions["db/DSR-696-699_migration.sql"] == 18
    assert instructions["db/fix_error.sql"] == 10
    assert instructions["db/DSR-696_site_trafic.sql"] == 9
    assert instructions["db/DSR-699_cles_calculees.sql"] == 12


def test_une_ecriture_reelle_sur_une_base_en_lecture_seule_leverait():
    """Garantit que le test précédent prouve quelque chose."""
    ecritures = _ecritures(lecture_seule=True)
    with pytest.raises(EcritureInterdite):
        asyncio.run(ecritures.execute_sql_script("SELECT 1;", label="x"))


# ---------------------------------------------------------------------------
# Câblage de la commande
# ---------------------------------------------------------------------------


def test_la_sous_commande_init_est_declaree():
    from app.main import build_parser

    args = build_parser().parse_args(["init", "7"])

    # Positionnel stocké sous `id_traitement` : nom repris par les logs et la corrélation.
    assert args.id_traitement == 7
    assert args.commande == "init"
    assert (args.depuis, args.etape, args.dry_run) == (None, None, False)


@pytest.mark.parametrize("etape", ETAPES)
def test_toutes_les_etapes_sont_acceptees_par_la_cli(etape):
    from app.main import build_parser

    assert build_parser().parse_args(["init", "1", "--etape", etape]).etape == etape


def test_une_etape_inconnue_est_refusee_par_argparse():
    """Refusée avant toute connexion, et `--help` documente la chaîne sans la recopier."""
    from app.main import build_parser

    with pytest.raises(SystemExit):
        build_parser().parse_args(["init", "1", "--etape", "calcul"])


def test_depuis_et_etape_sont_exclusifs_dans_la_cli():
    from app.main import build_parser

    with pytest.raises(SystemExit):
        build_parser().parse_args(["init", "1", "--depuis", "cles", "--etape", "cles"])


@pytest.mark.parametrize("reussi,code", [(True, 0), (False, 1)])
def test_le_code_retour_suit_le_verdict_du_rapport(monkeypatch, capsys, reussi, code):
    import app.main as main

    async def _faux(id_referentiel, **options):
        rapport = Rapport(titre="INIT", id_traitement=id_referentiel)
        rapport.ok("fait") if reussi else rapport.ko("raté")
        rapport.statut = SUCCES if reussi else "ECHEC"
        return rapport

    monkeypatch.setattr(main, "initialiser_cles_repartition", _faux)
    args = main.build_parser().parse_args(["init", "1"])

    assert asyncio.run(main.cmd_init(args)) == code
    assert "INIT" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# --skip-errors
# ---------------------------------------------------------------------------


def test_skip_errors_transmis_au_chargement_et_avertissements_remontes(monkeypatch):
    """Les lignes écartées remontent au rapport final de `init`, et la chaîne continue."""
    recu = {}

    async def _faux(
        id_referentiel, fichier=None, *, ignorer_erreurs=False
    ):
        recu["ignorer_erreurs"] = ignorer_erreurs
        rapport = _rapport_chargement()
        rapport.etats["LIGNES_IGNOREES"] = 2
        rapport.avertissements += ["Ligne 7 : vide", "Ligne 9 : doublon de PDI"]
        return rapport

    monkeypatch.setattr(initialisation, "charger_cles_repartition", _faux)

    rapport = _lancer(etape="chargement", ignorer_erreurs=True)

    assert rapport.reussi, rapport.motifs
    assert recu == {"ignorer_erreurs": True}
    assert rapport.etats["LIGNES_IGNOREES"] == 2
    assert rapport.avertissements == ["Ligne 7 : vide", "Ligne 9 : doublon de PDI"]


def test_sans_skip_errors_le_chargement_reste_strict(monkeypatch):
    recu = {}

    async def _faux(
        id_referentiel, fichier=None, *, ignorer_erreurs=False
    ):
        recu["ignorer_erreurs"] = ignorer_erreurs
        return _rapport_chargement()

    monkeypatch.setattr(initialisation, "charger_cles_repartition", _faux)

    _lancer(etape="chargement")

    assert recu == {"ignorer_erreurs": False}


# ---------------------------------------------------------------------------
# Étape « cles » : l'index unique reste en place (il sert la reprise), recréé s'il manque
# ---------------------------------------------------------------------------

INDEX_CLES_ABSENT = {"INDEX_NAME = 'uq_crc_version_pdi'": {"nb": 0}}


def test_l_index_unique_n_est_jamais_retire():
    ecritures = _ecritures()

    rapport = _lancer(db_ecriture=ecritures, etape="cles")

    assert rapport.reussi, rapport.motifs
    assert not any(label.startswith("cles/") for label in ecritures.scripts_joues())


def test_index_absent_recree_avant_le_premier_lot():
    """Laissé absent par un essai interrompu d'une version antérieure, qui le retirait."""
    ecritures = _ecritures(INDEX_CLES_ABSENT)

    rapport = _lancer(db_ecriture=ecritures, etape="cles")

    assert rapport.reussi, rapport.motifs
    joues = ecritures.scripts_joues()
    assert joues[0] == "cles/index-reconstruction"
    assert joues[1].startswith("db/DSR-699_cles_calculees.sql@")
    recree = ecritures.texte_du_script("index-reconstruction")
    assert "ADD UNIQUE KEY `uq_crc_version_pdi` (`id_version_cle`, `id_pdi`)" in recree
    assert recree.startswith("SET SESSION lock_wait_timeout")


def test_index_impossible_a_recreer_arrete_l_etape():
    ecritures = _ecritures(
        INDEX_CLES_ABSENT,
        echecs_scripts={"index-reconstruction": RuntimeError("1142 denied")},
    )

    rapport = _lancer(db_ecriture=ecritures, etape="cles")

    assert not rapport.reussi
    assert any("uq_crc_version_pdi non recréé" in m for m in rapport.motifs)
    assert not any("DSR-699" in label for label in ecritures.scripts_joues())


def test_les_scripts_de_la_chaine_ne_jouent_pas_leurs_select_d_affichage(
    chargement_reussi,
):
    ecritures = _ecritures()

    _lancer(db_ecriture=ecritures)

    scripts = [s for s in ecritures.scripts if s["label"].startswith("db/")]
    assert scripts and all(s["skip_selects"] for s in scripts)
    cles = next(s for s in scripts if "DSR-699" in s["label"])
    non_joues = [st.preview for st in cles["resultat"].statements if st.skipped]
    assert non_joues and all(apercu.upper().startswith("SELECT") for apercu in non_joues)
    joues = [st.preview for st in cles["resultat"].statements if not st.skipped]
    assert any(apercu.startswith("INSERT INTO trppu_cles_repartition_calcule") for apercu in joues)


def test_scripts_de_schema_attente_de_verrou_bornee():
    """Un ALTER bloqué par une transaction ouverte (client SQL) attendrait jusqu'à un an."""
    ecritures = _ecritures()

    _lancer(db_ecriture=ecritures, etape="correctif")

    assert ecritures.texte_du_script("fix_error").startswith("SET SESSION lock_wait_timeout = ")


def test_scripts_de_donnees_sans_injection():
    ecritures = _ecritures()

    _lancer(db_ecriture=ecritures, etape="agregats")

    assert not ecritures.texte_du_script("DSR-696_site").startswith("SET SESSION lock_wait")


def test_verrou_non_obtenu_explique_dans_le_rapport():
    import pymysql

    echec = SqlScriptError(
        "boum",
        source="db/fix_error.sql",
        index=7,
        statement="EXECUTE stmt",
        original=pymysql.err.OperationalError(1205, "Lock wait timeout exceeded"),
    )
    ecritures = _ecritures(echecs_scripts={"fix_error": echec})

    rapport = _lancer(db_ecriture=ecritures, etape="correctif")

    assert not rapport.reussi
    assert "SHOW FULL PROCESSLIST" in (rapport.erreur or "")


def test_rapport_d_echec_des_versions_exploitable():
    """Causes regroupées, détail par site, et la commande de reprise : de quoi corriger."""
    import pymysql

    def echec(valeur):
        return SqlScriptError(
            "boum",
            source="db/DSR-698_version_cle.sql@x",
            index=8,
            statement="INSERT INTO trppu_version_cle (…) SELECT …",
            original=pymysql.err.DataError(
                1366, f"Incorrect string value: '{valeur}' for column 'libelle' at row 1"
            ),
        )

    ecritures = _ecritures(echecs_scripts={"@000002": echec("a"), "@000003": echec("b")})

    rapport = _lancer(db_ecriture=ecritures, etape="versions")

    assert not rapport.reussi
    assert rapport.etats["SITES_VERSIONS_KO"] == 2
    assert rapport.etats["SITES_VERSIONS_KO_LISTE"] == "000002,000003"
    avertissements = "\n".join(rapport.avertissements)
    # Une seule cause pour les deux sites, malgré des valeurs différentes dans le message.
    assert "2 site(s) : (1366) Incorrect string value: '…' for column '…'" in avertissements
    assert "site 000002 : Échec de db/DSR-698_version_cle.sql@x, instruction 8" in avertissements
    assert "--depuis versions" in avertissements
    assert rapport.etats["REPRENDRE_A"] == "versions"



def test_versions_par_lots_une_transaction_par_lot(monkeypatch):
    """Une connexion et une transaction par lot, pas par site."""
    monkeypatch.setattr(initialisation, "INIT_VERSIONS_TAILLE_LOT", 2)
    ecritures = _ecritures()

    rapport = _lancer(db_ecriture=ecritures, etape="versions")

    assert rapport.reussi, rapport.motifs
    assert ecritures.lots_commites == 2  # 3 sites, lots de 2 : [1, 2] puis [3]
    assert rapport.etats["VERSIONS_CREEES"] == len(SITES)
    assert all(s.get("lot") for s in ecritures.scripts)


def test_un_lot_n_est_pas_rejoue_si_tout_passe():
    ecritures = _ecritures()

    _lancer(db_ecriture=ecritures, etape="versions")

    assert ecritures.lots_annules == 0
    assert len(ecritures.scripts_joues()) == len(SITES)



def test_chaine_refusee_si_le_chargement_n_est_pas_finalise():
    """Lignes en base sans index unique : des doublons sont possibles, il faut recharger."""
    lectures = _lectures(**{"AS chargement_uk": {"chargement_uk": 0, "chargement_tmp": 1}})
    ecritures = _ecritures(lecture_seule=True)

    rapport = _lancer(db_lecture=lectures, db_ecriture=ecritures, depuis="migration")

    assert not rapport.reussi
    assert "relancer l'étape « chargement »" in " ".join(rapport.motifs)
    assert ecritures.scripts_joues() == []


def test_les_controles_lisent_sur_l_instance_d_ecriture_par_defaut():
    """Sur un réplica, le dernier lot commité peut manquer : faux KO (2000 versions sur 2022)."""
    import inspect

    from app.db.mysql import db_write
    from app.traitements.initialisation import initialiser_cles_repartition

    parametres = inspect.signature(initialiser_cles_repartition).parameters
    assert parametres["db_lecture"].default is db_write
    assert parametres["db_ecriture"].default is db_write
