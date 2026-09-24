"""Orchestration de la chaîne d'initialisation des clés de répartition.

Aucune base, aucun réseau : `FausseBase` répond aux lectures et découpe réellement les scripts.
Ce qui est vérifié ici n'est pas que chaque étape « marche » — les scripts sont couverts par
`test_scripts_dsr.py` — mais ce que l'orchestration seule peut garantir : l'ordre, le mode
d'exécution de chaque script, le refus de démarrer sur un état incohérent, et surtout le refus
de franchir l'étape irréversible quand ce qui précède est incomplet.
"""

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
    """Base de lecture décrivant un référentiel sain, à trois sites.

    Les fragments de requête sont ceux qui distinguent chaque lecture sans ambiguïté ; les
    surcharges permettent à un test de casser un seul point de la chaîne.
    """
    reponses = {
        # Prérequis
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
        "HAVING SUM(cle_colis)": [],
    }
    reponses.update(surcharges)
    return FausseBase(reponses)


def _ecritures(**options) -> FausseBase:
    options.setdefault("rowcounts_scripts", ROWCOUNTS)
    return FausseBase({}, **options)


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

    async def _faux(id_referentiel, fichier=None):
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
    # Le script des versions est joué une fois par site : on ne garde que la première
    # occurrence de chacun pour comparer l'ordre.
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

    for fichier in ("DSR-696_site_trafic", "DSR-698_version_cle", "DSR-699_cles_calculees"):
        assert ecritures.script_joue(fichier)["transactional"] is True, fichier


def test_le_chargement_en_echec_arrete_la_chaine(monkeypatch):
    async def _echec(id_referentiel, fichier=None):
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

    assert len(ecritures.scripts_joues()) == len(SITES)
    assert not rapport.reussi
    assert rapport.etats["SITES_VERSIONS_KO"] == 1


def test_l_etape_cles_n_est_pas_jouee_si_un_site_a_echoue():
    """Le test le plus important du fichier : il protège l'irréversible.

    Calculer les clés d'une couverture partielle verrouillerait le référentiel par le CA4 de
    DSR-699 — les sites restants ne pourraient plus jamais être calculés sur ces versions.
    """
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
    """Quelques milliers de sites ne doivent pas produire autant de lignes de rapport.

    Soixante suffisent à le prouver : le rapport doit rendre un agrégat, pas une liste. Les
    détails par site vont dans les logs, où ils sont filtrables.
    """
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
    with caplog.at_level(logging.INFO, logger="app.traitements.initialisation"):
        _lancer(etape="versions")

    avancements = [r for r in caplog.records if r.getMessage().startswith("Avancement")]
    assert avancements
    assert "sites=2" in avancements[0].getMessage()


# ---------------------------------------------------------------------------
# Critères d'acceptation du calcul des clés
# ---------------------------------------------------------------------------


def test_un_denominateur_nul_bloque_avant_le_calcul():
    """Il annonce l'ERROR 1365, et évite des heures de calcul pour rien."""
    lectures = _lectures(
        **{"trafic_colis_total = 0": [{"co_regate_site": "000002"}]}
    )
    ecritures = _ecritures(lecture_seule=True)

    rapport = _lancer(db_lecture=lectures, db_ecriture=ecritures, etape="cles")

    assert not rapport.reussi
    assert "division par zéro" in " ".join(rapport.motifs)
    assert ecritures.scripts_joues() == []


def test_un_perimetre_deja_calcule_bloque_avant_le_calcul():
    """Le script se terminerait en succès après avoir écrit zéro ligne : un faux `[OK]`."""
    lectures = _lectures(**{"%s AND EXISTS": {"nb": 2}})
    ecritures = _ecritures(lecture_seule=True)

    rapport = _lancer(db_lecture=lectures, db_ecriture=ecritures, etape="cles")

    assert not rapport.reussi
    assert "CA4" in " ".join(rapport.motifs)
    assert ecritures.scripts_joues() == []


def test_le_garde_fou_du_ca4_joue_aussi_sur_une_chaine_complete(chargement_reussi):
    """Le chargement purge `trppu_cles_repartition`, jamais les clés déjà calculées."""
    lectures = _lectures(**{"%s AND EXISTS": {"nb": 1}})
    rapport = _lancer(db_lecture=lectures)

    assert not rapport.reussi
    assert rapport.etats["REPRENDRE_A"] == "cles"


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
            "HAVING SUM(cle_colis)": [
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
            "HAVING SUM(cle_colis)": [
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
    lectures = _lectures(**{"HAVING SUM(cle_colis)": anomalies})

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
    assert not any("HAVING SUM(cle_colis)" in sql for sql in lues)
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
    """La marche à blanc doit valider le découpage sans dépendre de l'état de la base.

    `FausseBase` lève `KeyError` sur toute requête sans réponse déclarée : une base vide est
    donc la façon la plus simple de prouver qu'aucune lecture n'a lieu.
    """
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
    assert instructions["db/DSR-696-699_migration.sql"] == 17
    assert instructions["db/fix_error.sql"] == 9
    assert instructions["db/DSR-696_site_trafic.sql"] == 9
    assert instructions["db/DSR-699_cles_calculees.sql"] == 11


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

    # Le positionnel est stocké sous `id_traitement` : c'est ce nom que `main()` reprend dans
    # ses logs `Début`/`Fin commande` et que `_executer_traitement` pose comme corrélation.
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
