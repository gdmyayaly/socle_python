"""DSR-703 — agrégation des trafics PDI par Agrébal.

Le traitement ne calcule rien lui-même : il somme. Les tests portent donc sur les contrôles
préalables, sur la forme des trois agrégations (une par couleur PIC), et sur la fin de calcul —
c'est ce traitement qui libère le scénario.
"""

import asyncio

from app.traitements.rapport import ECHEC, SUCCES
from app.traitements.trafic_agrebal import calcul_trafic_agrebal
from tests.conftest import FausseBase, reponses_eligibles


def reponses_agrebal(**surcharges):
    """Scénario dont les trafics PDI sont calculés, et l'état de ses 10 lignes PDI."""
    reponses = reponses_eligibles()
    reponses["FROM trppu_scenario WHERE id_scenario"].update(
        {"trafic_pdi_calcule": 1, "calcul_trafic_en_cours": 1}
    )
    reponses.update(
        {
            "FROM trppu_trafic_pdi": {
                "nb_lignes": 10,
                "id_agrebal_max": 2404,
                "total_dense": 1000,
                "total_faible1": 500,
                "total_faible2": 250,
            },
            "FROM trppu_trafic_agrebal": {"nb_lignes": 15, "total": 1750},
        }
    )
    reponses.update(surcharges)
    return reponses


def _agreger(reponses, *, rowcounts=None):
    lecture = FausseBase(reponses, lecture_seule=True)
    ecriture = FausseBase(reponses, rowcounts=rowcounts or {"INSERT INTO trppu_trafic_agrebal": 5})
    rapport = asyncio.run(
        calcul_trafic_agrebal(12345, db_lecture=lecture, db_ecriture=ecriture)
    )
    return rapport, ecriture


# ---------------------------------------------------------------------------
# Cas nominal
# ---------------------------------------------------------------------------


def test_agregation_nominale():
    rapport, ecriture = _agreger(reponses_agrebal())

    assert rapport.statut == SUCCES
    assert rapport.etats["TRAFIC_AGREBAL_CALCULE"] == 1
    assert rapport.etats["CALCUL_TRAFIC_EN_COURS"] == 0
    assert ecriture.transactions_commitees == 1


def test_une_agregation_par_couleur_pic():
    """CA-04 : DENSE, FAIBLE1 et FAIBLE2, chacune sommant sa propre colonne."""
    _, ecriture = _agreger(reponses_agrebal())

    insertions = [sql for sql in ecriture.ecritures() if "INSERT INTO trppu_trafic_agrebal" in sql]

    assert len(insertions) == 3
    assert any("SUM(dense)" in sql for sql in insertions)
    assert any("SUM(faible1)" in sql for sql in insertions)
    assert any("SUM(faible2)" in sql for sql in insertions)


def test_l_agregation_groupe_par_agrebal_produit_jour():
    _, ecriture = _agreger(reponses_agrebal())

    insertion = next(
        sql for sql in ecriture.ecritures() if "INSERT INTO trppu_trafic_agrebal" in sql
    )

    assert (
        "GROUP BY id_scenario, co_regate, id_agrebal, agrebal_uuid, co_produit, jour_semaine"
        in insertion
    )


def test_les_anciens_agregats_sont_purges_avant_insertion():
    _, ecriture = _agreger(reponses_agrebal())

    ecritures = ecriture.ecritures()
    suppression = next(i for i, sql in enumerate(ecritures) if sql.startswith("DELETE"))
    insertion = next(
        i for i, sql in enumerate(ecritures) if "INSERT INTO trppu_trafic_agrebal" in sql
    )

    assert suppression < insertion


def test_le_traitement_ne_prend_pas_le_verrou_mais_le_libere():
    """Il tourne sous le verrou posé par DSR-702 ; sa mise à jour finale le relâche."""
    _, ecriture = _agreger(reponses_agrebal())

    assert not ecriture.a_ecrit("SET calcul_trafic_en_cours = 1")
    assert ecriture.a_ecrit("SET trafic_agrebal_calcule = 1, calcul_trafic_en_cours = 0")


def test_aucune_cle_ni_coefficient_n_est_lu():
    """CA-06 : l'agrégation ne consulte ni les clés de répartition ni les coefficients."""
    lecture = FausseBase(reponses_agrebal(), lecture_seule=True)
    asyncio.run(calcul_trafic_agrebal(12345, db_lecture=lecture, db_ecriture=FausseBase({})))

    lues = " ".join(sql for genre, sql, _ in lecture.journal if genre == "fetch")

    assert "cles_repartition_calcule" not in lues
    assert "pic_coefficients" not in lues


# ---------------------------------------------------------------------------
# Contrôles préalables
# ---------------------------------------------------------------------------


def test_scenario_inexistant():
    reponses = reponses_agrebal()
    reponses["FROM trppu_scenario WHERE id_scenario"] = None

    rapport, ecriture = _agreger(reponses)

    assert rapport.statut == ECHEC
    assert rapport.motifs == ["Scénario inexistant"]
    assert ecriture.ecritures() == []


def test_trafics_pdi_non_calcules():
    """CA-02 : flag à 0, aucun calcul Agrébal, et le message rappelle la commande à jouer."""
    reponses = reponses_agrebal()
    reponses["FROM trppu_scenario WHERE id_scenario"]["trafic_pdi_calcule"] = 0

    rapport, ecriture = _agreger(reponses)

    assert rapport.statut == ECHEC
    assert "YB05 CALCUL_TRAFIC_PDI 12345" in rapport.erreur
    assert ecriture.ecritures() == []


def test_flag_pose_mais_table_vide():
    """Le flag ne suffit pas : le ticket exige aussi au moins une ligne dans la table."""
    reponses = reponses_agrebal()
    reponses["FROM trppu_trafic_pdi"] = {
        "nb_lignes": 0,
        "id_agrebal_max": None,
        "total_dense": None,
        "total_faible1": None,
        "total_faible2": None,
    }

    rapport, ecriture = _agreger(reponses)

    assert rapport.statut == ECHEC
    assert ecriture.ecritures() == []


# ---------------------------------------------------------------------------
# Anomalies
# ---------------------------------------------------------------------------


def test_ecart_de_totaux_signale():
    """La somme des volumes Agrébal doit égaler celle des trafics PDI."""
    reponses = reponses_agrebal()
    reponses["FROM trppu_trafic_agrebal"] = {"nb_lignes": 15, "total": 1700}

    rapport, _ = _agreger(reponses)

    assert rapport.statut == ECHEC
    assert "Écart entre trafics PDI (1750) et trafics Agrébal (1700)" in rapport.motifs


def test_id_agrebal_hors_capacite_de_la_cible():
    """`trppu_trafic_pdi.id_agrebal` est un bigint, la cible un int : l'écart est contrôlé."""
    reponses = reponses_agrebal()
    reponses["FROM trppu_trafic_pdi"]["id_agrebal_max"] = 3_000_000_000

    rapport, ecriture = _agreger(reponses)

    assert rapport.statut == ECHEC
    assert "au-delà de la capacité" in rapport.erreur
    assert rapport.etats["CALCUL_TRAFIC_EN_COURS"] == 0
    assert ecriture.a_ecrit("SET calcul_trafic_en_cours = 0")
    assert ecriture.a_ecrit("INSERT INTO trppu_recalcul_log")
    assert not ecriture.a_ecrit("INSERT INTO trppu_trafic_agrebal")


# ---------------------------------------------------------------------------
# Journalisation
# ---------------------------------------------------------------------------


def test_journalisation_initial():
    """CA-08 : aucun historique, donc premier calcul."""
    _, ecriture = _agreger(reponses_agrebal())

    params = ecriture.parametres_de("INSERT INTO trppu_recalcul_log")

    assert params[1] == "INITIAL"
    assert params[2] == "Premier calcul des trafics Agrébal"


def test_journalisation_reprend_le_dernier_motif():
    """CA-11 : le motif journalisé est celui de la dernière demande enregistrée."""
    reponses = reponses_agrebal()
    reponses["FROM trppu_recalcul_log"] = {"raison": "CLE_REPARTITION"}

    _, ecriture = _agreger(reponses)

    params = ecriture.parametres_de("INSERT INTO trppu_recalcul_log")

    assert params[1] == "CLE_REPARTITION"
    assert "nouvelle version de clés" in params[2]
