"""DSR-702 — calcul des trafics PDI : formule, ordre des écritures, échecs.

Le jeu de référence (`reponses_calcul`) est volontairement minuscule et calculable de tête :
un produit `CO` à 1000 de TMH, cinq jours, trois densités (1 / 0,5 / 0,25) et deux PDI dont les
clés colis valent 0,6 et 0,4. Attendu pour le PDI 1001 : 600 / 300 / 150, et 400 / 200 / 100
pour le 1002 — soit dix lignes.
"""

import asyncio
from decimal import Decimal

from app.traitements.rapport import ECHEC, SUCCES
from app.traitements.trafic_pdi import calcul_trafic_pdi
from tests.conftest import FausseBase, reponses_calcul


def _calculer(reponses, *, rowcounts=None):
    lecture = FausseBase(reponses, lecture_seule=True)
    ecriture = FausseBase(reponses, rowcounts=rowcounts)
    rapport = asyncio.run(
        calcul_trafic_pdi(12345, db_lecture=lecture, db_ecriture=ecriture)
    )
    return rapport, ecriture


def _lignes_inserees(ecriture):
    return ecriture.parametres_de("INSERT INTO trppu_trafic_pdi")


# ---------------------------------------------------------------------------
# Cas nominal
# ---------------------------------------------------------------------------


def test_calcul_nominal():
    rapport, ecriture = _calculer(reponses_calcul())

    assert rapport.statut == SUCCES
    assert rapport.etats["TRAFIC_PDI_CALCULE"] == 1
    # Laissé à 1 volontairement : c'est le calcul Agrébal qui libère le scénario (CA-05).
    assert rapport.etats["CALCUL_TRAFIC_EN_COURS"] == 1
    assert len(_lignes_inserees(ecriture)) == 10
    assert ecriture.transactions_commitees == 1


def test_la_formule_est_tmh_fois_coefficient_fois_cle():
    """CA-04, sur des valeurs vérifiables à la main."""
    _, ecriture = _calculer(reponses_calcul())

    lignes = {(ligne[4], ligne[6]): ligne[7:10] for ligne in _lignes_inserees(ecriture)}

    assert lignes[(1001, "LUNDI")] == (600, 300, 150)
    assert lignes[(1002, "LUNDI")] == (400, 200, 100)


def test_chaque_ligne_porte_son_agrebal():
    """Le PDI est rattaché à son Agrébal via `trppu_agrebal_pdi` (id + uuid)."""
    _, ecriture = _calculer(reponses_calcul())

    ligne = _lignes_inserees(ecriture)[0]

    assert ligne[0] == 12345  # id_scenario
    assert ligne[1] == "123456"  # co_regate
    assert ligne[2] == 2404  # id_agrebal
    assert ligne[3] == "uuid-2404"  # agrebal_uuid


def test_les_jours_suivent_la_semaine_du_scenario():
    """5 jours ouvrés : SAMEDI ne doit pas être calculé même s'il est coefficienté."""
    reponses = reponses_calcul()
    reponses["SELECT co_produit, jour_semaine, densite, coef"] = [
        {"co_produit": "CO", "jour_semaine": jour, "densite": densite, "coef": Decimal("1")}
        for jour in ("LUNDI", "MARDI", "MERCREDI", "JEUDI", "VENDREDI", "SAMEDI")
        for densite in (0, 1, 2)
    ]

    _, ecriture = _calculer(reponses)

    jours = {ligne[6] for ligne in _lignes_inserees(ecriture)}
    assert "SAMEDI" not in jours
    assert len(jours) == 5


def test_samedi_est_calcule_pour_une_semaine_de_six_jours():
    reponses = reponses_calcul()
    reponses["FROM trppu_scenario WHERE id_scenario"]["nb_jours_semaine"] = 6
    reponses["SELECT co_produit, jour_semaine, densite, coef"] = [
        {"co_produit": "CO", "jour_semaine": jour, "densite": densite, "coef": Decimal("1")}
        for jour in ("LUNDI", "MARDI", "MERCREDI", "JEUDI", "VENDREDI", "SAMEDI")
        for densite in (0, 1, 2)
    ]

    _, ecriture = _calculer(reponses)

    assert "SAMEDI" in {ligne[6] for ligne in _lignes_inserees(ecriture)}


# ---------------------------------------------------------------------------
# Ordre des écritures
# ---------------------------------------------------------------------------


def test_le_verrou_est_pose_avant_toute_autre_ecriture():
    """Étape 2 du ticket : le verrou d'abord, et commité seul pour être visible."""
    _, ecriture = _calculer(reponses_calcul())

    assert "SET calcul_trafic_en_cours = 1" in ecriture.ecritures()[0]


def test_la_tracabilite_est_ecrite_avant_le_calcul():
    """CA-03 (et DSR-700) : référentiel et version de clés mémorisés avant le premier calcul."""
    _, ecriture = _calculer(reponses_calcul())

    ecritures = ecriture.ecritures()
    index_tracabilite = next(
        i for i, sql in enumerate(ecritures) if "SET id_referentiel = %s" in sql
    )
    index_insertion = next(
        i for i, sql in enumerate(ecritures) if "INSERT INTO trppu_trafic_pdi" in sql
    )

    assert index_tracabilite < index_insertion
    assert ecriture.parametres_de("SET id_referentiel = %s") == (2, 4, 12345)


def test_les_trafics_agrebal_sont_purges_avant_les_trafics_pdi():
    """Étape 4 : les deux trafics sont indissociables, l'Agrébal part en premier."""
    _, ecriture = _calculer(reponses_calcul())

    ecritures = ecriture.ecritures()
    suppression_agrebal = next(
        i for i, sql in enumerate(ecritures) if "DELETE FROM trppu_trafic_agrebal" in sql
    )
    suppression_pdi = next(
        i for i, sql in enumerate(ecritures) if "DELETE FROM trppu_trafic_pdi" in sql
    )
    insertion = next(
        i for i, sql in enumerate(ecritures) if "INSERT INTO trppu_trafic_pdi" in sql
    )

    assert suppression_agrebal < suppression_pdi < insertion


def test_les_flags_sont_remis_a_zero_avant_le_rechargement():
    _, ecriture = _calculer(reponses_calcul())

    assert ecriture.a_ecrit("SET trafic_pdi_calcule = 0, trafic_agrebal_calcule = 0")
    assert ecriture.a_ecrit("SET trafic_pdi_calcule = 1")


# ---------------------------------------------------------------------------
# Refus
# ---------------------------------------------------------------------------


def test_scenario_non_eligible_n_ecrit_rien():
    """CA-01 : aucun calcul, et surtout aucun verrou posé."""
    reponses = reponses_calcul()
    reponses["FROM trppu_scenario WHERE id_scenario"]["est_fige"] = 0

    rapport, ecriture = _calculer(reponses)

    assert rapport.statut == ECHEC
    assert ecriture.ecritures() == []
    assert "Le scénario n'est pas figé" in rapport.motifs


def test_verrou_deja_pris_par_un_autre_processus():
    """0 ligne affectée : quelqu'un d'autre a réservé le scénario entre-temps."""
    rapport, ecriture = _calculer(
        reponses_calcul(), rowcounts={"SET calcul_trafic_en_cours = 1": 0}
    )

    assert rapport.statut == ECHEC
    assert "Un calcul de trafic est déjà en cours" in rapport.motifs
    assert not ecriture.a_ecrit("DELETE FROM trppu_trafic_pdi")


# ---------------------------------------------------------------------------
# Échecs en cours de calcul
# ---------------------------------------------------------------------------


def _verifier_echec_propre(rapport, ecriture, fragment_message):
    assert rapport.statut == ECHEC
    assert fragment_message in rapport.erreur
    assert rapport.etats["TRAFIC_PDI_CALCULE"] == 0
    assert rapport.etats["CALCUL_TRAFIC_EN_COURS"] == 0
    # Le verrou est libéré et l'incident tracé, hors de toute transaction annulée.
    assert ecriture.a_ecrit("SET calcul_trafic_en_cours = 0")
    assert ecriture.a_ecrit("INSERT INTO trppu_recalcul_log")
    assert not ecriture.a_ecrit("INSERT INTO trppu_trafic_pdi")


def test_produit_hors_mapping_fait_echouer_le_calcul():
    """Décision A : plutôt que d'inventer une clé, on refuse de calculer."""
    reponses = reponses_calcul()
    reponses["FROM trppu_tmh"] = [{"co_produit": "ZZ", "tmh": Decimal("1000")}]
    reponses["SELECT co_produit, jour_semaine, densite, coef"] = [
        {"co_produit": "ZZ", "jour_semaine": "LUNDI", "densite": d, "coef": Decimal("1")}
        for d in (0, 1, 2)
    ]

    rapport, ecriture = _calculer(reponses)

    _verifier_echec_propre(rapport, ecriture, "absent de CLES_PAR_PRODUIT")


def test_trafic_hors_capacite_de_la_colonne():
    """Décision D : `smallint unsigned` — au-delà de 65 535, on échoue au lieu de tronquer."""
    reponses = reponses_calcul()
    reponses["FROM trppu_tmh"] = [{"co_produit": "CO", "tmh": Decimal("10000000")}]

    rapport, ecriture = _calculer(reponses)

    _verifier_echec_propre(rapport, ecriture, "hors capacité de la colonne")


def test_aucun_tmh_exploitable():
    reponses = reponses_calcul()
    reponses["FROM trppu_tmh"] = []

    rapport, ecriture = _calculer(reponses)

    _verifier_echec_propre(rapport, ecriture, "Aucun TMH exploitable")


def test_aucune_cle_de_repartition():
    reponses = reponses_calcul()
    reponses["FROM trppu_cles_repartition_calcule"] = []

    rapport, ecriture = _calculer(reponses)

    _verifier_echec_propre(rapport, ecriture, "Aucune clé de répartition trouvée")


def test_un_produit_sans_coefficient_sur_un_jour_coefficiente_echoue():
    """Un produit qui manque un jour que les autres couvrent est un oubli de paramétrage.

    À distinguer du cas suivant : un jour qu'AUCUN produit ne coefficiente est un choix
    d'exploitation, et se traduit par un jour en moins, pas par un échec.
    """
    reponses = reponses_calcul()
    reponses["FROM trppu_tmh"] = [
        {"co_produit": "CO", "tmh": Decimal("1000")},
        {"co_produit": "OO", "tmh": Decimal("500")},
    ]
    reponses["SELECT co_produit, jour_semaine, densite, coef"] = [
        {"co_produit": "CO", "jour_semaine": jour, "densite": d, "coef": Decimal("1")}
        for jour in ("LUNDI", "MARDI", "MERCREDI", "JEUDI", "VENDREDI")
        for d in (0, 1, 2)
    ] + [
        # OO ne couvre pas VENDREDI, que CO couvre pourtant.
        {"co_produit": "OO", "jour_semaine": jour, "densite": d, "coef": Decimal("1")}
        for jour in ("LUNDI", "MARDI", "MERCREDI", "JEUDI")
        for d in (0, 1, 2)
    ]

    rapport, ecriture = _calculer(reponses)

    _verifier_echec_propre(rapport, ecriture, "Coefficients de rétention manquants")
    assert "OO/VENDREDI" in rapport.erreur


def test_jour_non_coefficiente_par_la_version_est_simplement_ignore():
    """Tolérance assumée au niveau de la version PIC : on calcule ce qui est paramétré."""
    reponses = reponses_calcul()
    reponses["SELECT co_produit, jour_semaine, densite, coef"] = [
        {"co_produit": "CO", "jour_semaine": jour, "densite": d, "coef": Decimal("1")}
        for jour in ("LUNDI", "MARDI")
        for d in (0, 1, 2)
    ]

    rapport, ecriture = _calculer(reponses)

    assert rapport.statut == SUCCES
    assert {ligne[6] for ligne in _lignes_inserees(ecriture)} == {"LUNDI", "MARDI"}


def test_aucun_jour_calculable():
    """La version ne coefficiente que le samedi, le scénario est en semaine de 5 jours."""
    reponses = reponses_calcul()
    reponses["SELECT co_produit, jour_semaine, densite, coef"] = [
        {"co_produit": "CO", "jour_semaine": "SAMEDI", "densite": d, "coef": Decimal("1")}
        for d in (0, 1, 2)
    ]

    rapport, ecriture = _calculer(reponses)

    _verifier_echec_propre(rapport, ecriture, "Aucun jour calculable")


# ---------------------------------------------------------------------------
# Journalisation
# ---------------------------------------------------------------------------


def test_raison_initial_sans_historique():
    """CA-08 : premier calcul, aucune ligne dans `trppu_recalcul_log`."""
    _, ecriture = _calculer(reponses_calcul())

    params = ecriture.parametres_de("INSERT INTO trppu_recalcul_log")

    assert params[1] == "INITIAL"
    assert params[2].startswith("Premier calcul des trafics du scénario")


def test_raison_reprise_de_la_derniere_demande():
    """CA-09 à CA-11 : le motif vient de la dernière demande enregistrée."""
    reponses = reponses_calcul()
    reponses["FROM trppu_recalcul_log"] = {"raison": "AGREBAL"}

    _, ecriture = _calculer(reponses)

    params = ecriture.parametres_de("INSERT INTO trppu_recalcul_log")

    assert params[1] == "AGREBAL"
    assert "évolution Agrébal" in params[2]


def test_raison_inconnue_ramenee_a_initial():
    """Une valeur hors enum ne doit pas casser l'insertion du journal."""
    reponses = reponses_calcul()
    reponses["FROM trppu_recalcul_log"] = {"raison": "FANTAISIE"}

    _, ecriture = _calculer(reponses)

    assert ecriture.parametres_de("INSERT INTO trppu_recalcul_log")[1] == "INITIAL"


# ---------------------------------------------------------------------------
# Écarts de périmètre
# ---------------------------------------------------------------------------


def test_pdi_sans_agrebal_est_signale_et_ignore():
    """Un PDI sans Agrébal ne peut pas être écrit : le rapport le dit au lieu de l'inventer."""
    reponses = reponses_calcul()
    reponses["FROM trppu_agrebal_pdi"] = [
        {
            "agrebal_id": 2404,
            "agrebal_uuid": "uuid-2404",
            "agrebal_pdiQuantity": 1,
            "agrebal_pdiList": '[{"pdi_id": 1001}]',
        }
    ]

    rapport, ecriture = _calculer(reponses)

    assert rapport.statut == SUCCES
    assert len(_lignes_inserees(ecriture)) == 5  # un seul PDI × 5 jours
    assert any("sans Agrébal" in c.libelle for c in rapport.controles)
