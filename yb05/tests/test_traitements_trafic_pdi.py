"""DSR-702 — calcul des trafics PDI : formule, ordre des écritures, échecs.

`reponses_calcul` : CO à 1000 de TMH, densités 1/0,5/0,25, PDI 1001 et 1002 à clés 0,6/0,4
→ 600/300/150 et 400/200/100 sur cinq jours (dix lignes).
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from decimal import Decimal

import pytest

from app.config import _entier_positif, _parse_trafic_max
from app.traitements import trafic_pdi
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
    # Un lot (10 lignes < TAILLE_LOT) puis la transaction finale trace + flag.
    assert ecriture.transactions_commitees == 2


def test_la_formule_est_tmh_fois_coefficient_fois_cle():
    """CA-04, sur des valeurs vérifiables à la main."""
    _, ecriture = _calculer(reponses_calcul())

    lignes = {(ligne[4], ligne[6]): ligne[7:10] for ligne in _lignes_inserees(ecriture)}

    assert lignes[(1001, "LUNDI")] == (600, 300, 150)
    assert lignes[(1002, "LUNDI")] == (400, 200, 100)


def test_le_coefficient_en_base_est_un_pourcentage():
    """coef = 52 en base vaut 52 % : TMH 1000 × 0,52 × clé 0,6 = 312, pas 31 200."""
    reponses = reponses_calcul()
    reponses["SELECT co_produit, jour_semaine, densite, coef"] = [
        {"co_produit": "CO", "jour_semaine": jour, "densite": densite, "coef": coef}
        for jour in ("LUNDI", "MARDI", "MERCREDI", "JEUDI", "VENDREDI")
        for densite, coef in ((0, Decimal("52.0000")), (1, Decimal("12.5000")), (2, Decimal("0")))
    ]

    rapport, ecriture = _calculer(reponses)

    assert rapport.statut == SUCCES
    lignes = {(ligne[4], ligne[6]): ligne[7:10] for ligne in _lignes_inserees(ecriture)}
    assert lignes[(1001, "LUNDI")] == (312, 75, 0)
    assert lignes[(1002, "LUNDI")] == (208, 50, 0)


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
        {"co_produit": "CO", "jour_semaine": jour, "densite": densite, "coef": Decimal("100")}
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
        {"co_produit": "CO", "jour_semaine": jour, "densite": densite, "coef": Decimal("100")}
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


def test_le_referentiel_trace_est_celui_de_la_version_de_cles():
    """DSR-702 étape 3 : référentiel lu dans `trppu_version_cle`, jamais dans
    `trppu_referentiel`."""
    reponses = reponses_calcul()
    reponses["FROM trppu_version_cle"] = {"id_version_cle": 7, "id_referentiel": 3}
    lecture = FausseBase(reponses, lecture_seule=True)
    ecriture = FausseBase(reponses)

    rapport = asyncio.run(calcul_trafic_pdi(12345, db_lecture=lecture, db_ecriture=ecriture))

    assert rapport.statut == SUCCES
    assert ecriture.parametres_de("SET id_referentiel = %s") == (3, 7, 12345)
    assert not any("trppu_referentiel" in sql for _, sql, _ in lecture.journal)


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
# Écriture par lots commités un à un (réplication de groupe, erreur 3100)
# ---------------------------------------------------------------------------


class BaseLots(FausseBase):
    """FausseBase dont les DELETE rendent une suite de rowcounts, et dont un lot
    d'insertion (numéroté à partir de 1) peut échouer au COMMIT."""

    def __init__(self, reponses, *, purges=None, lot_en_echec=None, purge_en_echec=False):
        super().__init__(reponses)
        self.purges = {k: list(v) for k, v in (purges or {}).items()}
        self.lot_en_echec = lot_en_echec
        self.purge_en_echec = purge_en_echec
        self.lots_tentes = 0

    async def execute(self, query, params=None, retries=None):
        for fragment, suite in self.purges.items():
            if fragment in query and suite:
                self._enregistrer("execute", query, params)
                return suite.pop(0)
        if self.purge_en_echec and self.lots_tentes and "DELETE FROM trppu_trafic_pdi" in query:
            raise RuntimeError("base indisponible")
        return await super().execute(query, params, retries)

    @asynccontextmanager
    async def transaction(self):
        async with super().transaction() as curseur:
            yield curseur
            if any("INSERT INTO trppu_trafic_pdi" in sql for _, sql, _ in self.journal[-1:]):
                self.lots_tentes += 1
                if self.lots_tentes == self.lot_en_echec:
                    raise RuntimeError(
                        "(3100, \"Error on observer while running replication hook "
                        "'before_commit'.\")"
                    )


def _calculer_par_lots(monkeypatch, taille_lot=3, **options):
    monkeypatch.setattr(trafic_pdi, "TAILLE_LOT", taille_lot)
    reponses = reponses_calcul()
    lecture = FausseBase(reponses, lecture_seule=True)
    ecriture = BaseLots(reponses, **options)
    rapport = asyncio.run(calcul_trafic_pdi(12345, db_lecture=lecture, db_ecriture=ecriture))
    return rapport, ecriture


def _lots_inseres(ecriture):
    return [p for g, sql, p in ecriture.journal if g != "fetch" and "INSERT INTO trppu_trafic_pdi" in sql]


def test_chaque_lot_est_commite_seul(monkeypatch):
    """10 lignes, lots de 3 : 4 transactions d'insertion + 1 finale (trace + flag)."""
    rapport, ecriture = _calculer_par_lots(monkeypatch)

    assert rapport.statut == SUCCES
    assert [len(lot) for lot in _lots_inseres(ecriture)] == [3, 3, 3, 1]
    assert ecriture.transactions_commitees == 5


def test_le_flag_calcule_n_est_pose_qu_apres_le_dernier_lot(monkeypatch):
    _, ecriture = _calculer_par_lots(monkeypatch)

    ecritures = ecriture.ecritures()
    dernier_lot = max(i for i, sql in enumerate(ecritures) if "INSERT INTO trppu_trafic_pdi" in sql)
    marquage = ecritures.index(next(sql for sql in ecritures if "SET trafic_pdi_calcule = 1" in sql))
    assert marquage > dernier_lot


def test_les_flags_sont_remis_a_zero_avant_la_purge(monkeypatch):
    """Dès la purge, plus rien ne doit prendre les trafics du scénario pour un calcul fini."""
    _, ecriture = _calculer_par_lots(monkeypatch)

    ecritures = ecriture.ecritures()
    remise_a_zero = next(i for i, sql in enumerate(ecritures) if "trafic_pdi_calcule = 0" in sql)
    purge = next(i for i, sql in enumerate(ecritures) if sql.startswith("DELETE FROM"))
    assert remise_a_zero < purge


def test_la_purge_boucle_par_lots_jusqu_a_epuisement(monkeypatch):
    """Recalcul d'un gros scénario : DELETE … LIMIT répété tant qu'un lot plein revient."""
    _, ecriture = _calculer_par_lots(
        monkeypatch, purges={"DELETE FROM trppu_trafic_pdi": [3, 3, 1]}
    )

    purges = [
        p for g, sql, p in ecriture.journal
        if g != "fetch" and "DELETE FROM trppu_trafic_pdi" in sql
    ]
    assert purges == [(12345, 3)] * 3
    assert all("LIMIT %s" in sql for sql in ecriture.ecritures() if sql.startswith("DELETE"))


def test_echec_d_un_lot_purge_les_lots_deja_commites(monkeypatch):
    """Erreur 3100 au 2e lot : le 1er, déjà commité, est retiré ; flag jamais posé."""
    rapport, ecriture = _calculer_par_lots(monkeypatch, lot_en_echec=2)

    assert rapport.statut == ECHEC
    assert "before_commit" in rapport.erreur
    assert rapport.etats["TRAFIC_PDI_CALCULE"] == 0
    assert not ecriture.a_ecrit("SET trafic_pdi_calcule = 1")
    ecritures = ecriture.ecritures()
    dernier_insert = max(i for i, sql in enumerate(ecritures) if "INSERT INTO trppu_trafic_pdi" in sql)
    assert any("DELETE FROM trppu_trafic_pdi" in sql for sql in ecritures[dernier_insert:])
    # Le verrou est quand même libéré et l'incident tracé.
    assert ecriture.a_ecrit("SET calcul_trafic_en_cours = 0")
    assert ecriture.a_ecrit("INSERT INTO trppu_recalcul_log")


def test_un_lot_en_echec_n_est_pas_rejoue(monkeypatch):
    """Sans clé unique, rejouer un lot dont le COMMIT est peut-être passé le doublerait."""
    _, ecriture = _calculer_par_lots(monkeypatch, lot_en_echec=2)

    assert ecriture.lots_tentes == 2
    assert len(_lots_inseres(ecriture)) == 2


def test_purge_impossible_apres_echec_garde_l_erreur_d_origine(monkeypatch, caplog):
    with caplog.at_level(logging.WARNING, logger="app.traitements.trafic_pdi"):
        rapport, _ = _calculer_par_lots(monkeypatch, lot_en_echec=2, purge_en_echec=True)

    assert rapport.statut == ECHEC
    assert "before_commit" in rapport.erreur
    assert any(
        r.getMessage().startswith("Purge des trafics PDI partiels impossible")
        for r in caplog.records
    )


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
    """Produit absent de CLES_PAR_PRODUIT : échec plutôt qu'un trafic faux."""
    reponses = reponses_calcul()
    reponses["FROM trppu_tmh"] = [{"co_produit": "ZZ", "tmh": Decimal("1000")}]
    reponses["SELECT co_produit, jour_semaine, densite, coef"] = [
        {"co_produit": "ZZ", "jour_semaine": "LUNDI", "densite": d, "coef": Decimal("100")}
        for d in (0, 1, 2)
    ]

    rapport, ecriture = _calculer(reponses)

    _verifier_echec_propre(rapport, ecriture, "absent de CLES_PAR_PRODUIT")


def test_trafic_hors_capacite_de_la_colonne(monkeypatch):
    """TRAFIC_MAX renseigné : au-delà, échec explicite plutôt qu'erreur MySQL."""
    monkeypatch.setattr(trafic_pdi, "TRAFIC_MAX", 99999999)
    reponses = reponses_calcul()
    reponses["FROM trppu_tmh"] = [{"co_produit": "CO", "tmh": Decimal("1000000000")}]

    rapport, ecriture = _calculer(reponses)

    _verifier_echec_propre(rapport, ecriture, "hors capacité de la colonne")
    # La formule complète, pour rejouer le calcul à la main.
    assert "PDI 1001, dense : TMH 1000000000 × coef 1.0000 × clé 0.6" in rapport.erreur


def test_sans_trafic_max_aucun_controle(monkeypatch):
    """TRAFIC_MAX vide : le batch écrit tel quel, c'est MySQL qui refusera le cas échéant."""
    monkeypatch.setattr(trafic_pdi, "TRAFIC_MAX", None)
    reponses = reponses_calcul()
    reponses["FROM trppu_tmh"] = [{"co_produit": "CO", "tmh": Decimal("1000000000")}]

    rapport, ecriture = _calculer(reponses)

    assert rapport.statut == SUCCES
    lignes = {(ligne[4], ligne[6]): ligne[7:10] for ligne in _lignes_inserees(ecriture)}
    assert lignes[(1001, "LUNDI")][0] == 600000000


@pytest.mark.parametrize(
    "brut, attendu",
    [(None, None), ("", None), ("  ", None), ("99999999", 99999999), (" 65535 ", 65535)],
)
def test_parse_trafic_max(brut, attendu):
    assert _parse_trafic_max(brut) == attendu


@pytest.mark.parametrize("brut", ["abc", "1.5", "-1"])
def test_parse_trafic_max_refuse_une_valeur_invalide(brut):
    with pytest.raises(ValueError, match="TRAFIC_MAX"):
        _parse_trafic_max(brut)


def test_trafic_au_dela_de_lancien_plafond_smallint(monkeypatch):
    """Prod, scénario 15 : 108 716 rejeté tant que les colonnes étaient en smallint (65 535)."""
    monkeypatch.setattr(trafic_pdi, "TRAFIC_MAX", 99999999)
    reponses = reponses_calcul()
    reponses["FROM trppu_tmh"] = [{"co_produit": "CO", "tmh": Decimal("200000")}]

    rapport, ecriture = _calculer(reponses)

    assert rapport.statut == SUCCES
    lignes = {(ligne[4], ligne[6]): ligne[7:10] for ligne in _lignes_inserees(ecriture)}
    assert lignes[(1001, "LUNDI")] == (120000, 60000, 30000)


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
    """Jour coefficienté pour un produit mais pas un autre : oubli de paramétrage, échec."""
    reponses = reponses_calcul()
    reponses["FROM trppu_tmh"] = [
        {"co_produit": "CO", "tmh": Decimal("1000")},
        {"co_produit": "OO", "tmh": Decimal("500")},
    ]
    reponses["SELECT co_produit, jour_semaine, densite, coef"] = [
        {"co_produit": "CO", "jour_semaine": jour, "densite": d, "coef": Decimal("100")}
        for jour in ("LUNDI", "MARDI", "MERCREDI", "JEUDI", "VENDREDI")
        for d in (0, 1, 2)
    ] + [
        # OO ne couvre pas VENDREDI, que CO couvre pourtant.
        {"co_produit": "OO", "jour_semaine": jour, "densite": d, "coef": Decimal("100")}
        for jour in ("LUNDI", "MARDI", "MERCREDI", "JEUDI")
        for d in (0, 1, 2)
    ]

    rapport, ecriture = _calculer(reponses)

    _verifier_echec_propre(rapport, ecriture, "Coefficients de rétention manquants")
    assert "OO/VENDREDI" in rapport.erreur


def test_jour_non_coefficiente_par_la_version_est_simplement_ignore():
    """Jour qu'aucun produit ne coefficiente : un jour en moins, pas un échec."""
    reponses = reponses_calcul()
    reponses["SELECT co_produit, jour_semaine, densite, coef"] = [
        {"co_produit": "CO", "jour_semaine": jour, "densite": d, "coef": Decimal("100")}
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
        {"co_produit": "CO", "jour_semaine": "SAMEDI", "densite": d, "coef": Decimal("100")}
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


@pytest.mark.parametrize(
    "brut, attendu", [(None, 5000), ("", 5000), ("abc", 5000), ("2000", 2000), ("0", 1)]
)
def test_taille_lot_lue_dans_lenvironnement(monkeypatch, brut, attendu):
    if brut is None:
        monkeypatch.delenv("TAILLE_LOT", raising=False)
    else:
        monkeypatch.setenv("TAILLE_LOT", brut)
    assert _entier_positif("TAILLE_LOT", 5000) == attendu
