"""DSR-704 — mode ALL : sélection, parallélisme, isolation des échecs, bilan.

L'orchestrateur n'a pas de logique métier propre : les trois traitements sont donc remplacés par
des doublures, et les tests portent sur ce qui lui appartient réellement — qui est appelé, dans
quel ordre, combien de fois en même temps, et ce que dit le bilan.

Le parallélisme est vérifié par un **compteur de concurrence observée**, jamais par des durées :
une assertion sur le temps passe ou échoue selon la charge de la machine, ce qui produirait un
test qui ment une fois sur dix.
"""

import asyncio

import pytest

from app.traitements import orchestrateur
from app.traitements.rapport import ECHEC, NON_ELIGIBLE, SUCCES, Bilan, Rapport
from tests.conftest import FausseBase


# ---------------------------------------------------------------------------
# Doublures
# ---------------------------------------------------------------------------


def _rapport(statut, motif=None, erreur=None):
    """Rapport minimal, réussi ou non selon `statut`."""
    rapport = Rapport(titre="doublure", id_scenario=0, statut=statut)
    if motif:
        rapport.ko(motif)
    if erreur:
        rapport.erreur = erreur
    return rapport


class Traitements:
    """Doublures des trois traitements, avec journal des appels et suivi de concurrence."""

    def __init__(self, *, non_eligibles=(), echecs_pdi=(), echecs_agrebal=(), delai=0.0):
        self.non_eligibles = set(non_eligibles)
        self.echecs_pdi = set(echecs_pdi)
        self.echecs_agrebal = set(echecs_agrebal)
        self.delai = delai
        self.appels: list[tuple[str, int]] = []
        self.en_cours = 0
        self.concurrence_max = 0

    async def eligibilite(self, id_scenario, *, db_lecture=None):
        self.appels.append(("eligibilite", id_scenario))
        if id_scenario in self.non_eligibles:
            return _rapport(NON_ELIGIBLE, motif="Le scénario n'est pas figé")
        return _rapport("ELIGIBLE")

    async def pdi(self, id_scenario, *, db_lecture=None, db_ecriture=None):
        self.appels.append(("pdi", id_scenario))
        await self._travailler()
        if id_scenario in self.echecs_pdi:
            return _rapport(ECHEC, erreur="Aucune clé de répartition trouvée")
        return _rapport(SUCCES)

    async def agrebal(self, id_scenario, *, db_lecture=None, db_ecriture=None):
        self.appels.append(("agrebal", id_scenario))
        await self._travailler()
        if id_scenario in self.echecs_agrebal:
            return _rapport(ECHEC, erreur="Trafics PDI non calculés")
        return _rapport(SUCCES)

    async def _travailler(self):
        """Simule le temps passé à attendre MySQL, en mesurant la concurrence réelle."""
        self.en_cours += 1
        self.concurrence_max = max(self.concurrence_max, self.en_cours)
        try:
            await asyncio.sleep(self.delai)
        finally:
            self.en_cours -= 1

    def ids(self, etape):
        return [id_scenario for nom, id_scenario in self.appels if nom == etape]


def _base(eligibles=(12345, 12346), a_moitie=()):
    return FausseBase(
        {
            "AND trafic_pdi_calcule = 0": [{"id_scenario": i} for i in eligibles],
            "AND trafic_pdi_calcule = 1": [{"id_scenario": i} for i in a_moitie],
        }
    )


def _executer(monkeypatch, traitements, *, id_scenario=None, nb_workers=1, base=None, ecriture=None):
    monkeypatch.setattr(orchestrateur, "controle_eligibilite", traitements.eligibilite)
    monkeypatch.setattr(orchestrateur, "calcul_trafic_pdi", traitements.pdi)
    monkeypatch.setattr(orchestrateur, "calcul_trafic_agrebal", traitements.agrebal)
    lecture = base if base is not None else _base()
    ecriture = ecriture if ecriture is not None else FausseBase({})
    return asyncio.run(
        orchestrateur.executer_tout(
            id_scenario,
            nb_workers=nb_workers,
            db_lecture=lecture,
            db_ecriture=ecriture,
        )
    )


# ---------------------------------------------------------------------------
# Sélection des scénarios
# ---------------------------------------------------------------------------


def test_all_traite_tous_les_scenarios_trouves(monkeypatch):
    """CA-01 : sans identifiant, le batch cherche lui-même les scénarios éligibles."""
    traitements = Traitements()

    bilan = _executer(monkeypatch, traitements, base=_base((12345, 12346, 12347)))

    assert bilan.scenarios_trouves == [12345, 12346, 12347]
    assert traitements.ids("pdi") == [12345, 12346, 12347]
    assert len(bilan.succes) == 3


def test_all_avec_identifiant_ne_traite_que_celui_la(monkeypatch):
    """CA-02 : un identifiant explicite court-circuite la recherche."""
    traitements = Traitements()

    bilan = _executer(monkeypatch, traitements, id_scenario=99999)

    assert bilan.scenarios_trouves == [99999]
    assert traitements.ids("pdi") == [99999]
    # Le parallélisme n'a aucun objet sur un seul scénario.
    assert bilan.nb_workers == 1


def test_aucun_scenario_eligible_n_est_pas_une_erreur(monkeypatch):
    """Le ticket n'en parle pas : un batch qui ne trouve rien a simplement fini son travail."""
    bilan = _executer(monkeypatch, Traitements(), base=_base(()))

    assert bilan.scenarios_trouves == []
    assert bilan.reussi
    assert bilan.resultats == []


def test_les_scenarios_a_moitie_calcules_sont_signales(monkeypatch):
    """Hors critères du ticket : jamais repris, mais nommés pour ne pas rester invisibles."""
    bilan = _executer(monkeypatch, Traitements(), base=_base((12345,), a_moitie=(12347, 12351)))

    assert bilan.scenarios_a_moitie_calcules == [12347, 12351]
    assert "calcul-trafic-agrebal 12347" in bilan.texte()
    # Signalés, mais pas traités.
    assert bilan.scenarios_trouves == [12345]


# ---------------------------------------------------------------------------
# Parallélisme
# ---------------------------------------------------------------------------


def test_un_seul_worker_traite_en_sequence(monkeypatch):
    """CA-04 : NB_WORKER = 1 donne un comportement strictement séquentiel."""
    traitements = Traitements(delai=0.01)

    _executer(
        monkeypatch, traitements, nb_workers=1, base=_base((1, 2, 3, 4))
    )

    assert traitements.concurrence_max == 1


def test_plusieurs_workers_traitent_en_parallele(monkeypatch):
    """CA-05 : NB_WORKER > 1 autorise le traitement simultané de plusieurs scénarios."""
    traitements = Traitements(delai=0.01)

    _executer(monkeypatch, traitements, nb_workers=4, base=_base((1, 2, 3, 4, 5, 6, 7, 8)))

    assert traitements.concurrence_max > 1
    assert traitements.concurrence_max <= 4


def test_la_file_est_entierement_videe(monkeypatch):
    """Plus de scénarios que de workers : le traitement continue jusqu'à épuisement."""
    traitements = Traitements(delai=0.001)
    scenarios = tuple(range(1, 13))

    bilan = _executer(monkeypatch, traitements, nb_workers=4, base=_base(scenarios))

    assert sorted(traitements.ids("agrebal")) == list(scenarios)
    assert len(bilan.succes) == 12


# ---------------------------------------------------------------------------
# Enchaînement et isolation des échecs
# ---------------------------------------------------------------------------


def test_les_trois_etapes_sont_jouees_dans_l_ordre(monkeypatch):
    """CA-07 : éligibilité, puis trafics PDI, puis trafics Agrébal."""
    traitements = Traitements()

    _executer(monkeypatch, traitements, id_scenario=12345)

    assert [nom for nom, _ in traitements.appels] == ["eligibilite", "pdi", "agrebal"]


def test_scenario_non_eligible_abandonne_sans_calcul(monkeypatch):
    """Le ticket distingue « non éligible » de « en erreur » dans son bilan."""
    traitements = Traitements(non_eligibles=(12346,))

    bilan = _executer(monkeypatch, traitements, base=_base((12345, 12346)))

    assert traitements.ids("pdi") == [12345]
    assert [r.id_scenario for r in bilan.non_eligibles] == [12346]
    assert bilan.echecs == []
    # Un scénario non éligible n'empêche pas le batch de rendre un verdict positif.
    assert bilan.reussi


def test_echec_pdi_n_enchaine_pas_sur_agrebal(monkeypatch):
    traitements = Traitements(echecs_pdi=(12345,))

    bilan = _executer(monkeypatch, traitements, base=_base((12345, 12346)))

    assert traitements.ids("agrebal") == [12346]
    assert [r.id_scenario for r in bilan.echecs] == [12345]
    assert bilan.echecs[0].motif == "Aucune clé de répartition trouvée"


def test_un_echec_n_arrete_pas_les_autres_scenarios(monkeypatch):
    """CA-08, le point le plus important du mode ALL en production."""
    traitements = Traitements(echecs_pdi=(12347,))

    bilan = _executer(
        monkeypatch, traitements, nb_workers=2, base=_base((12345, 12346, 12347, 12348))
    )

    assert [r.id_scenario for r in bilan.succes] == [12345, 12346, 12348]
    assert [r.id_scenario for r in bilan.echecs] == [12347]


def test_une_exception_imprevue_est_isolee(monkeypatch):
    """Même une erreur non prévue par les traitements ne doit pas vider la file."""
    traitements = Traitements()

    async def pdi_qui_explose(id_scenario, *, db_lecture=None, db_ecriture=None):
        traitements.appels.append(("pdi", id_scenario))
        if id_scenario == 12345:
            raise RuntimeError("connexion perdue")
        return _rapport(SUCCES)

    monkeypatch.setattr(orchestrateur, "controle_eligibilite", traitements.eligibilite)
    monkeypatch.setattr(orchestrateur, "calcul_trafic_pdi", pdi_qui_explose)
    monkeypatch.setattr(orchestrateur, "calcul_trafic_agrebal", traitements.agrebal)

    bilan = asyncio.run(
        orchestrateur.executer_tout(
            nb_workers=1, db_lecture=_base((12345, 12346)), db_ecriture=FausseBase({})
        )
    )

    assert [r.id_scenario for r in bilan.echecs] == [12345]
    assert bilan.echecs[0].motif == "connexion perdue"
    assert [r.id_scenario for r in bilan.succes] == [12346]


# ---------------------------------------------------------------------------
# Verrou et écritures
# ---------------------------------------------------------------------------


def test_le_verrou_est_libere_si_l_etape_agrebal_echoue(monkeypatch):
    """Filet de sécurité : DSR-703 s'arrête sans libérer un verrou qu'il ne détient pas.

    Dans la chaîne, c'est l'étape PDI qui vient de le poser : le scénario resterait bloqué à
    CALCUL_TRAFIC_EN_COURS = 1, ce que DSR-704 interdit explicitement.
    """
    traitements = Traitements(echecs_agrebal=(12345,))
    ecriture = FausseBase({})

    bilan = _executer(monkeypatch, traitements, id_scenario=12345, ecriture=ecriture)

    assert ecriture.a_ecrit("SET calcul_trafic_en_cours = 0")
    assert [r.id_scenario for r in bilan.echecs] == [12345]


def test_aucun_verrou_libere_quand_tout_se_passe_bien(monkeypatch):
    """C'est l'étape Agrébal qui libère le scénario : l'orchestrateur ne repasse pas derrière."""
    ecriture = FausseBase({})

    _executer(monkeypatch, Traitements(), id_scenario=12345, ecriture=ecriture)

    assert ecriture.ecritures() == []


def test_l_orchestrateur_ne_journalise_jamais(monkeypatch):
    """CA-09 : aucune règle métier propre, donc aucune ligne de recalcul_log de son fait.

    DSR-702 et DSR-703 écrivent déjà la leur ; en ajouter une ici ferait de l'orchestrateur un
    troisième auteur du journal.
    """
    ecriture = FausseBase({})

    _executer(
        monkeypatch,
        Traitements(echecs_agrebal=(12345,)),
        base=_base((12345, 12346)),
        ecriture=ecriture,
    )

    assert not ecriture.a_ecrit("INSERT INTO trppu_recalcul_log")


def test_base_injoignable_rend_un_bilan_et_pas_une_exception(monkeypatch):
    """Erreur système : le batch s'arrête, mais rend un bilan lisible."""

    class BaseCassee:
        async def fetch_all(self, query, params=None):
            raise RuntimeError("MySQL injoignable")

    monkeypatch.setattr(orchestrateur, "controle_eligibilite", Traitements().eligibilite)

    bilan = asyncio.run(
        orchestrateur.executer_tout(db_lecture=BaseCassee(), db_ecriture=FausseBase({}))
    )

    assert bilan.erreur == "MySQL injoignable"
    assert not bilan.reussi
    assert "[ERREUR]" in bilan.texte()


# ---------------------------------------------------------------------------
# Bilan
# ---------------------------------------------------------------------------


def test_le_bilan_compte_et_ordonne(monkeypatch):
    traitements = Traitements(non_eligibles=(12346,), echecs_pdi=(12347,), delai=0.001)

    bilan = _executer(
        monkeypatch, traitements, nb_workers=4, base=_base((12345, 12346, 12347, 12348))
    )

    assert len(bilan.scenarios_trouves) == 4
    assert len(bilan.succes) == 2
    assert len(bilan.echecs) == 1
    assert len(bilan.non_eligibles) == 1
    # Malgré le parallélisme, le bilan reste trié : il est fait pour être lu.
    assert [r.id_scenario for r in bilan.resultats] == [12345, 12346, 12347, 12348]


def test_rendu_texte_du_bilan(monkeypatch):
    bilan = _executer(
        monkeypatch, Traitements(echecs_pdi=(12346,)), nb_workers=2, base=_base((12345, 12346))
    )

    texte = bilan.texte()

    assert "YB05 - Mode ALL" in texte
    assert "NB_WORKER = 2" in texte
    assert "[OK] 12345" in texte
    assert "[KO] 12346" in texte
    assert "Aucune clé de répartition trouvée" in texte
    assert "RESULTAT : ECHEC" in texte


def test_rendu_json_du_bilan(monkeypatch):
    bilan = _executer(monkeypatch, Traitements(), base=_base((12345,)))

    charge = bilan.to_dict()

    assert charge["compteurs"] == {
        "trouves": 1,
        "eligibles": 1,
        "succes": 1,
        "echecs": 0,
        "non_eligibles": 0,
    }
    assert charge["reussi"] is True
    assert charge["duree_totale"].count(":") == 2


@pytest.mark.parametrize(
    "secondes, attendu",
    [(0, "00:00:00"), (9.4, "00:00:09"), (192, "00:03:12"), (3661, "01:01:01")],
)
def test_format_des_durees(secondes, attendu):
    """Le ticket montre des durées en HH:MM:SS dans son bilan."""
    from app.traitements.rapport import duree_hms

    assert duree_hms(secondes) == attendu


def test_bilan_vide_ne_divise_pas_par_zero():
    """Durée moyenne d'un batch qui n'a rien traité : 0, pas une ZeroDivisionError."""
    assert Bilan(nb_workers=1).duree_moyenne_s == 0.0
