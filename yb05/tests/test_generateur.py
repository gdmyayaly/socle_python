"""Générateur de scénarios de test (`app/traitements/generateur.py`).

Le test central ne se contente pas de compter des INSERT : il reconstruit, à partir de ce que
le générateur a réellement écrit, les réponses que liraient DSR-701 et DSR-702, puis joue le
vrai contrôle d'éligibilité et le vrai calcul. Un scénario généré qui ne passerait pas les
douze règles, ou que le calcul refuserait, ferait échouer ce test.
"""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest

from app.config import CLES_PAR_PRODUIT
from app.traitements import generateur
from app.traitements.eligibilite import controle_eligibilite
from app.traitements.rapport import ECHEC, ELIGIBLE, SUCCES
from app.traitements.trafic_pdi import calcul_trafic_pdi
from tests.conftest import FausseBase


class BaseGeneration(FausseBase):
    """`FausseBase` dont `LAST_INSERT_ID()` rend des identifiants croissants."""

    def __init__(self, reponses=None, **options):
        super().__init__({"MAX(co) AS dernier": {"dernier": None}, **(reponses or {})}, **options)
        self.prochain_id = 100

    async def fetch_one(self, query, params=None):
        if "LAST_INSERT_ID" in query:
            self.prochain_id += 1
            return {"id": self.prochain_id}
        return await super().fetch_one(query, params)


def generer(nombre=1, base=None, **options):
    base = base or BaseGeneration()
    rapport = asyncio.run(
        generateur.generer_scenarios(nombre, db_lecture=base, db_ecriture=base, **options)
    )
    return rapport, base


def ecritures(base, fragment):
    """Paramètres de toutes les écritures contenant `fragment` (lots dépliés)."""
    lignes = []
    for genre, sql, params in base.journal:
        if genre == "execute_many" and fragment in sql:
            lignes += list(params)
        elif genre == "execute" and fragment in sql:
            lignes.append(params)
    return lignes


# --- Le scénario généré passe réellement l'éligibilité et le calcul ---------------------------


def _reponses_du_scenario_genere(base, nb_jours):
    """Ce que liraient DSR-701/702, reconstruit depuis les INSERT du générateur."""
    ids = iter(range(101, 1000))
    id_pic_version, id_version_cle, id_scenario = (next(ids) for _ in range(3))
    id_referentiel = generateur.REFERENTIEL_TEST
    (scenario,) = ecritures(base, "INSERT INTO trppu_scenario")
    roc, site, libelle, _, _, jours, pic = scenario
    assert (pic, jours) == (id_pic_version, nb_jours)

    coefficients = ecritures(base, "INSERT INTO trppu_pic_coefficients")
    cles = ecritures(base, "INSERT INTO trppu_cles_repartition_calcule")
    agrebals = ecritures(base, "INSERT INTO trppu_agrebal_pdi")
    tmh = ecritures(base, "INSERT INTO trppu_tmh")
    return {
        "FROM trppu_scenario WHERE id_scenario": {
            "id_scenario": id_scenario,
            "co_roc": roc,
            "co_regate": site,
            "lb_scenario": libelle,
            "statut": "VALIDE",
            "est_fige": 1,
            "nb_jours_semaine": jours,
            "id_pic_version": pic,
            "id_referentiel": 0,
            "id_version_cle": 0,
            "trafic_pdi_calcule": 0,
            "trafic_agrebal_calcule": 0,
            "calcul_trafic_en_cours": 0,
        },
        "COUNT(*) AS nb FROM trppu_pic_coefficients": {"nb": len(coefficients)},
        "FROM trppu_version_cle": {
            "id_version_cle": id_version_cle,
            "id_referentiel": id_referentiel,
        },
        "FROM trppu_agrebal_pdi": [
            {
                "agrebal_id": a[0],
                "agrebal_uuid": a[1],
                "agrebal_pdiQuantity": a[5],
                "agrebal_pdiList": a[6],
            }
            for a in agrebals
        ],
        "FROM trppu_recalcul_log": None,
        "FROM trppu_tmh": [{"co_produit": t[1], "tmh": t[5]} for t in tmh],
        "SELECT co_produit, jour_semaine, densite, coef": [
            {"co_produit": c[1], "jour_semaine": c[2], "coef": c[3], "densite": c[4]}
            for c in coefficients
        ],
        "FROM trppu_cles_repartition_calcule": [
            {
                "id_pdi": c[2],
                "cle_colis": c[4],
                "cle_oo": c[5],
                "cle_3s": c[6],
                "cle_potentielip": c[7],
            }
            for c in cles
        ],
    }


@pytest.mark.parametrize("nb_jours", [5, 6])
def test_le_scenario_genere_est_eligible_et_se_calcule(nb_jours):
    rapport, base = generer(nb_pdi=12, nb_agrebals=3, nb_jours=nb_jours)
    assert rapport.statut == SUCCES, (rapport.motifs, rapport.erreur)

    reponses = _reponses_du_scenario_genere(base, nb_jours)
    lecture = FausseBase(reponses, lecture_seule=True)

    eligibilite = asyncio.run(controle_eligibilite(103, db_lecture=lecture))
    assert eligibilite.statut == ELIGIBLE, eligibilite.motifs

    ecriture = FausseBase(reponses)
    calcul = asyncio.run(calcul_trafic_pdi(103, db_lecture=lecture, db_ecriture=ecriture))
    assert calcul.statut == SUCCES, (calcul.motifs, calcul.erreur)
    lignes = ecriture.parametres_de("INSERT INTO trppu_trafic_pdi")
    # Chaque PDI × chaque produit × chaque jour de la semaine du scénario.
    assert len(lignes) == 12 * len(CLES_PAR_PRODUIT) * nb_jours


# --- Forme des données ------------------------------------------------------------------------


def test_chaque_famille_de_cles_somme_exactement_a_un():
    _, base = generer(nb_pdi=17)
    cles = ecritures(base, "INSERT INTO trppu_cles_repartition_calcule")
    for colonne in range(4, 8):
        assert sum(Decimal(c[colonne]) for c in cles) == Decimal(1)


def test_chaque_pdi_est_dans_exactement_un_agrebal():
    _, base = generer(nb_pdi=10, nb_agrebals=3)
    portes = [
        pdi["pdi_id"]
        for a in ecritures(base, "INSERT INTO trppu_agrebal_pdi")
        for pdi in json.loads(a[6])
    ]
    pdis = [c[2] for c in ecritures(base, "INSERT INTO trppu_cles_repartition_calcule")]
    assert sorted(portes) == sorted(pdis)
    assert len(set(portes)) == 10


def test_coefficients_pour_tous_les_produits_jours_et_densites():
    _, base = generer()
    coefficients = ecritures(base, "INSERT INTO trppu_pic_coefficients")
    assert len(coefficients) == len(CLES_PAR_PRODUIT) * 6 * 3
    assert {c[1] for c in coefficients} == set(CLES_PAR_PRODUIT)


def test_identifiants_hors_des_plages_reelles_et_donnees_marquees():
    _, base = generer()
    (site,) = ecritures(base, "INSERT INTO trppu_site")
    assert site[0] == "ZT0001" and site[1].startswith("SITE TEST YB05")
    assert all(c[2] > 9 * 10**12 for c in ecritures(base, "INSERT INTO trppu_cles_repartition_calcule"))
    assert all(a[0] >= 900_000_000 for a in ecritures(base, "INSERT INTO trppu_agrebal_pdi"))
    (scenario,) = ecritures(base, "INSERT INTO trppu_scenario")
    assert scenario[2] == "TEST YB05 0001"


def test_trppu_referentiel_n_est_jamais_touchee():
    """Le référentiel est porté par la version de clés : ni INSERT ni DELETE sur la table."""
    _, base = generer(nombre=2)
    asyncio.run(
        generateur.supprimer_scenarios_test(
            db_lecture=BaseGeneration({"FROM trppu_site WHERE co_regate LIKE": [
                {"co_regate": "ZT0001"}]}),
            db_ecriture=base,
        )
    )
    assert not any("trppu_referentiel" in sql for _, sql, _ in base.journal)
    versions = ecritures(base, "INSERT INTO trppu_version_cle")
    assert {v[0] for v in versions} == {generateur.REFERENTIEL_TEST}


def test_numerotation_reprend_apres_le_dernier_site_de_test():
    _, base = generer(nombre=2, base=BaseGeneration({"MAX(co) AS dernier": {"dernier": "ZT0041"}}))
    assert [s[0] for s in ecritures(base, "INSERT INTO trppu_site")] == ["ZT0042", "ZT0043"]


def test_une_seule_transaction_pour_toute_la_generation():
    _, base = generer(nombre=3)
    assert base.transactions_commitees == 1


# --- Refus et échecs -----------------------------------------------------------------------------


def test_generation_refusee_en_production(monkeypatch):
    monkeypatch.setattr(generateur, "APP_ENV", "prod")
    rapport, base = generer()
    assert rapport.statut == ECHEC
    assert "production" in " ".join(rapport.motifs)
    assert base.ecritures() == []


@pytest.mark.parametrize(
    ("options", "fragment"),
    [
        ({"nombre": 0}, "Nombre de scénarios"),
        ({"nombre": 501}, "Nombre de scénarios"),
        ({"nb_pdi": 0}, "Nombre de PDI"),
        ({"nb_pdi": 2, "nb_agrebals": 3}, "Agrébals"),
        ({"nb_jours": 7}, "5 ou 6"),
    ],
)
def test_parametres_hors_bornes(options, fragment):
    nombre = options.pop("nombre", 1)
    rapport, base = generer(nombre, **options)
    assert rapport.statut == ECHEC
    assert fragment in " ".join(rapport.motifs)
    assert base.ecritures() == []


def test_plus_de_codes_libres():
    rapport, _ = generer(
        nombre=2, base=BaseGeneration({"MAX(co) AS dernier": {"dernier": "ZT9999"}})
    )
    assert rapport.statut == ECHEC
    assert "supprimer-scenarios-test" in " ".join(rapport.motifs)


def test_echec_en_cours_annule_tout():
    """Une écriture refusée en cours de route : la transaction est annulée, rien ne reste."""

    class BaseQuiRefuseLeScenario(BaseGeneration):
        def _enregistrer(self, genre, query, params):
            if "INSERT INTO trppu_scenario" in query:
                raise RuntimeError("1452 foreign key")
            return super()._enregistrer(genre, query, params)

    base = BaseQuiRefuseLeScenario()
    rapport, _ = generer(base=base)
    assert rapport.statut == ECHEC
    assert base.transactions_commitees == 0
    assert "rien n'a été écrit" in " ".join(rapport.motifs)


# --- Suppression ---------------------------------------------------------------------------------


def test_suppression_enfants_avant_parents_et_seulement_les_sites_de_test():
    base = FausseBase(
        {
            "FROM trppu_site WHERE co_regate LIKE": [{"co_regate": "ZT0001"}],
            "SELECT id_scenario FROM trppu_scenario": [{"id_scenario": 501}],
        }
    )

    rapport = asyncio.run(generateur.supprimer_scenarios_test(db_lecture=base, db_ecriture=base))

    assert rapport.statut == SUCCES
    ordre = base.ecritures()
    assert ordre.index(
        next(sql for sql in ordre if sql.startswith("DELETE FROM trppu_trafic_agrebal"))
    ) < ordre.index(next(sql for sql in ordre if sql.startswith("DELETE FROM trppu_scenario ")))
    assert ordre[-1].startswith("DELETE FROM trppu_site WHERE co_regate IN")
    assert base.parametres_de("DELETE FROM trppu_site") == ("ZT0001",)
    assert base.parametres_de("DELETE FROM trppu_pic_version") == ("TEST YB05", "ZT0001")


def test_suppression_sans_donnees_de_test():
    base = FausseBase({"FROM trppu_site WHERE co_regate LIKE": []})

    rapport = asyncio.run(generateur.supprimer_scenarios_test(db_lecture=base, db_ecriture=base))

    assert rapport.statut == SUCCES
    assert base.ecritures() == []


def test_commandes_de_la_cli():
    from app.main import build_parser, cmd_generer_scenarios, cmd_supprimer_scenarios_test

    args = build_parser().parse_args(["generer-scenarios", "5", "--pdi", "8", "--jours", "6"])
    assert (args.handler, args.nombre, args.pdi, args.agrebals, args.jours) == (
        cmd_generer_scenarios,
        5,
        8,
        3,
        6,
    )
    assert build_parser().parse_args(["supprimer-scenarios-test"]).handler is (
        cmd_supprimer_scenarios_test
    )
