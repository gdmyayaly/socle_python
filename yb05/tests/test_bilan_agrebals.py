"""Agrébals à calculer / calculés : rapports, logs, bilan du mode ALL et rapport final journalisé.

Un Agrébal peut ne produire aucun trafic sans que rien n'échoue (PDI sans clé, `agrebal_pdiList`
vide, PDI captés par un autre Agrébal). Ces tests verrouillent qu'il est désormais compté,
nommé et motivé — dans le rapport (`[!!]`, non bloquant), dans les logs, et dans le bilan.
"""

from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal

import pytest

from app import main
from app.json_formatter import JsonFormatter
from app.traitements import orchestrateur
from app.traitements.rapport import ECHEC, SUCCES, Bilan, Rapport
from app.traitements.trafic_agrebal import calcul_trafic_agrebal
from app.traitements.trafic_pdi import calcul_trafic_pdi
from tests.conftest import FausseBase, reponses_calcul
from tests.test_traitements_trafic_agrebal import reponses_agrebal


def _agrebal(agrebal_id: int, pdi: list[int] | None) -> dict:
    liste = None if pdi is None else json.dumps([{"pdi_id": p} for p in pdi])
    return {
        "agrebal_id": agrebal_id,
        "agrebal_uuid": f"uuid-{agrebal_id}",
        "agrebal_pdiQuantity": len(pdi or []),
        "agrebal_pdiList": liste,
    }


def _calcul_pdi(agrebals: list[dict]) -> tuple[Rapport, FausseBase]:
    reponses = reponses_calcul(**{"FROM trppu_agrebal_pdi": agrebals})
    lecture = FausseBase(reponses, lecture_seule=True)
    ecriture = FausseBase(reponses)
    rapport = asyncio.run(calcul_trafic_pdi(12345, db_lecture=lecture, db_ecriture=ecriture))
    return rapport, ecriture


# ---------------------------------------------------------------------------
# Calcul PDI : c'est là qu'un Agrébal se perd, donc là que se lit la cause
# ---------------------------------------------------------------------------


def test_pdi_nominal_tous_les_agrebals_calcules():
    rapport, _ = _calcul_pdi([_agrebal(2404, [1001, 1002])])

    assert rapport.statut == SUCCES
    assert rapport.etats["NB_AGREBAL_A_CALCULER"] == 1
    assert rapport.etats["NB_AGREBAL_CALCULES"] == 1
    assert rapport.alertes == []
    assert "[OK] Agrébals : 1 calculé(s) sur 1 à calculer" in rapport.texte()


def test_pdi_agrebal_sans_pdi_est_nomme():
    rapport, _ = _calcul_pdi([_agrebal(2404, [1001, 1002]), _agrebal(2405, [])])

    assert rapport.statut == SUCCES  # non bloquant
    assert rapport.etats["NB_AGREBAL_A_CALCULER"] == 2
    assert rapport.etats["NB_AGREBAL_CALCULES"] == 1
    [alerte] = rapport.alertes
    assert "1 Agrébal(s) non calculé(s)" in alerte
    assert "2405 : aucun PDI dans agrebal_pdiList" in alerte


def test_pdi_agrebal_dont_aucun_pdi_n_a_de_cle():
    # 9999 n'existe pas dans les clés (seuls 1001 et 1002 en ont).
    rapport, _ = _calcul_pdi([_agrebal(2404, [1001, 1002]), _agrebal(2405, [9999])])

    [alerte] = rapport.alertes
    assert "2405 : aucun de ses 1 PDI n'a de clé de répartition" in alerte
    assert "[!!]" in rapport.texte()


def test_pdi_partages_le_premier_agrebal_perd_ses_pdi():
    """Un PDI cité par deux Agrébals va au dernier lu : l'autre disparaît sans erreur."""
    rapport, ecriture = _calcul_pdi(
        [_agrebal(2404, [1001, 1002]), _agrebal(2405, [1001, 1002])]
    )

    agrebals_ecrits = {ligne[2] for ligne in ecriture.parametres_de("INSERT INTO trppu_trafic_pdi")}
    assert agrebals_ecrits == {2405}  # comportement existant inchangé
    assert rapport.etats["NB_AGREBAL_CALCULES"] == 1
    alertes = " | ".join(rapport.alertes)
    assert "2404 : ses 2 PDI sont tous rattachés à un autre Agrébal" in alertes
    assert "2 PDI cité(s) par plusieurs Agrébals" in alertes


def test_pdi_agrebals_non_calcules_journalises(caplog):
    with caplog.at_level(logging.INFO, logger="app.traitements.trafic_pdi"):
        _calcul_pdi([_agrebal(2404, [1001, 1002]), _agrebal(2405, [])])

    [avertissement] = [
        r for r in caplog.records if r.getMessage().startswith("Agrébals non calculés")
    ]
    assert avertissement.levelno == logging.WARNING
    assert "nb=1" in avertissement.getMessage()
    assert "2405" in avertissement.getMessage()
    [fin] = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Fin calcul trafics PDI")]
    assert "agrebals_a_calculer=2" in fin
    assert "agrebals_calcules=1" in fin


def test_pdi_sans_agrebal_manquant_aucun_avertissement(caplog):
    with caplog.at_level(logging.INFO, logger="app.traitements.trafic_pdi"):
        _calcul_pdi([_agrebal(2404, [1001, 1002])])

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


# ---------------------------------------------------------------------------
# Calcul Agrébal : décompte attendu / présent dans trppu_trafic_agrebal
# ---------------------------------------------------------------------------


def _calcul_agrebal(**surcharges) -> Rapport:
    reponses = reponses_agrebal(**surcharges)
    lecture = FausseBase(reponses, lecture_seule=True)
    ecriture = FausseBase(reponses)
    return asyncio.run(calcul_trafic_agrebal(12345, db_lecture=lecture, db_ecriture=ecriture))


def test_agrebal_nominal_compte_a_calculer_et_calcules():
    rapport = _calcul_agrebal()

    assert rapport.statut == SUCCES
    assert rapport.etats["NB_AGREBAL_A_CALCULER"] == 1
    assert rapport.etats["NB_AGREBAL_CALCULES"] == 1
    assert rapport.alertes == []


def test_agrebal_manquant_signale_sans_changer_le_verdict(caplog):
    with caplog.at_level(logging.INFO, logger="app.traitements.trafic_agrebal"):
        rapport = _calcul_agrebal(
            **{"FROM trppu_agrebal_pdi": [_agrebal(2404, [1001]), _agrebal(2405, [1002])]}
        )

    assert rapport.statut == SUCCES
    assert rapport.etats["NB_AGREBAL_A_CALCULER"] == 2
    assert rapport.etats["NB_AGREBAL_CALCULES"] == 1
    [alerte] = rapport.alertes
    assert "1 Agrébal(s) non calculé(s) ([2405])" in alerte
    [avertissement] = [
        r.getMessage() for r in caplog.records if r.getMessage().startswith("Agrébals non calculés")
    ]
    assert "agrebals=[2405]" in avertissement
    [fin] = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Fin calcul trafics Agrébal")]
    assert "agrebals_a_calculer=2" in fin
    assert "agrebals_calcules=1" in fin
    assert "agrebals_non_calcules=1" in fin


def test_agrebal_calcule_hors_site_signale():
    rapport = _calcul_agrebal(
        **{
            "FROM trppu_trafic_agrebal": [
                {"id_agrebal": 2404, "nb_lignes": 15, "total": 1000},
                {"id_agrebal": 7777, "nb_lignes": 15, "total": 750},
            ]
        }
    )

    assert rapport.statut == SUCCES  # totaux toujours cohérents (1750)
    assert rapport.etats["NB_AGREBAL_CALCULES"] == 2
    assert any("plus actif(s) sur le site ([7777])" in a for a in rapport.alertes)


def test_agrebal_totaux_sommes_sur_tous_les_agrebals():
    rapport = _calcul_agrebal(
        **{
            "FROM trppu_agrebal_pdi": [_agrebal(2404, [1001]), _agrebal(2405, [1002])],
            "FROM trppu_trafic_agrebal": [
                {"id_agrebal": 2404, "nb_lignes": 15, "total": 1000},
                {"id_agrebal": 2405, "nb_lignes": 15, "total": 750},
            ],
        }
    )

    assert "[OK] Totaux cohérents (1750)" in rapport.texte()
    assert rapport.alertes == []


# ---------------------------------------------------------------------------
# Rapport : alerte non bloquante
# ---------------------------------------------------------------------------


def test_alerte_rendue_sans_effet_sur_le_verdict():
    rapport = Rapport(titre="t", id_scenario=1, statut=SUCCES)
    rapport.ok("contrôle")
    rapport.alerte("à regarder")

    assert rapport.reussi
    assert rapport.motifs == []
    assert "[!!] à regarder" in rapport.texte()
    charge = rapport.to_dict()
    assert charge["alertes"] == ["à regarder"]
    assert charge["controles"][1]["alerte"] is True


# ---------------------------------------------------------------------------
# Bilan du mode ALL
# ---------------------------------------------------------------------------


def _bilan() -> Bilan:
    bilan = Bilan(nb_workers=1, scenarios_trouves=[1, 2, 3])
    bilan.ajouter(1, SUCCES, agrebals_a_calculer=3, agrebals_calcules=3)
    bilan.ajouter(2, SUCCES, agrebals_a_calculer=4, agrebals_calcules=2)
    bilan.ajouter(3, ECHEC, "Aucune clé")  # échec PDI : pas de calcul Agrébal
    return bilan


def test_bilan_texte_agrebals_par_scenario_et_totaux():
    texte = _bilan().texte()

    assert "[OK] 1  Agrébals : 3/3" in texte
    assert "[OK] 2  Agrébals : 2/4 (2 non calculé(s))" in texte
    assert "[KO] 3\n" in texte  # aucun décompte quand l'étape Agrébal n'a pas tourné
    assert "Agrébals à calculer  : 7" in texte
    assert "Agrébals calculés    : 5" in texte
    assert "Agrébals manquants   : 2 (1 scénario(s))" in texte


def test_bilan_json_agrebals():
    charge = _bilan().to_dict()

    assert charge["agrebals"] == {
        "a_calculer": 7,
        "calcules": 5,
        "manquants": 2,
        "scenarios_concernes": [2],
    }
    assert charge["resultats"][1]["agrebals_calcules"] == 2
    assert charge["resultats"][2]["agrebals_a_calculer"] is None


def test_orchestrateur_reporte_les_compteurs_du_rapport_agrebal(monkeypatch):
    async def eligibilite(id_scenario, *, db_lecture=None):
        return Rapport(titre="e", id_scenario=id_scenario, statut="ELIGIBLE")

    async def pdi(id_scenario, *, db_lecture=None, db_ecriture=None):
        return Rapport(titre="p", id_scenario=id_scenario, statut=SUCCES)

    async def agrebal(id_scenario, *, db_lecture=None, db_ecriture=None):
        rapport = Rapport(titre="a", id_scenario=id_scenario, statut=SUCCES)
        rapport.etats.update({"NB_AGREBAL_A_CALCULER": 5, "NB_AGREBAL_CALCULES": 4})
        return rapport

    monkeypatch.setattr(orchestrateur, "controle_eligibilite", eligibilite)
    monkeypatch.setattr(orchestrateur, "calcul_trafic_pdi", pdi)
    monkeypatch.setattr(orchestrateur, "calcul_trafic_agrebal", agrebal)
    base = FausseBase({"AND trafic_pdi_calcule = 0": [{"id_scenario": 12345}],
                       "AND trafic_pdi_calcule = 1": []})

    bilan = asyncio.run(
        orchestrateur.executer_tout(nb_workers=1, db_lecture=base, db_ecriture=FausseBase({}))
    )

    [resultat] = bilan.resultats
    assert (resultat.agrebals_a_calculer, resultat.agrebals_calcules) == (5, 4)
    assert bilan.to_dict()["agrebals"]["manquants"] == 1


# ---------------------------------------------------------------------------
# Rapport final dans les logs (champ JSON racine `rapport`)
# ---------------------------------------------------------------------------


def _formater(**extra) -> dict:
    record = logging.LogRecord("yb05", logging.INFO, "t.py", 1, "message", (), None)
    for cle, valeur in extra.items():
        setattr(record, cle, valeur)
    return json.loads(JsonFormatter().format(record))


def test_formatter_sans_rapport_n_ajoute_pas_le_champ():
    assert "rapport" not in _formater()


def test_formatter_rapport_objet_json_structure():
    ligne = _formater(rapport={"statut": SUCCES, "etats": {"NB_AGREBAL_CALCULES": 3}})

    assert ligne["rapport"]["etats"]["NB_AGREBAL_CALCULES"] == 3


def test_formatter_rapport_non_serialisable_ne_leve_pas():
    ligne = _formater(rapport={"total": Decimal("12.5")})

    assert ligne["rapport"]["total"] == "12.5"


class _CaptureJson(logging.Handler):
    """Formate au moment de l'émission : le contexte (id_scenario, site) est celui du log."""

    def __init__(self):
        super().__init__(logging.DEBUG)
        self.setFormatter(JsonFormatter())
        self.lignes: list[dict] = []

    def emit(self, record):
        self.lignes.append(json.loads(self.format(record)))


@pytest.fixture
def capture_json():
    handler = _CaptureJson()
    journal = logging.getLogger("yb05")
    niveau = journal.level
    journal.addHandler(handler)
    journal.setLevel(logging.DEBUG)
    yield handler
    journal.removeHandler(handler)
    journal.setLevel(niveau)


class _Args:
    def __init__(self, commande="calcul-trafic-pdi", id_scenario=12345):
        self.commande = commande
        self.id_scenario = id_scenario
        self.json = True


def _lignes_rapport(capture):
    return [l for l in capture.lignes if l["app_message"].startswith("Rapport final")]


def test_commande_mono_scenario_journalise_le_rapport(capture_json, capsys):
    async def traitement(id_scenario):
        rapport = Rapport(titre="Calcul", id_scenario=id_scenario, statut=SUCCES)
        rapport.alerte("1 Agrébal(s) non calculé(s)")
        rapport.etats["NB_AGREBAL_CALCULES"] = 2
        return rapport

    code = asyncio.run(main._executer_traitement(traitement, _Args()))

    assert code == main.EXIT_OK
    [ligne] = _lignes_rapport(capture_json)
    assert ligne["severity_label"] == "INFO"
    # Émis avant le reset du contexte : la ligne porte encore le scénario.
    assert ligne["id_scenario"] == 12345
    assert ligne["rapport"]["etats"]["NB_AGREBAL_CALCULES"] == 2
    assert "alertes=1" in ligne["app_message"]
    # stdout inchangé : toujours le rapport JSON de la commande.
    assert json.loads(capsys.readouterr().out)["statut"] == SUCCES


def test_rapport_en_echec_journalise_en_warning(capture_json, capsys):
    async def traitement(id_scenario):
        raise RuntimeError("base KO")

    code = asyncio.run(main._executer_traitement(traitement, _Args()))

    assert code == main.EXIT_KO
    [ligne] = _lignes_rapport(capture_json)
    assert ligne["severity_label"] == "WARNING"
    assert ligne["rapport"]["statut"] == ECHEC
    assert "base KO" in ligne["rapport"]["erreur"]


def test_mode_all_journalise_le_bilan(capture_json, capsys, monkeypatch):
    async def executer_tout(id_scenario):
        return _bilan()

    monkeypatch.setattr(main, "executer_tout", executer_tout)

    asyncio.run(main.cmd_all(_Args(commande="all", id_scenario=None)))

    [ligne] = _lignes_rapport(capture_json)
    assert ligne["severity_label"] == "WARNING"  # un scénario en échec dans le bilan
    assert ligne["rapport"]["agrebals"]["manquants"] == 2
    assert "statut=ECHEC" in ligne["app_message"]
