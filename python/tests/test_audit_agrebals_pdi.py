"""DSR-737 — POST /trppu-api/audit/agrebals-pdi : Agrébals et PDI d'un scénario ou d'un site.

Les appels passent par `TestClient` : le contrat camelCase du ticket (`idScenario`,
`codeRegate`, `agrebalUuid`…) et les 422 de validation ne se vérifient qu'à travers la
sérialisation FastAPI. `db_read` est remplacé par une doublure, sans MySQL.
"""

import json
import logging

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routes.trppu_audit import helpers, routes

URL = "/trppu-api/audit/agrebals-pdi"


class FakeRead:
    """`db_read` scripté par fragment de requête ; journalise ce qui part en base."""

    def __init__(self, *, scenario=None, site=None, trafics=(), agrebals=(), boom=False):
        self.scenario = scenario
        self.site = site
        self.trafics = list(trafics)
        self.agrebals = list(agrebals)
        self.boom = boom
        self.executed: list[tuple[str, tuple]] = []

    async def fetch_one(self, sql, params=None):
        self.executed.append((sql, params))
        if "FROM trppu_scenario" in sql:
            return self.scenario
        if "FROM trppu_site" in sql:
            return self.site
        raise AssertionError(sql)

    async def fetch_all(self, sql, params=None):
        self.executed.append((sql, params))
        if self.boom:
            raise RuntimeError("MySQL indisponible")
        if "FROM trppu_trafic_pdi" in sql:
            return self.trafics
        if "FROM trppu_agrebal_pdi" in sql:
            return self.agrebals
        raise AssertionError(sql)


SITE = {"co_regate": "372920", "lb_regate": "SORIGNY PDC1"}


def _appeler(monkeypatch, db, corps):
    monkeypatch.setattr(routes, "db_read", db)
    return TestClient(app).post(URL, json=corps)


# --- Recherche par scénario (RG-003) ---------------------------------------------------


def test_ca01_scenario_rend_les_agrebals_et_pdi_du_calcul(monkeypatch):
    db = FakeRead(
        scenario={"id_scenario": 12345, "co_regate": "372920", "trafic_pdi_calcule": 1},
        site=SITE,
        trafics=[
            {"agrebal_uuid": "AGREBAL-001", "id_pdi": 100005554},
            {"agrebal_uuid": "AGREBAL-001", "id_pdi": 100011320},
            {"agrebal_uuid": "AGREBAL-002", "id_pdi": 100075421},
        ],
    )
    reponse = _appeler(monkeypatch, db, {"idScenario": 12345})

    assert reponse.status_code == 200
    assert reponse.json() == {
        "typeRecherche": "SCENARIO",
        "idScenario": 12345,
        "codeRegate": "372920",
        "libelleSite": "SORIGNY PDC1",
        "nbAgrebals": 2,
        "nbPdis": 3,
        "agrebals": [
            {"agrebalUuid": "AGREBAL-001", "pdis": [100005554, 100011320]},
            {"agrebalUuid": "AGREBAL-002", "pdis": [100075421]},
        ],
        "message": None,
    }


def test_ca03_scenario_lu_dans_la_photo_du_calcul_pas_dans_les_agrebals_actifs(monkeypatch):
    """Les données historisées viennent de `trppu_trafic_pdi`, jamais de l'état actuel."""
    db = FakeRead(
        scenario={"id_scenario": 7, "co_regate": "372920"},
        site=SITE,
        trafics=[{"agrebal_uuid": "AGREBAL-SUPPRIME", "id_pdi": 1}],
    )
    reponse = _appeler(monkeypatch, db, {"idScenario": 7})

    assert reponse.json()["agrebals"][0]["agrebalUuid"] == "AGREBAL-SUPPRIME"
    requetes = " ".join(sql for sql, _ in db.executed)
    assert "trppu_trafic_pdi" in requetes
    assert "trppu_agrebal_pdi" not in requetes


def test_scenario_non_calcule_rend_une_liste_vide_et_un_message(monkeypatch):
    db = FakeRead(scenario={"id_scenario": 8, "co_regate": "372920"}, site=SITE, trafics=[])
    corps = _appeler(monkeypatch, db, {"idScenario": 8}).json()

    assert corps["agrebals"] == [] and corps["nbPdis"] == 0
    assert corps["message"] == routes.MESSAGE_SCENARIO_NON_CALCULE


def test_ca04_scenario_inexistant(monkeypatch):
    reponse = _appeler(monkeypatch, FakeRead(scenario=None), {"idScenario": 999})

    assert reponse.status_code == 404
    assert reponse.json()["detail"] == "Scénario introuvable."


# --- Recherche par site (RG-002) -------------------------------------------------------


def test_ca02_site_rend_les_agrebals_actifs(monkeypatch):
    db = FakeRead(
        site=SITE,
        agrebals=[
            {
                "agrebal_uuid": "AGREBAL-001",
                "agrebal_pdiList": json.dumps([{"pdi_id": 100005554}, {"pdi_id": 10004389}]),
            },
            {"agrebal_uuid": "AGREBAL-002", "agrebal_pdiList": [{"pdi_id": 100075421}]},
        ],
    )
    corps = _appeler(monkeypatch, db, {"codeRegate": "372920"}).json()

    assert corps["typeRecherche"] == "SITE" and corps["idScenario"] is None
    assert corps["libelleSite"] == "SORIGNY PDC1"
    assert corps["agrebals"] == [
        {"agrebalUuid": "AGREBAL-001", "pdis": [100005554, 10004389]},
        {"agrebalUuid": "AGREBAL-002", "pdis": [100075421]},
    ]
    assert (corps["nbAgrebals"], corps["nbPdis"]) == (2, 3)


def test_site_filtre_les_agrebals_supprimes():
    """RG-002 : « actif » = non supprimé logiquement, équivalent du DATE_FIN_VALIDITE."""
    assert "agrebal_deleteddAt IS NULL" in helpers.SELECT_AGREBALS_PDI_SITE_SQL


def test_ca05_site_inexistant(monkeypatch):
    reponse = _appeler(monkeypatch, FakeRead(site=None), {"codeRegate": "999999"})

    assert reponse.status_code == 404
    assert reponse.json()["detail"] == "Site introuvable."


def test_site_sans_agrebal_actif(monkeypatch):
    corps = _appeler(monkeypatch, FakeRead(site=SITE, agrebals=[]), {"codeRegate": "372920"}).json()

    assert corps["agrebals"] == []
    assert corps["message"] == routes.MESSAGE_SITE_SANS_AGREBAL


@pytest.mark.parametrize(
    ("brut", "attendu"),
    [
        ('[{"pdi_id": 1}, {"pdi_id": "2"}]', [1, 2]),
        ([{"pdi_id": 3}, {"autre": 4}], [3]),
        ("pas du json", []),
        (None, []),
        ({"pdi_id": 1}, []),
    ],
)
def test_extraction_des_pdi(brut, attendu):
    assert helpers.extraire_pdi_ids(brut) == attendu


# --- Validation de l'entrée (RG-001) ---------------------------------------------------


@pytest.mark.parametrize(
    "corps",
    [
        {},
        {"idScenario": 1, "codeRegate": "372920"},
        {"codeRegate": "12"},
        {"idScenario": 0},
        {"idScenario": 1, "inconnu": True},
    ],
)
def test_entree_invalide_422(monkeypatch, corps):
    db = FakeRead()
    reponse = _appeler(monkeypatch, db, corps)

    assert reponse.status_code == 422
    assert db.executed == []


# --- Consultation seule, erreurs, journalisation ---------------------------------------


def test_rg006_aucune_ecriture(monkeypatch):
    """La doublure n'a ni `execute` ni `transaction` : toute écriture lèverait."""
    db = FakeRead(scenario={"id_scenario": 1, "co_regate": "372920"}, site=SITE)
    assert _appeler(monkeypatch, db, {"idScenario": 1}).status_code == 200
    assert all(sql.lstrip().upper().startswith("SELECT") for sql, _ in db.executed)


def test_erreur_technique_500(monkeypatch):
    db = FakeRead(site=SITE, boom=True)
    reponse = _appeler(monkeypatch, db, {"codeRegate": "372920"})

    assert reponse.status_code == 500


def test_journalisation_kibana(monkeypatch, caplog):
    """Type de recherche, paramètre, nombre d'Agrébals et de PDI, durée."""
    db = FakeRead(
        scenario={"id_scenario": 12345, "co_regate": "372920"},
        site=SITE,
        trafics=[{"agrebal_uuid": "A", "id_pdi": 1}, {"agrebal_uuid": "A", "id_pdi": 2}],
    )
    with caplog.at_level(logging.INFO, logger="app.routes.trppu_audit.routes"):
        _appeler(monkeypatch, db, {"idScenario": 12345})

    (fin,) = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Fin audit")]
    for attendu in ("type_recherche=SCENARIO", "id_scenario=12345", "co_regate=372920",
                    "nb_agrebals=1", "nb_pdis=2", "duration_ms="):
        assert attendu in fin


def test_rejet_404_trace(monkeypatch, caplog):
    with caplog.at_level(logging.WARNING, logger="app.routes.trppu_audit.routes"):
        _appeler(monkeypatch, FakeRead(site=None), {"codeRegate": "999999"})

    (rejet,) = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Rejet")]
    assert "http=404" in rejet and "Site introuvable" in rejet
