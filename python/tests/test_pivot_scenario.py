"""Date pivot d'un scénario : écrite seulement quand un pivot est transmis.

`GET /trafics/get_trafics_pivot?id_scenario=…` écrit
`trppu_scenario.dt_pivot` et les bornes `periode_realise_*` / `periode_prev_*` :
réalisé = [debut, pivot - 1], prév = [pivot, fin] — les plages mêmes qui ont servi à
interroger Databricks. Même règle au PUT si `date_pivot` est transmis et au PATCH
/periodes (où il est obligatoire) ; la création n'y touche pas.

Databricks et MySQL sont stubés ; le code async est exécuté via ``asyncio.run``.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import date, datetime
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.routes.trppu_scenario import helpers as scenario_helpers
from app.routes.trppu_scenario import routes as scenario_routes
from app.routes.trppu_scenario.schemas import (
    PeriodeUpdate,
    ScenarioCreate,
    ScenarioMajRequest,
)
from app.routes.trppu_trafics import helpers as trafics_helpers
from app.routes.trppu_trafics import routes as trafics_routes
from app.routes.trppu_trafics import scenario_pivot

COLONNES_PIVOT = (
    "dt_pivot",
    "periode_realise_debut",
    "periode_realise_fin",
    "periode_prev_debut",
    "periode_prev_fin",
)


class FakeTx:
    """Transaction factice : enregistre (sql, params)."""

    def __init__(self, erreur: Exception | None = None):
        self.calls: list[tuple[str, tuple | None]] = []
        self._erreur = erreur

    async def execute(self, sql, params=None):
        if self._erreur is not None:
            raise self._erreur
        self.calls.append((sql, params))
        return 1

    async def fetch_one(self, sql, params=None):
        self.calls.append((sql, params))
        return {"id": 42}


class FakeDb:
    """Remplace `db_write` / `db_read` : `transaction()` délivre toujours le même FakeTx."""

    def __init__(self, tx: FakeTx, fetch_one_result=None):
        self.tx = tx
        self._fetch_one_result = fetch_one_result

    @asynccontextmanager
    async def transaction(self):
        yield self.tx

    async def fetch_one(self, sql, params=None):
        return self._fetch_one_result


def _scenario(**surcharges) -> dict:
    base = {
        "id_scenario": 42,
        "co_regate": "400300",
        "statut": "EN COURS",
        "periode_debut": date(2025, 1, 1),
        "periode_fin": date(2025, 12, 31),
    }
    return {**base, **surcharges}


async def _audit_muet(**_kwargs):
    return None


# --- Helper d'écriture ------------------------------------------------------------------


@pytest.mark.parametrize(
    "debut,fin,pivot,attendu",
    [
        # Pivot dans la période : réalisé jusqu'à la veille du pivot, prév à partir du pivot.
        (
            date(2025, 1, 1), date(2025, 12, 31), date(2025, 7, 1),
            (date(2025, 1, 1), date(2025, 6, 30), date(2025, 7, 1), date(2025, 12, 31)),
        ),
        # Pivot après la fin : tout est réalisé.
        (
            date(2025, 1, 1), date(2025, 3, 31), date(2025, 10, 1),
            (date(2025, 1, 1), date(2025, 3, 31), None, None),
        ),
        # Pivot avant (ou au) début : tout est prévisionnel.
        (
            date(2027, 1, 1), date(2027, 6, 30), date(2027, 1, 1),
            (None, None, date(2027, 1, 1), date(2027, 6, 30)),
        ),
    ],
)
def test_enregistrer_pivot_bornes_issues_du_decoupage(debut, fin, pivot, attendu):
    reel, prev = trafics_helpers.split_by_pivot(debut, fin, pivot)
    tx = FakeTx()

    asyncio.run(scenario_helpers.enregistrer_pivot(tx, 42, pivot, reel, prev))

    [(sql, params)] = tx.calls
    assert sql == scenario_helpers.UPDATE_PIVOT_SQL
    assert params == (pivot, *attendu, 42)


# --- GET /trafics/get_trafics_pivot?id_scenario=… --------------------------------------

PARAMS_PIVOT = {
    "co_regate": "400300",
    "date_debut": "20250101",
    "date_fin": "20250131",
    "date_pivot": "20250115",
    "is_day": True,
}

LIGNES_OK = [{trafics_helpers.OBJ_ALIAS: "OO", trafics_helpers.LIBELLE_ALIAS: "OBJETS ORDINAIRES",
              trafics_helpers.SOMME_ALIAS: 10}]


@pytest.fixture
def env_trafics(monkeypatch):
    """Databricks, jours fermés, lecture scénario, transaction et audit stubés."""
    etat = {
        "scenario": _scenario(),
        "databricks": [],
        "tx": FakeTx(),
        "audit": [],
    }

    def _fetch_all(sql, params=None):
        etat["databricks"].append(sql)
        resultat = etat.get("databricks_erreur")
        if resultat is not None:
            raise resultat
        return LIGNES_OK

    async def _fetch_scenario(id_scenario):
        if etat["scenario"] is None:
            raise HTTPException(status_code=404, detail=f"Scénario {id_scenario} introuvable.")
        return etat["scenario"]

    async def _faux_compute(debut, fin):
        raise RuntimeError("jours fermés hors périmètre")

    async def _audit(**kwargs):
        etat["audit"].append(kwargs)

    monkeypatch.setattr(trafics_helpers.databricks, "fetch_all", _fetch_all)
    monkeypatch.setattr(trafics_routes, "compute_nb_jours", _faux_compute)
    monkeypatch.setattr(scenario_pivot, "fetch_scenario_or_404", _fetch_scenario)
    monkeypatch.setattr(scenario_pivot, "db_write", FakeDb(etat["tx"]))
    monkeypatch.setattr(scenario_pivot, "enregistrer_appel", _audit)
    return etat


def _appeler(**surcharges):
    return asyncio.run(trafics_routes.get_trafics_pivot(**{**PARAMS_PIVOT, **surcharges}))


def _statut_et_detail(**surcharges):
    with pytest.raises(HTTPException) as exc:
        _appeler(**surcharges)
    return exc.value.status_code, exc.value.detail


def test_sans_id_scenario_aucune_ecriture(env_trafics):
    reponse = _appeler()

    assert reponse["id_scenario"] is None
    assert reponse["pivot_scenario"] is None
    assert env_trafics["tx"].calls == []
    assert env_trafics["audit"] == []


def test_avec_id_scenario_ecrit_pivot_et_bornes(env_trafics):
    reponse = _appeler(id_scenario=42)

    [(sql, params)] = env_trafics["tx"].calls
    assert sql == scenario_helpers.UPDATE_PIVOT_SQL
    assert params == (
        date(2025, 1, 15),
        date(2025, 1, 1), date(2025, 1, 14),
        date(2025, 1, 15), date(2025, 1, 31),
        42,
    )
    assert reponse["id_scenario"] == 42
    assert reponse["pivot_scenario"] == {
        "dt_pivot": date(2025, 1, 15),
        "periode_realise_debut": date(2025, 1, 1),
        "periode_realise_fin": date(2025, 1, 14),
        "periode_prev_debut": date(2025, 1, 15),
        "periode_prev_fin": date(2025, 1, 31),
    }
    # Les trafics sont toujours restitués.
    assert reponse["count"] == 1


def test_avec_id_scenario_trace_l_audit(env_trafics):
    _appeler(id_scenario=42)

    [audit] = env_trafics["audit"]
    assert audit["id_scenario"] == 42
    assert audit["regate"] == "400300"
    assert audit["params"]["cible"] == "pivot"
    assert audit["params"]["dt_pivot"] == "2025-01-15"


def test_periode_entierement_passee_prev_null(env_trafics):
    reponse = _appeler(id_scenario=42, date_pivot="20250301")

    assert reponse["pivot_scenario"]["periode_realise_fin"] == date(2025, 1, 31)
    assert reponse["pivot_scenario"]["periode_prev_debut"] is None
    assert reponse["pivot_scenario"]["periode_prev_fin"] is None


def test_scenario_introuvable_404_sans_databricks(env_trafics):
    env_trafics["scenario"] = None

    statut, _ = _statut_et_detail(id_scenario=999)

    assert statut == 404
    assert env_trafics["databricks"] == []
    assert env_trafics["tx"].calls == []


@pytest.mark.parametrize("statut_scenario", ["VALIDE", "EN PRODUCTION", "ARCHIVE"])
def test_scenario_non_editable_409_sans_databricks(env_trafics, statut_scenario):
    env_trafics["scenario"] = _scenario(statut=statut_scenario)

    statut, detail = _statut_et_detail(id_scenario=42)

    assert statut == 409
    assert detail["code"] == 409
    assert statut_scenario in detail["message"]
    assert env_trafics["databricks"] == []
    assert env_trafics["tx"].calls == []


def test_scenario_simulation_editable(env_trafics):
    env_trafics["scenario"] = _scenario(statut="SIMULATION")

    _appeler(id_scenario=42)

    assert len(env_trafics["tx"].calls) == 1


def test_co_regate_different_400_sans_databricks(env_trafics):
    env_trafics["scenario"] = _scenario(co_regate="570800")

    statut, detail = _statut_et_detail(id_scenario=42)

    assert statut == 400
    assert "570800" in detail["message"]
    assert env_trafics["databricks"] == []
    assert env_trafics["tx"].calls == []


def test_parametres_invalides_400_avant_lecture_scenario(env_trafics):
    env_trafics["scenario"] = None  # un 404 trahirait une lecture du scénario

    statut, _ = _statut_et_detail(id_scenario=42, date_pivot=None)

    assert statut == 400


def test_echec_databricks_n_ecrit_pas_le_pivot(env_trafics):
    env_trafics["databricks_erreur"] = RuntimeError("warehouse KO")

    statut, _ = _statut_et_detail(id_scenario=42)

    assert statut == 500
    assert env_trafics["tx"].calls == []
    assert env_trafics["audit"] == []


def test_echec_ecriture_pivot_500(env_trafics, monkeypatch):
    monkeypatch.setattr(
        scenario_pivot, "db_write", FakeDb(FakeTx(erreur=RuntimeError("MySQL KO")))
    )

    statut, detail = _statut_et_detail(id_scenario=42)

    assert statut == 500
    assert "date pivot" in detail["message"]
    assert env_trafics["audit"] == []


# --- Création / MAJ / PATCH /periodes ---------------------------------------------------


def _sans_colonne_pivot(sql: str) -> bool:
    return not any(col in sql for col in COLONNES_PIVOT)


def _maj_principale(calls) -> tuple[str, tuple]:
    [appel] = [
        c for c in calls
        if c[0].startswith("UPDATE trppu_scenario SET") and c[0] != scenario_helpers.UPDATE_PIVOT_SQL
    ]
    return appel


def _maj_pivot(calls) -> list[tuple]:
    return [params for sql, params in calls if sql == scenario_helpers.UPDATE_PIVOT_SQL]


@pytest.fixture
def env_scenario(monkeypatch):
    tx = FakeTx()
    nbj = SimpleNamespace(nb_jours_ouvres=250, nb_jours_ouvrables=300)

    async def _compute(debut, fin):
        return nbj

    async def _fetch_scenario(id_scenario):
        return _scenario(id_scenario=id_scenario, est_fige=False)

    async def _ensure_site(tx, **kwargs):
        return False

    async def _increment(tx, id_scenario):
        return 2

    async def _last_id(tx):
        return 42

    monkeypatch.setattr(scenario_routes, "db_write", FakeDb(tx))
    monkeypatch.setattr(scenario_routes, "db_read", FakeDb(tx, fetch_one_result={"s": 0}))
    monkeypatch.setattr(scenario_routes, "compute_nb_jours", _compute)
    monkeypatch.setattr(scenario_routes, "fetch_scenario_or_404", _fetch_scenario)
    monkeypatch.setattr(scenario_routes, "ensure_site_exists", _ensure_site)
    monkeypatch.setattr(scenario_routes, "increment_version", _increment)
    monkeypatch.setattr(scenario_routes, "last_insert_id", _last_id)
    monkeypatch.setattr(scenario_routes, "enregistrer_appel", _audit_muet)
    monkeypatch.setattr(scenario_routes, "encrypt_id_rh", lambda v: f"enc:{v}")
    return tx


def test_creation_n_ecrit_ni_pivot_ni_bornes(env_scenario):
    payload = ScenarioCreate(
        co_regate="400300", lb_scenario="S1", co_roc="400000", lb_regate="SITE",
        type_site="PPDC", id_pic_version=1, periode_debut=date(2025, 1, 1),
        periode_fin=date(2025, 12, 31), dt_mise_en_oeuvre=date(2025, 7, 1), id_rh="RH1",
    )

    asyncio.run(scenario_routes.create_scenario(payload))

    [(sql, params)] = [c for c in env_scenario.calls if c[0].startswith("INSERT INTO trppu_scenario")]
    assert _sans_colonne_pivot(sql)
    # Autant de %s que de paramètres : aucune valeur décalée après le retrait des colonnes.
    assert sql.count("%s") == len(params)
    assert params[3:6] == (date(2025, 7, 1), date(2025, 1, 1), date(2025, 12, 31))
    assert _maj_pivot(env_scenario.calls) == []


def _maj_request(**surcharges) -> ScenarioMajRequest:
    base = dict(
        periode_debut=date(2025, 1, 1), periode_fin=date(2025, 12, 31),
        nb_jours_semaine=5, id_rh="RH1",
    )
    return ScenarioMajRequest(**{**base, **surcharges})


@pytest.mark.parametrize("dt_mise_en_oeuvre", [None, date(2025, 7, 1)])
def test_maj_sans_date_pivot_conserve_pivot_et_bornes(env_scenario, dt_mise_en_oeuvre):
    asyncio.run(scenario_routes.update_scenario(42, _maj_request(dt_mise_en_oeuvre=dt_mise_en_oeuvre)))

    sql, params = _maj_principale(env_scenario.calls)
    assert _sans_colonne_pivot(sql)
    assert sql.count("%s") == len(params)
    assert params[-1] == 42
    assert _maj_pivot(env_scenario.calls) == []
    if dt_mise_en_oeuvre is None:
        assert "dt_mise_en_oeuvre" not in sql
    else:
        # La colonne et sa valeur doivent occuper la même position.
        assignations = sql.split(" SET ")[1].split(" WHERE ")[0].split(", ")
        avec_param = [a for a in assignations if "%s" in a]
        index = avec_param.index("dt_mise_en_oeuvre = %s")
        assert params[index] == dt_mise_en_oeuvre


@pytest.mark.parametrize(
    "pivot,bornes",
    [
        (date(2025, 7, 1), (date(2025, 1, 1), date(2025, 6, 30), date(2025, 7, 1), date(2025, 12, 31))),
        (date(2026, 3, 1), (date(2025, 1, 1), date(2025, 12, 31), None, None)),
        (date(2024, 6, 1), (None, None, date(2025, 1, 1), date(2025, 12, 31))),
    ],
)
def test_maj_avec_date_pivot_ecrit_pivot_et_bornes(env_scenario, pivot, bornes):
    asyncio.run(scenario_routes.update_scenario(42, _maj_request(date_pivot=pivot)))

    assert _sans_colonne_pivot(_maj_principale(env_scenario.calls)[0])
    assert _maj_pivot(env_scenario.calls) == [(pivot, *bornes, 42)]


def test_maj_date_pivot_bornes_sur_la_nouvelle_periode(env_scenario):
    """Les bornes suivent la période du body, pas celle stockée avant la MAJ."""
    payload = _maj_request(
        periode_debut=date(2026, 1, 1), periode_fin=date(2026, 6, 30), date_pivot=date(2026, 3, 1)
    )

    asyncio.run(scenario_routes.update_scenario(42, payload))

    assert _maj_pivot(env_scenario.calls) == [
        (date(2026, 3, 1), date(2026, 1, 1), date(2026, 2, 28), date(2026, 3, 1), date(2026, 6, 30), 42)
    ]


def test_patch_periodes_date_pivot_obligatoire():
    with pytest.raises(ValidationError):
        PeriodeUpdate(periode_fin=date(2026, 6, 30))


def test_patch_periodes_date_pivot_seule_acceptee():
    assert PeriodeUpdate(date_pivot=date(2025, 7, 1)).date_pivot == date(2025, 7, 1)


def test_patch_periodes_ecrit_pivot_sur_la_periode_resultante(env_scenario):
    # periode_debut absente : on garde celle du scénario (2025-01-01).
    payload = PeriodeUpdate(periode_fin=date(2026, 6, 30), date_pivot=date(2026, 1, 1))

    asyncio.run(scenario_routes.update_periodes(42, payload))

    sql, params = _maj_principale(env_scenario.calls)
    assert _sans_colonne_pivot(sql)
    assert params == (date(2025, 1, 1), date(2026, 6, 30), 42)
    assert _maj_pivot(env_scenario.calls) == [
        (date(2026, 1, 1), date(2025, 1, 1), date(2025, 12, 31), date(2026, 1, 1), date(2026, 6, 30), 42)
    ]


def test_patch_periodes_fin_avant_debut_400_sans_ecriture(env_scenario):
    payload = PeriodeUpdate(periode_fin=date(2024, 6, 30), date_pivot=date(2024, 3, 1))

    with pytest.raises(HTTPException) as exc:
        asyncio.run(scenario_routes.update_periodes(42, payload))

    assert exc.value.status_code == 400
    assert env_scenario.calls == []
