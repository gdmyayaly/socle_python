"""Tests de la mise en production OPTIPACC (DSR-707).

Quatre familles, dans l'esprit de `test_optipacc.py` (pas de TestClient dans ce
projet : on appelle directement les coroutines d'endpoint avec de faux pools) :

1. **Contrôles C1 à C5** — chacun avec son code HTTP et son message de ticket.
2. **Effet de bord de l'écriture** — c'est le point le plus subtil du ticket : une
   *seule* date envoyée doit alimenter `dt_mise_en_oeuvre` **et** `dt_mise_en_prod`
   (Cas 1 + RG-API-PROD-006). On inspecte les paramètres réellement liés à l'UPDATE.
3. **Durcissement de la route IHM** — C4 et C5 sont des helpers partagés avec
   `POST /trppu-api/scenarios/{id}/mise-en-prod`, qui ne les appliquait pas avant ce
   ticket et constituait donc un contournement de RG-API-PROD-005.
4. **Non-régression schéma** — les colonnes citées existent dans `db/db_new.sql`.
"""

import asyncio
import re
from datetime import date
from pathlib import Path

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.routes.trppu_optipacc import helpers, routes
from app.routes.trppu_optipacc.schemas import MiseEnProductionRequest
from app.routes.trppu_scenario import helpers as scenario_helpers


# --- Outils communs -------------------------------------------------------------


DATE_MEP = date(2027, 4, 1)


def _scenario(
    statut="VALIDE",
    co_regate="123456",
    id_scenario=125,
    pdi=1,
    agrebal=1,
    en_cours=0,
    est_fige=0,
):
    return {
        "id_scenario": id_scenario,
        "co_regate": co_regate,
        "statut": statut,
        "est_fige": est_fige,
        "trafic_pdi_calcule": pdi,
        "trafic_agrebal_calcule": agrebal,
        "calcul_trafic_en_cours": en_cours,
    }


class FakeRead:
    """`db_read` minimal : une seule réponse préprogrammée pour la garde."""

    def __init__(self, one=None):
        self._one = one
        self.executed: list[tuple[str, tuple]] = []

    async def fetch_one(self, sql, params=None):
        self.executed.append((sql, params))
        return self._one


class FakeTx:
    """Curseur transactionnel : journalise les écritures, simule C5 au besoin."""

    def __init__(self, autre_en_prod=None, boom=False):
        self._autre = autre_en_prod
        self._boom = boom
        self.executed: list[tuple[str, tuple]] = []

    async def fetch_one(self, sql, params=None):
        self.executed.append((sql, params))
        if "EN PRODUCTION" in sql and "FOR UPDATE" in sql:
            return {"id_scenario": self._autre} if self._autre else None
        # increment_version relit la version après l'UPDATE.
        return {"version_scenario": 3}

    async def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if self._boom:
            raise RuntimeError("MySQL indisponible")
        return 1


class FakeWrite:
    """`db_write` minimal : expose `transaction()` comme un context manager async."""

    def __init__(self, tx):
        self.tx = tx

    def transaction(self):
        tx = self.tx

        class _Cm:
            async def __aenter__(self):
                return tx

            async def __aexit__(self, *exc):
                return False

        return _Cm()


@pytest.fixture
def site_existant(monkeypatch):
    """Neutralise le contrôle d'existence du site (testé séparément)."""

    async def _ok(co_regate):
        return {"co_regate": co_regate}

    monkeypatch.setattr(routes, "fetch_site_or_404", _ok)


@pytest.fixture
def audit_muet(monkeypatch):
    """L'écriture dans trppu_api_log n'a pas à toucher la base pendant les tests."""

    async def _noop(**kwargs):
        return None

    monkeypatch.setattr(routes, "enregistrer_appel", _noop)


def _appeler(scenario, *, monkeypatch, tx=None, date_mep=DATE_MEP):
    monkeypatch.setattr(routes, "db_read", FakeRead(one=scenario))
    tx = tx or FakeTx()
    monkeypatch.setattr(routes, "db_write", FakeWrite(tx))
    payload = MiseEnProductionRequest(
        code_regate="123456", scenario_id=125, date_mise_en_oeuvre=date_mep
    )
    return asyncio.run(routes.scenario_mise_en_production(payload, id_session_ihm=None))


# --- 1. Contrat d'entrée --------------------------------------------------------


def test_route_exposee_en_post():
    chemin = "/trppu-api/optipacc/scenario/mise-en-production"
    methodes = {
        m for r in routes.router.routes if r.path == chemin for m in r.methods
    }
    assert methodes == {"POST"}


def test_date_obligatoire():
    """RG-API-PROD-006 : sans date, pas de référence officielle de mise en œuvre."""
    with pytest.raises(ValidationError):
        MiseEnProductionRequest(code_regate="123456", scenario_id=125)


def test_champ_inconnu_refuse():
    """`extra="forbid"` : un `date_mise_en_prod` envoyé par erreur doit être signalé
    plutôt que silencieusement ignoré (le ticket cite les deux noms)."""
    with pytest.raises(ValidationError):
        MiseEnProductionRequest(
            code_regate="123456",
            scenario_id=125,
            date_mise_en_oeuvre=DATE_MEP,
            date_mise_en_prod=DATE_MEP,
        )


def test_code_regate_borne_a_six_caracteres():
    with pytest.raises(ValidationError):
        MiseEnProductionRequest(
            code_regate="12345", scenario_id=125, date_mise_en_oeuvre=DATE_MEP
        )


# --- 2. Contrôles C1 à C5 -------------------------------------------------------


def test_c1_404_si_scenario_inexistant(monkeypatch, site_existant, audit_muet):
    with pytest.raises(HTTPException) as exc:
        _appeler(None, monkeypatch=monkeypatch)
    assert exc.value.status_code == 404


def test_site_inconnu_404(monkeypatch, audit_muet):
    async def _ko(co_regate):
        raise HTTPException(status_code=404, detail=f"Site {co_regate} introuvable.")

    monkeypatch.setattr(routes, "fetch_site_or_404", _ko)
    with pytest.raises(HTTPException) as exc:
        _appeler(_scenario(), monkeypatch=monkeypatch)
    assert exc.value.status_code == 404


def test_c2_400_si_scenario_dun_autre_site(monkeypatch, site_existant, audit_muet):
    """DSR-707 impose 400 ici, là où DSR-689 répond 404 : divergence assumée."""
    with pytest.raises(HTTPException) as exc:
        _appeler(_scenario(co_regate="999999"), monkeypatch=monkeypatch)
    assert exc.value.status_code == 400
    assert exc.value.detail == "Le scénario 125 n'appartient pas au site 123456"


@pytest.mark.parametrize(
    "statut", ["EN COURS", "SIMULATION", "ARCHIVE", "EN PRODUCTION"]
)
def test_c3_409_sur_tout_statut_autre_que_valide(
    statut, monkeypatch, site_existant, audit_muet
):
    """Cas 3 du ticket inclus : un scénario déjà EN PRODUCTION est refusé ici."""
    with pytest.raises(HTTPException) as exc:
        _appeler(_scenario(statut=statut), monkeypatch=monkeypatch)
    assert exc.value.status_code == 409
    assert exc.value.detail == (
        "Les paramètres du scénario 125 ne permettent pas "
        "la mise en production du scénario"
    )


@pytest.mark.parametrize(
    "flags",
    [
        {"pdi": 0},
        {"agrebal": 0},  # Cas 2 du ticket
        {"en_cours": 1},
        {"pdi": None, "agrebal": None},  # colonnes nullables en base
    ],
)
def test_c4_409_si_trafics_non_completement_calcules(
    flags, monkeypatch, site_existant, audit_muet
):
    with pytest.raises(HTTPException) as exc:
        _appeler(_scenario(**flags), monkeypatch=monkeypatch)
    assert exc.value.status_code == 409
    assert exc.value.detail == (
        "Les trafics du scénario 125 ne sont pas complètement calculés"
    )


def test_c5_409_si_un_autre_scenario_du_site_est_deja_en_production(
    monkeypatch, site_existant, audit_muet
):
    """Cas 4 du ticket / RG-API-PROD-005."""
    tx = FakeTx(autre_en_prod=130)
    with pytest.raises(HTTPException) as exc:
        _appeler(_scenario(), monkeypatch=monkeypatch, tx=tx)
    assert exc.value.status_code == 409
    assert exc.value.detail == "Un scénario est déjà en production pour ce site"


def test_c5_verrouille_la_ligne_et_exclut_le_scenario_courant():
    """Sans FOR UPDATE, deux appels concurrents passeraient le contrôle tous les deux.

    L'exclusion `id_scenario <> %s` évite qu'un rejeu sur un scénario *déjà* en
    production se bloque lui-même — il est déjà refusé en amont par C3.
    """
    sql = scenario_helpers.SELECT_AUTRE_SCENARIO_EN_PROD_SQL
    assert "FOR UPDATE" in sql
    assert "id_scenario <> %s" in sql
    assert "statut = 'EN PRODUCTION'" in sql


def test_c5_est_verifie_avant_lupdate(monkeypatch, site_existant, audit_muet):
    """L'ordre compte : le contrôle doit précéder l'écriture dans la transaction."""
    tx = FakeTx()
    _appeler(_scenario(), monkeypatch=monkeypatch, tx=tx)
    sqls = [sql for sql, _ in tx.executed]
    assert "FOR UPDATE" in sqls[0]
    assert any("UPDATE trppu_scenario" in s for s in sqls[1:])


# --- 3. Écriture et réponse -----------------------------------------------------


def test_cas_nominal_renvoie_le_contrat_du_ticket(
    monkeypatch, site_existant, audit_muet
):
    reponse = _appeler(_scenario(), monkeypatch=monkeypatch)
    assert reponse.scenario_id == 125
    assert reponse.code_regate == "123456"
    assert reponse.statut == "EN PRODUCTION"
    assert reponse.date_mise_en_oeuvre == DATE_MEP


def test_la_date_recue_alimente_les_deux_colonnes(
    monkeypatch, site_existant, audit_muet
):
    """Le cœur de DSR-707 : une seule date en entrée, deux colonnes en base.

    Cas 1 du ticket : DT_MISE_EN_OEUVRE et DT_MISE_EN_PROD valent toutes deux la
    date transmise — et non NOW(), contrairement à la route IHM.
    """
    tx = FakeTx()
    _appeler(_scenario(), monkeypatch=monkeypatch, tx=tx)
    update = next(
        (sql, params) for sql, params in tx.executed if "dt_mise_en_oeuvre" in sql
    )
    sql, params = update
    assert "dt_mise_en_prod = %s" in sql
    assert params[:2] == (DATE_MEP, DATE_MEP)
    assert params[2] == 125


def test_lupdate_pose_le_statut_et_le_figement():
    """RG-API-PROD-002 : le scénario devient définitivement figé."""
    sql = helpers.UPDATE_MISE_EN_PROD_SQL
    assert "statut = 'EN PRODUCTION'" in sql
    assert "est_fige = 1" in sql
    # dt_maj est ON UPDATE CURRENT_TIMESTAMP en base : l'écrire serait redondant.
    assert "dt_maj" not in sql
    # Même effet de bord que apply_transition_side_effects.
    assert "dt_validation = COALESCE(dt_validation, NOW())" in sql


def test_la_version_du_scenario_est_incrementee(
    monkeypatch, site_existant, audit_muet
):
    tx = FakeTx()
    _appeler(_scenario(), monkeypatch=monkeypatch, tx=tx)
    assert any("version_scenario = version_scenario + 1" in s for s, _ in tx.executed)


def test_lappel_est_audite(monkeypatch, site_existant):
    """La mise en production est une écriture : elle doit laisser une trace d'audit."""
    appels = []

    async def _capture(**kwargs):
        appels.append(kwargs)

    monkeypatch.setattr(routes, "enregistrer_appel", _capture)
    _appeler(_scenario(), monkeypatch=monkeypatch)
    assert len(appels) == 1
    assert appels[0]["id_scenario"] == 125
    assert appels[0]["regate"] == "123456"
    assert appels[0]["params"]["statut_apres"] == "EN PRODUCTION"
    assert appels[0]["params"]["origine"] == "OPTIPACC"


def test_erreur_sgbd_donne_un_500(monkeypatch, site_existant, audit_muet):
    tx = FakeTx(boom=True)
    with pytest.raises(HTTPException) as exc:
        _appeler(_scenario(), monkeypatch=monkeypatch, tx=tx)
    assert exc.value.status_code == 500


def test_un_refus_metier_nest_pas_transforme_en_500(
    monkeypatch, site_existant, audit_muet
):
    """C5 lève depuis l'intérieur du `try` : le 409 ne doit pas être avalé."""
    tx = FakeTx(autre_en_prod=130)
    with pytest.raises(HTTPException) as exc:
        _appeler(_scenario(), monkeypatch=monkeypatch, tx=tx)
    assert exc.value.status_code == 409


# --- 6. Non-régression schéma ---------------------------------------------------

_DB_SQL = Path(__file__).resolve().parents[1] / "db" / "db_new.sql"


def _bloc_de(table: str) -> str:
    contenu = _DB_SQL.read_text(encoding="utf-8")
    bloc = re.search(rf"CREATE TABLE `{table}` \((.*?)\n\) ENGINE", contenu, re.S)
    assert bloc, f"Table {table} introuvable dans {_DB_SQL.name}"
    return bloc.group(1)


def _colonnes_de(table: str) -> set[str]:
    return set(re.findall(r"^\s*`(\w+)`", _bloc_de(table), re.M))


def test_les_colonnes_de_la_mise_en_prod_existent_en_base():
    attendues = {
        "id_scenario",
        "co_regate",
        "statut",
        "est_fige",
        "dt_mise_en_oeuvre",
        "dt_mise_en_prod",
        "dt_validation",
        "trafic_pdi_calcule",
        "trafic_agrebal_calcule",
        "Calcul_trafic_en_cours",
    }
    assert attendues <= _colonnes_de("trppu_scenario")


def test_le_flag_de_calcul_en_cours_est_aliase():
    """`Calcul_trafic_en_cours` est la seule colonne capitalisée du schéma.

    Sans alias, le driver renvoie la clé telle quelle et les gardes — qui lisent
    `calcul_trafic_en_cours` — retomberaient silencieusement sur 0, donc
    laisseraient passer un scénario en cours de calcul.
    """
    for sql in (
        helpers.SELECT_SCENARIO_MISE_EN_PROD_SQL,
        helpers.SELECT_SCENARIO_VISIBLE_SQL,
        scenario_helpers.SELECT_SCENARIO_SQL,
    ):
        assert "Calcul_trafic_en_cours AS calcul_trafic_en_cours" in sql


def test_dt_maj_est_bien_automatique_en_base():
    """Justifie l'absence de `dt_maj` dans l'UPDATE (le ticket demande DT_MAJ = NOW())."""
    bloc = _bloc_de("trppu_scenario")
    ligne = next(l for l in bloc.splitlines() if "`dt_maj`" in l)
    assert "ON UPDATE CURRENT_TIMESTAMP" in ligne


# --- 5. Durcissement de la route IHM --------------------------------------------
#
# `POST /trppu-api/scenarios/{id}/mise-en-prod` existait avant DSR-707 et ne
# contrôlait ni les flags de calcul (C4) ni l'unicité par site (C5) : elle était un
# contournement direct de RG-API-PROD-005. Elle partage désormais les deux gardes.


def _scenario_ihm(**kwargs):
    """Scénario au format de fetch_scenario_or_404 (SELECT_SCENARIO_SQL)."""
    base = _scenario(**kwargs)
    base["lb_scenario"] = "Scénario Avril"
    return base


def _appeler_ihm(scenario, *, monkeypatch, tx=None):
    from app.routes.trppu_scenario import routes as scenario_routes

    tx = tx or FakeTx()

    async def _fetch(id_scenario):
        return scenario

    async def _side_effects(tx_, scen, target):
        await tx_.execute(
            "UPDATE trppu_scenario SET statut = %s WHERE id_scenario = %s",
            (target, scen["id_scenario"]),
        )

    async def _noop(**kwargs):
        return None

    monkeypatch.setattr(scenario_routes, "fetch_scenario_or_404", _fetch)
    monkeypatch.setattr(scenario_routes, "apply_transition_side_effects", _side_effects)
    monkeypatch.setattr(scenario_routes, "enregistrer_appel", _noop)
    monkeypatch.setattr(scenario_routes, "db_write", FakeWrite(tx))
    return asyncio.run(scenario_routes.mise_en_prod(125))


def test_ihm_c4_refuse_un_scenario_non_calcule(monkeypatch):
    with pytest.raises(HTTPException) as exc:
        _appeler_ihm(_scenario_ihm(agrebal=0), monkeypatch=monkeypatch)
    assert exc.value.status_code == 409
    assert exc.value.detail == (
        "Les trafics du scénario 125 ne sont pas complètement calculés"
    )


def test_ihm_c5_refuse_un_second_scenario_en_production(monkeypatch):
    """Le cœur du durcissement : sans C5 ici, RG-API-PROD-005 reste contournable."""
    with pytest.raises(HTTPException) as exc:
        _appeler_ihm(_scenario_ihm(), monkeypatch=monkeypatch, tx=FakeTx(autre_en_prod=130))
    assert exc.value.status_code == 409
    assert exc.value.detail == "Un scénario est déjà en production pour ce site"


def test_ihm_cas_nominal_reste_fonctionnel(monkeypatch):
    """Le durcissement ne doit pas casser le chemin normal de l'IHM."""
    tx = FakeTx()
    _appeler_ihm(_scenario_ihm(), monkeypatch=monkeypatch, tx=tx)
    assert any("UPDATE trppu_scenario" in s for s, _ in tx.executed)


def test_ihm_c5_est_verifie_avant_lecriture(monkeypatch):
    tx = FakeTx()
    _appeler_ihm(_scenario_ihm(), monkeypatch=monkeypatch, tx=tx)
    assert "FOR UPDATE" in tx.executed[0][0]


def test_le_scenario_lu_par_lihm_porte_les_flags_necessaires():
    """fetch_scenario_or_404 doit remonter les trois flags, sinon C4 lirait 0 partout
    et refuserait tous les scénarios."""
    sql = scenario_helpers.SELECT_SCENARIO_SQL
    for colonne in (
        "trafic_pdi_calcule",
        "trafic_agrebal_calcule",
        "calcul_trafic_en_cours",
    ):
        assert colonne in sql
