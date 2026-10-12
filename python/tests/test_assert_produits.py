"""Tests du contrôle d'existence des produits avant toute écriture TMH (sans base).

Les produits sont alimentés hors des écritures TMH : un `co_produit` absent de
`trppu_produit` n'est plus créé à la volée, il est rejeté en 422 avant que la FK
`fk_tmh_produit` ne casse la transaction.
"""

import asyncio
import inspect
from decimal import Decimal

import pytest
from fastapi import HTTPException

from app.routes.trppu_produit.helpers import (
    SELECT_CODES_PRODUITS_SQL,
    assert_produits_exist,
)
from app.routes.trppu_scenario import routes as scenario_routes
from app.routes.trppu_tmh import routes as tmh_routes
from app.routes.trppu_tmh.schemas import TmhBatchUpdate, TmhCreate, TmhUpsert


class FakeTx:
    """Transaction factice : `trppu_produit` contient `existants`, journalise les requêtes."""

    def __init__(self, existants=()):
        self.existants = set(existants)
        self.fetch_alls: list[tuple[str, tuple]] = []
        self.executes: list[tuple[str, tuple]] = []

    async def fetch_all(self, sql, params=None):
        self.fetch_alls.append((sql, params))
        return [{"co_produit": c} for c in params if c in self.existants]

    async def execute(self, sql, params=None):
        self.executes.append((sql, params))
        return 1


def _rejet(tx, codes) -> HTTPException:
    with pytest.raises(HTTPException) as exc:
        asyncio.run(assert_produits_exist(tx, codes))
    return exc.value


def test_produits_tous_presents_passe_sans_ecrire():
    tx = FakeTx(existants={"OO", "CO"})
    asyncio.run(assert_produits_exist(tx, ["OO", "CO"]))
    assert tx.executes == []
    sql, params = tx.fetch_alls[0]
    assert sql == SELECT_CODES_PRODUITS_SQL.format("%s, %s")
    assert params == ("OO", "CO")


def test_produit_absent_rejete_en_422_avec_les_codes_manquants():
    tx = FakeTx(existants={"OO"})
    exc = _rejet(tx, ["OO", "PR", "PPI"])
    assert exc.status_code == 422
    assert "PR, PPI" in exc.detail
    assert "OO" not in exc.detail
    assert tx.executes == []  # aucune création de produit


def test_codes_normalises_et_dedoublonnes():
    tx = FakeTx(existants={"OO"})
    asyncio.run(assert_produits_exist(tx, [" oo", "OO", "oo "]))
    assert tx.fetch_alls[0][1] == ("OO",)


def test_comparaison_insensible_a_la_casse_du_referentiel():
    """MySQL compare en collation insensible à la casse : un code stocké en minuscules passe."""

    class TxMinuscules(FakeTx):
        async def fetch_all(self, sql, params=None):
            return [{"co_produit": "oo"}]

    asyncio.run(assert_produits_exist(TxMinuscules(), ["OO"]))


def test_lot_vide_ou_codes_vides_ninterroge_pas_la_base():
    tx = FakeTx()
    asyncio.run(assert_produits_exist(tx, []))
    asyncio.run(assert_produits_exist(tx, [None, " "]))
    assert tx.fetch_alls == []


# --- Routes -------------------------------------------------------------------


def _patch_tmh_routes(monkeypatch, tx):
    class FakeDbWrite:
        def transaction(self):
            class _Ctx:
                async def __aenter__(self_inner):
                    return tx

                async def __aexit__(self_inner, *exc):
                    return False

            return _Ctx()

    async def _scenario(id_scenario):
        return {"id_scenario": id_scenario, "statut": "EN COURS", "est_fige": 0}

    monkeypatch.setattr(tmh_routes, "db_write", FakeDbWrite())
    monkeypatch.setattr(tmh_routes, "fetch_scenario_or_404", _scenario)


_LIGNE = {
    "volume_realise": 10,
    "moyenne_journaliere": Decimal("1.00"),
    "moyenne_hebdo": Decimal("2.00"),
}


def test_post_tmh_produit_inconnu_422_sans_insert(monkeypatch):
    tx = FakeTx(existants=set())
    _patch_tmh_routes(monkeypatch, tx)
    payload = TmhCreate(co_produit="ZZ", id_rh="u1", **_LIGNE)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(tmh_routes.create_tmh(12, payload))
    assert exc.value.status_code == 422
    assert "ZZ" in exc.value.detail
    assert tx.executes == []


def test_put_tmh_batch_produit_inconnu_422_sans_ecriture(monkeypatch):
    tx = FakeTx(existants={"OO"})
    _patch_tmh_routes(monkeypatch, tx)
    payload = TmhBatchUpdate(
        tmh=[TmhUpsert(co_produit="OO", **_LIGNE), TmhUpsert(co_produit="ZZ", **_LIGNE)],
        id_rh="u1",
    )
    with pytest.raises(HTTPException) as exc:
        asyncio.run(tmh_routes.upsert_tmh(12, payload))
    assert exc.value.status_code == 422
    assert "ZZ" in exc.value.detail
    assert tx.executes == []


@pytest.mark.parametrize(
    "fonction",
    [
        tmh_routes.create_tmh,
        tmh_routes.upsert_tmh,
        scenario_routes.create_scenario,
        scenario_routes.update_scenario,
    ],
)
def test_les_ecritures_tmh_controlent_sans_creer_de_produit(fonction):
    source = inspect.getsource(fonction)
    assert "assert_produits_exist" in source
    assert "INSERT INTO trppu_produit" not in source
