"""Tests de la suppression en cascade d'un scénario (`trppu_scenario/helpers.py`).

Deux garanties :

1. **Le hard-delete ne doit pas buter sur les FK des tables de logs.**
   `trppu_api_log` et `trppu_recalcul_log` référencent `trppu_scenario` par des
   FK déclarées sans `ON DELETE` (donc RESTRICT, cf. db/db_new.sql:95 et :245).
   Dès que ces tables sont alimentées, supprimer le scénario parent échoue en
   MySQL 1451 — c'est l'écart n°10 de db/RAPPORT-ECARTS-db_new-2026-08-17.md.
2. **La volumétrie supprimée est restituée**, seule base de reconstitution de
   l'ampleur d'une suppression après coup.
"""

import asyncio

from app.routes.trppu_scenario.helpers import (
    SCENARIO_CHILD_TABLES,
    delete_scenario_cascade,
    detach_logs_scenario,
    last_insert_id,
)


class FakeTx:
    """Transaction factice : enregistre (sql, params) et retourne un rowcount fixe."""

    def __init__(self, rowcount=2, fetch_one_results=None):
        self.calls: list[tuple[str, tuple | None]] = []
        self._rowcount = rowcount
        self._fetch_one_results = list(fetch_one_results or [])

    async def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return self._rowcount

    async def fetch_one(self, sql, params=None):
        self.calls.append((sql, params))
        return self._fetch_one_results.pop(0) if self._fetch_one_results else None


def test_last_insert_id():
    tx = FakeTx(fetch_one_results=[{"id": 52}])
    assert asyncio.run(last_insert_id(tx)) == 52
    assert "LAST_INSERT_ID" in tx.calls[0][0]


def test_api_log_detache_et_non_supprime():
    """`trppu_api_log.id_scenario` est nullable : on détache pour garder la trace."""
    tx = FakeTx()
    asyncio.run(detach_logs_scenario(tx, 52))
    sql_api = [s for s, _ in tx.calls if "trppu_api_log" in s]
    assert len(sql_api) == 1
    assert sql_api[0].startswith("UPDATE trppu_api_log SET id_scenario = NULL")
    assert not any(s.startswith("DELETE FROM trppu_api_log") for s, _ in tx.calls)


def test_recalcul_log_supprime():
    """`trppu_recalcul_log.id_scenario` est NOT NULL : pas de détachement possible."""
    tx = FakeTx()
    asyncio.run(detach_logs_scenario(tx, 52))
    sql_recalcul = [s for s, _ in tx.calls if "trppu_recalcul_log" in s]
    assert len(sql_recalcul) == 1
    assert sql_recalcul[0].startswith("DELETE FROM trppu_recalcul_log")


def test_tables_de_logs_traitees_avant_le_parent():
    """Sinon le DELETE du scénario échoue en MySQL 1451 (FK RESTRICT)."""
    tx = FakeTx()
    asyncio.run(delete_scenario_cascade(tx, 52))
    ordre = [sql for sql, _ in tx.calls]
    index_logs = max(
        i for i, s in enumerate(ordre) if "trppu_api_log" in s or "trppu_recalcul_log" in s
    )
    index_parent = next(
        i for i, s in enumerate(ordre) if s.startswith("DELETE FROM trppu_scenario ")
    )
    assert index_logs < index_parent


def test_toutes_les_tables_filles_traitees_avant_le_parent():
    tx = FakeTx()
    asyncio.run(delete_scenario_cascade(tx, 52))
    ordre = [sql for sql, _ in tx.calls]
    index_parent = next(
        i for i, s in enumerate(ordre) if s.startswith("DELETE FROM trppu_scenario ")
    )
    for table in SCENARIO_CHILD_TABLES:
        index_fille = next(i for i, s in enumerate(ordre) if f"FROM {table} " in s)
        assert index_fille < index_parent, table


def test_volumetrie_retournee_par_table():
    tx = FakeTx(rowcount=3)
    supprimes = asyncio.run(delete_scenario_cascade(tx, 52))

    attendu = set(SCENARIO_CHILD_TABLES) | {
        "trppu_scenario",
        "trppu_api_log",
        "trppu_recalcul_log",
    }
    assert set(supprimes) == attendu
    assert all(n == 3 for n in supprimes.values())


def test_id_scenario_passe_en_parametre_lie():
    """Aucune interpolation de l'id dans le SQL (l'id vient de l'URL)."""
    tx = FakeTx()
    asyncio.run(delete_scenario_cascade(tx, 52))
    assert all(params == (52,) for _sql, params in tx.calls)
