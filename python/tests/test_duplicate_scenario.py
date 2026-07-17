"""Tests de la duplication profonde de scénario (helpers + schéma)."""

import asyncio

import pytest
from pydantic import ValidationError

from app.routes.trppu_scenario.helpers import (
    DUPLICATE_CHILD_SPECS,
    SCENARIO_CHILD_TABLES,
    duplicate_scenario_children,
    duplicate_scenario_pic_version,
)
from app.routes.trppu_scenario.schemas import DuplicateRequest


class FakeTx:
    """Transaction factice : enregistre (sql, params) et rejoue des réponses scriptées."""

    def __init__(self, fetch_one_results=None, rowcount=3):
        self.calls: list[tuple[str, tuple | None]] = []
        self._fetch_one_results = list(fetch_one_results or [])
        self._rowcount = rowcount

    async def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return self._rowcount

    async def fetch_one(self, sql, params=None):
        self.calls.append((sql, params))
        return self._fetch_one_results.pop(0) if self._fetch_one_results else None


def test_duplicate_child_specs_couvre_child_tables():
    """Garde-fou : la copie couvre toutes les tables filles de la suppression,
    sauf trppu_pic_version (gérée par duplicate_scenario_pic_version)."""
    specs = {table for table, _, _ in DUPLICATE_CHILD_SPECS}
    assert specs == set(SCENARIO_CHILD_TABLES) - {"trppu_pic_version"}


def test_duplicate_children_sql_et_params():
    tx = FakeTx(rowcount=2)
    counts = asyncio.run(duplicate_scenario_children(tx, 10, 99, "RH-TOKEN"))

    assert len(tx.calls) == len(DUPLICATE_CHILD_SPECS)
    assert counts == {table: 2 for table, _, _ in DUPLICATE_CHILD_SPECS}

    for (sql, params), (table, cols, replace_rh) in zip(tx.calls, DUPLICATE_CHILD_SPECS):
        assert sql.startswith(f"INSERT INTO {table} (id_scenario, ")
        assert f"FROM {table} WHERE id_scenario = %s" in sql
        for col in cols:
            assert col in sql
        if replace_rh:
            assert "id_rh) " in sql
            assert params == (99, "RH-TOKEN", 10)
        else:
            assert "id_rh" not in sql
            assert params == (99, 10)


def test_duplicate_children_tables_avec_id_rh():
    """Seules tmh, neutralisations et variations_prev portent un id_rh remplacé."""
    avec_rh = {table for table, _, replace_rh in DUPLICATE_CHILD_SPECS if replace_rh}
    assert avec_rh == {
        "trppu_tmh",
        "trppu_neutralisations",
        "trppu_scenario_variations_prev",
    }


def test_duplicate_pic_version_absente():
    tx = FakeTx(fetch_one_results=[None])
    result = asyncio.run(duplicate_scenario_pic_version(tx, 10, 99, "123456", "RH"))
    assert result is None
    # Seule la lecture de la version source a eu lieu, aucune écriture.
    assert len(tx.calls) == 1
    assert tx.calls[0][0].lstrip().upper().startswith("SELECT")


def test_duplicate_pic_version_presente():
    tx = FakeTx(
        fetch_one_results=[
            {"id_pic_version": 7, "niveau": "SCENARIO"},  # version source
            {"id": 42},  # LAST_INSERT_ID
        ]
    )
    result = asyncio.run(duplicate_scenario_pic_version(tx, 10, 99, "123456", "RH"))
    assert result == 42

    # Ordre attendu : SELECT version source, INSERT version, LAST_INSERT_ID,
    # INSERT...SELECT coefficients, UPDATE entête.
    assert len(tx.calls) == 5
    select_src, insert_version, last_id, copy_coeffs, update_entete = tx.calls

    assert "trppu_pic_version" in select_src[0]
    assert select_src[1] == (10,)

    assert insert_version[0].startswith("INSERT INTO trppu_pic_version")
    assert "'SCENARIO'" in insert_version[0]
    assert insert_version[1] == ("123456_99", "123456", 99, "RH", "RH")

    assert "LAST_INSERT_ID" in last_id[0]

    assert copy_coeffs[0].startswith("INSERT INTO trppu_pic_coefficients")
    assert "WHERE id_pic_version = %s" in copy_coeffs[0]
    assert copy_coeffs[1] == (42, "RH", 7)

    assert update_entete[0].startswith("UPDATE trppu_scenario SET id_pic_version")
    assert update_entete[1] == (42, 99)


def test_duplicate_request_id_rh_requis():
    with pytest.raises(ValidationError):
        DuplicateRequest()
    with pytest.raises(ValidationError):
        DuplicateRequest(lb_scenario="copie")


def test_duplicate_request_valide():
    req = DuplicateRequest(id_rh="U123456")
    assert req.id_rh == "U123456"
    assert req.lb_scenario is None
    req = DuplicateRequest(id_rh="U123456", lb_scenario="Mon clone")
    assert req.lb_scenario == "Mon clone"


def test_duplicate_request_extra_interdit():
    with pytest.raises(ValidationError):
        DuplicateRequest(id_rh="U123456", inconnu="x")
