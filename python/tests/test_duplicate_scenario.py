"""Tests de la duplication profonde de scénario (helpers + schéma)."""

import asyncio
import re
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.routes.trppu_scenario.helpers import (
    DUPLICATE_CHILD_SPECS,
    SCENARIO_CHILD_TABLES,
    duplicate_scenario_children,
    duplicate_scenario_pic_version,
)
from app.routes.trppu_scenario.schemas import DuplicateRequest

SCHEMA_SQL = Path(__file__).resolve().parents[1] / "db" / "db_new.sql"

# Une ligne de colonne commence par une backtick ; les lignes d'index et de
# contrainte commencent par un mot-clé (PRIMARY KEY / UNIQUE KEY / KEY /
# CONSTRAINT) et sont donc naturellement écartées.
_COLUMN_RE = re.compile(r"^\s*`(\w+)`\s+(.*?),?\s*$")
_TABLE_RE = re.compile(r"CREATE TABLE `(\w+)` \((.*?)\n\) ENGINE", re.S)


def _parse_schema() -> dict[str, dict[str, str]]:
    """{table: {colonne: définition}} extrait de db/db_new.sql."""
    sql = SCHEMA_SQL.read_text(encoding="utf-8")
    tables: dict[str, dict[str, str]] = {}
    for match in _TABLE_RE.finditer(sql):
        name, body = match.group(1), match.group(2)
        colonnes: dict[str, str] = {}
        for ligne in body.splitlines():
            col = _COLUMN_RE.match(ligne)
            if col:
                colonnes[col.group(1)] = col.group(2)
        tables[name] = colonnes
    return tables


def _colonnes_obligatoires(colonnes: dict[str, str]) -> set[str]:
    """Colonnes NOT NULL sans valeur automatique : elles DOIVENT être fournies."""
    return {
        nom
        for nom, definition in colonnes.items()
        if "NOT NULL" in definition
        and "DEFAULT" not in definition
        and "AUTO_INCREMENT" not in definition
        and "GENERATED" not in definition
    }


SCHEMA = _parse_schema()


def test_schema_de_reference_lisible():
    """Garde-fou du parseur : si db_new.sql change de forme, les tests suivants
    deviendraient silencieusement vides au lieu d'échouer."""
    assert len(SCHEMA) == 25
    for table, _, _ in DUPLICATE_CHILD_SPECS:
        assert table in SCHEMA, f"Table {table} absente du schéma de référence."
        assert SCHEMA[table], f"Aucune colonne extraite pour {table}."


@pytest.mark.parametrize("table,cols,replace_rh", DUPLICATE_CHILD_SPECS)
def test_duplicate_specs_colonnes_existent(table, cols, replace_rh):
    """Chaque colonne copiée existe réellement dans la table.

    Sans ce contrôle, une colonne inventée ne se voit qu'à l'exécution, sous forme
    d'un ERROR 1054 qui fait échouer toute la duplication.
    """
    reelles = SCHEMA[table]
    inconnues = [c for c in cols if c not in reelles]
    assert not inconnues, (
        f"{table} : colonne(s) copiée(s) inexistante(s) en base : {inconnues}. "
        f"Colonnes réelles : {sorted(reelles)}"
    )


@pytest.mark.parametrize("table,cols,replace_rh", DUPLICATE_CHILD_SPECS)
def test_duplicate_specs_couvre_colonnes_obligatoires(table, cols, replace_rh):
    """Toute colonne NOT NULL sans défaut est fournie par l'INSERT ... SELECT.

    id_scenario est ajouté par la requête elle-même, id_rh l'est quand la table le
    porte. Une colonne obligatoire oubliée provoque un ERROR 1364 à l'exécution.
    """
    fournies = set(cols) | {"id_scenario"}
    if replace_rh:
        fournies.add("id_rh")
    manquantes = _colonnes_obligatoires(SCHEMA[table]) - fournies
    assert not manquantes, (
        f"{table} : colonne(s) NOT NULL sans défaut non copiée(s) : "
        f"{sorted(manquantes)}"
    )


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
        # La liste de colonnes doit apparaître à l'identique côté INSERT et côté
        # SELECT — sinon les valeurs seraient décalées d'une colonne à l'autre.
        # La validité des noms eux-mêmes est vérifiée contre db/db_new.sql par
        # test_duplicate_specs_colonnes_existent (un `assert col in sql` ne
        # pourrait pas la détecter : il relit le SQL qu'on vient de générer).
        assert sql.count(", ".join(cols)) == 2
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
