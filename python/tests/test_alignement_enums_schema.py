"""Tests de garde des écarts P0 n°1 à n°3 du rapport
`db/RAPPORT-ECARTS-db_new-2026-08-17.md`.

Ils confrontent, sans base de données, les enums Pydantic et la machine à états
au dump de référence `db/db_new.sql`, et vérifient que chaque requête SQL reçoit
autant de paramètres qu'elle porte de placeholders.
"""

import re
import typing
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.routes.trppu_pic_version.schemas import (
    NiveauCreationEnum,
    NiveauEnum,
    PicVersionCreate,
    PicVersionOut,
)
from app.routes.trppu_scenario.schemas import Statut
from app.routes.trppu_scenario.statuts import (
    ALLOWED_TRANSITIONS,
    STATUTS,
    STATUTS_EDITABLES,
)
from app.routes.trppu_variations.helpers import SELECT_VARIATIONS_SQL

SCHEMA_SQL = Path(__file__).resolve().parents[1] / "db" / "db_new.sql"
_CREATE_RE = re.compile(r"CREATE TABLE `(\w+)` \((.*?)\n\) ENGINE", re.S)


def _enum_de_colonne(table: str, colonne: str) -> tuple[str, ...]:
    """Valeurs de l'ENUM `table`.`colonne` telles que déclarées dans le dump."""
    sql = SCHEMA_SQL.read_text(encoding="utf-8")
    corps = {m.group(1): m.group(2) for m in _CREATE_RE.finditer(sql)}
    assert table in corps, f"table {table} absente du dump"
    match = re.search(rf"`{colonne}` enum\(([^)]*)\)", corps[table])
    assert match, f"{table}.{colonne} n'est pas un ENUM dans le dump"
    return tuple(re.findall(r"'([^']*)'", match.group(1)))


# --- Écart n°2 : statut SIMULATION ------------------------------------------


def test_literal_statut_couvre_exactement_lenum_de_la_base():
    attendu = _enum_de_colonne("trppu_scenario", "statut")
    assert set(typing.get_args(Statut)) == set(attendu)
    assert set(STATUTS) == set(attendu)


def test_simulation_se_comporte_comme_en_cours():
    """SIMULATION est un statut de travail : mêmes cibles de transition que
    EN COURS (au retour vers EN COURS près), et scénario modifiable."""
    assert "SIMULATION" in STATUTS_EDITABLES and "EN COURS" in STATUTS_EDITABLES
    assert ALLOWED_TRANSITIONS["EN COURS"] - {"SIMULATION"} <= ALLOWED_TRANSITIONS["SIMULATION"]
    assert "EN PRODUCTION" not in ALLOWED_TRANSITIONS["SIMULATION"]


def test_toute_valeur_de_lenum_a_une_entree_de_transition():
    assert set(ALLOWED_TRANSITIONS) == set(STATUTS)


# --- Écart n°3 : niveau SCENARIO --------------------------------------------


def test_niveau_enum_couvre_exactement_lenum_de_la_base():
    attendu = _enum_de_colonne("trppu_pic_version", "niveau")
    assert {e.value for e in NiveauEnum} == set(attendu)


def test_niveau_scenario_lisible_mais_non_creable():
    """SCENARIO doit passer en lecture (versions créées par trppu_scenario_pic)
    et rester refusé en écriture par le CRUD."""
    commun = dict(
        co_regate="ABC123",
        id_scenario=42,
        dt_activation="2026-01-01T00:00:00",
    )
    assert PicVersionOut(
        niveau="SCENARIO",
        id_pic_version=1,
        dt_creation="2026-01-01T00:00:00",
        dt_maj="2026-01-01T00:00:00",
        **commun,
    ).niveau is NiveauEnum.SCENARIO

    with pytest.raises(ValidationError):
        PicVersionCreate(niveau="SCENARIO", **commun)

    assert {e.value for e in NiveauCreationEnum} < {e.value for e in NiveauEnum}


# --- Écart n°1 : placeholders de SELECT_VARIATIONS_SQL ----------------------


def test_edition_passe_autant_de_parametres_que_de_placeholders():
    """`GET /scenarios/{id}/edition` doit répéter id_scenario autant de fois que
    la requête porte de `%s` (deux : sous-requête TMH + jointure)."""
    attendus = SELECT_VARIATIONS_SQL.count("%s")
    routes = (
        Path(__file__).resolve().parents[1] / "app" / "routes" / "trppu_scenario" / "routes.py"
    ).read_text(encoding="utf-8")
    appel = re.search(r"SELECT_VARIATIONS_SQL,\s*\(([^)]*)\)", routes)
    assert appel, "appel à SELECT_VARIATIONS_SQL introuvable dans trppu_scenario/routes.py"
    fournis = len([p for p in appel.group(1).split(",") if p.strip()])
    assert fournis == attendus == 2
