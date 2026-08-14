"""Tests des services OPTIPACC (DSR-689 volumes bruts, DSR-690 liste de scénarios).

Trois familles de tests :

1. **Arithmétique du volume brut** — la formule vit dans une constante SQL, donc on
   l'exécute réellement sur une base SQLite en mémoire (la requête n'utilise que du
   SQL standard : SUM / COALESCE / GROUP BY). C'est ce qui protège le point le plus
   subtil du ticket : plusieurs lignes `trppu_tmh` pour un même produit doivent être
   sommées, et `volume_previsionnel_recalcule` NULL doit retomber sur
   `volume_previsionnel`.
2. **Gardes et contrat HTTP** — appel direct des fonctions d'endpoint avec des faux
   `db_read`, comme le reste de la suite (pas de TestClient dans ce projet).
3. **Non-régression schéma** — les colonnes citées par les constantes SQL existent
   bien dans `db/db_new.sql`.
"""

import asyncio
import re
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi import HTTPException

from app.routes.trppu_optipacc import helpers, routes
from app.routes.trppu_optipacc.schemas import SiteScenariosRequest, TraficBrutRequest


# --- Outils communs -------------------------------------------------------------


class FakeDb:
    """`db_read` minimal : renvoie des réponses préprogrammées et journalise le SQL."""

    def __init__(self, *, one=None, all_rows=None, boom=False):
        self._one = one
        self._all = all_rows or []
        self._boom = boom
        self.executed: list[tuple[str, tuple]] = []

    async def fetch_one(self, sql, params=None):
        self.executed.append((sql, params))
        return self._one

    async def fetch_all(self, sql, params=None):
        self.executed.append((sql, params))
        if self._boom:
            raise RuntimeError("MySQL indisponible")
        return self._all


def _scenario(statut="VALIDE", agrebal=1, co_regate="123456", id_scenario=12):
    return {
        "id_scenario": id_scenario,
        "co_regate": co_regate,
        "statut": statut,
        "trafic_agrebal_calcule": agrebal,
    }


@pytest.fixture
def site_existant(monkeypatch):
    """Neutralise le contrôle d'existence du site (testé séparément)."""

    async def _ok(co_regate):
        return {"co_regate": co_regate}

    monkeypatch.setattr(routes, "fetch_site_or_404", _ok)


def _appeler_trafic_brut(**kwargs):
    payload = TraficBrutRequest(**{"codeRegate": "123456", "scenarioId": 12, **kwargs})
    return asyncio.run(routes.scenario_trafic_brut(payload, id_session_ihm=None))


# --- 1. Arithmétique du volume brut (SQLite en mémoire) -------------------------

_DDL_TMH = """
CREATE TABLE trppu_tmh (
    id_tmh INTEGER PRIMARY KEY AUTOINCREMENT,
    id_scenario INTEGER NOT NULL,
    co_produit TEXT NOT NULL,
    volume_realise INTEGER,
    volume_previsionnel INTEGER,
    volume_previsionnel_recalcule INTEGER,
    bl_exclu INTEGER NOT NULL DEFAULT 0,
    bl_manuel INTEGER NOT NULL DEFAULT 0
)
"""


def _executer(sql: str, lignes: list[tuple], id_scenario: int = 12) -> dict[str, int]:
    """Joue une constante SQL du module sur SQLite et renvoie {co_produit: volume}."""
    cx = sqlite3.connect(":memory:")
    cx.execute(_DDL_TMH)
    cx.executemany(
        "INSERT INTO trppu_tmh (id_scenario, co_produit, volume_realise, "
        "volume_previsionnel, volume_previsionnel_recalcule, bl_exclu, bl_manuel) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        lignes,
    )
    rows = cx.execute(sql.replace("%s", "?"), (id_scenario,)).fetchall()
    cx.close()
    return {co_produit: volume for co_produit, volume in rows}


def test_volume_brut_somme_constate_et_previsionnel_recalcule():
    """RG4 : volume brut = constaté + prévisionnel recalculé."""
    lignes = [(12, "OS", 1_000_000, 200_000, 250_000, 0, 0)]
    assert _executer(helpers.SELECT_VOLUMES_BRUTS_SQL, lignes) == {"OS": 1_250_000}


def test_volume_brut_somme_les_lignes_multiples_du_meme_produit():
    """Un trafic manuel est une ligne TMH supplémentaire : elle doit s'additionner.

    C'est le cas qui justifie le GROUP BY : depuis la migration du 24/06/2026 un même
    co_produit peut avoir plusieurs lignes pour un scénario.
    """
    lignes = [
        (12, "IP", 5_000_000, 400_000, 400_000, 0, 0),  # ligne calculée
        (12, "IP", 500_000, 0, 0, 0, 1),  # ajout manuel
    ]
    assert _executer(helpers.SELECT_VOLUMES_BRUTS_SQL, lignes) == {"IP": 5_900_000}


def test_volume_brut_retombe_sur_le_previsionnel_quand_le_recalcule_est_null():
    """Lignes legacy / batch : volume_previsionnel_recalcule peut être NULL."""
    lignes = [(12, "CO", 700_000, 91_000, None, 0, 0)]
    assert _executer(helpers.SELECT_VOLUMES_BRUTS_SQL, lignes) == {"CO": 791_000}


def test_volume_brut_tolere_les_volumes_null():
    lignes = [(12, "EP", None, None, None, 0, 0)]
    assert _executer(helpers.SELECT_VOLUMES_BRUTS_SQL, lignes) == {"EP": 0}


def test_volume_brut_ignore_les_produits_exclus_par_defaut():
    lignes = [
        (12, "OS", 100, 0, 0, 0, 0),
        (12, "PQ", 999, 0, 0, 1, 0),  # exclu
    ]
    assert _executer(helpers.SELECT_VOLUMES_BRUTS_SQL, lignes) == {"OS": 100}


def test_volume_brut_inclut_les_exclus_avec_la_variante_dediee():
    lignes = [
        (12, "OS", 100, 0, 0, 0, 0),
        (12, "PQ", 999, 0, 0, 1, 0),
    ]
    obtenu = _executer(helpers.SELECT_VOLUMES_BRUTS_TOUS_SQL, lignes)
    assert obtenu == {"OS": 100, "PQ": 999}


def test_volume_brut_ne_filtre_jamais_sur_bl_manuel():
    """bl_manuel est un flag de provenance : le filtrer double-compterait ou perdrait
    des lignes (une correction DSR-649 le pose aussi sur une ligne calculée)."""
    for sql in (helpers.SELECT_VOLUMES_BRUTS_SQL, helpers.SELECT_VOLUMES_BRUTS_TOUS_SQL):
        assert "bl_manuel" not in sql


def test_volume_brut_isole_le_scenario_demande():
    lignes = [
        (12, "OS", 100, 0, 0, 0, 0),
        (99, "OS", 5_000, 0, 0, 0, 0),  # autre scénario
    ]
    assert _executer(helpers.SELECT_VOLUMES_BRUTS_SQL, lignes) == {"OS": 100}


# --- 2. DSR-689 : gardes et contrat --------------------------------------------


def test_trafic_brut_404_si_site_inconnu(monkeypatch):
    async def _ko(co_regate):
        raise HTTPException(status_code=404, detail=f"Site {co_regate} introuvable.")

    monkeypatch.setattr(routes, "fetch_site_or_404", _ko)
    with pytest.raises(HTTPException) as exc:
        _appeler_trafic_brut()
    assert exc.value.status_code == 404
    assert "Site 123456 introuvable." in exc.value.detail


def test_trafic_brut_404_si_scenario_inexistant(monkeypatch, site_existant):
    monkeypatch.setattr(routes, "db_read", FakeDb(one=None))
    with pytest.raises(HTTPException) as exc:
        _appeler_trafic_brut()
    assert exc.value.status_code == 404
    assert "Scénario 12 introuvable pour le site 123456." in exc.value.detail


def test_trafic_brut_404_si_scenario_dun_autre_site(monkeypatch, site_existant):
    monkeypatch.setattr(routes, "db_read", FakeDb(one=_scenario(co_regate="999999")))
    with pytest.raises(HTTPException) as exc:
        _appeler_trafic_brut()
    assert exc.value.status_code == 404


@pytest.mark.parametrize(
    "statut, agrebal",
    [
        ("EN COURS", 1),
        ("SIMULATION", 1),
        ("ARCHIVE", 1),
        ("VALIDE", 0),  # batch Agrébal pas encore passé
        ("EN PRODUCTION", 0),
    ],
)
def test_trafic_brut_409_si_scenario_non_exploitable(
    monkeypatch, site_existant, statut, agrebal
):
    monkeypatch.setattr(
        routes, "db_read", FakeDb(one=_scenario(statut=statut, agrebal=agrebal))
    )
    with pytest.raises(HTTPException) as exc:
        _appeler_trafic_brut()
    assert exc.value.status_code == 409
    assert "n'est pas disponible pour OPTIPACC" in exc.value.detail


@pytest.mark.parametrize("statut", ["VALIDE", "EN PRODUCTION"])
def test_trafic_brut_accepte_valide_et_en_production(monkeypatch, site_existant, statut):
    fake = FakeDb(
        one=_scenario(statut=statut),
        all_rows=[{"co_produit": "OS", "volume_brut": Decimal("1250000")}],
    )
    monkeypatch.setattr(routes, "db_read", fake)
    reponse = _appeler_trafic_brut()
    assert reponse.produits[0].volume_brut == 1_250_000


def test_trafic_brut_respecte_le_contrat_camel_case(monkeypatch, site_existant):
    """La sortie doit être exactement celle du contrat DSR-689."""
    fake = FakeDb(
        one=_scenario(),
        all_rows=[
            {"co_produit": "OS", "volume_brut": Decimal("1250000")},
            {"co_produit": "IP", "volume_brut": Decimal("5900000")},
        ],
    )
    monkeypatch.setattr(routes, "db_read", fake)
    dump = _appeler_trafic_brut().model_dump(by_alias=True)
    assert dump == {
        "codeRegate": "123456",
        "scenarioId": 12,
        "produits": [
            {"codeProduit": "OS", "volumeBrut": 1_250_000},
            {"codeProduit": "IP", "volumeBrut": 5_900_000},
        ],
    }


def test_trafic_brut_choisit_la_requete_selon_inclure_exclus(monkeypatch, site_existant):
    fake = FakeDb(one=_scenario(), all_rows=[])
    monkeypatch.setattr(routes, "db_read", fake)

    _appeler_trafic_brut()
    assert fake.executed[-1][0] is helpers.SELECT_VOLUMES_BRUTS_SQL

    _appeler_trafic_brut(inclureExclus=True)
    assert fake.executed[-1][0] is helpers.SELECT_VOLUMES_BRUTS_TOUS_SQL


def test_trafic_brut_500_sur_erreur_technique(monkeypatch, site_existant):
    monkeypatch.setattr(routes, "db_read", FakeDb(one=_scenario(), boom=True))
    with pytest.raises(HTTPException) as exc:
        _appeler_trafic_brut()
    assert exc.value.status_code == 500
    assert exc.value.detail == (
        "Une erreur est survenue lors de la récupération des trafics."
    )


def test_trafic_brut_scenario_sans_tmh_renvoie_une_liste_vide(monkeypatch, site_existant):
    monkeypatch.setattr(routes, "db_read", FakeDb(one=_scenario(), all_rows=[]))
    assert _appeler_trafic_brut().produits == []


# --- 3. DSR-690 : liste des scénarios -------------------------------------------


def _appeler_liste(code_regate="123456"):
    payload = SiteScenariosRequest(codeRegate=code_regate)
    return asyncio.run(routes.site_liste_scenarios(payload, id_session_ihm=None))


def test_liste_scenarios_retourne_id_et_libelle(monkeypatch):
    fake = FakeDb(
        all_rows=[
            {"id_scenario": 125, "lb_scenario": "Scénario Septembre 2026"},
            {"id_scenario": 128, "lb_scenario": "Scénario Vieillissement"},
        ]
    )
    monkeypatch.setattr(routes, "db_read", fake)
    dump = _appeler_liste().model_dump(by_alias=True, exclude_none=True)
    assert dump == {
        "codeRegate": "123456",
        "scenarios": [
            {"id_scenario": 125, "lb_scenario": "Scénario Septembre 2026"},
            {"id_scenario": 128, "lb_scenario": "Scénario Vieillissement"},
        ],
    }


def test_liste_scenarios_message_si_aucun_resultat(monkeypatch):
    monkeypatch.setattr(routes, "db_read", FakeDb(all_rows=[]))
    reponse = _appeler_liste("654321")
    assert reponse.scenarios == []
    assert reponse.message == "Aucun scénario trouvé pour le site 654321."


def test_liste_scenarios_pas_de_message_quand_il_y_a_des_resultats(monkeypatch):
    monkeypatch.setattr(
        routes, "db_read", FakeDb(all_rows=[{"id_scenario": 1, "lb_scenario": "S1"}])
    )
    assert _appeler_liste().message is None


def test_liste_scenarios_filtre_valide_et_agrebal():
    """RG3 + RG4 : les deux conditions sont portées par la constante SQL."""
    sql = helpers.SELECT_SCENARIOS_EXPLOITABLES_SQL
    assert "statut = 'VALIDE'" in sql
    assert "trafic_agrebal_calcule = 1" in sql


def test_liste_scenarios_500_sur_erreur_technique(monkeypatch):
    monkeypatch.setattr(routes, "db_read", FakeDb(boom=True))
    with pytest.raises(HTTPException) as exc:
        _appeler_liste()
    assert exc.value.status_code == 500


# --- 4. Non-régression schéma ---------------------------------------------------

_DB_SQL = Path(__file__).resolve().parents[1] / "db" / "db_new.sql"


def _bloc_de(table: str) -> str:
    """Corps du CREATE TABLE d'une table dans le dump de schéma."""
    contenu = _DB_SQL.read_text(encoding="utf-8")
    bloc = re.search(rf"CREATE TABLE `{table}` \((.*?)\n\) ENGINE", contenu, re.S)
    assert bloc, f"Table {table} introuvable dans {_DB_SQL.name}"
    return bloc.group(1)


def _colonnes_de(table: str) -> set[str]:
    """Colonnes déclarées pour une table dans le dump de schéma."""
    return set(re.findall(r"^\s*`(\w+)`", _bloc_de(table), re.M))


@pytest.mark.parametrize(
    "sql, table, attendues",
    [
        (
            "SELECT_VOLUMES_BRUTS_SQL",
            "trppu_tmh",
            {
                "co_produit",
                "volume_realise",
                "volume_previsionnel",
                "volume_previsionnel_recalcule",
                "bl_exclu",
                "id_scenario",
            },
        ),
        (
            "SELECT_SCENARIOS_EXPLOITABLES_SQL",
            "trppu_scenario",
            {"id_scenario", "lb_scenario", "co_regate", "statut", "trafic_agrebal_calcule"},
        ),
    ],
)
def test_les_colonnes_citees_existent_en_base(sql, table, attendues):
    reelles = _colonnes_de(table)
    requete = getattr(helpers, sql)
    citees = {c for c in attendues if c in requete}
    assert citees == attendues, f"{sql} ne cite plus : {attendues - citees}"
    assert attendues <= reelles, f"{table} n'a pas : {attendues - reelles}"


def test_statuts_exploitables_existent_dans_lenum_de_la_base():
    """Les statuts acceptés doivent être des valeurs réelles de l'ENUM MySQL.

    Garde utile : l'ENUM en base contient SIMULATION, absent du Literal Python du
    module scénario — on ne veut pas dépendre de cette liste-là.
    """
    enum_statut = re.search(r"`statut`\s+enum\(([^)]*)\)", _bloc_de("trppu_scenario"))
    assert enum_statut, "ENUM statut de trppu_scenario introuvable"
    valeurs = set(re.findall(r"'([^']+)'", enum_statut.group(1)))
    assert set(helpers.STATUTS_EXPLOITABLES) <= valeurs
