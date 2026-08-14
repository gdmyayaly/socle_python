"""Tests de la persistance du volume brut d'une ligne TMH (`trppu_tmh.volume_brut`).

La colonne existait en base mais n'était alimentée par aucun chemin d'écriture :
seul OPTIPACC (DSR-689) recalculait la valeur à la volée. Elle est désormais
écrite par tous les INSERT / UPDATE du module TMH. Ces tests couvrent :

1. l'arithmétique de `compute_volume_brut` (constaté + prévisionnel recalculé) ;
2. sa présence effective dans chaque requête d'écriture (INSERT, UPDATE batch,
   PATCH ciblé DSR-649) et dans les SELECT du module ;
3. sa cohérence avec la somme calculée par `SELECT_VOLUMES_BRUTS_SQL`, rejouée
   sur SQLite : les deux formes doivent donner exactement le même résultat, sans
   quoi OPTIPACC et la colonne stockée diverge­raient.
"""

import asyncio
import re
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from app.routes.trppu_optipacc.helpers import SELECT_VOLUMES_BRUTS_SQL
from app.routes.trppu_scenario.helpers import DUPLICATE_CHILD_SPECS
from app.routes.trppu_tmh import helpers, routes
from app.routes.trppu_tmh.helpers import (
    SELECT_TMH_BY_ID_SQL,
    SELECT_TMH_SQL,
    compute_volume_brut,
    insert_tmh_row,
    resolve_previsionnel_recalcule,
    update_tmh_row,
    upsert_tmh_rows,
)
from app.routes.trppu_tmh.schemas import TmhOut, TmhUpsert, TmhVolumeUpdate

# Position de volume_brut dans les paramètres des deux requêtes d'écriture.
_IDX_BRUT_INSERT = 5  # id_scenario, co_produit, realise, prev, prev_recalcule, BRUT
_IDX_BRUT_UPDATE = 4  # co_produit, realise, prev, prev_recalcule, BRUT


class FakeTx:
    """Transaction factice : journalise (sql, params) et sert des réponses scriptées."""

    def __init__(self, fetch_one_results=None):
        self.calls: list[tuple[str, tuple | None]] = []
        self._fetch_one_results = list(fetch_one_results or [])

    async def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return 1

    async def fetch_one(self, sql, params=None):
        self.calls.append((sql, params))
        return self._fetch_one_results.pop(0) if self._fetch_one_results else None


def _insert(**kwargs):
    """Joue insert_tmh_row et renvoie (sql, params) de l'INSERT."""
    tx = FakeTx(fetch_one_results=[{"id": 1}])
    base = {
        "co_produit": "OS",
        "volume_realise": 1_000_000,
        "volume_previsionnel": 200_000,
        "moyenne_journaliere": Decimal("1.00"),
        "moyenne_hebdo": Decimal("2.00"),
        "bl_exclu": False,
    }
    asyncio.run(insert_tmh_row(tx, 12, **{**base, **kwargs}))
    return tx.calls[0]


def _update(**kwargs):
    """Joue update_tmh_row (ligne existante) et renvoie (sql, params) de l'UPDATE."""
    tx = FakeTx(fetch_one_results=[{"id_tmh": 7}])
    base = {
        "co_produit": "OS",
        "volume_realise": 1_000_000,
        "volume_previsionnel": 200_000,
        "moyenne_journaliere": Decimal("1.00"),
        "moyenne_hebdo": Decimal("2.00"),
        "bl_exclu": False,
    }
    asyncio.run(update_tmh_row(tx, 12, 7, **{**base, **kwargs}))
    return tx.calls[1]  # calls[0] = SELECT de contrôle d'appartenance


# --- 1. Arithmétique -------------------------------------------------------------


def test_volume_brut_somme_constate_et_previsionnel_recalcule():
    """RG4 (DSR-689) : constaté + prévisionnel recalculé."""
    assert compute_volume_brut(1_000_000, 200_000, 250_000) == 1_250_000


def test_volume_brut_retombe_sur_le_previsionnel_quand_le_recalcule_est_absent():
    """Aucune variation appliquée : le prévisionnel de base fait foi."""
    assert compute_volume_brut(700_000, 91_000, None) == 791_000


def test_volume_brut_prend_un_recalcule_nul_au_pied_de_la_lettre():
    """0 n'est pas 'absent' : une variation de -100 % ramène bien le brut au constaté."""
    assert compute_volume_brut(700_000, 91_000, 0) == 700_000
    assert resolve_previsionnel_recalcule(91_000, 0) == 0


def test_volume_brut_tolere_les_volumes_null():
    assert compute_volume_brut(None, None, None) == 0
    assert compute_volume_brut(None, 5_000, None) == 5_000


def test_volume_brut_accepte_un_constate_negatif():
    """volume_realise autorise les valeurs négatives (cf. TmhVolumeUpdate)."""
    assert compute_volume_brut(-500, 2_000, None) == 1_500


# --- 2. Écriture effective -------------------------------------------------------


def test_insert_ecrit_le_volume_brut():
    sql, params = _insert(volume_previsionnel_recalcule=250_000)
    assert "volume_brut" in sql
    assert params[_IDX_BRUT_INSERT] == 1_250_000
    # Autant de placeholders que de valeurs : un oubli décalerait toutes les colonnes.
    assert sql.count("%s") == len(params)


def test_insert_sans_recalcule_stocke_le_brut_du_previsionnel_de_base():
    _, params = _insert()
    assert params[_IDX_BRUT_INSERT] == 1_200_000


def test_update_ecrit_le_volume_brut():
    sql, params = _update(volume_previsionnel_recalcule=250_000)
    assert "volume_brut = %s" in sql
    assert params[_IDX_BRUT_UPDATE] == 1_250_000
    assert sql.count("%s") == len(params)


def test_upsert_batch_ecrit_le_volume_brut_sur_chaque_ligne():
    """Chemin utilisé par PUT /tmh, la création (DSR-634) et la MAJ scénario (DSR-656)."""
    tx = FakeTx(fetch_one_results=[{"id_tmh": 7}, {"id": 1}])
    items = [
        TmhUpsert(
            id_tmh=7,
            co_produit="OS",
            volume_realise=1_000_000,
            volume_previsionnel=200_000,
            volume_previsionnel_recalcule=250_000,
            moyenne_journaliere=Decimal("1.00"),
            moyenne_hebdo=Decimal("2.00"),
        ),
        TmhUpsert(
            co_produit="IP",
            volume_realise=500_000,
            volume_previsionnel=0,
            moyenne_journaliere=Decimal("1.00"),
            moyenne_hebdo=Decimal("2.00"),
        ),
    ]
    nb_ins, nb_upd = asyncio.run(upsert_tmh_rows(tx, 12, items, id_rh="RH-TOKEN"))
    assert (nb_ins, nb_upd) == (1, 1)

    ecritures = [(sql, p) for sql, p in tx.calls if "volume_brut" in sql]
    assert len(ecritures) == 2
    update_sql, update_params = ecritures[0]
    insert_sql, insert_params = ecritures[1]
    assert update_sql.startswith("UPDATE trppu_tmh")
    assert update_params[_IDX_BRUT_UPDATE] == 1_250_000
    assert insert_sql.startswith("INSERT INTO trppu_tmh")
    assert insert_params[_IDX_BRUT_INSERT] == 500_000


def test_patch_cible_recalcule_le_volume_brut(monkeypatch):
    """DSR-649 : la correction manuelle du constaté doit rafraîchir le brut.

    Le prévisionnel recalculé étant réaligné sur `volume_previsionnel`, le brut
    est écrit à partir du volume reçu et de cette colonne, non modifiée ici.
    """
    tx = FakeTx(fetch_one_results=[{"id_tmh": 7, "co_produit": "OS"}])

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

    monkeypatch.setattr(routes, "db_write", FakeDbWrite())
    monkeypatch.setattr(routes, "fetch_scenario_or_404", _scenario)

    payload = TmhVolumeUpdate(
        volume_realise=1_500_000,
        moyenne_journaliere=Decimal("1.00"),
        moyenne_hebdo=Decimal("2.00"),
    )
    asyncio.run(routes.update_tmh_volume(12, 7, payload))

    sql, params = tx.calls[0]
    assert "volume_brut = COALESCE(%s, 0) + COALESCE(volume_previsionnel, 0)" in sql
    assert params.count(1_500_000) == 2  # volume_realise + terme du volume brut
    assert sql.count("%s") == len(params)


def test_patch_exclusion_ne_touche_pas_au_volume_brut():
    """bl_exclu est un filtre d'agrégation : le brut de la ligne reste inchangé."""
    source = Path(routes.__file__).read_text(encoding="utf-8")
    bloc = source[source.index("async def toggle_tmh_exclusion") :]
    assert "volume_brut" not in bloc[: bloc.index("async def delete_tmh")]


# --- 3. Lecture, duplication et cohérence ---------------------------------------


def test_le_volume_brut_est_restitue_en_lecture():
    for sql in (SELECT_TMH_SQL, SELECT_TMH_BY_ID_SQL):
        assert "volume_brut" in sql
    assert "volume_brut" in TmhOut.model_fields


def test_le_volume_brut_est_recopie_a_la_duplication():
    """Un scénario dupliqué doit conserver le brut de ses lignes TMH."""
    cols = next(c for table, c, _ in DUPLICATE_CHILD_SPECS if table == "trppu_tmh")
    assert "volume_brut" in cols


def test_le_volume_brut_nest_pas_saisissable_par_le_client():
    """Valeur dérivée : l'accepter en entrée permettrait de la désynchroniser."""
    assert "volume_brut" not in TmhUpsert.model_fields
    assert "volume_brut" not in TmhVolumeUpdate.model_fields


_DDL_TMH = """
CREATE TABLE trppu_tmh (
    id_tmh INTEGER PRIMARY KEY AUTOINCREMENT,
    id_scenario INTEGER NOT NULL,
    co_produit TEXT NOT NULL,
    volume_realise INTEGER,
    volume_previsionnel INTEGER,
    volume_previsionnel_recalcule INTEGER,
    bl_exclu INTEGER NOT NULL DEFAULT 0
)
"""

_LIGNES = [
    (1_000_000, 200_000, 250_000),
    (700_000, 91_000, None),
    (None, None, None),
    (-500, 2_000, None),
    (500_000, 0, 0),
]


def test_colonne_stockee_et_somme_optipacc_donnent_le_meme_resultat():
    """Garde anti-divergence : la colonne écrite et la somme DSR-689 sont une seule
    et même formule. Elle est rejouée telle quelle sur SQLite (SUM/COALESCE standard)."""
    cx = sqlite3.connect(":memory:")
    cx.execute(_DDL_TMH)
    cx.executemany(
        "INSERT INTO trppu_tmh (id_scenario, co_produit, volume_realise, "
        "volume_previsionnel, volume_previsionnel_recalcule) VALUES (?, ?, ?, ?, ?)",
        [(12, f"P{i}", *ligne) for i, ligne in enumerate(_LIGNES)],
    )
    attendu = {
        f"P{i}": compute_volume_brut(*ligne) for i, ligne in enumerate(_LIGNES)
    }
    obtenu = dict(
        cx.execute(SELECT_VOLUMES_BRUTS_SQL.replace("%s", "?"), (12,)).fetchall()
    )
    cx.close()
    assert obtenu == attendu


# --- 4. Non-régression schéma ----------------------------------------------------

_DB_SQL = Path(__file__).resolve().parents[1] / "db" / "db_new.sql"


@pytest.mark.parametrize("requete", ["SELECT_TMH_SQL", "SELECT_TMH_BY_ID_SQL"])
def test_les_colonnes_lues_existent_en_base(requete):
    bloc = re.search(
        r"CREATE TABLE `trppu_tmh` \((.*?)\n\) ENGINE",
        _DB_SQL.read_text(encoding="utf-8"),
        re.S,
    )
    assert bloc, f"Table trppu_tmh introuvable dans {_DB_SQL.name}"
    reelles = set(re.findall(r"^\s*`(\w+)`", bloc.group(1), re.M))
    citees = {
        c
        for c in re.findall(r"\w+", getattr(helpers, requete).split("FROM")[0])
        if c in reelles or c.startswith("volume_") or c.startswith("bl_")
    }
    assert "volume_brut" in citees
    assert citees <= reelles, f"{requete} cite des colonnes absentes : {citees - reelles}"
