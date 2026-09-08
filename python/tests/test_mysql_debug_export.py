"""Tests des exports MySQL de debug (`/mysql/export`, `/mysql/dump`).

Ces routes servent de canal de rapatriement de données entre environnements : ce qu'elles
produisent doit se recharger **à l'identique**, sur des tables pouvant atteindre plusieurs
dizaines de millions de lignes. Les tests couvrent donc trois familles :

1. **Fidélité du littéral SQL** — un caractère mal échappé (quote, backslash, NUL, Ctrl-Z)
   ou un TIME mal formaté casse silencieusement le rechargement, parfois des milliers de
   lignes plus loin. C'est le point le plus coûteux à diagnostiquer a posteriori.
2. **Découpage** — la requête doit être triée dès qu'on tronque (sinon deux lots peuvent
   se recouvrir ou sauter des lignes) et ne PAS l'être sur un export intégral (un tri sur
   30 M de lignes pour rien).
3. **Contrat de streaming** — sortie JSON analysable et réinjectable via `POST /import`,
   marqueur de fin présent, et sortie explicitement marquée incomplète si le flux casse
   en cours de route (le code HTTP, lui, est déjà parti).

Comme le reste de la suite : appel direct des fonctions d'endpoint avec de faux `db_read`,
pas de TestClient ni de base réelle.
"""

import asyncio
import json
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
from fastapi import HTTPException

from app.routes import mysql_debug
from app.routes.mysql_debug import (
    ImportPayload,
    _reprise,
    _select_rows_sql,
    _sql_literal,
    _tamponner,
    _time_mysql,
)


# --- Outils communs -------------------------------------------------------------


class FakeDb:
    """`db_read` minimal : sert le schéma, le DDL et les lignes, et retient le SQL joué."""

    def __init__(self, *, colonnes=None, pk=None, lignes=None, ddl=None, boom_apres=None):
        self.colonnes = colonnes if colonnes is not None else ["id", "lb"]
        self.pk = pk if pk is not None else ["id"]
        self.lignes = lignes or []
        self.ddl = ddl if ddl is not None else "CREATE TABLE `t` (`id` int)"
        self.boom_apres = boom_apres  # lève après N lignes streamées
        self.sql_lignes: list[tuple[str, tuple]] = []

    async def fetch_all(self, sql, params=None):
        if "key_column_usage" in sql:
            return [{"column_name": c} for c in self.pk]
        if "information_schema.columns" in sql:
            return [{"column_name": c} for c in self.colonnes]
        if "information_schema.tables" in sql:
            return [{"table_name": "t", "table_type": "BASE TABLE"}]
        raise AssertionError(f"fetch_all inattendu : {sql}")

    async def fetch_one(self, sql, params=None):
        if sql.startswith("SHOW CREATE"):
            return {"Create Table": self.ddl}
        raise AssertionError(f"fetch_one inattendu : {sql}")

    async def iter_rows(self, sql, params=None, chunk_size=1000):
        self.sql_lignes.append((sql, params))
        for i, ligne in enumerate(self.lignes):
            if self.boom_apres is not None and i >= self.boom_apres:
                raise RuntimeError("connexion perdue")
            yield ligne


async def _lire(response) -> str:
    """Consomme une StreamingResponse et rend le corps complet en texte."""
    morceaux = []
    async for m in response.body_iterator:
        morceaux.append(m if isinstance(m, bytes) else str(m).encode("utf-8"))
    return b"".join(morceaux).decode("utf-8")


def _appeler_export(**kwargs):
    """Appelle l'endpoint avec tous ses paramètres (pas d'injection FastAPI ici)."""
    params = dict(
        table="t",
        fmt="json",
        schema=False,
        data=True,
        drop=False,
        truncate=True,
        limit=None,
        after=None,
        offset=0,
        rows_per_insert=2,
        download=False,
    )
    params.update(kwargs)

    async def _run():
        response = await mysql_debug.export_table(**params)
        return response, await _lire(response)

    return asyncio.run(_run())


def _appeler_dump(**kwargs):
    params = dict(
        fmt="sql",
        drop=True,
        data=True,
        limit_per_table=None,
        rows_per_insert=2,
        download=False,
    )
    params.update(kwargs)

    async def _run():
        response = await mysql_debug.dump_create_sql(**params)
        return response, await _lire(response)

    return asyncio.run(_run())


# --- 1. Fidélité du littéral SQL ------------------------------------------------


def test_time_mysql_depasse_24h_et_gere_le_negatif():
    """`str(timedelta)` donnerait '1 day, 1:00:00' / '-1 day, 23:00:00', refusés par MySQL."""
    assert _time_mysql(timedelta(minutes=90)) == "01:30:00"
    assert _time_mysql(timedelta(hours=25)) == "25:00:00"
    assert _time_mysql(timedelta(hours=-1)) == "-01:00:00"


@pytest.mark.parametrize(
    "valeur, attendu",
    [
        (None, "NULL"),
        (True, "1"),
        (False, "0"),
        (42, "42"),
        (Decimal("1.50"), "1.50"),
        (date(2026, 9, 7), "'2026-09-07'"),
        (datetime(2026, 9, 7, 14, 30, 5), "'2026-09-07 14:30:05'"),
        (timedelta(hours=25), "'25:00:00'"),
        (b"\x00\xff", "0x00ff"),
        (b"", "''"),
    ],
)
def test_sql_literal_types(valeur, attendu):
    assert _sql_literal(valeur) == attendu


def test_sql_literal_echappe_les_caracteres_qui_cassent_un_script():
    """NUL et Ctrl-Z tronquent la valeur s'ils partent bruts (Ctrl-Z = EOF sous Windows)."""
    assert _sql_literal("O'Brien") == "'O\\'Brien'"
    assert _sql_literal("a\\b") == "'a\\\\b'"
    assert _sql_literal("l1\nl2") == "'l1\\nl2'"
    assert _sql_literal("a\x00b") == "'a\\0b'"
    assert _sql_literal("a\x1ab") == "'a\\Zb'"


# --- 2. Découpage ---------------------------------------------------------------


def test_select_sans_decoupage_ne_trie_pas():
    """Un export intégral doit laisser MySQL suivre l'index clusterisé, sans tri."""
    sql, params = _select_rows_sql("t", pk_cols=["id"])
    assert "ORDER BY" not in sql
    assert "LIMIT" not in sql
    assert params == ()


def test_select_avec_limit_trie_sur_la_pk():
    """Sans ORDER BY, deux lots LIMIT/OFFSET peuvent se recouvrir ou sauter des lignes."""
    sql, params = _select_rows_sql("t", limit=10_000, pk_cols=["id"])
    assert "ORDER BY `id`" in sql
    assert sql.rstrip().endswith("LIMIT %s")
    assert params == (10_000,)


def test_select_avec_after_attaque_lindex_au_lieu_de_relire():
    sql, params = _select_rows_sql("t", limit=100, pk_cols=["id"], after="500")
    assert "WHERE `id` > %s" in sql
    assert "ORDER BY `id`" in sql
    assert params == ("500", 100)


def test_select_offset_seul_ajoute_le_limit_sentinelle():
    """MySQL refuse OFFSET sans LIMIT : 2^64-1 signifie « tout le reste »."""
    sql, params = _select_rows_sql("t", offset=50, pk_cols=["id"])
    assert "LIMIT 18446744073709551615 OFFSET %s" in sql
    assert params == (50,)


def test_select_pk_composite_trie_sur_toutes_les_colonnes():
    sql, _ = _select_rows_sql("t", limit=10, pk_cols=["a", "b"])
    assert "ORDER BY `a`, `b`" in sql


def test_select_sans_pk_ne_trie_pas_meme_en_decoupant():
    """Rien à trier : le cas est signalé par un WARNING côté route, pas bloqué."""
    sql, _ = _select_rows_sql("t", limit=10, pk_cols=[])
    assert "ORDER BY" not in sql


@pytest.mark.parametrize(
    "stats, limit, attendu",
    [
        ({"rows": 10, "last_pk": 42}, None, None),  # pas de découpage : pas de suite
        ({"rows": 3, "last_pk": 42}, 10, None),  # lot incomplet : fin de table
        ({"rows": 10, "last_pk": 42}, 10, "42"),  # lot plein : il reste probablement
        ({"rows": 10}, 10, None),  # pas de PK mono-colonne
        ({"rows": 10, "last_pk": b"\x01"}, 10, None),  # PK binaire : non transportable
    ],
)
def test_reprise(stats, limit, attendu):
    assert _reprise(stats, limit) == attendu


# --- 3. Contrat de streaming ----------------------------------------------------


def test_tamponner_regroupe_les_fragments():
    async def _flux():
        for _ in range(5):
            yield "x" * 10

    async def _run():
        return [m async for m in _tamponner(_flux())]

    blocs = asyncio.run(_run())
    assert b"".join(blocs) == b"x" * 50
    assert len(blocs) == 1  # sous le seuil de tampon : un seul bloc


def test_export_sql_structure_et_donnees(monkeypatch):
    fake = FakeDb(
        colonnes=["id", "lb"],
        lignes=[{"id": 1, "lb": "a"}, {"id": 2, "lb": "b"}, {"id": 3, "lb": "c"}],
        ddl="CREATE TABLE `t` (`id` int, `lb` varchar(10))",
    )
    monkeypatch.setattr(mysql_debug, "db_read", fake)
    _, corps = _appeler_export(fmt="sql", schema=True, data=True, rows_per_insert=2)

    assert "CREATE TABLE `t`" in corps
    assert "SET FOREIGN_KEY_CHECKS = 0;" in corps
    # rows_per_insert=2 sur 3 lignes : deux INSERT, dont un multi-lignes.
    assert corps.count("INSERT INTO `t`") == 2
    assert "(1,'a'),\n(2,'b')" in corps
    assert "-- FIN DE L'EXPORT — 3 ligne(s)" in corps


def test_export_sql_sans_donnees_ne_produit_aucun_insert(monkeypatch):
    fake = FakeDb(lignes=[{"id": 1, "lb": "a"}])
    monkeypatch.setattr(mysql_debug, "db_read", fake)
    _, corps = _appeler_export(fmt="sql", schema=True, data=False)

    assert "CREATE TABLE" in corps
    assert "INSERT INTO" not in corps
    assert fake.sql_lignes == []  # la table n'a même pas été lue


def test_export_sql_drop_rend_le_truncate_inutile(monkeypatch):
    """DROP + CREATE recrée la table vide : un TRUNCATE ferait double emploi."""
    monkeypatch.setattr(mysql_debug, "db_read", FakeDb(lignes=[{"id": 1, "lb": "a"}]))
    _, corps = _appeler_export(fmt="sql", schema=True, drop=True, truncate=True)
    assert "DROP TABLE IF EXISTS `t`;" in corps
    assert "TRUNCATE TABLE" not in corps

    monkeypatch.setattr(mysql_debug, "db_read", FakeDb(lignes=[{"id": 1, "lb": "a"}]))
    _, corps = _appeler_export(fmt="sql", schema=False, truncate=True)
    assert "TRUNCATE TABLE `t`;" in corps


def test_export_sql_annonce_le_lot_suivant(monkeypatch):
    monkeypatch.setattr(
        mysql_debug,
        "db_read",
        FakeDb(lignes=[{"id": 1, "lb": "a"}, {"id": 2, "lb": "b"}]),
    )
    _, corps = _appeler_export(fmt="sql", limit=2)
    assert "-- Lot suivant : &after=2" in corps


def test_export_json_est_analysable_et_reinjectable(monkeypatch):
    """La sortie doit pouvoir être renvoyée telle quelle à POST /mysql/import."""
    fake = FakeDb(
        colonnes=["id", "dt", "montant", "blob"],
        lignes=[
            {
                "id": 1,
                "dt": datetime(2026, 9, 7, 14, 30, 5),
                "montant": Decimal("12.30"),
                "blob": b"\x01\x02",
            }
        ],
    )
    monkeypatch.setattr(mysql_debug, "db_read", fake)
    response, corps = _appeler_export(fmt="json")

    assert response.media_type == "application/json; charset=utf-8"
    charge = json.loads(corps)
    assert charge["table"] == "t"
    assert charge["columns"] == ["id", "dt", "montant", "blob"]
    assert charge["count"] == 1
    assert charge["complete"] is True
    assert charge["rows"][0]["dt"] == "2026-09-07 14:30:05"
    assert charge["rows"][0]["montant"] == "12.30"
    assert charge["rows"][0]["blob"] == {"__b64__": "AQI="}

    # Le corps est un ImportPayload valide : le round-trip export -> import tient.
    payload = ImportPayload(**charge)
    assert payload.table == "t"
    assert len(payload.rows) == 1


def test_export_json_sans_donnees_garde_un_tableau_rows_vide(monkeypatch):
    """`rows` reste présent pour que la sortie soit toujours acceptée par /import."""
    monkeypatch.setattr(mysql_debug, "db_read", FakeDb(lignes=[{"id": 1, "lb": "a"}]))
    _, corps = _appeler_export(fmt="json", schema=True, data=False)
    charge = json.loads(corps)
    assert charge["rows"] == []
    assert charge["count"] == 0
    assert "create_sql" in charge


def test_export_json_signale_une_coupure_en_cours_de_flux(monkeypatch):
    """Les en-têtes sont partis : l'incident ne peut plus être qu'un marqueur dans le corps."""
    fake = FakeDb(
        lignes=[{"id": i, "lb": "x"} for i in range(5)],
        boom_apres=2,
    )
    monkeypatch.setattr(mysql_debug, "db_read", fake)
    _, corps = _appeler_export(fmt="json")

    charge = json.loads(corps)  # doit rester du JSON analysable
    assert charge["complete"] is False
    assert "connexion perdue" in charge["error"]
    # Le compteur reflète ce qui est réellement sorti, pas 0 : c'est ce qui permet de
    # savoir jusqu'où l'export a tenu.
    assert charge["count"] == 2
    assert len(charge["rows"]) == 2


def test_export_sql_signale_une_coupure_en_cours_de_flux(monkeypatch):
    fake = FakeDb(lignes=[{"id": i, "lb": "x"} for i in range(5)], boom_apres=2)
    monkeypatch.setattr(mysql_debug, "db_read", fake)
    _, corps = _appeler_export(fmt="sql")

    assert "EXPORT INCOMPLET" in corps
    assert "FIN DE L'EXPORT" not in corps


def test_export_404_table_inconnue(monkeypatch):
    monkeypatch.setattr(mysql_debug, "db_read", FakeDb(colonnes=[]))
    with pytest.raises(HTTPException) as exc:
        _appeler_export()
    assert exc.value.status_code == 404


def test_export_400_si_ni_schema_ni_donnees(monkeypatch):
    monkeypatch.setattr(mysql_debug, "db_read", FakeDb())
    with pytest.raises(HTTPException) as exc:
        _appeler_export(schema=False, data=False)
    assert exc.value.status_code == 400
    assert "Rien à exporter" in exc.value.detail


@pytest.mark.parametrize("pk", [[], ["a", "b"]])
def test_export_400_si_after_sans_pk_mono_colonne(monkeypatch, pk):
    monkeypatch.setattr(mysql_debug, "db_read", FakeDb(pk=pk))
    with pytest.raises(HTTPException) as exc:
        _appeler_export(after="10")
    assert exc.value.status_code == 400
    assert "after" in exc.value.detail


def test_export_download_pose_le_content_disposition(monkeypatch):
    monkeypatch.setattr(mysql_debug, "db_read", FakeDb(lignes=[]))
    response, _ = _appeler_export(fmt="sql", download=True)
    assert response.headers["content-disposition"].startswith("attachment; filename=")


def test_dump_limite_les_lignes_par_table(monkeypatch):
    fake = FakeDb(lignes=[{"id": 1, "lb": "a"}, {"id": 2, "lb": "b"}])
    monkeypatch.setattr(mysql_debug, "db_read", fake)
    _, corps = _appeler_dump(fmt="sql", data=True, limit_per_table=10_000)

    assert "au plus 10000 ligne(s) par table" in corps
    sql, params = fake.sql_lignes[-1]
    assert "ORDER BY `id`" in sql  # tri requis dès qu'on tronque
    assert params == (10_000,)


def test_dump_sans_donnees_ne_lit_aucune_table(monkeypatch):
    fake = FakeDb(lignes=[{"id": 1, "lb": "a"}])
    monkeypatch.setattr(mysql_debug, "db_read", fake)
    _, corps = _appeler_dump(fmt="sql", data=False)
    assert "CREATE TABLE" in corps
    assert "INSERT INTO" not in corps
    assert fake.sql_lignes == []


def test_dump_json_sans_donnees_fournit_le_script(monkeypatch):
    monkeypatch.setattr(mysql_debug, "db_read", FakeDb(lignes=[]))
    _, corps = _appeler_dump(fmt="json", data=False)
    charge = json.loads(corps)
    assert charge["complete"] is True
    assert "CREATE TABLE" in charge["sql"]
    assert charge["objects"][0]["type"] == "TABLE"


def test_dump_json_reste_analysable_si_les_colonnes_sont_illisibles(monkeypatch):
    """L'échec survient avant l'ouverture de "rows" : ne pas émettre de ] orphelin."""

    class DbSansColonnes(FakeDb):
        async def fetch_all(self, sql, params=None):
            if "information_schema.columns" in sql:
                raise RuntimeError("colonnes illisibles")
            return await super().fetch_all(sql, params)

    monkeypatch.setattr(mysql_debug, "db_read", DbSansColonnes(lignes=[{"id": 1}]))
    _, corps = _appeler_dump(fmt="json", data=True)
    charge = json.loads(corps)
    assert "colonnes illisibles" in charge["objects"][0]["data_error"]
    assert "rows" not in charge["objects"][0]


def test_dump_json_avec_donnees_omet_le_script(monkeypatch):
    """Reproduire tout le script en mémoire annulerait le bénéfice du streaming."""
    monkeypatch.setattr(mysql_debug, "db_read", FakeDb(lignes=[{"id": 1, "lb": "a"}]))
    _, corps = _appeler_dump(fmt="json", data=True)
    charge = json.loads(corps)
    assert "sql" not in charge
    assert "note" in charge
    assert charge["objects"][0]["rows"] == [{"id": 1, "lb": "a"}]
    assert charge["row_count"] == 1


# --- 4. Curseur streamé (app/db/mysql.py) ---------------------------------------
#
# C'est la brique qui rend les exports tenables : sans elle, aiomysql bufferise le
# résultat complet côté client et une table de 30 M de lignes fait tomber le process.


class _FauxCurseur:
    def __init__(self, lignes, taille_lot):
        self._restantes = list(lignes)
        self._taille_lot = taille_lot
        self.ferme = False

    async def execute(self, query, params=None):
        self.query = query
        self.params = params

    async def fetchmany(self, n):
        lot, self._restantes = self._restantes[:n], self._restantes[n:]
        return lot

    async def close(self):
        self.ferme = True


class _FausseConnexion:
    def __init__(self, lignes):
        self._lignes = lignes
        self.fermee = False
        self.curseur = None

    async def cursor(self, *_types):
        self.curseur = _FauxCurseur(self._lignes, 2)
        return self.curseur

    def close(self):
        self.fermee = True


class _FauxPool:
    def __init__(self, lignes):
        self.conn = _FausseConnexion(lignes)
        self.relachee = False

    async def acquire(self):
        return self.conn

    def release(self, conn):
        self.relachee = True


def _db_avec(lignes):
    from app.db.mysql import Database

    db = Database()
    pool = _FauxPool(lignes)
    db._pool = pool
    return db, pool


def test_iter_rows_streame_toutes_les_lignes_et_rend_la_connexion():
    lignes = [{"id": i} for i in range(5)]
    db, pool = _db_avec(lignes)

    async def _run():
        return [r async for r in db.iter_rows("SELECT 1", chunk_size=2)]

    assert asyncio.run(_run()) == lignes
    assert pool.conn.curseur.ferme is True  # curseur clos proprement
    assert pool.conn.fermee is False  # connexion réutilisable
    assert pool.relachee is True


def test_iter_rows_ferme_la_connexion_si_le_flux_est_abandonne():
    """Un abandon laisse le reste du résultat dans le tampon réseau : drainer coûterait
    un scan complet, la connexion est donc sacrifiée plutôt que rendue au pool."""
    db, pool = _db_avec([{"id": i} for i in range(1000)])

    async def _run():
        vues = []
        async for row in db.iter_rows("SELECT 1", chunk_size=2):
            vues.append(row)
            if len(vues) == 3:
                break  # déclenche le GeneratorExit
        return vues

    assert len(asyncio.run(_run())) == 3
    assert pool.conn.fermee is True
    assert pool.conn.curseur.ferme is False  # pas de close() : pas de drainage
    assert pool.relachee is True
