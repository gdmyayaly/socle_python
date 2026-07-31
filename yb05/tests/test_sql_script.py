"""Tests du découpage et de l'exécution des scripts SQL.

Aucune base MySQL n'est nécessaire : ``aiomysql.connect`` est remplacé par une
connexion factice. Le code async est exécuté via ``asyncio.run`` pour ne pas
dépendre de pytest-asyncio.
"""

import asyncio
import logging

import pytest

from app.db import mysql
from app.db.sql_script import (
    ScriptResult,
    SqlScriptError,
    first_keyword,
    is_ddl,
    split_sql_script,
    statement_preview,
)


# ---------------------------------------------------------------------------
# Doublures
# ---------------------------------------------------------------------------


class FakeCursor:
    """Curseur factice : enregistre (sql, params) et échoue sur motif."""

    def __init__(self, conn):
        self._conn = conn
        self.rowcount = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, sql, params=None):
        self._conn.calls.append((sql, params))
        for motif in self._conn.fail_on:
            if motif in sql:
                raise RuntimeError(f"echec simule: {motif}")
        self.rowcount = 1
        return 1


class FakeConnection:
    """Connexion factice : trace l'ordre begin/commit/rollback/close."""

    def __init__(self, fail_on=()):
        self.calls: list[tuple[str, tuple | None]] = []
        self.events: list[str] = []
        self.fail_on = tuple(fail_on)

    def cursor(self, *args, **kwargs):
        return FakeCursor(self)

    async def begin(self):
        self.events.append("begin")

    async def commit(self):
        self.events.append("commit")

    async def rollback(self):
        self.events.append("rollback")

    async def ensure_closed(self):
        self.events.append("close")

    @property
    def sql(self) -> list[str]:
        """Raccourci de lecture : uniquement le SQL, sans les params."""
        return [s for s, _ in self.calls]


def _patch_connect(monkeypatch, conn):
    """Remplace aiomysql.connect ; retourne le dict des kwargs capturés."""
    captured: dict = {"count": 0}

    async def fake_connect(**kwargs):
        captured.update(kwargs)
        captured["count"] += 1
        return conn

    monkeypatch.setattr(mysql.aiomysql, "connect", fake_connect)
    return captured


def _patch_connect_interdit(monkeypatch):
    """Fait échouer tout appel à aiomysql.connect (vérifie l'absence de connexion)."""

    async def fake_connect(**kwargs):
        raise AssertionError("aiomysql.connect ne devait pas être appelé")

    monkeypatch.setattr(mysql.aiomysql, "connect", fake_connect)


def _db(**kwargs) -> mysql.Database:
    return mysql.Database(host="h", user="u", password="p", database="ma_base", **kwargs)


def _ecrire(tmp_path, nom: str, contenu: str, encoding: str = "utf-8"):
    chemin = tmp_path / nom
    chemin.write_text(contenu, encoding=encoding)
    return chemin


# Extrait représentatif de python/db/db_new.sql.
DUMP_EXTRAIT = """\
-- =============================================================================
-- Dump du schema de la base `dsr_mercure_aa`
-- =============================================================================

CREATE DATABASE IF NOT EXISTS `dsr_mercure_aa`;
USE `dsr_mercure_aa`;

SET FOREIGN_KEY_CHECKS = 0;

-- ----- TABLE `trppu_produit` -------------------------------------------------
DROP TABLE IF EXISTS `trppu_produit`;
CREATE TABLE `trppu_produit` (
  `co_produit` varchar(3) NOT NULL,
  `statut` enum('EN COURS','SIMULATION','VALIDE','EN PRODUCTION','ARCHIVE') NOT NULL,
  PRIMARY KEY (`co_produit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
"""


# ---------------------------------------------------------------------------
# Découpage
# ---------------------------------------------------------------------------


def test_split_instructions_simples():
    """Deux instructions séparées par ';', sans délimiteur résiduel."""
    stmts = split_sql_script("SELECT 1; SELECT 2;")
    assert stmts == ["SELECT 1", "SELECT 2"]


def test_split_ignore_bloc_de_commentaires_seul():
    """Une bannière de commentaires seule ne produit aucune instruction."""
    assert split_sql_script("-- ==========\n-- Titre\n-- ==========\n") == []


def test_split_ne_coupe_pas_sur_point_virgule_dans_chaine():
    """Un ';' à l'intérieur d'une chaîne littérale ne coupe pas l'instruction."""
    stmts = split_sql_script("INSERT INTO t (c) VALUES ('a;b');")
    assert len(stmts) == 1
    assert "a;b" in stmts[0]


def test_split_enum_avec_quotes_et_virgules():
    """Un enum('...','...') reste intact et forme une seule instruction."""
    sql = "CREATE TABLE t (s enum('EN COURS','SIMULATION','VALIDE') NOT NULL);"
    stmts = split_sql_script(sql)
    assert len(stmts) == 1
    assert "enum('EN COURS','SIMULATION','VALIDE')" in stmts[0]


def test_split_derniere_instruction_sans_point_virgule():
    """La dernière instruction est retournée même sans ';' final."""
    assert split_sql_script("SELECT 1;\nSELECT 2") == ["SELECT 1", "SELECT 2"]


@pytest.mark.parametrize("contenu", ["", "\n\n   \n", "   "])
def test_split_fichier_vide_ou_blanc(contenu):
    """Un script vide ou uniquement composé de blancs ne produit rien."""
    assert split_sql_script(contenu) == []


def test_split_commentaires_ligne_et_bloc():
    """Un ';' situé dans un commentaire ne coupe pas l'instruction."""
    sql = """\
-- commentaire ; avec point-virgule
# autre commentaire ; MySQL
/* bloc ; multi
   lignes ; */
SELECT 1;
"""
    stmts = split_sql_script(sql)
    assert len(stmts) == 1
    assert stmts[0].endswith("SELECT 1")


def test_split_delimiter_procedure():
    """Un bloc DELIMITER produit une seule instruction, ';' internes conservés."""
    sql = """\
SELECT 1;
DELIMITER $$
CREATE PROCEDURE p()
BEGIN
  SELECT 1;
  SELECT 2;
END$$
DELIMITER ;
SELECT 3;
"""
    stmts = split_sql_script(sql)
    assert len(stmts) == 3
    assert stmts[0] == "SELECT 1"
    assert stmts[1].startswith("CREATE PROCEDURE p()")
    assert stmts[1].count(";") == 2  # les ';' du corps sont préservés
    assert stmts[1].endswith("END")
    assert stmts[2] == "SELECT 3"
    assert not any("DELIMITER" in s.upper() for s in stmts)


def test_split_point_virgule_final_du_fichier():
    """Pas de fragment vide en queue de fichier."""
    assert split_sql_script("SELECT 1;\n\n-- fin\n") == ["SELECT 1"]


def test_split_extrait_reel_du_dump():
    """Découpage de l'extrait réel de db_new.sql : compte et mots-clés attendus."""
    stmts = split_sql_script(DUMP_EXTRAIT)
    assert len(stmts) == 6
    assert [first_keyword(s) for s in stmts] == [
        "CREATE", "USE", "SET", "DROP", "CREATE", "SET",
    ]


# ---------------------------------------------------------------------------
# is_ddl / first_keyword / statement_preview
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE t (a int)",
        "DROP TABLE IF EXISTS t",
        "ALTER TABLE t ADD COLUMN b int",
        "TRUNCATE TABLE t",
    ],
)
def test_is_ddl_vrai(sql):
    """Les instructions à COMMIT implicite sont bien détectées."""
    assert is_ddl(sql) is True


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t (a) VALUES (1)",
        "UPDATE t SET a = 1",
        "SELECT * FROM t",
        "SET FOREIGN_KEY_CHECKS = 0",
        "USE `ma_base`",
    ],
)
def test_is_ddl_faux(sql):
    """SET / USE / DML ne sont pas du DDL."""
    assert is_ddl(sql) is False


def test_is_ddl_ignore_le_commentaire_en_tete():
    """Une bannière en tête d'instruction ne masque pas le mot-clé DDL."""
    assert is_ddl("-- ===== TABLE t =====\nCREATE TABLE t (a int)") is True


def test_preview_retire_commentaires_et_tronque():
    """L'aperçu est mono-ligne, sans commentaire, et tronqué."""
    sql = "-- une bannière\nSELECT " + ", ".join(f"colonne_{i}" for i in range(40))
    apercu = statement_preview(sql)
    assert "--" not in apercu
    assert "\n" not in apercu
    assert len(apercu) <= 120
    assert apercu.endswith("…")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def test_script_ordre_et_commit(monkeypatch):
    """Les instructions passent dans l'ordre et la transaction est validée."""
    conn = FakeConnection()
    _patch_connect(monkeypatch, conn)

    res = asyncio.run(_db().execute_sql_script("SELECT 1; SELECT 2; SELECT 3;"))

    assert conn.sql == ["SELECT 1", "SELECT 2", "SELECT 3"]
    assert conn.events == ["begin", "commit", "close"]
    assert res.committed is True
    assert res.ok is True
    assert res.executed_count == 3
    assert res.total_count == 3


def test_script_aucun_parametre_transmis(monkeypatch):
    """Aucun params n'est passé : un '%' dans le SQL ne doit pas être interpolé."""
    conn = FakeConnection()
    _patch_connect(monkeypatch, conn)

    res = asyncio.run(
        _db().execute_sql_script("SELECT * FROM t WHERE co LIKE 'TRP%';")
    )

    assert all(params is None for _, params in conn.calls)
    assert conn.sql == ["SELECT * FROM t WHERE co LIKE 'TRP%'"]
    assert res.ok is True


def test_rollback_sur_erreur(monkeypatch):
    """Une erreur interrompt le script, déclenche un ROLLBACK et remonte le détail."""
    conn = FakeConnection(fail_on=["SELECT 2"])
    _patch_connect(monkeypatch, conn)

    with pytest.raises(SqlScriptError) as exc_info:
        asyncio.run(
            _db().execute_sql_script("SELECT 1; SELECT 2; SELECT 3;", label="mon_script")
        )

    err = exc_info.value
    assert err.source == "mon_script"
    assert err.index == 2
    assert err.statement == "SELECT 2"
    assert isinstance(err.original, RuntimeError)
    assert isinstance(err.result, ScriptResult)
    assert err.result.total_count == 2  # la 3e n'a jamais été tentée
    assert err.result.statements[1].error is not None
    assert err.result.committed is False
    assert "rollback" in conn.events
    assert "commit" not in conn.events
    assert conn.sql == ["SELECT 1", "SELECT 2"]


def test_continue_on_error_poursuit_et_valide(monkeypatch):
    """Avec continue_on_error, les instructions suivantes sont tentées et commitées."""
    conn = FakeConnection(fail_on=["SELECT 2"])
    _patch_connect(monkeypatch, conn)

    res = asyncio.run(
        _db().execute_sql_script("SELECT 1; SELECT 2; SELECT 3;", continue_on_error=True)
    )

    assert conn.sql == ["SELECT 1", "SELECT 2", "SELECT 3"]
    assert res.error_count == 1
    assert res.executed_count == 2
    assert res.ok is False
    assert conn.events == ["begin", "commit", "close"]


def test_dry_run_nouvre_aucune_connexion(monkeypatch):
    """Le dry_run analyse le script sans ouvrir la moindre connexion."""
    _patch_connect_interdit(monkeypatch)

    res = asyncio.run(_db().execute_sql_script("SELECT 1; DROP TABLE t;", dry_run=True))

    assert res.total_count == 2
    assert all(s.skipped for s in res.statements)
    assert all(s.rowcount == -1 for s in res.statements)
    assert res.committed is False
    assert res.executed_count == 0


def test_multi_fichiers_ordre(monkeypatch, tmp_path):
    """Les fichiers sont exécutés dans l'ordre, avec un index 1-based par fichier."""
    conn = FakeConnection()
    _patch_connect(monkeypatch, conn)
    f1 = _ecrire(tmp_path, "01_schema.sql", "SELECT 1;\nSELECT 2;\n")
    f2 = _ecrire(tmp_path, "02_data.sql", "SELECT 3;\n")

    res = asyncio.run(_db().execute_sql_files([f1, f2]))

    assert conn.sql == ["SELECT 1", "SELECT 2", "SELECT 3"]
    assert res.sources == [str(f1), str(f2)]
    assert [(s.source, s.index) for s in res.statements] == [
        (str(f1), 1),
        (str(f1), 2),
        (str(f2), 1),
    ]


def test_multi_fichiers_une_seule_transaction(monkeypatch, tmp_path):
    """Un échec dans le 2e fichier annule aussi le 1er : une seule transaction."""
    conn = FakeConnection(fail_on=["SELECT 3"])
    captured = _patch_connect(monkeypatch, conn)
    f1 = _ecrire(tmp_path, "01.sql", "SELECT 1;\nSELECT 2;\n")
    f2 = _ecrire(tmp_path, "02.sql", "SELECT 3;\n")

    with pytest.raises(SqlScriptError) as exc_info:
        asyncio.run(_db().execute_sql_files([f1, f2]))

    assert exc_info.value.source == str(f2)
    assert exc_info.value.index == 1
    assert captured["count"] == 1
    assert conn.events.count("begin") == 1
    assert conn.events.count("rollback") == 1
    assert "commit" not in conn.events


def test_fichier_absent_avant_toute_connexion(monkeypatch, tmp_path):
    """Un fichier manquant échoue sans qu'aucune connexion n'ait été ouverte."""
    _patch_connect_interdit(monkeypatch)
    existant = _ecrire(tmp_path, "ok.sql", "SELECT 1;\n")

    with pytest.raises(FileNotFoundError):
        asyncio.run(_db().execute_sql_files([existant, tmp_path / "absent.sql"]))


def test_non_transactionnel_autocommit(monkeypatch):
    """transactional=False ouvre la connexion en autocommit, sans BEGIN ni COMMIT."""
    conn = FakeConnection()
    captured = _patch_connect(monkeypatch, conn)

    res = asyncio.run(_db().execute_sql_script("SELECT 1;", transactional=False))

    assert captured["autocommit"] is True
    assert conn.events == ["close"]
    assert res.committed is False
    assert res.ok is True


def test_transactionnel_autocommit_false(monkeypatch):
    """transactional=True (défaut) ouvre la connexion avec autocommit désactivé."""
    conn = FakeConnection()
    captured = _patch_connect(monkeypatch, conn)

    asyncio.run(_db().execute_sql_script("SELECT 1;"))

    assert captured["autocommit"] is False


def test_disable_foreign_keys(monkeypatch):
    """disable_foreign_keys envoie SET FOREIGN_KEY_CHECKS = 0 en première instruction."""
    conn = FakeConnection()
    _patch_connect(monkeypatch, conn)

    asyncio.run(
        _db().execute_sql_script("DROP TABLE t;", disable_foreign_keys=True)
    )

    assert conn.sql[0] == "SET FOREIGN_KEY_CHECKS = 0"
    assert conn.sql[1] == "DROP TABLE t"


def test_database_selon_le_parametre(monkeypatch):
    """La sentinelle utilise le schéma de l'instance ; None se connecte sans schéma."""
    conn = FakeConnection()

    captured = _patch_connect(monkeypatch, conn)
    asyncio.run(_db().execute_sql_script("SELECT 1;"))
    assert captured["db"] == "ma_base"

    captured = _patch_connect(monkeypatch, FakeConnection())
    asyncio.run(_db().execute_sql_script("SELECT 1;", database=None))
    assert captured["db"] is None

    captured = _patch_connect(monkeypatch, FakeConnection())
    asyncio.run(_db().execute_sql_script("SELECT 1;", database="autre"))
    assert captured["db"] == "autre"


def test_avertissement_ddl_en_mode_transactionnel(monkeypatch, caplog):
    """Le DDL en mode transactionnel déclenche un avertissement explicite."""
    conn = FakeConnection()
    _patch_connect(monkeypatch, conn)

    with caplog.at_level(logging.WARNING, logger="app.db.mysql"):
        asyncio.run(_db().execute_sql_script("CREATE TABLE t (a int);"))

    messages = [r.getMessage() for r in caplog.records]
    assert any("DDL" in m and "ROLLBACK" in m for m in messages)


def test_pas_davertissement_ddl_sans_ddl(monkeypatch, caplog):
    """Un script purement DML ne déclenche aucun avertissement DDL."""
    conn = FakeConnection()
    _patch_connect(monkeypatch, conn)

    with caplog.at_level(logging.WARNING, logger="app.db.mysql"):
        asyncio.run(_db().execute_sql_script("INSERT INTO t (a) VALUES (1);"))

    assert not any("DDL" in r.getMessage() for r in caplog.records)


def test_encodage_personnalise(monkeypatch, tmp_path):
    """Un fichier latin-1 se lit avec encoding=latin-1 et échoue sans."""
    conn = FakeConnection()
    _patch_connect(monkeypatch, conn)
    fichier = _ecrire(tmp_path, "latin.sql", "-- créé\nSELECT 1;\n", encoding="latin-1")

    res = asyncio.run(_db().execute_sql_file(fichier, encoding="latin-1"))
    assert res.ok is True

    with pytest.raises(UnicodeDecodeError):
        asyncio.run(_db().execute_sql_file(fichier))


def test_bom_utf8_ignore(monkeypatch, tmp_path):
    """Le BOM UTF-8 est absorbé par l'encodage par défaut utf-8-sig."""
    conn = FakeConnection()
    _patch_connect(monkeypatch, conn)
    fichier = _ecrire(tmp_path, "bom.sql", "SELECT 1;\n", encoding="utf-8-sig")

    asyncio.run(_db().execute_sql_file(fichier))

    assert conn.sql == ["SELECT 1"]


def test_pas_de_retry_sur_instruction(monkeypatch):
    """Une instruction en échec n'est jamais rejouée, malgré max_retries=3."""
    conn = FakeConnection(fail_on=["SELECT 2"])
    _patch_connect(monkeypatch, conn)

    with pytest.raises(SqlScriptError):
        asyncio.run(_db(max_retries=3).execute_sql_script("SELECT 1; SELECT 2;"))

    assert conn.sql.count("SELECT 2") == 1


def test_le_runner_nutilise_pas_le_pool(monkeypatch):
    """Le script passe par une connexion dédiée, jamais par le pool."""
    conn = FakeConnection()
    _patch_connect(monkeypatch, conn)

    async def pool_interdit():
        raise AssertionError("le pool ne devait pas être utilisé")

    db = _db()
    monkeypatch.setattr(db, "_ensure_pool", pool_interdit)

    asyncio.run(db.execute_sql_script("SELECT 1;"))

    assert db._pool is None
