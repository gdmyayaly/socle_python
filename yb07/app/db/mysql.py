"""Connexion MySQL asynchrone : pools, retry, transactions et exécution de scripts .sql."""

import asyncio
import logging
import socket
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Sequence

import aiomysql

from app.config import (
    MYSQL_DATABASE,
    MYSQL_HOST_WRITE,
    MYSQL_HOST_READ,
    MYSQL_MAX_RETRIES,
    MYSQL_PASSWORD_READ,
    MYSQL_COLLATION,
    MYSQL_PASSWORD_WRITE,
    MYSQL_PORT,
    MYSQL_RETRY_DELAY,
    MYSQL_USER_WRITE,
    MYSQL_USER_READ,
    MYSQL_POOL_RECYCLE,
    MYSQL_TCP_KEEPALIVE,
    MYSQL_POOL_SIZE,
    MYSQL_SUIVI_INSTRUCTION,
    SQL_SCRIPT_WARN_SIZE,
)
from app.db.sql_script import (
    ScriptResult,
    SqlScriptError,
    StatementResult,
    is_ddl,
    is_display_select,
    is_write,
    split_sql_script,
    statement_preview,
)
from app.log_utils import ctx

logger = logging.getLogger(__name__)

# Sentinelle de `database` : "" = self.database, None = sans schéma, "xxx" = schéma explicite.
_CONFIGURED_DB = ""


def _activer_keepalive(conn: Any) -> None:
    """Active le keepalive TCP sur la socket d'une connexion (une seule fois).

    Une instruction longue laisse la connexion muette : un équipement réseau à délai
    d'inactivité la couperait (erreur 2013) alors que MySQL travaille encore.
    """
    if getattr(conn, "_yb07_keepalive", False):
        return
    transport = getattr(conn, "_writer", None)
    sock = transport.get_extra_info("socket") if transport is not None else None
    if sock is None:
        return
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        for nom, valeur in (
            ("TCP_KEEPIDLE", MYSQL_TCP_KEEPALIVE),
            ("TCP_KEEPINTVL", max(5, MYSQL_TCP_KEEPALIVE // 2)),
            ("TCP_KEEPCNT", 5),
        ):
            option = getattr(socket, nom, None)
            if option is not None:
                sock.setsockopt(socket.IPPROTO_TCP, option, valeur)
    except OSError as erreur:
        logger.debug("Keepalive TCP non activé %s", ctx(motif=str(erreur)))
        return
    try:
        conn._yb07_keepalive = True
    except AttributeError:  # pragma: no cover - objet sans __dict__
        pass


# Suivi d'une instruction longue, lu depuis une autre connexion : état côté serveur.
SUIVI_ETAT_SQL = """
SELECT STATE AS etat, TIME AS secondes_serveur
  FROM information_schema.PROCESSLIST
 WHERE ID = %s
"""
# Phase et avancement d'un ALTER InnoDB ; vide si les instruments performance_schema
# correspondants sont désactivés côté serveur.
SUIVI_ETAPE_SQL = """
SELECT s.EVENT_NAME AS etape, s.WORK_COMPLETED AS fait, s.WORK_ESTIMATED AS estime
  FROM performance_schema.events_stages_current s
  JOIN performance_schema.threads t ON t.THREAD_ID = s.THREAD_ID
 WHERE t.PROCESSLIST_ID = %s
"""


def _thread_id(conn: Any) -> int | None:
    """Identifiant MySQL de la session (celui de SHOW PROCESSLIST), ou None."""
    lire = getattr(conn, "thread_id", None)
    try:
        return int(lire()) if callable(lire) else None
    except Exception:  # noqa: BLE001 - connexion factice ou fermée : pas de suivi fin
        return None


def _init_command() -> str:
    """Instruction d'ouverture de connexion alignant son classement sur celui de la base.

    Sinon littéraux et variables `@…` (utf8mb4_general_ci) comparés aux colonnes
    (utf8mb4_0900_ai_ci) lèvent l'erreur 1267. SGBD_COLLATION peut l'imposer.
    """
    if MYSQL_COLLATION:
        return f"SET collation_connection = '{MYSQL_COLLATION}'"
    return "SET collation_connection = @@collation_database"


class Database:
    """Classe utilitaire pour la connexion MySQL avec pool, retry et transactions."""

    def __init__(
        self,
        host: str = MYSQL_HOST_WRITE,
        port: int = MYSQL_PORT,
        user: str = MYSQL_USER_WRITE,
        password: str = MYSQL_PASSWORD_WRITE,
        database: str = MYSQL_DATABASE,
        min_connections: int = 1,
        max_connections: int = 10,
        max_retries: int = MYSQL_MAX_RETRIES,
        retry_delay: float = MYSQL_RETRY_DELAY,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._pool: aiomysql.Pool | None = None

    async def connect(self) -> None:
        """Crée le pool de connexions avec mécanisme de retry."""
        for attempt in range(1, self.max_retries + 1):
            try:
                self._pool = await aiomysql.create_pool(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    db=self.database,
                    charset="utf8mb4",
                    init_command=_init_command(),
                    minsize=self.min_connections,
                    maxsize=self.max_connections,
                    autocommit=True,
                    # Renouvelle une connexion inactive avant que `wait_timeout` ne la coupe.
                    pool_recycle=MYSQL_POOL_RECYCLE,
                )
                logger.info(
                    "Connexion au pool MySQL établie %s",
                    ctx(hote=self.host, port=self.port, base=self.database),
                )
                return
            except Exception as e:
                logger.warning(
                    "Tentative de connexion MySQL échouée %s",
                    ctx(
                        tentative=attempt,
                        max_tentatives=self.max_retries,
                        hote=self.host,
                        erreur=str(e),
                    ),
                )
                if attempt == self.max_retries:
                    raise
                await asyncio.sleep(self.retry_delay * attempt)

    async def disconnect(self) -> None:
        """Ferme le pool de connexions."""
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
            logger.info("Pool MySQL fermé %s", ctx(hote=self.host))

    async def _ensure_pool(self) -> aiomysql.Pool:
        if self._pool is None:
            logger.debug("Connexion lazy à MySQL %s", ctx(hote=self.host))
            await self.connect()
        return self._pool

    async def execute(
        self, query: str, params: tuple | None = None, retries: int | None = None
    ) -> int:
        """Exécute une requête INSERT/UPDATE/DELETE et retourne le nombre de lignes affectées."""
        max_retries = retries if retries is not None else self.max_retries
        pool = await self._ensure_pool()

        for attempt in range(1, max_retries + 1):
            try:
                async with pool.acquire() as conn:
                    _activer_keepalive(conn)
                    async with conn.cursor() as cur:
                        await cur.execute(query, params)
                        return cur.rowcount
            except Exception as e:
                logger.warning(
                    "Tentative d'exécution MySQL échouée %s",
                    ctx(tentative=attempt, max_tentatives=max_retries, erreur=str(e)),
                )
                if attempt == max_retries:
                    raise
                await asyncio.sleep(self.retry_delay * attempt)

    async def fetch_one(
        self, query: str, params: tuple | None = None
    ) -> dict[str, Any] | None:
        """Exécute une requête SELECT et retourne une seule ligne sous forme de dict."""
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            _activer_keepalive(conn)
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, params)
                return await cur.fetchone()

    async def fetch_all(
        self, query: str, params: tuple | None = None
    ) -> list[dict[str, Any]]:
        """Exécute une requête SELECT et retourne toutes les lignes sous forme de list[dict]."""
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            _activer_keepalive(conn)
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, params)
                return await cur.fetchall()

    @asynccontextmanager
    async def transaction(self):
        """Transaction : commit à la sortie du bloc, rollback sur exception."""
        pool = await self._ensure_pool()
        conn = await pool.acquire()
        _activer_keepalive(conn)
        try:
            await conn.begin()
            yield _TransactionCursor(conn)
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
        finally:
            pool.release(conn)

    # ------------------------------------------------------------------
    # Exécution de scripts SQL (fichiers .sql)
    # ------------------------------------------------------------------

    async def execute_sql_file(
        self,
        path: str | Path,
        *,
        transactional: bool = True,
        continue_on_error: bool = False,
        dry_run: bool = False,
        encoding: str = "utf-8-sig",
        database: str | None = _CONFIGURED_DB,
        disable_foreign_keys: bool = False,
        skip_selects: bool = False,
    ) -> ScriptResult:
        """Exécute un fichier .sql (cf. ``execute_sql_files``)."""
        return await self.execute_sql_files(
            [path],
            transactional=transactional,
            continue_on_error=continue_on_error,
            dry_run=dry_run,
            encoding=encoding,
            database=database,
            disable_foreign_keys=disable_foreign_keys,
            skip_selects=skip_selects,
        )

    async def execute_sql_files(
        self,
        paths: Sequence[str | Path],
        *,
        transactional: bool = True,
        continue_on_error: bool = False,
        dry_run: bool = False,
        encoding: str = "utf-8-sig",
        database: str | None = _CONFIGURED_DB,
        disable_foreign_keys: bool = False,
        skip_selects: bool = False,
    ) -> ScriptResult:
        """Exécute plusieurs fichiers .sql dans l'ordre, sur une seule connexion dédiée.

        Fichiers lus et découpés avant toute connexion. ``transactional`` : un seul
        BEGIN/COMMIT, mais le DDL fait un COMMIT implicite et n'est jamais annulé.
        ``database=None`` : connexion sans schéma. ``skip_selects`` : les SELECT
        d'affichage ne sont pas joués (la base ferait le travail pour rien).
        Lève ``SqlScriptError`` (avec le résultat partiel) sauf ``continue_on_error``.
        """
        units: list[tuple[str, list[str]]] = []
        for path in paths:
            p = Path(path)
            size = p.stat().st_size
            if size > SQL_SCRIPT_WARN_SIZE:
                logger.warning(
                    "Script SQL volumineux %s",
                    ctx(
                        fichier=str(p),
                        taille_octets=size,
                        seuil_octets=SQL_SCRIPT_WARN_SIZE,
                        consequence="chargé intégralement en mémoire",
                    ),
                )
            units.append((str(p), split_sql_script(p.read_text(encoding=encoding))))

        return await self._run_units(
            units,
            transactional=transactional,
            continue_on_error=continue_on_error,
            dry_run=dry_run,
            database=database,
            disable_foreign_keys=disable_foreign_keys,
            skip_selects=skip_selects,
        )

    async def execute_sql_script(
        self,
        script: str,
        *,
        label: str = "<script>",
        transactional: bool = True,
        continue_on_error: bool = False,
        dry_run: bool = False,
        database: str | None = _CONFIGURED_DB,
        disable_foreign_keys: bool = False,
        skip_selects: bool = False,
    ) -> ScriptResult:
        """Exécute un script SQL fourni en chaîne (cf. ``execute_sql_files``)."""
        return await self._run_units(
            [(label, split_sql_script(script))],
            transactional=transactional,
            continue_on_error=continue_on_error,
            dry_run=dry_run,
            database=database,
            disable_foreign_keys=disable_foreign_keys,
            skip_selects=skip_selects,
        )

    async def execute_sql_units(
        self,
        units: Sequence[tuple[str, Sequence[str]]],
        *,
        transactional: bool = True,
        continue_on_error: bool = False,
        dry_run: bool = False,
        database: str | None = _CONFIGURED_DB,
        disable_foreign_keys: bool = False,
        skip_selects: bool = False,
    ) -> ScriptResult:
        """Exécute des scripts déjà découpés `(libellé, instructions)`, dans l'ordre.

        Une seule connexion (et transaction) pour des milliers de petits scripts par site.
        """
        return await self._run_units(
            [(label, list(statements)) for label, statements in units],
            transactional=transactional,
            continue_on_error=continue_on_error,
            dry_run=dry_run,
            database=database,
            disable_foreign_keys=disable_foreign_keys,
            skip_selects=skip_selects,
        )

    @asynccontextmanager
    async def _script_connection(self, database: str | None, autocommit: bool):
        """Connexion dédiée hors pool : l'état de session du script (USE, SET, @vars)
        ne doit pas contaminer le pool, et ``autocommit`` reste au choix."""
        conn = None
        for attempt in range(1, self.max_retries + 1):
            try:
                conn = await aiomysql.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    db=database,
                    charset="utf8mb4",
                    init_command=_init_command(),
                    autocommit=autocommit,
                )
                _activer_keepalive(conn)
                break
            except Exception as e:
                logger.warning(
                    "Tentative de connexion MySQL (script SQL) échouée %s",
                    ctx(
                        tentative=attempt,
                        max_tentatives=self.max_retries,
                        erreur=str(e),
                    ),
                )
                if attempt == self.max_retries:
                    raise
                await asyncio.sleep(self.retry_delay * attempt)
        try:
            yield conn
        finally:
            await conn.ensure_closed()

    async def _executer_avec_suivi(
        self, conn, sql: str, label: str, index: int, apercu: str
    ) -> int:
        """Exécute une instruction ; une écriture journalise son avancement en cours."""
        if not is_write(sql):
            return await self._run_statement(conn, sql)
        suivi = asyncio.ensure_future(self._suivre(conn, label, index, apercu))
        try:
            return await self._run_statement(conn, sql)
        finally:
            suivi.cancel()
            await asyncio.gather(suivi, return_exceptions=True)

    async def _suivre(self, conn, label: str, index: int, apercu: str) -> None:
        """Boucle de suivi d'une instruction en cours ; ne lève jamais."""
        debut = time.perf_counter()
        session = _thread_id(conn)
        etapes_lisibles = session is not None
        while True:
            await asyncio.sleep(MYSQL_SUIVI_INSTRUCTION)
            infos: dict[str, Any] = {}
            if session is not None:
                try:
                    ligne = await self.fetch_one(SUIVI_ETAT_SQL, (session,)) or {}
                    infos["etat"] = ligne.get("etat")
                except Exception:  # noqa: BLE001 - voir docstring
                    pass
            if etapes_lisibles:
                try:
                    etape = await self.fetch_one(SUIVI_ETAPE_SQL, (session,)) or {}
                except Exception:  # noqa: BLE001 - droits absents : on n'insiste pas
                    etapes_lisibles = False
                    etape = {}
                if etape.get("etape"):
                    infos["phase"] = str(etape["etape"]).rsplit("/", 1)[-1]
                    if etape.get("estime"):
                        infos["pct"] = round(100 * etape["fait"] / etape["estime"], 1)
            logger.info(
                "Avancement instruction SQL %s",
                ctx(
                    source=label,
                    index=index,
                    apercu=apercu,
                    **infos,
                    duration_ms=round((time.perf_counter() - debut) * 1000, 1),
                ),
            )

    async def _run_statement(self, conn, sql: str) -> int:
        """Exécute une instruction unique sur la connexion du script."""
        async with conn.cursor() as cur:
            # Aucun paramètre : avec `args` non None, PyMySQL appliquerait `query % args`
            # et casserait tout `%` littéral (LIKE 'TRP%').
            await cur.execute(sql)
            return cur.rowcount

    async def _run_units(
        self,
        units: list[tuple[str, list[str]]],
        *,
        transactional: bool,
        continue_on_error: bool,
        dry_run: bool,
        database: str | None,
        disable_foreign_keys: bool,
        skip_selects: bool = False,
    ) -> ScriptResult:
        """Cœur de l'exécution : parcourt les scripts déjà lus et découpés."""
        started = time.perf_counter()
        result = ScriptResult(
            sources=[label for label, _ in units],
            transactional=transactional,
            dry_run=dry_run,
        )
        total = sum(len(stmts) for _, stmts in units)
        nb_ddl = sum(1 for _, stmts in units for sql in stmts if is_ddl(sql))

        logger.info(
            "Début script SQL %s",
            ctx(
                sources=", ".join(result.sources),
                instructions=total,
                transactionnel=transactional,
                dry_run=dry_run,
            ),
        )

        if transactional and nb_ddl and not dry_run:
            logger.warning(
                "Script SQL : DDL en mode transactionnel %s",
                ctx(
                    nb_ddl=nb_ddl,
                    consequence="COMMIT implicite MySQL, un ROLLBACK ne les annulera PAS",
                ),
            )
        if transactional and continue_on_error and not dry_run:
            logger.warning(
                "Script SQL : continue_on_error en mode transactionnel %s",
                ctx(consequence="transaction validée malgré les erreurs"),
            )

        if dry_run:
            for label, stmts in units:
                for i, sql in enumerate(stmts, start=1):
                    result.statements.append(
                        StatementResult(
                            source=label,
                            index=i,
                            preview=statement_preview(sql),
                            is_ddl=is_ddl(sql),
                            skipped=True,
                        )
                    )
            result.duration_ms = (time.perf_counter() - started) * 1000
            logger.info(
                "Fin script SQL (dry_run) %s",
                ctx(instructions=total, duration_ms=result.duration_ms),
            )
            return result

        db = self.database if database == _CONFIGURED_DB else database

        async with self._script_connection(db, autocommit=not transactional) as conn:
            try:
                if transactional:
                    await conn.begin()
                if disable_foreign_keys:
                    await self._run_statement(conn, "SET FOREIGN_KEY_CHECKS = 0")
                    logger.warning(
                        "Script SQL : contraintes de clés étrangères désactivées %s",
                        ctx(portee="connexion"),
                    )

                for label, stmts in units:
                    for i, sql in enumerate(stmts, start=1):
                        entry = StatementResult(
                            source=label,
                            index=i,
                            preview=statement_preview(sql),
                            is_ddl=is_ddl(sql),
                        )
                        result.statements.append(entry)
                        if skip_selects and is_display_select(sql):
                            entry.skipped = True
                            logger.debug(
                                "Instruction SQL non jouée %s",
                                ctx(source=label, index=i, motif="SELECT d'affichage"),
                            )
                            continue
                        if is_write(sql):
                            # Annoncée avant : une écriture peut durer des minutes.
                            logger.info(
                                "Début instruction SQL %s",
                                ctx(source=label, index=i, apercu=entry.preview),
                            )
                        else:
                            logger.debug(
                                "Instruction SQL %s",
                                ctx(source=label, index=i, apercu=entry.preview),
                            )

                        t0 = time.perf_counter()
                        try:
                            entry.rowcount = await self._executer_avec_suivi(
                                conn, sql, label, i, entry.preview
                            )
                        except Exception as e:
                            entry.error = str(e)
                            entry.duration_ms = (time.perf_counter() - t0) * 1000
                            if continue_on_error:
                                logger.warning(
                                    "Instruction SQL ignorée %s",
                                    ctx(
                                        source=label,
                                        index=i,
                                        motif="continue_on_error",
                                        erreur=str(e),
                                        apercu=entry.preview,
                                    ),
                                )
                                continue
                            logger.exception(
                                "Erreur instruction SQL %s",
                                ctx(source=label, index=i, apercu=entry.preview),
                            )
                            raise SqlScriptError(
                                f"Échec du script SQL {label} à l'instruction {i}",
                                source=label,
                                index=i,
                                statement=sql,
                                original=e,
                                result=result,
                            ) from e
                        entry.duration_ms = (time.perf_counter() - t0) * 1000
                        if is_write(sql):
                            logger.info(
                                "Fin instruction SQL %s",
                                ctx(
                                    source=label,
                                    index=i,
                                    apercu=entry.preview,
                                    lignes=entry.rowcount,
                                    duration_ms=round(entry.duration_ms, 1),
                                ),
                            )

                if transactional:
                    await conn.commit()
                    result.committed = True
            except BaseException:
                if transactional:
                    try:
                        await conn.rollback()
                        logger.warning(
                            "Script SQL : ROLLBACK effectué %s",
                            ctx(reserve="le DDL déjà exécuté n'est pas annulé"),
                        )
                    except Exception as rb:
                        logger.exception(
                            "Erreur ROLLBACK script SQL %s", ctx(erreur=str(rb))
                        )
                raise
            finally:
                result.duration_ms = (time.perf_counter() - started) * 1000

        logger.info(
            "Fin script SQL %s",
            ctx(
                sources=", ".join(result.sources),
                executees=result.executed_count,
                selects_non_joues=sum(s.skipped for s in result.statements) or None,
                instructions=result.total_count,
                erreurs=result.error_count,
                commit=result.committed if transactional else None,
                duration_ms=result.duration_ms,
            ),
        )
        return result


class _TransactionCursor:
    """Wrapper de connexion utilisé à l'intérieur d'une transaction."""

    def __init__(self, conn: aiomysql.Connection):
        self._conn = conn

    async def execute(self, query: str, params: tuple | None = None) -> int:
        async with self._conn.cursor() as cur:
            await cur.execute(query, params)
            return cur.rowcount

    async def execute_many(self, query: str, params_seq) -> int:
        """Exécute une requête en masse (INSERT/UPDATE) sur la connexion de la transaction."""
        async with self._conn.cursor() as cur:
            await cur.executemany(query, params_seq)
            return cur.rowcount

    async def fetch_one(
        self, query: str, params: tuple | None = None
    ) -> dict[str, Any] | None:
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, params)
            return await cur.fetchone()

    async def fetch_all(
        self, query: str, params: tuple | None = None
    ) -> list[dict[str, Any]]:
        async with self._conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query, params)
            return await cur.fetchall()


# Instances globales. Une tâche parallèle tient une connexion par pool : au-delà de
# MYSQL_POOL_SIZE tâches, les suivantes attendent sur `pool.acquire()`.
_TAILLE_POOL = MYSQL_POOL_SIZE

db_write = Database(
    host=MYSQL_HOST_WRITE,
    user=MYSQL_USER_WRITE,
    password=MYSQL_PASSWORD_WRITE,
    max_connections=_TAILLE_POOL,
)
db_read = Database(
    host=MYSQL_HOST_READ,
    user=MYSQL_USER_READ,
    password=MYSQL_PASSWORD_READ,
    max_connections=_TAILLE_POOL,
)
