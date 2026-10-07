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
    MYSQL_COLLATION,
    MYSQL_DATABASE,
    MYSQL_HOST_WRITE,
    MYSQL_HOST_READ,
    MYSQL_MAX_RETRIES,
    MYSQL_PASSWORD_READ,
    MYSQL_PASSWORD_WRITE,
    MYSQL_POOL_RECYCLE,
    MYSQL_PORT,
    MYSQL_RETRY_DELAY,
    MYSQL_TCP_KEEPALIVE,
    MYSQL_USER_WRITE,
    MYSQL_USER_READ,
    NB_WORKER,
    SQL_SCRIPT_WARN_SIZE,
)
from app.db.sql_script import (
    ScriptResult,
    SqlScriptError,
    StatementResult,
    is_ddl,
    split_sql_script,
    statement_preview,
)
from app.log_utils import ctx

logger = logging.getLogger(__name__)

# Sentinelle de `database` : "" = self.database, None = sans schéma, "xxx" = schéma explicite.
_CONFIGURED_DB = ""


def _activer_keepalive(conn: Any) -> None:
    """Active le keepalive TCP (une fois par connexion).

    Sans lui, une requête longue laisse la connexion muette et un équipement réseau la coupe
    après son délai d'inactivité (erreur 2013). Options absentes de la plateforme ignorées.
    """
    if getattr(conn, "_yb05_keepalive", False):
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
        conn._yb05_keepalive = True
    except AttributeError:  # pragma: no cover - objet sans __dict__
        pass


def _init_command() -> str:
    """Aligne la collation de connexion sur la base (ou SGBD_COLLATION) : évite l'erreur 1267."""
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
                    # Renouvelle une connexion restée inactive au-delà de `wait_timeout`.
                    pool_recycle=MYSQL_POOL_RECYCLE,
                )
                logger.info(
                    "Connexion au pool MySQL établie %s",
                    ctx(
                        hote=self.host,
                        port=self.port,
                        base=self.database,
                        taille_max=self.max_connections,
                        recyclage_s=MYSQL_POOL_RECYCLE,
                        keepalive_s=MYSQL_TCP_KEEPALIVE,
                    ),
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
        """Transaction : commit à la sortie, rollback si exception."""
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
    ) -> ScriptResult:
        """Équivalent à ``execute_sql_files([path], ...)``."""
        return await self.execute_sql_files(
            [path],
            transactional=transactional,
            continue_on_error=continue_on_error,
            dry_run=dry_run,
            encoding=encoding,
            database=database,
            disable_foreign_keys=disable_foreign_keys,
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
    ) -> ScriptResult:
        """Exécute plusieurs fichiers .sql dans l'ordre, sur une seule connexion (via db_write).

        Les fichiers sont lus et découpés avant toute connexion. ``transactional`` encadre
        le tout d'un BEGIN/COMMIT, mais le DDL fait un COMMIT implicite MySQL : l'atomicité
        ne vaut que pour le DML, d'où des scripts rejouables. ``database=None`` : sans schéma.
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
    ) -> ScriptResult:
        """Comme ``execute_sql_files`` pour un script en chaîne (``label`` = source dans les logs)."""
        return await self._run_units(
            [(label, split_sql_script(script))],
            transactional=transactional,
            continue_on_error=continue_on_error,
            dry_run=dry_run,
            database=database,
            disable_foreign_keys=disable_foreign_keys,
        )

    @asynccontextmanager
    async def _script_connection(self, database: str | None, autocommit: bool):
        """Connexion dédiée hors pool : l'état de session du script (USE, SET …) ne doit pas
        contaminer le pool, et ``autocommit`` doit pouvoir être choisi."""
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

    async def _run_statement(self, conn, sql: str) -> int:
        async with conn.cursor() as cur:
            # Aucun paramètre : PyMySQL appliquerait `query % args` et casserait les `%` du SQL.
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
    ) -> ScriptResult:
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
                        logger.debug(
                            "Instruction SQL %s",
                            ctx(source=label, index=i, apercu=entry.preview),
                        )

                        t0 = time.perf_counter()
                        try:
                            entry.rowcount = await self._run_statement(conn, sql)
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
                executees=result.executed_count,
                instructions=result.total_count,
                duration_ms=result.duration_ms,
                erreurs=result.error_count,
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


# Pools dimensionnés sur NB_WORKER (DSR-704) : une connexion par worker et par pool,
# sinon les workers au-delà de 10 attendraient sur `pool.acquire()`.
_TAILLE_POOL = max(10, NB_WORKER)

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
