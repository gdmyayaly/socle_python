"""Connexion asynchrone à MySQL avec pool de connexions, retry, transactions et
exécution de scripts SQL (fichiers .sql)."""

import asyncio
import logging
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
    MYSQL_PASSWORD_WRITE,
    MYSQL_PORT,
    MYSQL_RETRY_DELAY,
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

logger = logging.getLogger(__name__)

# Sentinelle pour le paramètre `database` des exécutions de script :
#   ""     -> utiliser self.database (comportement par défaut)
#   None   -> se connecter SANS schéma sélectionné
#   "xxx"  -> schéma explicite
_CONFIGURED_DB = ""


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
                    minsize=self.min_connections,
                    maxsize=self.max_connections,
                    autocommit=True,
                )
                logger.info("Connexion au pool MySQL réussie.")
                return
            except Exception as e:
                logger.warning(
                    "Tentative %d/%d de connexion MySQL échouée : %s",
                    attempt,
                    self.max_retries,
                    e,
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
            logger.info("Pool MySQL fermé.")

    async def _ensure_pool(self) -> aiomysql.Pool:
        if self._pool is None:
            logger.info("Connexion lazy à MySQL (premier appel)...")
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
                    async with conn.cursor() as cur:
                        await cur.execute(query, params)
                        return cur.rowcount
            except Exception as e:
                logger.warning(
                    "Tentative %d/%d pour execute échouée : %s",
                    attempt,
                    max_retries,
                    e,
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
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, params)
                return await cur.fetchone()

    async def fetch_all(
        self, query: str, params: tuple | None = None
    ) -> list[dict[str, Any]]:
        """Exécute une requête SELECT et retourne toutes les lignes sous forme de list[dict]."""
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, params)
                return await cur.fetchall()

    @asynccontextmanager
    async def transaction(self):
        """Context manager pour exécuter des requêtes dans une transaction.

        Usage:
            async with db.transaction() as tx:
                await tx.execute("INSERT INTO ...", (...))
                await tx.execute("UPDATE ...", (...))
            # commit automatique à la sortie, rollback en cas d'exception
        """
        pool = await self._ensure_pool()
        conn = await pool.acquire()
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
        """Exécute un fichier .sql, instruction par instruction.

        Équivalent à ``execute_sql_files([path], ...)`` — voir cette méthode pour le
        détail des options et les limites.
        """
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
        """Exécute plusieurs fichiers .sql **dans l'ordre**, sur une seule connexion.

        Tous les fichiers sont lus et découpés **avant** l'ouverture de la connexion :
        un fichier manquant ou mal encodé échoue sans que la base ait été touchée.

        Args:
            paths: chemins des fichiers, exécutés dans l'ordre donné.
            transactional: si True (défaut), l'ensemble des fichiers est encadré par
                un unique BEGIN/COMMIT, avec ROLLBACK en cas d'échec. Si False, chaque
                instruction est validée immédiatement (autocommit).
            continue_on_error: si False (défaut), la première erreur interrompt le
                script et lève ``SqlScriptError``. Si True, l'erreur est journalisée,
                enregistrée dans le résultat, et l'exécution se poursuit.
            dry_run: lit et découpe les fichiers sans **aucune** connexion à la base ;
                le résultat liste les instructions avec ``skipped=True``.
            encoding: encodage de lecture. Le défaut ``utf-8-sig`` décode aussi
                l'UTF-8 nu et absorbe le BOM ajouté par certains outils de dump.
            database: schéma à sélectionner. Par défaut celui de l'instance ; ``None``
                pour se connecter **sans schéma** (nécessaire si le script fait lui-même
                ``CREATE DATABASE`` puis ``USE``).
            disable_foreign_keys: exécute ``SET FOREIGN_KEY_CHECKS = 0`` avant la
                première instruction, sur la même connexion.

        Returns:
            ScriptResult: détail instruction par instruction (aperçu, rowcount, durée,
            erreur éventuelle) et compteurs agrégés.

        Raises:
            SqlScriptError: échec d'une instruction avec ``continue_on_error=False``.
                L'exception porte le fichier, l'index 1-based, le SQL complet,
                l'exception d'origine et le ``ScriptResult`` partiel.
            FileNotFoundError, UnicodeDecodeError: à la lecture, avant toute connexion.

        LIMITE IMPORTANTE — MySQL effectue un COMMIT IMPLICITE sur les instructions
        DDL (CREATE, DROP, ALTER, TRUNCATE, RENAME…). Le mode ``transactional=True``
        ne garantit donc l'atomicité QUE pour le DML (INSERT/UPDATE/DELETE). Un script
        de schéma qui échoue à mi-parcours laisse la base dans un état intermédiaire :
        prévoir un script rejouable (``DROP TABLE IF EXISTS`` / ``CREATE TABLE``).
        Un avertissement est journalisé dès qu'une instruction DDL est détectée en
        mode transactionnel.

        Note: à utiliser via ``db_write`` — ``db_read`` porte des identifiants en
        lecture seule.
        """
        units: list[tuple[str, list[str]]] = []
        for path in paths:
            p = Path(path)
            size = p.stat().st_size
            if size > SQL_SCRIPT_WARN_SIZE:
                logger.warning(
                    "Script SQL %s : %d octets chargés intégralement en mémoire.",
                    p,
                    size,
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
        """Exécute un script SQL fourni sous forme de chaîne.

        Mêmes options et mêmes limites que ``execute_sql_files`` ; ``label`` sert
        d'identifiant de source dans le résultat et les logs.
        """
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
        """Connexion dédiée (HORS POOL) pour l'exécution d'un script SQL.

        Hors pool volontairement : un script modifie l'état de session (``USE``,
        ``SET FOREIGN_KEY_CHECKS``, variables de session). Rendre une telle connexion
        au pool contaminerait toutes les requêtes applicatives suivantes. Ici l'état
        de session disparaît avec la connexion, fermée en fin de script.

        C'est aussi ce qui garantit que ``SET FOREIGN_KEY_CHECKS = 0`` et les DROP qui
        suivent tournent bien sur la **même** session, et qui permet de choisir
        ``autocommit`` (le pool, lui, est créé avec ``autocommit=True``).
        """
        conn = None
        for attempt in range(1, self.max_retries + 1):
            try:
                conn = await aiomysql.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    db=database,
                    autocommit=autocommit,
                )
                break
            except Exception as e:
                logger.warning(
                    "Tentative %d/%d de connexion MySQL (script SQL) échouée : %s",
                    attempt,
                    self.max_retries,
                    e,
                )
                if attempt == self.max_retries:
                    raise
                await asyncio.sleep(self.retry_delay * attempt)
        try:
            yield conn
        finally:
            await conn.ensure_closed()

    async def _run_statement(self, conn, sql: str) -> int:
        """Exécute une instruction unique sur la connexion du script."""
        async with conn.cursor() as cur:
            # ATTENTION : aucun paramètre n'est transmis. PyMySQL applique
            # `query % args` dès que `args is not None` — même un tuple vide ferait
            # échouer toute instruction contenant un `%` (LIKE 'TRP%', DEFAULT '100%').
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
            "Script SQL %s : %d instruction(s), transactionnel=%s, dry_run=%s.",
            ", ".join(result.sources),
            total,
            transactional,
            dry_run,
        )

        if transactional and nb_ddl and not dry_run:
            logger.warning(
                "Script SQL : %d instruction(s) DDL détectée(s). MySQL effectue un "
                "COMMIT implicite sur le DDL : un ROLLBACK ne les annulera PAS.",
                nb_ddl,
            )
        if transactional and continue_on_error and not dry_run:
            logger.warning(
                "Script SQL : continue_on_error=True en mode transactionnel — la "
                "transaction sera validée malgré les erreurs rencontrées."
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
                "Script SQL en dry_run : %d instruction(s) analysée(s), aucune exécution.",
                total,
            )
            return result

        db = self.database if database == _CONFIGURED_DB else database

        async with self._script_connection(db, autocommit=not transactional) as conn:
            try:
                if transactional:
                    await conn.begin()
                if disable_foreign_keys:
                    await self._run_statement(conn, "SET FOREIGN_KEY_CHECKS = 0")
                    logger.info(
                        "Script SQL : contraintes de clés étrangères désactivées sur la connexion."
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
                        logger.debug("[%s] instruction %d : %s", label, i, entry.preview)

                        t0 = time.perf_counter()
                        try:
                            entry.rowcount = await self._run_statement(conn, sql)
                        except Exception as e:
                            entry.error = str(e)
                            entry.duration_ms = (time.perf_counter() - t0) * 1000
                            if continue_on_error:
                                logger.warning(
                                    "[%s] instruction %d ignorée (continue_on_error) : %s | %s",
                                    label,
                                    i,
                                    e,
                                    entry.preview,
                                )
                                continue
                            logger.error(
                                "[%s] échec de l'instruction %d : %s | %s",
                                label,
                                i,
                                e,
                                entry.preview,
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
                            "Script SQL : ROLLBACK effectué (le DDL déjà exécuté n'est pas annulé)."
                        )
                    except Exception as rb:
                        logger.error("Script SQL : échec du ROLLBACK : %s", rb)
                raise
            finally:
                result.duration_ms = (time.perf_counter() - started) * 1000

        logger.info(
            "Script SQL terminé : %d/%d instruction(s) exécutée(s) en %.0f ms (%d erreur(s)).",
            result.executed_count,
            result.total_count,
            result.duration_ms,
            result.error_count,
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


# Instances globales : écriture et lecture.
#
# Le pool est dimensionné sur NB_WORKER (DSR-704) : le mode ALL traite NB_WORKER scénarios
# simultanément, et chacun détient au plus une connexion par pool à un instant donné. Sans
# cela, les workers au-delà de la dixième s'attendraient sur `pool.acquire()` et le
# parallélisme annoncé n'existerait pas.
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
