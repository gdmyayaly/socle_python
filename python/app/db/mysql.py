"""Connexion asynchrone à MySQL avec pool de connexions et retry."""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

import aiomysql

from app.config import (
    MYSQL_DATABASE,
    MYSQL_HOST_WRITE,
    MYSQL_HOST_READ,
    MYSQL_MAX_RETRIES,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_RETRY_DELAY,
    MYSQL_USER_WRITE,
    MYSQL_USER_READ,
)

logger = logging.getLogger(__name__)


class Database:
    """Classe utilitaire pour la connexion MySQL avec pool, retry et transactions."""

    def __init__(
        self,
        host: str = MYSQL_HOST_WRITE,
        port: int = MYSQL_PORT,
        user: str = MYSQL_USER_WRITE,
        password: str = MYSQL_PASSWORD,
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


class _TransactionCursor:
    """Wrapper de connexion utilisé à l'intérieur d'une transaction."""

    def __init__(self, conn: aiomysql.Connection):
        self._conn = conn

    async def execute(self, query: str, params: tuple | None = None) -> int:
        async with self._conn.cursor() as cur:
            await cur.execute(query, params)
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


# Instances globales : écriture et lecture
db_write = Database(
    host=MYSQL_HOST_WRITE,
    user=MYSQL_USER_WRITE,
)
db_read = Database(
    host=MYSQL_HOST_READ,
    user=MYSQL_USER_READ,
)
