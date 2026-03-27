import logging
import time
from typing import Any

from databricks import sql as databricks_sql
from databricks.sdk.core import Config, oauth_service_principal

from app.config import (
    DATABRICKS_CATALOG,
    DATABRICKS_CLIENT_ID,
    DATABRICKS_CLIENT_SECRET,
    DATABRICKS_HTTP_PATH,
    DATABRICKS_SCHEMA,
    DATABRICKS_SERVER_HOSTNAME,
    DATABRICKS_TIMEOUT,
)

logger = logging.getLogger(__name__)


class DatabricksDB:
    """Classe utilitaire pour la connexion à une SQL Warehouse Databricks via OAuth M2M."""

    def __init__(
        self,
        server_hostname: str = DATABRICKS_SERVER_HOSTNAME,
        http_path: str = DATABRICKS_HTTP_PATH,
        client_id: str = DATABRICKS_CLIENT_ID,
        client_secret: str = DATABRICKS_CLIENT_SECRET,
        catalog: str = DATABRICKS_CATALOG,
        schema: str = DATABRICKS_SCHEMA,
        timeout: int = DATABRICKS_TIMEOUT,
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ):
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.client_id = client_id
        self.client_secret = client_secret
        self.catalog = catalog
        self.schema = schema
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._connection = None

    def _credential_provider(self):
        config = Config(
            host=f"https://{self.server_hostname}",
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return oauth_service_principal(config)

    def connect(self) -> None:
        """Ouvre la connexion à Databricks avec mécanisme de retry."""
        if not self.server_hostname:
            raise ValueError(
                "DATABRICKS_SERVER_HOSTNAME est vide. "
                "Renseignez-le dans votre fichier .env"
            )
        logger.info(
            "Connexion à Databricks en cours... (host=%s, catalogue=%s, schema=%s, timeout=%ds)",
            self.server_hostname,
            self.catalog,
            self.schema,
            self.timeout,
        )
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info("Tentative de connexion %d/%d...", attempt, self.max_retries)
                self._connection = databricks_sql.connect(
                    server_hostname=self.server_hostname,
                    http_path=self.http_path,
                    credentials_provider=self._credential_provider,
                    catalog=self.catalog,
                    schema=self.schema,
                    _socket_timeout=self.timeout,
                )
                logger.info("Connexion à Databricks SQL Warehouse réussie.")
                return
            except Exception as e:
                logger.warning(
                    "Tentative %d/%d de connexion Databricks échouée : %s",
                    attempt,
                    self.max_retries,
                    e,
                )
                if attempt == self.max_retries:
                    logger.error("Échec définitif de la connexion à Databricks après %d tentatives.", self.max_retries)
                    raise
                logger.info("Nouvelle tentative dans %.1fs...", self.retry_delay * attempt)
                time.sleep(self.retry_delay * attempt)

    def disconnect(self) -> None:
        """Ferme la connexion à Databricks."""
        if self._connection:
            logger.info("Fermeture de la connexion Databricks...")
            self._connection.close()
            self._connection = None
            logger.info("Connexion Databricks fermée.")

    def _ensure_connection(self):
        if self._connection is None:
            raise RuntimeError(
                "La connexion Databricks n'est pas initialisée. Appelez connect() d'abord."
            )
        return self._connection

    def _execute_with_retry(self, operation, *args, **kwargs):
        """Exécute une opération avec retry et reconnexion automatique."""
        for attempt in range(1, self.max_retries + 1):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                logger.warning(
                    "Tentative %d/%d Databricks échouée : %s",
                    attempt,
                    self.max_retries,
                    e,
                )
                if attempt == self.max_retries:
                    logger.error("Échec définitif de la requête Databricks après %d tentatives.", self.max_retries)
                    raise
                time.sleep(self.retry_delay * attempt)
                # Tenter une reconnexion
                logger.info("Reconnexion à Databricks en cours...")
                try:
                    self.connect()
                except Exception:
                    logger.warning("Reconnexion échouée, nouvelle tentative à venir...")

    def execute(self, query: str, params: list | None = None) -> int:
        """Exécute une requête et retourne le nombre de lignes affectées."""
        logger.info("Databricks execute : %s", query)
        def _run():
            conn = self._ensure_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.rowcount if cursor.rowcount >= 0 else 0

        return self._execute_with_retry(_run)

    def fetch_one(self, query: str, params: list | None = None) -> dict[str, Any] | None:
        """Exécute une requête et retourne une seule ligne sous forme de dict."""
        logger.info("Databricks fetch_one : %s", query)
        def _run():
            conn = self._ensure_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                columns = [desc[0] for desc in cursor.description]
                row = cursor.fetchone()
                if row is None:
                    return None
                return dict(zip(columns, row))

        return self._execute_with_retry(_run)

    def fetch_all(self, query: str, params: list | None = None) -> list[dict[str, Any]]:
        """Exécute une requête et retourne toutes les lignes sous forme de list[dict]."""
        logger.info("Databricks fetch_all : %s", query)
        def _run():
            conn = self._ensure_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]

        return self._execute_with_retry(_run)

    def tables(self, schema: str | None = None) -> list[dict[str, Any]]:
        """Liste les tables disponibles dans un schéma."""
        schema = schema or self.schema
        logger.info("Databricks tables : catalogue=%s, schema=%s", self.catalog, schema)
        def _run():
            conn = self._ensure_connection()
            with conn.cursor() as cursor:
                cursor.tables(catalog_name=self.catalog, schema_name=schema)
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]

        return self._execute_with_retry(_run)

    def schemas(self) -> list[dict[str, Any]]:
        """Liste les schémas disponibles dans le catalogue."""
        logger.info("Databricks schemas : catalogue=%s", self.catalog)
        def _run():
            conn = self._ensure_connection()
            with conn.cursor() as cursor:
                cursor.schemas(catalog_name=self.catalog)
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]

        return self._execute_with_retry(_run)

    def catalogs(self) -> list[dict[str, Any]]:
        """Liste tous les catalogues accessibles."""
        logger.info("Databricks catalogs : listage des catalogues...")
        def _run():
            conn = self._ensure_connection()
            with conn.cursor() as cursor:
                cursor.catalogs()
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]

        return self._execute_with_retry(_run)

    def columns(self, schema: str | None = None, table: str = "") -> list[dict[str, Any]]:
        """Liste les colonnes d'une table."""
        schema = schema or self.schema
        logger.info("Databricks columns : %s.%s.%s", self.catalog, schema, table)
        def _run():
            conn = self._ensure_connection()
            with conn.cursor() as cursor:
                cursor.columns(
                    catalog_name=self.catalog,
                    schema_name=schema,
                    table_name=table,
                )
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]

        return self._execute_with_retry(_run)


# Instance globale
databricks = DatabricksDB()
