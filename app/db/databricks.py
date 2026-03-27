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
    DATABRICKS_SERVER_HOSTNAME,
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
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ):
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.client_id = client_id
        self.client_secret = client_secret
        self.catalog = catalog
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
        for attempt in range(1, self.max_retries + 1):
            try:
                self._connection = databricks_sql.connect(
                    server_hostname=self.server_hostname,
                    http_path=self.http_path,
                    credentials_provider=self._credential_provider,
                    catalog=self.catalog,
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
                    raise
                time.sleep(self.retry_delay * attempt)

    def disconnect(self) -> None:
        """Ferme la connexion à Databricks."""
        if self._connection:
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
                    raise
                time.sleep(self.retry_delay * attempt)
                # Tenter une reconnexion
                try:
                    self.connect()
                except Exception:
                    pass

    def execute(self, query: str, params: list | None = None) -> int:
        """Exécute une requête et retourne le nombre de lignes affectées."""
        def _run():
            conn = self._ensure_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.rowcount if cursor.rowcount >= 0 else 0

        return self._execute_with_retry(_run)

    def fetch_one(self, query: str, params: list | None = None) -> dict[str, Any] | None:
        """Exécute une requête et retourne une seule ligne sous forme de dict."""
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
        def _run():
            conn = self._ensure_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]

        return self._execute_with_retry(_run)

    def tables(self, schema: str = "default") -> list[dict[str, Any]]:
        """Liste les tables disponibles dans un schéma."""
        def _run():
            conn = self._ensure_connection()
            with conn.cursor() as cursor:
                cursor.tables(catalog_name=self.catalog, schema_name=schema)
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]

        return self._execute_with_retry(_run)

    def schemas(self) -> list[dict[str, Any]]:
        """Liste les schémas disponibles dans le catalogue."""
        def _run():
            conn = self._ensure_connection()
            with conn.cursor() as cursor:
                cursor.schemas(catalog_name=self.catalog)
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]

        return self._execute_with_retry(_run)

    def catalogs(self) -> list[dict[str, Any]]:
        """Liste tous les catalogues accessibles."""
        def _run():
            conn = self._ensure_connection()
            with conn.cursor() as cursor:
                cursor.catalogs()
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]

        return self._execute_with_retry(_run)

    def columns(self, schema: str, table: str) -> list[dict[str, Any]]:
        """Liste les colonnes d'une table."""
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
