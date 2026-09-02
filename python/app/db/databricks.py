"""Connexion à Databricks SQL Warehouse via OAuth M2M avec retry automatique."""
# Url help: https://learn.microsoft.com/en-us/azure/databricks/dev-tools/python-sql-connector#auth-m2m
import logging

from app.log_utils import ctx
import os
import time
from typing import Any

from databricks import sql as databricks_sql
from databricks.sdk.core import Config, oauth_service_principal

from app.config import (
    DATABRICKS_CATALOG,
    DATABRICKS_CLIENT_ID,
    DATABRICKS_CLIENT_SECRET,
    DATABRICKS_HTTP_PATH,
    DATABRICKS_MAX_RETRIES,
    DATABRICKS_RETRY_DELAY,
    DATABRICKS_SCHEMA,
    DATABRICKS_SERVER_HOSTNAME,
    DATABRICKS_TIMEOUT,
)

logger = logging.getLogger(__name__)


class DatabricksDB:
    """Classe utilitaire pour la connexion à une SQL Warehouse Databricks via OAuth M2M."""

    def __init__(self):
        self.server_hostname = DATABRICKS_SERVER_HOSTNAME
        self.http_path = DATABRICKS_HTTP_PATH
        self.client_id = DATABRICKS_CLIENT_ID
        self.client_secret = DATABRICKS_CLIENT_SECRET
        self.catalog = DATABRICKS_CATALOG
        self.schema = DATABRICKS_SCHEMA
        self.timeout = DATABRICKS_TIMEOUT
        self.max_retries = DATABRICKS_MAX_RETRIES
        self.retry_delay = DATABRICKS_RETRY_DELAY
        self._connection = None

    def connect(self) -> None:
        """Ouvre la connexion à Databricks avec mécanisme de retry."""
        # Validation des variables obligatoires
        missing = []
        if not self.server_hostname:
            missing.append("DATABRICKS_SERVER_HOSTNAME")
        if not self.http_path:
            missing.append("DATABRICKS_HTTP_PATH")
        if not self.client_id:
            missing.append("DATABRICKS_CLIENT_ID")
        if not self.client_secret:
            missing.append("DATABRICKS_CLIENT_SECRET")
        if missing:
            raise ValueError(
                f"Variables d'environnement manquantes dans .env : {', '.join(missing)}"
            )

        # Le SDK Databricks (Config) lit DATABRICKS_HOST en interne
        # On injecte toutes les variables pour que le SDK les trouve
        os.environ["DATABRICKS_HOST"] = f"https://{self.server_hostname}"
        os.environ["DATABRICKS_CLIENT_ID"] = self.client_id
        os.environ["DATABRICKS_CLIENT_SECRET"] = self.client_secret
        os.environ["DATABRICKS_SERVER_HOSTNAME"] = self.server_hostname
        os.environ["DATABRICKS_HTTP_PATH"] = self.http_path

        server_hostname = self.server_hostname
        client_id = self.client_id
        client_secret = self.client_secret

        def credential_provider():
            config = Config(
                host=f"https://{server_hostname}",
                client_id=client_id,
                client_secret=client_secret,
            )
            return oauth_service_principal(config)

        logger.info(
            "Connexion à Databricks en cours... (host=%s, catalogue=%s, schema=%s, timeout=%ds)",
            self.server_hostname,
            self.catalog,
            self.schema,
            self.timeout,
        )
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(
                    "Tentative de connexion %d/%d... (cold start possible, peut prendre plusieurs minutes)",
                    attempt,
                    self.max_retries,
                )
                self._connection = databricks_sql.connect(
                    server_hostname=self.server_hostname,
                    http_path=self.http_path,
                    credentials_provider=credential_provider,
                    catalog=self.catalog,
                    schema=self.schema,
                    _socket_timeout=self.timeout,
                )
                logger.info("Connexion à Databricks SQL Warehouse réussie.")
                return
            except Exception as e:
                logger.warning(
                    "Tentative de connexion Databricks échouée %s",
                    ctx(tentative=attempt, max_tentatives=self.max_retries, erreur=str(e)),
                )
                if attempt == self.max_retries:
                    # Dernière tentative : la stacktrace est indispensable, l'appelant
                    # ne verra qu'une exception remontée.
                    logger.exception(
                        "Échec définitif de la connexion Databricks %s",
                        ctx(tentatives=self.max_retries),
                    )
                    raise
                logger.info(
                    "Nouvelle tentative de connexion Databricks %s",
                    ctx(dans_s=self.retry_delay * attempt),
                )
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
            logger.info("Connexion lazy à Databricks (premier appel)...")
            self.connect()
        return self._connection

    def _execute_with_retry(self, operation, *args, **kwargs):
        """Exécute une opération avec retry et reconnexion automatique."""
        for attempt in range(1, self.max_retries + 1):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                logger.warning(
                    "Tentative de requête Databricks échouée %s",
                    ctx(tentative=attempt, max_tentatives=self.max_retries, erreur=str(e)),
                )
                if attempt == self.max_retries:
                    logger.exception(
                        "Échec définitif de la requête Databricks %s",
                        ctx(tentatives=self.max_retries),
                    )
                    raise
                time.sleep(self.retry_delay * attempt)
                logger.info("Reconnexion Databricks en cours")
                try:
                    self.connect()
                except Exception:
                    logger.warning(
                        "Reconnexion Databricks échouée %s",
                        ctx(consequence="nouvelle tentative à venir"),
                        exc_info=True,
                    )

    def execute(self, query: str, params: list | None = None) -> int:
        """Exécute une requête et retourne le nombre de lignes affectées."""
        # SQL brut en DEBUG : en INFO il expose la structure des tables sur
        # chaque requête (cf. AUDIT_SECURITE_PERFORMANCE.md).
        logger.debug("Databricks execute %s", ctx(query=query))
        def _run():
            conn = self._ensure_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.rowcount if cursor.rowcount >= 0 else 0

        return self._execute_with_retry(_run)

    def fetch_one(self, query: str, params: list | None = None) -> dict[str, Any] | None:
        """Exécute une requête et retourne une seule ligne sous forme de dict."""
        logger.debug("Databricks fetch_one %s", ctx(query=query))
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
        logger.debug("Databricks fetch_all %s", ctx(query=query))
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
        logger.debug(
            "Databricks tables %s", ctx(catalogue=self.catalog, schema=schema)
        )
        def _run():
            conn = self._ensure_connection()
            with conn.cursor() as cursor:
                cursor.tables(catalog_name=self.catalog, schema_name=schema)
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]

        return self._execute_with_retry(_run)

    def schemas(self) -> list[dict[str, Any]]:
        """Liste les schémas disponibles dans le catalogue."""
        logger.debug("Databricks schemas %s", ctx(catalogue=self.catalog))
        def _run():
            conn = self._ensure_connection()
            with conn.cursor() as cursor:
                cursor.schemas(catalog_name=self.catalog)
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]

        return self._execute_with_retry(_run)

    def catalogs(self) -> list[dict[str, Any]]:
        """Liste tous les catalogues accessibles."""
        logger.debug("Databricks catalogs")
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
        logger.debug(
            "Databricks columns %s",
            ctx(catalogue=self.catalog, schema=schema, table=table),
        )
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
