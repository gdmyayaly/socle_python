"""Configuration centralisée de l'application, chargée depuis les variables d'environnement."""

import os
from pathlib import Path

from dotenv import load_dotenv

# Charge .env.local en priorité, sinon .env
_project_root = Path(__file__).resolve().parent.parent
_env_local = _project_root / ".env.local"
_env_default = _project_root / ".env"
load_dotenv(dotenv_path=_env_local if _env_local.exists() else _env_default)

# MySQL
SKIP_MYSQL = os.getenv("SKIP_MYSQL", "false").lower() == "true"
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "trppu")
MYSQL_MAX_RETRIES = int(os.getenv("MYSQL_MAX_RETRIES", "3"))
MYSQL_RETRY_DELAY = float(os.getenv("MYSQL_RETRY_DELAY", "1.0"))

# Databricks
DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME", "")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "")
DATABRICKS_CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID", "")
DATABRICKS_CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET", "")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "gold")
DATABRICKS_SCHEMA = os.getenv("DATABRICKS_SCHEMA", "default")
DATABRICKS_TIMEOUT = int(os.getenv("DATABRICKS_TIMEOUT", "120"))
DATABRICKS_MAX_RETRIES = int(os.getenv("DATABRICKS_MAX_RETRIES", "3"))
DATABRICKS_RETRY_DELAY = float(os.getenv("DATABRICKS_RETRY_DELAY", "2.0"))

# Application / Logging
APP = os.getenv("APP", "dsr")
APP_ENV = os.getenv("APP_ENV", "sdev")
MODULE = os.getenv("MODULE", "ys04")
APP_VERSION = os.getenv("APP_VERSION", "1.0.0")
LOGS_DIR = os.getenv("LOGS_DIR", "")

# Validation
MAX_DATE_RANGE_DAYS = 730 # SOIT 365 * 2

# Debug
DEBUG_SHOW_QUERY = os.getenv("DEBUG_SHOW_QUERY", "false").lower() == "true"
