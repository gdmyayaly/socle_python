"""Configuration centralisée de l'application, chargée depuis les variables d'environnement."""
 
import os
from pathlib import Path
 
from dotenv import load_dotenv
 
# Chemin absolu vers le .env à la racine du projet (ys04/.env)
_env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=_env_path)
# MySQL
SKIP_MYSQL = os.getenv("SKIP_MYSQL", "false").lower() == "true"
_SGBD_APP_USER_DEFAULT = os.getenv("SGBD_APP_USER", "root")
MYSQL_HOST_WRITE = os.getenv("SGBD_SERVER_WRITE", "localhost")
MYSQL_HOST_READ = os.getenv("SGBD_SERVER_READ", MYSQL_HOST_WRITE)
MYSQL_PORT = int(os.getenv("SGBD_PORT", "3306"))
MYSQL_USER_WRITE = os.getenv("SGBD_APP_USER_WRITE", _SGBD_APP_USER_DEFAULT)
MYSQL_USER_READ = os.getenv("SGBD_APP_USER_READ", _SGBD_APP_USER_DEFAULT)
MYSQL_PASSWORD = os.getenv("SGBD_APP_PWD", "")
MYSQL_DATABASE = os.getenv("SGBD_DB_NAME", "trppu")
MYSQL_MAX_RETRIES = int(os.getenv("SGBD_MAX_RETRIES", "3"))
MYSQL_RETRY_DELAY = float(os.getenv("SGBD_RETRY_DELAY", "1.0"))
 
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
 
# Requêtes utilitaires pour les checks
HEALTH_CHECK_QUERY ="SELECT 1 AS ok"