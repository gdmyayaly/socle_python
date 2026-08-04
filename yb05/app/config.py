"""Configuration centralisée de l'application, chargée depuis les variables d'environnement."""

import os
from pathlib import Path

from dotenv import load_dotenv

_env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=_env_path)

# MySQL
_SGBD_APP_USER_DEFAULT = os.getenv("SGBD_APP_USER", "root")
_SGBD_APP_PWD_DEFAULT = os.getenv("SGBD_APP_PWD", "")
MYSQL_HOST_WRITE = os.getenv("SGBD_SERVER_WRITE", "localhost")
MYSQL_HOST_READ = os.getenv("SGBD_SERVER_READ", MYSQL_HOST_WRITE)
MYSQL_PORT = int(os.getenv("SGBD_PORT", "3306"))
MYSQL_USER_WRITE = os.getenv("SGBD_APP_USER_WRITE", _SGBD_APP_USER_DEFAULT)
MYSQL_USER_READ = os.getenv("SGBD_APP_USER_READ", _SGBD_APP_USER_DEFAULT)
MYSQL_PASSWORD_WRITE = os.getenv("SGBD_APP_PWD_WRITE", _SGBD_APP_PWD_DEFAULT)
MYSQL_PASSWORD_READ = os.getenv("SGBD_APP_PWD_READ", _SGBD_APP_PWD_DEFAULT)
MYSQL_DATABASE = os.getenv("SGBD_DB_NAME", "yb05")
MYSQL_MAX_RETRIES = int(os.getenv("SGBD_MAX_RETRIES", "3"))
MYSQL_RETRY_DELAY = float(os.getenv("SGBD_RETRY_DELAY", "1.0"))

# Application / Logging
APP = os.getenv("APP", "dsr")
APP_ENV = os.getenv("APP_ENV", "sdev")
MODULE = os.getenv("MODULE", "yb05")
APP_VERSION = os.getenv("APP_VERSION", "1.0.0")
LOGS_DIR = os.getenv("LOGS_DIR", "")

# Debug
DEBUG_SHOW_QUERY = os.getenv("DEBUG_SHOW_QUERY", "false").lower() == "true"

# Requêtes utilitaires pour les checks
HEALTH_CHECK_QUERY = "SELECT 1 AS ok"

# Scripts SQL : seuil au-delà duquel on avertit que le fichier est chargé en mémoire.
SQL_SCRIPT_WARN_SIZE = int(os.getenv("SQL_SCRIPT_WARN_SIZE", str(10 * 1024 * 1024)))  # 10 Mo
