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
MYSQL_DATABASE = os.getenv("SGBD_DB_NAME", "yb07")
MYSQL_MAX_RETRIES = int(os.getenv("SGBD_MAX_RETRIES", "3"))
MYSQL_RETRY_DELAY = float(os.getenv("SGBD_RETRY_DELAY", "1.0"))

# Application / Logging
APP = os.getenv("APP", "dsr")
APP_ENV = os.getenv("APP_ENV", "sdev")
MODULE = os.getenv("MODULE", "yb07")
APP_VERSION = os.getenv("APP_VERSION", "1.0.0")
LOGS_DIR = os.getenv("LOGS_DIR", "")


def _entier_positif(nom: str, defaut: int) -> int:
    """Lit une variable d'environnement entière, strictement positive.

    Toute valeur inexploitable — absente, vide, non numérique, nulle ou négative — est
    ramenée au défaut : un batch d'exploitation ne doit pas refuser de démarrer pour une
    variable d'environnement mal saisie.
    """
    try:
        return max(1, int(os.getenv(nom, "")))
    except (TypeError, ValueError):
        return defaut


MYSQL_POOL_SIZE = _entier_positif("MYSQL_POOL_SIZE", 10)

# Requêtes utilitaires pour les checks
HEALTH_CHECK_QUERY = "SELECT 1 AS ok"

# Scripts SQL : seuil au-delà duquel on avertit que le fichier est chargé en mémoire.
SQL_SCRIPT_WARN_SIZE = _entier_positif("SQL_SCRIPT_WARN_SIZE", 10 * 1024 * 1024)  # 10 Mo

# S3 — source des fichiers à charger.
#
# Les identifiants sont facultatifs : renseignés, ils priment ; absents, boto3 résout seul
# via sa chaîne habituelle (rôle de la machine, profil ~/.aws, variables AWS_*). Les deux
# chemins sont documentés dans le README.
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_PREFIXE = os.getenv("S3_PREFIXE", "")
S3_TIMEOUT = _entier_positif("S3_TIMEOUT", 60)

# Fichiers CSV. Le nom du fichier est une donnée d'exploitation : il change à chaque
# livraison du métier, il n'a rien à faire dans le code.
CSV_CLES_REPARTITION = os.getenv("CSV_CLES_REPARTITION", "")
CSV_DELIMITEUR = os.getenv("CSV_DELIMITEUR", ";")
# utf-8-sig comme le runner de scripts SQL : décode aussi l'UTF-8 nu et absorbe le BOM.
CSV_ENCODAGE = os.getenv("CSV_ENCODAGE", "utf-8-sig")

# Chargements de masse.
CHARGEMENT_TAILLE_LOT = _entier_positif("CHARGEMENT_TAILLE_LOT", 5000)
CHARGEMENT_LOG_TOUTES_LES = _entier_positif("CHARGEMENT_LOG_TOUTES_LES", 100_000)
