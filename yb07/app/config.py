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
    """Entier >= 1 lu dans l'environnement ; toute valeur inexploitable donne le défaut."""
    try:
        return max(1, int(os.getenv(nom, "")))
    except (TypeError, ValueError):
        return defaut


MYSQL_POOL_SIZE = _entier_positif("MYSQL_POOL_SIZE", 10)
# Collation des connexions (vide = `@@collation_database`). Sans alignement, une variable
# `SET @…` comparée à une colonne utf8mb4_0900_ai_ci lève l'erreur 1267.
_collation = os.getenv("SGBD_COLLATION", "").strip()
MYSQL_COLLATION = _collation if _collation.replace("_", "").isalnum() else ""
# Renouvellement (s) des connexions inactives du pool, à garder sous `wait_timeout` de MySQL
# (sinon « server has gone away » après une étape longue).
MYSQL_POOL_RECYCLE = _entier_positif("MYSQL_POOL_RECYCLE", 600)
# Keepalive TCP (s) : une instruction longue et muette est coupée par le réseau vers 300 s.
MYSQL_TCP_KEEPALIVE = _entier_positif("SGBD_TCP_KEEPALIVE", 60)
# Période (s) du suivi des instructions longues ; lu sur une autre connexion, ne maintient
# pas en vie celle qui exécute.
MYSQL_SUIVI_INSTRUCTION = _entier_positif("SGBD_SUIVI_INSTRUCTION", 30)

# Requêtes utilitaires pour les checks
HEALTH_CHECK_QUERY = "SELECT 1 AS ok"

# Taille au-delà de laquelle un script SQL chargé en mémoire est signalé.
SQL_SCRIPT_WARN_SIZE = _entier_positif("SQL_SCRIPT_WARN_SIZE", 10 * 1024 * 1024)  # 10 Mo

# S3. Identifiants facultatifs : absents, boto3 suit sa chaîne habituelle (rôle, ~/.aws).
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_PREFIXE = os.getenv("S3_PREFIXE", "")
S3_TIMEOUT = _entier_positif("S3_TIMEOUT", 60)
# Bundle CA pour TLS (proxy d'inspection), relatif à la racine du module ou absolu.
# Vide = magasin par défaut de boto3.
_s3_ca = os.getenv("S3_CA_BUNDLE", "").strip()
if _s3_ca and not os.path.isabs(_s3_ca):
    _s3_ca = str(Path(__file__).resolve().parent.parent / _s3_ca)
S3_CA_BUNDLE = _s3_ca
# Désactive complètement la vérification TLS. Dépannage en dev UNIQUEMENT (risque MITM).
S3_VERIFY_SSL = os.getenv("S3_VERIFY_SSL", "true").strip().lower() == "true"

# Fichiers CSV (le nom change à chaque livraison métier).
CSV_CLES_REPARTITION = os.getenv("CSV_CLES_REPARTITION", "")
CSV_DELIMITEUR = os.getenv("CSV_DELIMITEUR", ";")
# utf-8-sig : décode l'UTF-8 nu et absorbe le BOM.
CSV_ENCODAGE = os.getenv("CSV_ENCODAGE", "utf-8-sig")

# Chargements de masse.
CHARGEMENT_TAILLE_LOT = _entier_positif("CHARGEMENT_TAILLE_LOT", 1000)
CHARGEMENT_LOG_TOUTES_LES = _entier_positif("CHARGEMENT_LOG_TOUTES_LES", 100_000)
# --skip-errors : lignes rejetées détaillées dans le rapport ; au-delà, seulement comptées.
CHARGEMENT_MAX_REJETS_DETAILLES = _entier_positif("CHARGEMENT_MAX_REJETS_DETAILLES", 100)
# `lock_wait_timeout` (s) de tout le DDL de yb07 : sans borne, MySQL attend un verrou de
# métadonnées jusqu'à un an et les requêtes des autres sessions s'empilent derrière.
CHARGEMENT_LOCK_WAIT_TIMEOUT = _entier_positif("CHARGEMENT_LOCK_WAIT_TIMEOUT", 60)

# Commande `init` : une ligne d'avancement toutes les N itérations (sites).
INIT_LOG_TOUS_LES_SITES = _entier_positif("INIT_LOG_TOUS_LES_SITES", 50)
# Étape « versions » : sites par lot et transaction ; un lot en échec est rejoué site par site.
INIT_VERSIONS_TAILLE_LOT = _entier_positif("INIT_VERSIONS_TAILLE_LOT", 1000)
# Anomalies de somme de clés journalisées une à une (le total est toujours rendu).
INIT_MAX_ANOMALIES_LOGUEES = _entier_positif("INIT_MAX_ANOMALIES_LOGUEES", 50)
