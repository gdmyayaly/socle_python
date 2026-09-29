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
# Classement (collation) des connexions. Vide = celui de la base (`@@collation_database`).
# Sans alignement, pymysql ouvre en utf8mb4_general_ci : une variable de script
# (`SET @co_regate := '…'`) comparée à une colonne en utf8mb4_0900_ai_ci lève l'erreur 1267
# « Illegal mix of collations ». Seuls lettres, chiffres et `_` sont acceptés.
_collation = os.getenv("SGBD_COLLATION", "").strip()
MYSQL_COLLATION = _collation if _collation.replace("_", "").isalnum() else ""
# Âge maximal (secondes) d'une connexion inactive du pool avant renouvellement. Pendant une
# étape longue, la connexion de lecture reste inactive : si MySQL la coupe (`wait_timeout`),
# la requête suivante échouerait (« server has gone away »). À garder sous `wait_timeout`.
MYSQL_POOL_RECYCLE = _entier_positif("MYSQL_POOL_RECYCLE", 600)

# Requêtes utilitaires pour les checks
HEALTH_CHECK_QUERY = "SELECT 1 AS ok"

# Scripts SQL : seuil au-delà duquel on avertit que le fichier est chargé en mémoire.
SQL_SCRIPT_WARN_SIZE = _entier_positif("SQL_SCRIPT_WARN_SIZE", 10 * 1024 * 1024)  # 10 Mo

# S3 — source des fichiers à charger.
#
# Les identifiants portent les noms standard AWS. Ils sont facultatifs : renseignés, ils
# priment ; absents, boto3 résout seul via sa chaîne habituelle (rôle de la machine, profil
# ~/.aws). Pas de région : boto3 retombe sur us-east-1, que les S3 internes acceptent pour
# la signature. Les deux chemins sont documentés dans le README.
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_PREFIXE = os.getenv("S3_PREFIXE", "")
S3_TIMEOUT = _entier_positif("S3_TIMEOUT", 60)
# Vérification TLS, sur le modèle de l'API jours fermés de `python/` : chemin vers un bundle
# CA (ex. certif/cacert.pem, pour le proxy d'inspection TLS de l'entreprise), relatif à la
# racine du module ou absolu. Vide = magasin de certificats par défaut de boto3.
_s3_ca = os.getenv("S3_CA_BUNDLE", "").strip()
if _s3_ca and not os.path.isabs(_s3_ca):
    _s3_ca = str(Path(__file__).resolve().parent.parent / _s3_ca)
S3_CA_BUNDLE = _s3_ca
# Désactive complètement la vérification TLS. Dépannage en dev UNIQUEMENT (risque MITM).
S3_VERIFY_SSL = os.getenv("S3_VERIFY_SSL", "true").strip().lower() == "true"

# Fichiers CSV. Le nom du fichier est une donnée d'exploitation : il change à chaque
# livraison du métier, il n'a rien à faire dans le code.
CSV_CLES_REPARTITION = os.getenv("CSV_CLES_REPARTITION", "")
CSV_DELIMITEUR = os.getenv("CSV_DELIMITEUR", ";")
# utf-8-sig comme le runner de scripts SQL : décode aussi l'UTF-8 nu et absorbe le BOM.
CSV_ENCODAGE = os.getenv("CSV_ENCODAGE", "utf-8-sig")

# Chargements de masse.
CHARGEMENT_TAILLE_LOT = _entier_positif("CHARGEMENT_TAILLE_LOT", 5000)
CHARGEMENT_LOG_TOUTES_LES = _entier_positif("CHARGEMENT_LOG_TOUTES_LES", 100_000)
# Avec --skip-errors : nombre de lignes écartées détaillées dans le rapport et les logs.
# Au-delà, elles sont seulement comptées — un fichier entièrement faux ne doit pas produire
# un rapport de 22 M de lignes.
CHARGEMENT_MAX_REJETS_DETAILLES = _entier_positif("CHARGEMENT_MAX_REJETS_DETAILLES", 100)
# Attente maximale (secondes) d'un verrou de métadonnées pour tout le DDL de yb07 : TRUNCATE
# et index du chargement, index des clés, scripts `migration` et `correctif`. Sans borne, MySQL attend jusqu'à un an, et toutes les requêtes des autres
# sessions (API) s'empilent derrière : mieux vaut échouer vite et relancer.
CHARGEMENT_LOCK_WAIT_TIMEOUT = _entier_positif("CHARGEMENT_LOCK_WAIT_TIMEOUT", 60)

# Initialisation des clés de répartition (commande `init`).
#
# La boucle de création des versions traite un site par itération : journaliser chacun
# noierait le journal sous plusieurs milliers de lignes INFO, ne rien journaliser laisserait
# l'étape muette. D'où une ligne d'avancement toutes les N itérations, cadencée sur le
# volume comme celle du chargement.
INIT_LOG_TOUS_LES_SITES = _entier_positif("INIT_LOG_TOUS_LES_SITES", 50)
# Plafond des anomalies de somme de clés journalisées une à une : sur un référentiel
# intégralement faux, des milliers d'avertissements identiques ne servent personne. Le
# compte total, lui, est toujours rendu.
INIT_MAX_ANOMALIES_LOGUEES = _entier_positif("INIT_MAX_ANOMALIES_LOGUEES", 50)
