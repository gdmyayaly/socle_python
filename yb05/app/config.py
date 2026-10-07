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


def _entier_positif(nom: str, defaut: int) -> int:
    """Entier > 0 lu dans l'environnement, le défaut si la valeur est mal saisie."""
    try:
        return max(1, int(os.getenv(nom, "")))
    except (TypeError, ValueError):
        return defaut


# Recyclage (s) des connexions inactives, avant que MySQL ne les coupe (`wait_timeout`).
MYSQL_POOL_RECYCLE = _entier_positif("MYSQL_POOL_RECYCLE", 600)
# Keepalive TCP (s) : pendant une requête longue la connexion est muette et le réseau la
# coupe après ~300 s d'inactivité (erreur 2013).
MYSQL_TCP_KEEPALIVE = _entier_positif("SGBD_TCP_KEEPALIVE", 60)
# Collation de connexion ; vide = celle de la base. pymysql ouvre en utf8mb4_general_ci,
# les tables sont en utf8mb4_0900_ai_ci : erreur 1267. Lettres, chiffres et `_` seulement.
_collation = os.getenv("SGBD_COLLATION", "").strip()
MYSQL_COLLATION = _collation if _collation.replace("_", "").isalnum() else ""

# Application / Logging
APP = os.getenv("APP", "dsr")
APP_ENV = os.getenv("APP_ENV", "sdev")
MODULE = os.getenv("MODULE", "yb05")
APP_VERSION = os.getenv("APP_VERSION", "1.0.0")
LOGS_DIR = os.getenv("LOGS_DIR", "")

# Debug
DEBUG_SHOW_QUERY = os.getenv("DEBUG_SHOW_QUERY", "false").lower() == "true"

# DSR-702 : code produit -> famille de clé (colis, oo, 3s, potentielip). Rien en base ne
# porte cette correspondance, d'où la configuration. Format `CODE:famille,CODE:famille`.
CLES_PAR_PRODUIT_DEFAUT = "CO:colis,OO:oo,IP:potentielip,OS:3s,PR:3s,PPI:3s"


def _parse_cles_par_produit(brut: str) -> dict[str, str]:
    """`CO:colis,OO:oo` -> `{"CO": "colis", ...}` ; lève si malformé (plutôt que trafic faux)."""
    mapping: dict[str, str] = {}
    for entree in brut.split(","):
        entree = entree.strip()
        if not entree:
            continue
        if entree.count(":") != 1:
            raise ValueError(
                f"CLES_PAR_PRODUIT : entrée invalide '{entree}', format attendu CODE:famille"
            )
        code, famille = (part.strip() for part in entree.split(":"))
        if not code or not famille:
            raise ValueError(f"CLES_PAR_PRODUIT : entrée incomplète '{entree}'")
        mapping[code.upper()] = famille.lower()
    return mapping


CLES_PAR_PRODUIT = _parse_cles_par_produit(
    os.getenv("CLES_PAR_PRODUIT", CLES_PAR_PRODUIT_DEFAUT)
)


def _parse_nb_worker(brut: str) -> int:
    """Scénarios traités en parallèle par ALL (DSR-704) ; valeur inexploitable -> 1 (séquentiel)."""
    try:
        return max(1, int(brut))
    except (TypeError, ValueError):
        return 1


NB_WORKER = _parse_nb_worker(os.getenv("NB_WORKER", "1"))

# Requêtes utilitaires pour les checks
HEALTH_CHECK_QUERY = "SELECT 1 AS ok"

# Scripts SQL : seuil au-delà duquel on avertit que le fichier est chargé en mémoire.
SQL_SCRIPT_WARN_SIZE = int(os.getenv("SQL_SCRIPT_WARN_SIZE", str(10 * 1024 * 1024)))  # 10 Mo
