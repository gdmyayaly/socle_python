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
    """Entier strictement positif lu dans l'environnement ; le défaut sinon.

    Un batch d'exploitation ne refuse pas de démarrer pour une variable mal saisie.
    """
    try:
        return max(1, int(os.getenv(nom, "")))
    except (TypeError, ValueError):
        return defaut


# Âge maximal (s) d'une connexion inactive du pool avant renouvellement : MySQL la couperait
# au-delà de `wait_timeout`, et la requête suivante échouerait (« server has gone away »).
MYSQL_POOL_RECYCLE = _entier_positif("MYSQL_POOL_RECYCLE", 600)
# Keepalive TCP (s d'inactivité avant la première sonde). Pendant une requête longue, MySQL
# n'envoie rien : un équipement réseau à délai d'inactivité (souvent 300 s) couperait la
# connexion (erreur 2013). Les sondes la maintiennent visible.
MYSQL_TCP_KEEPALIVE = _entier_positif("SGBD_TCP_KEEPALIVE", 60)
# Classement des connexions. Vide = celui de la base (`@@collation_database`) : pymysql ouvre en
# utf8mb4_general_ci, les tables sont en utf8mb4_0900_ai_ci — un littéral ou une variable de
# script comparé à une colonne lèverait l'erreur 1267. Lettres, chiffres et `_` seulement.
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

# Calcul des trafics (DSR-702) — correspondance code produit -> famille de clé de répartition.
#
# `trppu_cles_repartition_calcule` porte quatre clés (colis, oo, 3s, potentielip) alors que
# `trppu_produit` contient des codes objets alimentés dynamiquement depuis Databricks (OO, OS,
# PR, PPI, CO, IP…). Rien en base ne dit à quelle famille appartient un code : la
# correspondance est donc une donnée de configuration, corrigeable sans livraison.
#
# Format : `CODE:famille,CODE:famille`. Familles reconnues : colis, oo, 3s, potentielip.
CLES_PAR_PRODUIT_DEFAUT = "CO:colis,OO:oo,IP:potentielip,OS:3s,PR:3s,PPI:3s"


def _parse_cles_par_produit(brut: str) -> dict[str, str]:
    """Transforme `CO:colis,OO:oo` en `{"CO": "colis", "OO": "oo"}`.

    Une entrée malformée lève : une correspondance produit/clé silencieusement ignorée
    produirait des trafics faux, ce qui est bien pire qu'un démarrage refusé.
    """
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
    """Nombre de scénarios traités simultanément par le mode ALL (DSR-704).

    Toute valeur inexploitable — vide, non numérique, nulle ou négative — est ramenée à 1,
    c'est-à-dire au mode séquentiel qui est le défaut du ticket. Un batch d'exploitation ne
    doit pas refuser de démarrer pour une variable d'environnement mal saisie.
    """
    try:
        return max(1, int(brut))
    except (TypeError, ValueError):
        return 1


NB_WORKER = _parse_nb_worker(os.getenv("NB_WORKER", "1"))

# Requêtes utilitaires pour les checks
HEALTH_CHECK_QUERY = "SELECT 1 AS ok"

# Scripts SQL : seuil au-delà duquel on avertit que le fichier est chargé en mémoire.
SQL_SCRIPT_WARN_SIZE = int(os.getenv("SQL_SCRIPT_WARN_SIZE", str(10 * 1024 * 1024)))  # 10 Mo
