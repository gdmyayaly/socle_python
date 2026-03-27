"""
Script de debug pour tester la connexion Databricks en isolation.
Lancer avec : python debug_databricks.py
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Charger le .env
env_path = Path(__file__).resolve().parent / ".env"
print(f"[1] Chargement du .env depuis : {env_path}")
print(f"    Fichier existe : {env_path.exists()}")
load_dotenv(dotenv_path=env_path)

# Afficher les variables (masquer les secrets)
server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
client_id = os.getenv("DATABRICKS_CLIENT_ID")
client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
catalog = os.getenv("DATABRICKS_CATALOG", "gold")
schema = os.getenv("DATABRICKS_SCHEMA", "default")

print(f"\n[2] Variables d'environnement :")
print(f"    DATABRICKS_SERVER_HOSTNAME = {repr(server_hostname)}")
print(f"    DATABRICKS_HTTP_PATH       = {repr(http_path)}")
print(f"    DATABRICKS_CLIENT_ID       = {repr(client_id[:8] + '...' if client_id else None)}")
print(f"    DATABRICKS_CLIENT_SECRET   = {repr('***' if client_secret else None)}")
print(f"    DATABRICKS_CATALOG         = {repr(catalog)}")
print(f"    DATABRICKS_SCHEMA          = {repr(schema)}")

# Vérifier les valeurs manquantes
missing = []
if not server_hostname: missing.append("DATABRICKS_SERVER_HOSTNAME")
if not http_path: missing.append("DATABRICKS_HTTP_PATH")
if not client_id: missing.append("DATABRICKS_CLIENT_ID")
if not client_secret: missing.append("DATABRICKS_CLIENT_SECRET")

if missing:
    print(f"\n[ERREUR] Variables manquantes : {', '.join(missing)}")
    print("Vérifiez votre fichier .env")
    sys.exit(1)

print("\n[3] Toutes les variables sont présentes.")

# Test du Config
print("\n[4] Création du Config Databricks SDK...")
from databricks.sdk.core import Config, oauth_service_principal

host_url = f"https://{server_hostname}"
print(f"    host passé au Config : {repr(host_url)}")

try:
    config = Config(
        host=host_url,
        client_id=client_id,
        client_secret=client_secret,
    )
    print(f"    Config.host résolu   : {repr(config.host)}")
    print(f"    Config.client_id     : {repr(config.client_id[:8] + '...' if config.client_id else None)}")
    print("    [OK] Config créé avec succès")
except Exception as e:
    print(f"    [ERREUR] Config a échoué : {e}")
    sys.exit(1)

# Test oauth_service_principal
print("\n[5] Création du credential provider (oauth_service_principal)...")
try:
    provider = oauth_service_principal(config)
    print(f"    Type retourné : {type(provider)}")
    print("    [OK] Provider créé avec succès")
except Exception as e:
    print(f"    [ERREUR] oauth_service_principal a échoué : {e}")
    sys.exit(1)

# Test de la connexion sql.connect
print("\n[6] Tentative de connexion sql.connect...")
print("    (peut prendre plusieurs minutes si cold start de la warehouse)")

from databricks import sql as databricks_sql

def credential_provider():
    config = Config(
        host=f"https://{server_hostname}",
        client_id=client_id,
        client_secret=client_secret,
    )
    return oauth_service_principal(config)

try:
    connection = databricks_sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=credential_provider,
        catalog=catalog,
        schema=schema,
    )
    print("    [OK] Connexion réussie !")
except Exception as e:
    print(f"    [ERREUR] sql.connect a échoué : {e}")
    print(f"    Type d'erreur : {type(e).__name__}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test requête SELECT 1
print("\n[7] Test requête : SELECT 1 AS ok")
try:
    cursor = connection.cursor()
    cursor.execute("SELECT 1 AS ok")
    result = cursor.fetchone()
    print(f"    Résultat : {result}")
    print("    [OK] Requête réussie !")
    cursor.close()
except Exception as e:
    print(f"    [ERREUR] Requête échouée : {e}")

# Fermeture
connection.close()
print("\n[8] Connexion fermée. Tout est OK !")
