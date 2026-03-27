"""
Script de debug robuste pour Databricks (évite les blocages silencieux)
Lancer avec : python debug_databricks.py
"""

import os
import sys
import time
import socket
from pathlib import Path
from dotenv import load_dotenv

# =========================

# 1. Chargement .env

# =========================

env_path = Path(**file**).resolve().parent / ".env"
print(f"\n[1] Chargement du .env depuis : {env_path}")
print(f"    Fichier existe : {env_path.exists()}")

load_dotenv(dotenv_path=env_path)

# =========================

# 2. Lecture variables

# =========================

server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
client_id = os.getenv("DATABRICKS_CLIENT_ID")
client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
catalog = os.getenv("DATABRICKS_CATALOG", "gold")
schema = os.getenv("DATABRICKS_SCHEMA", "default")

print(f"\n[2] Variables d'environnement :")
print(f"    HOST        = {repr(server_hostname)}")
print(f"    HTTP_PATH   = {repr(http_path)}")
print(f"    CLIENT_ID   = {repr(client_id[:8] + '...' if client_id else None)}")
print(f"    SECRET      = {repr('***' if client_secret else None)}")

# =========================

# 3. Validation

# =========================

missing = []
if not server_hostname: missing.append("DATABRICKS_SERVER_HOSTNAME")
if not http_path: missing.append("DATABRICKS_HTTP_PATH")
if not client_id: missing.append("DATABRICKS_CLIENT_ID")
if not client_secret: missing.append("DATABRICKS_CLIENT_SECRET")

if missing:
print(f"\n[ERREUR] Variables manquantes : {', '.join(missing)}")
sys.exit(1)

print("\n[3] Variables OK")

# =========================

# 4. Test réseau (IMPORTANT)

# =========================

print("\n[4] Test réseau vers Databricks...")
try:
socket.create_connection((server_hostname, 443), timeout=5)
print("    [OK] Réseau accessible")
except Exception as e:
print(f"    [ERREUR] Problème réseau : {e}")
sys.exit(1)

# =========================

# 5. Création Config (protégé)

# =========================

print("\n[5] Création Config Databricks SDK...")

from databricks.sdk.core import Config, oauth_service_principal

host_url = f"https://{server_hostname}"
print(f"    host = {host_url}")

def build_config():
return Config(
host=host_url,
client_id=client_id,
client_secret=client_secret,
)

try:
start = time.time()
config = build_config()
print(f"    Config créé en {time.time() - start:.2f}s")
print(f"    host résolu : {config.host}")
except Exception as e:
print(f"    [ERREUR] Config échoué : {e}")
print(f"    Type : {type(e).**name**}")
sys.exit(1)

# =========================

# 6. Provider OAuth (timeout visuel)

# =========================

print("\n[6] Création credential provider...")

try:
start = time.time()
provider = oauth_service_principal(config)
print(f"    Provider OK ({time.time() - start:.2f}s)")
except Exception as e:
print(f"    [ERREUR] OAuth échoué : {e}")
sys.exit(1)

# =========================

# 7. Connexion SQL

# =========================

print("\n[7] Connexion SQL Databricks...")
from databricks import sql as databricks_sql

def credential_provider():
return oauth_service_principal(build_config())

connection = None

try:
start = time.time()

```
connection = databricks_sql.connect(
    server_hostname=server_hostname,
    http_path=http_path,
    credentials_provider=credential_provider,
    catalog=catalog,
    schema=schema,
    _socket_timeout=120,  # IMPORTANT
)

print(f"    [OK] Connecté en {time.time() - start:.2f}s")
```

except Exception as e:
print(f"    [ERREUR] Connexion échouée : {e}")
print(f"    Type : {type(e).**name**}")
import traceback
traceback.print_exc()
sys.exit(1)

# =========================

# 8. Test requête

# =========================

print("\n[8] Test requête SELECT 1")

try:
cursor = connection.cursor()
cursor.execute("SELECT 1 AS ok")
result = cursor.fetchone()
print(f"    Résultat : {result}")
print("    [OK] Query OK")
cursor.close()
except Exception as e:
print(f"    [ERREUR] Query échouée : {e}")

# =========================

# 9. Fermeture propre

# =========================

print("\n[9] Fermeture connexion")

try:
if connection:
connection.close()
print("    [OK] Fermé proprement")
except Exception as e:
print(f"    [WARNING] Erreur fermeture : {e}")

print("\n✅ DEBUG TERMINÉ")
