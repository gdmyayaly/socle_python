"""
Debug connexion Databricks — sans databricks.sdk.core.Config
Teste d'abord la connectivité réseau, puis le token OAuth, puis la requête SQL.

Lancer avec : python debug_databricks.py
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
client_id = os.getenv("DATABRICKS_CLIENT_ID")
client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")

print(f"server_hostname = {server_hostname}")
print(f"http_path       = {http_path}")
print(f"client_id       = {client_id[:8]}..." if client_id else "client_id = None")

# --- Étape 1 : Test réseau ---
print("\n[1] Test de connectivité réseau...")
import requests
try:
    r = requests.get(f"https://{server_hostname}", timeout=10)
    print(f"    [OK] Réponse HTTP {r.status_code}")
except requests.exceptions.Timeout:
    print("    [ERREUR] Timeout — le host n'est pas joignable (proxy/VPN ?)")
    exit(1)
except Exception as e:
    print(f"    [ERREUR] {e}")
    exit(1)

# --- Étape 2 : Token OAuth M2M ---
print("\n[2] Récupération du token OAuth M2M...")
token_url = f"https://{server_hostname}/oidc/v1/token"
print(f"    Token URL : {token_url}")
try:
    r = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "all-apis",
        },
        timeout=30,
    )
    print(f"    HTTP {r.status_code}")
    if r.status_code == 200:
        token = r.json()["access_token"]
        print(f"    [OK] Token obtenu : {token[:20]}...")
    else:
        print(f"    [ERREUR] Réponse : {r.text}")
        exit(1)
except Exception as e:
    print(f"    [ERREUR] {e}")
    exit(1)

# --- Étape 3 : Connexion SQL avec access_token ---
print("\n[3] Connexion SQL avec le token...")
from databricks import sql

try:
    connection = sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=token,
    )
    print("    [OK] Connecté !")
except Exception as e:
    print(f"    [ERREUR] {e}")
    exit(1)

# --- Étape 4 : Requête test ---
print("\n[4] Exécution de SELECT 1+1 ...")
cursor = connection.cursor()
cursor.execute("SELECT 1+1 AS result")
result = cursor.fetchall()
for row in result:
    print(f"    Résultat : {row}")
cursor.close()

connection.close()
print("\n[OK] Tout fonctionne !")
