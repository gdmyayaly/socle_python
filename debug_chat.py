"""
Version propre inspirée de m2m_oauth.py (Databricks officiel)
Objectif : exécuter une requête sur catalog.schema.table
"""

import os
import time
from pathlib import Path

from dotenv import load_dotenv
from databricks.sdk.core import Config, oauth_service_principal
from databricks import sql

# =========================
# 1. Chargement .env
# =========================

server_hostname = ""
http_path = ""
client_id = ""
client_secret = ""

catalog = ""
schema = ""
table = ""

# =========================
# 2. Validation
# =========================

if not all([server_hostname, http_path, client_id, client_secret]):
    raise ValueError("Variables d'environnement manquantes")

print("\n[CONFIG]")
print(f"HOST     = {server_hostname}")
print(f"CATALOG  = {catalog}")
print(f"SCHEMA   = {schema}")
print(f"TABLE    = {table}")

# =========================
# 3. Credential provider (OFFICIEL)
# =========================

def credential_provider():
    config = Config(
        host=f"https://{server_hostname}",
        client_id=client_id,
        client_secret=client_secret,
    )
    return oauth_service_principal(config)

# =========================
# 4. Connexion Databricks
# =========================

print("\n[1] Connexion Databricks...")

start = time.time()

with sql.connect(
    server_hostname=server_hostname,
    http_path=http_path,
    credentials_provider=credential_provider,
    catalog=catalog,
    schema=schema,
    _socket_timeout=120,
) as connection:

    print(f"[OK] Connecté en {time.time() - start:.2f}s")

    # =========================
    # 5. Requête réelle
    # =========================
    query = f"""
        SELECT *
        FROM {catalog}.{schema}.{table}
        LIMIT 10
    """

    print("\n[2] Exécution requête...")
    print(query.strip())

    with connection.cursor() as cursor:
        cursor.execute(query)

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        print(f"\n[OK] {len(rows)} lignes récupérées\n")

        for row in rows:
            print(dict(zip(columns, row)))

print("\n TERMINÉ")