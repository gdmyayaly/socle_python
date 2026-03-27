"""
Script de debug — copie exacte du pattern officiel Databricks.
https://github.com/databricks/databricks-sql-python/blob/main/examples/m2m_oauth.py

Lancer avec : python debug_databricks.py
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Charger le .env
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

from databricks.sdk.core import Config, oauth_service_principal
from databricks import sql

server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")

print(f"server_hostname = {server_hostname}")
print(f"http_path       = {http_path}")
print("Connexion en cours...")


def credential_provider():
    config = Config(
        host=f"https://{server_hostname}",
        client_id=os.getenv("DATABRICKS_CLIENT_ID"),
        client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
    )
    return oauth_service_principal(config)


with sql.connect(
    server_hostname=server_hostname,
    http_path=http_path,
    credentials_provider=credential_provider,
) as connection:
    print("Connecté !")
    cursor = connection.cursor()
    cursor.execute("SELECT 1+1")
    result = cursor.fetchall()
    for row in result:
        print(f"Résultat : {row}")
    cursor.close()
    print("Test terminé avec succès !")
