# trppu API

Application FastAPI avec connexion MySQL et Databricks SQL Warehouse, mécanismes de retry et gestion des transactions.

## Prérequis

- Python 3.10 ou supérieur
- Serveur MySQL accessible
- Accès à une SQL Warehouse Databricks (avec un service principal OAuth M2M)

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Copier le fichier d'exemple et renseigner vos identifiants MySQL :

```bash
cp .env.example .env
```

Variables disponibles dans `.env` :

| Variable         | Par défaut  | Description              |
|------------------|-------------|--------------------------|
| `MYSQL_HOST`     | `localhost` | Hôte du serveur MySQL    |
| `MYSQL_PORT`     | `3306`      | Port du serveur MySQL    |
| `MYSQL_USER`     | `root`      | Utilisateur MySQL        |
| `MYSQL_PASSWORD` | *(vide)*    | Mot de passe MySQL       |
| `MYSQL_DATABASE` | `trppu`     | Nom de la base de données|

**Databricks :**

| Variable                      | Par défaut | Description                          |
|-------------------------------|------------|--------------------------------------|
| `DATABRICKS_SERVER_HOSTNAME`  | *(vide)*   | Hostname de l'instance Databricks    |
| `DATABRICKS_HTTP_PATH`        | *(vide)*   | HTTP path de la SQL Warehouse        |
| `DATABRICKS_CLIENT_ID`        | *(vide)*   | Client ID du service principal OAuth |
| `DATABRICKS_CLIENT_SECRET`    | *(vide)*   | Client secret OAuth                  |
| `DATABRICKS_CATALOG`          | `gold`     | Catalogue par défaut                 |

## Lancement

**En dev (sans Docker) :**

```bash
uvicorn app.main:app --reload
```

**Avec Docker :**

```bash
docker compose up --build
```

Le serveur démarre sur `http://localhost:8000`.

## Routes disponibles

| Méthode | URL       | Description                                    |
|---------|-----------|------------------------------------------------|
| GET     | `/`       | Message de bienvenue                                |
| GET     | `/health` | Statut de l'API + état MySQL et Databricks          |
| GET     | `/databricks/test` | Requête de test (`SELECT 1`) sur Databricks  |
| GET     | `/databricks/catalogs` | Liste les catalogues accessibles           |
| GET     | `/databricks/schemas` | Liste les schémas du catalogue gold         |
| GET     | `/databricks/tables/{schema}` | Liste les tables d'un schéma         |
| GET     | `/databricks/columns/{schema}/{table}` | Liste les colonnes d'une table |
| GET     | `/databricks/trafics_jours?limit=10` | Données de `gold.trafics_jours` |
| GET     | `/databricks/trafics_semaines?limit=10` | Données de `gold.trafics_semaines` |
| GET     | `/databricks/trafics_mois?limit=10` | Données de `gold.trafics_mois`    |

## Classe utilitaire `Database`

La classe `Database` (dans `app/db/mysql.py`) fournit :

### Connexion avec pool et retry

```python
from app.db.mysql import db

await db.connect()    # crée le pool (retry automatique si MySQL n'est pas prêt)
await db.disconnect() # ferme le pool
```

Le pool est configuré avec `min_connections` et `max_connections`. En cas d'échec de connexion, le système retente jusqu'à `max_retries` fois avec un délai croissant.

### Requêtes simples

```python
# INSERT / UPDATE / DELETE → retourne le nombre de lignes affectées
rows = await db.execute("INSERT INTO users (name) VALUES (%s)", ("Alice",))

# SELECT une ligne → dict ou None
user = await db.fetch_one("SELECT * FROM users WHERE id = %s", (1,))

# SELECT plusieurs lignes → list[dict]
users = await db.fetch_all("SELECT * FROM users")
```

### Transactions

```python
async with db.transaction() as tx:
    await tx.execute("INSERT INTO orders (user_id, total) VALUES (%s, %s)", (1, 99.99))
    await tx.execute("UPDATE users SET order_count = order_count + 1 WHERE id = %s", (1,))
# commit automatique — rollback si une exception est levée
```

### Retry sur les requêtes

La méthode `execute()` retente automatiquement en cas d'erreur (connexion perdue, etc.). Le nombre de tentatives est configurable via le paramètre `retries` ou la valeur par défaut `max_retries`.

## Classe utilitaire `DatabricksDB`

La classe `DatabricksDB` (dans `app/db/databricks.py`) fournit :

### Connexion OAuth M2M avec retry

```python
from app.db.databricks import databricks

databricks.connect()    # connexion avec retry automatique
databricks.disconnect() # fermeture propre
```

### Requêtes

```python
# SELECT une ligne → dict ou None
row = databricks.fetch_one("SELECT * FROM gold.trafics_jours LIMIT 1")

# SELECT plusieurs lignes → list[dict]
rows = databricks.fetch_all("SELECT * FROM gold.trafics_mois LIMIT 100")

# INSERT / UPDATE / DELETE → nombre de lignes affectées
count = databricks.execute("DELETE FROM gold.temp WHERE id = ?", [42])
```

### Exploration des métadonnées

```python
databricks.catalogs()                         # liste des catalogues
databricks.schemas()                          # schémas du catalogue gold
databricks.tables(schema="default")           # tables d'un schéma
databricks.columns(schema="default", table="trafics_jours")  # colonnes d'une table
```

## Logging

L'application logge automatiquement toute son activité (requêtes HTTP, connexions base de données, erreurs) dans des fichiers journaliers.

### Fonctionnement

- Les logs sont écrits dans le dossier `logs/`
- Un fichier par jour avec rotation automatique à minuit :
  - `logs/app.log` — fichier du jour en cours
  - `logs/app.log.2026-03-27` — archive du 27 mars 2026
- Les 30 derniers jours sont conservés, les plus anciens sont supprimés automatiquement
- Les logs s'affichent aussi dans la console

### Format des logs

```
2026-03-27 14:30:12 | INFO     | trppu | >>> GET /health
2026-03-27 14:30:12 | INFO     | trppu | <<< GET /health 200 (3.2ms)
2026-03-27 14:30:12 | INFO     | database | Connexion au pool MySQL réussie.
2026-03-27 14:30:15 | WARNING  | database | Tentative 1/3 de connexion MySQL échouée : ...
```

### Ce qui est loggé

- Chaque requête HTTP entrante (méthode, chemin)
- Chaque réponse HTTP (code statut, durée en ms)
- Connexions et déconnexions à la base de données
- Tentatives de retry et erreurs
- Démarrage et arrêt de l'application

## Documentation Swagger

Documentation interactive générée automatiquement par FastAPI :

- Swagger UI : `http://localhost:8000/docs`
- ReDoc : `http://localhost:8000/redoc`
