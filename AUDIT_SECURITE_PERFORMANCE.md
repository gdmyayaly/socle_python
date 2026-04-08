# Audit Securite & Performance - socle_python (trppu API)

**Date** : 2026-04-08
**Outils utilises** : Bandit 1.9.4, pip-audit 2.10.0, revue manuelle du code
**Perimetre** : Ensemble du projet (`app/`, `Dockerfile`, `docker-compose.yml`, `requirements.txt`)

---

## Resume executif

| Categorie | Niveau | Constats principaux |
|-----------|--------|---------------------|
| Injection SQL | **CRITIQUE** | 3 points d'injection via f-strings dans les routes |
| Authentification | **CRITIQUE** | Aucune authentification sur aucun endpoint |
| Dependances | OK | pip-audit : 0 vulnerabilite connue |
| Docker | MOYEN | Container root, pas de healthcheck |
| Performance | MOYEN | Pagination incorrecte en mode multi-segments |
| Tests | **ELEVE** | Aucun test unitaire ni integration |
| CI/CD | **ELEVE** | Aucun pipeline configure |

---

## 1. Securite

### 1.1 Injection SQL (CRITIQUE)

**Bandit B608 - CWE-89** : 3 alertes de severite MEDIUM detectees.

Les requetes SQL sont construites par concatenation de f-strings avec des parametres utilisateur non echappes :

**Fichier `app/routes/trafics.py` lignes 201-209 :**
```python
# VULNERABLE - co_regate vient directement du body de la requete
sql = (
    f"SELECT {select} FROM {table} "
    f"WHERE co_regate = '{co_regate}' "   # <-- INJECTION
    f"AND {date_col} BETWEEN '{fmt_date(dt_start, periode)}' AND '{fmt_date(dt_end, periode)}'"
)
```

Un attaquant peut injecter via `co_regate`:
```
' OR '1'='1' --
```

**Fichier `app/routes/databricks.py` lignes 136-140 :**
```python
# VULNERABLE - co_regate et da_comptage viennent des query params
conditions.append(f"co_regate = '{co_regate}'")    # <-- INJECTION
conditions.append(f"da_comptage = '{da_comptage}'") # <-- INJECTION
query = f"SELECT * FROM {table}{where_clause} LIMIT {limit}"
```

**Recommandation** : Utiliser des requetes parametrees. Le connecteur Databricks supporte les placeholders :
```python
# CORRIGE
sql = "SELECT ... WHERE co_regate = ? AND da_comptage BETWEEN ? AND ?"
databricks.fetch_all(sql, [co_regate, dt_start, dt_end])
```

> **Note** : Les valeurs de `TRAFICS_IN_VALUES` (lignes 58-65) sont des constantes hardcodees, donc non exploitables. Mais `TRAFICS_IN_COLUMN` devrait etre valide contre une liste blanche si rendu configurable.

---

### 1.2 Absence d'authentification (CRITIQUE)

Aucun endpoint n'est protege :
- `/health` expose l'etat des connexions BDD
- `/databricks/*` permet d'explorer le catalogue, les schemas, les tables et les colonnes
- `/trafics/*` permet d'executer des requetes sur les donnees

**Recommandation** : Implementer au minimum une authentification par API key ou JWT via un middleware FastAPI :
```python
from fastapi import Depends, HTTPException, Security
from fastapi.security import APIKeyHeader

api_key_header = APIKeyHeader(name="X-API-Key")

async def verify_api_key(api_key: str = Security(api_key_header)):
    if api_key != config.API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")
```

---

### 1.3 Divulgation d'information (MOYEN)

- **`DEBUG_SHOW_QUERY=true`** : Retourne les requetes SQL completes dans la reponse API (`trafics.py:362`, `databricks.py:154`). En production, cela expose la structure des tables et les noms de colonnes.
- **`/health`** : Expose l'etat des connexions MySQL et Databricks (utile pour la reconnaissance).
- **Stacktraces** : FastAPI retourne les stacktraces en mode debug par defaut.

**Recommandation** : S'assurer que `DEBUG_SHOW_QUERY=false` en prod. Restreindre `/health` a un reseau interne ou ajouter une authentification.

---

### 1.4 Configuration reseau (MOYEN)

**Bandit B104** : `app/main.py:50` - Binding sur `0.0.0.0` :
```python
uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
```

Le `reload=True` ne doit JAMAIS etre actif en production (surveillance du filesystem, overhead CPU).

**`docker-compose.yml`** : Le port 8000 est expose sur toutes les interfaces de l'hote :
```yaml
ports:
  - "8000:8000"  # Expose sur 0.0.0.0 de l'hote
```

**Recommandation** : En production, limiter a `"127.0.0.1:8000:8000"` ou utiliser un reverse proxy (nginx/traefik).

---

### 1.5 Dependances

**pip-audit** : **0 vulnerabilite connue** sur les 8 dependances directes.

| Dependance | Version installee | Vulnerabilites |
|------------|-------------------|----------------|
| fastapi | 0.135.3 | 0 |
| uvicorn | - | 0 |
| aiomysql | 0.3.2 | 0 |
| python-dotenv | - | 0 |
| databricks-sql-connector | 4.2.5 | 0 |
| databricks-sdk | 0.102.0 | 0 |
| pyarrow | - | 0 |
| python-json-logger | - | 0 |

**Probleme** : Aucune version n'est pinnee dans `requirements.txt` :
```
fastapi
uvicorn
aiomysql
```

**Recommandation** : Pincher les versions pour garantir la reproductibilite et eviter les regressions :
```
fastapi==0.135.3
uvicorn==0.34.0
aiomysql==0.3.2
databricks-sql-connector==4.2.5
databricks-sdk==0.102.0
```

---

### 1.6 Docker (MOYEN)

**`Dockerfile`** :
```dockerfile
FROM python:3.12-slim    # OK - image slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

| Probleme | Impact | Recommandation |
|----------|--------|----------------|
| Execution en root | Eleve | Ajouter `RUN useradd -r appuser` + `USER appuser` |
| Pas de HEALTHCHECK | Moyen | Ajouter `HEALTHCHECK CMD curl -f http://localhost:8000/health` |
| Pas de multi-stage build | Faible | Reduirait la taille de l'image |
| `COPY . .` copie tout | Faible | Le `.dockerignore` est correct, mais verifier qu'aucun secret ne fuite |

**`docker-compose.yml`** :
| Probleme | Recommandation |
|----------|----------------|
| Pas de limites de ressources | Ajouter `deploy.resources.limits` (CPU, memoire) |
| Pas de healthcheck | Ajouter `healthcheck` dans le service |
| Pas de politique de logs | Ajouter `logging.driver: json-file` avec `max-size` |

---

### 1.7 Gestion des secrets (BON)

- `.env` est dans `.gitignore` et `.dockerignore`
- Les secrets passent par variables d'environnement via `python-dotenv`
- Les valeurs par defaut dans `config.py` ne contiennent pas de vrais secrets
- Pas de `.env` commite dans le repo

---

## 2. Performance

### 2.1 Bug de pagination multi-segments (ELEVE)

**Fichier `app/routes/trafics.py` lignes 409-416 :**

En mode `auto`, la decomposition produit plusieurs segments (ex: jours + mois + semaines). La pagination applique LIMIT/OFFSET independamment a chaque segment :

```python
for q in queries:
    query_str = f"{q['query']} LIMIT {page_size} OFFSET {offset}"
    results.extend(databricks.fetch_all(query_str))
```

**Probleme** : Si `page_size=50` et il y a 3 segments, chaque segment retourne jusqu'a 50 lignes = jusqu'a 150 resultats par page. L'offset est aussi incorrectement applique a chaque segment au lieu de l'ensemble.

**Recommandation** : Executer toutes les requetes sans pagination, agreger les resultats, puis appliquer la pagination en Python :
```python
all_results = []
for q in queries:
    all_results.extend(databricks.fetch_all(q["query"]))
paginated = all_results[offset:offset + page_size]
```
Ou mieux : utiliser une seule requete UNION ALL.

---

### 2.2 Connexion Databricks non poole (MOYEN)

**Fichier `app/db/databricks.py`** : Une seule connexion globale (`self._connection`), pas de pool.

- Requetes sequentielles uniquement (pas de concurrence)
- Si la connexion tombe, toutes les requetes en cours echouent
- Pas adapte a un usage multi-worker (uvicorn avec `--workers > 1`)

**Recommandation** : Pour la charge actuelle (API interne), c'est acceptable. Si la charge augmente, envisager un pool de connexions ou un pattern singleton par worker.

---

### 2.3 Absence de timeout par requete (MOYEN)

- **MySQL** (`app/db/mysql.py`) : Aucun `connect_timeout` ni `read_timeout` sur le pool aiomysql
- **Databricks** (`app/db/databricks.py`) : `_socket_timeout` est configure (120s), mais pas de timeout par requete individuelle

Une requete lente peut bloquer un worker indefiniment.

**Recommandation** : Ajouter des timeouts explicites :
```python
# MySQL - dans create_pool
connect_timeout=10,
read_timeout=30,

# Databricks - dans chaque fetch
import signal  # ou asyncio.wait_for pour l'async
```

---

### 2.4 Absence de cache (FAIBLE)

Les donnees de trafic sont relativement statiques (historiques). Des requetes identiques sont re-executees a chaque appel.

**Recommandation** : Envisager un cache en memoire (`functools.lru_cache` ou Redis) pour les requetes frequentes avec un TTL adapte.

---

### 2.5 Calcul de jours itere jour par jour (FAIBLE)

**Fichier `app/routes/calcl_nbr_jours.py` lignes 80-105 :**
```python
while current <= dt_fin:
    # Itere jour par jour sur 730 jours max
    current += timedelta(days=1)
```

Pour 2 ans (730 jours), cela reste rapide (~1ms). Mais un calcul mathematique serait plus elegant :
- Nombre de semaines completes * 6 + jours restants sans dimanche

**Impact** : Negligeable en pratique.

---

## 3. Qualite du code

### 3.1 Absence de tests (CRITIQUE)

Aucun fichier de test dans le projet. Aucun repertoire `tests/`, aucun `test_*.py`.

**Recommandation** : Ajouter au minimum :
- Tests unitaires pour `parse_date`, `fmt_date`, `decompose_auto`, `_decompose_semaines_jours`
- Tests unitaires pour `get_nbr_jours` (calcul pur, pas de BDD)
- Tests d'integration pour les endpoints avec une BDD mockee
- Framework : `pytest` + `httpx` (TestClient FastAPI)

---

### 3.2 Absence de CI/CD (ELEVE)

Pas de pipeline GitHub Actions, GitLab CI, ou autre. Les vulnerabilites et regressions ne sont detectees qu'en revue manuelle.

**Recommandation** : Mettre en place un pipeline minimal :
```yaml
# .github/workflows/ci.yml
- pip install -r requirements.txt
- bandit -r app/
- pip-audit -r requirements.txt
- pytest tests/
```

---

### 3.3 Fichier orphelin

`app/db/rien.txt` : Fichier vide/legacy. A supprimer.

---

## 4. Synthese des actions par priorite

### Critique (a corriger immediatement)
1. **Injection SQL** : Passer aux requetes parametrees dans `trafics.py` et `databricks.py` (routes)
2. **Authentification** : Ajouter un middleware d'authentification API key ou JWT
3. **Tests** : Creer une suite de tests minimale

### Eleve (a planifier sous 2 semaines)
4. **CI/CD** : Mettre en place un pipeline avec bandit + pip-audit + pytest
5. **Pagination** : Corriger le bug multi-segments dans `get_trafics_paginated`
6. **Versions** : Pincher les dependances dans `requirements.txt`

### Moyen (a planifier sous 1 mois)
7. **Docker** : Ajouter USER non-root, HEALTHCHECK, limites de ressources
8. **Timeouts** : Ajouter des timeouts explicites sur MySQL et Databricks
9. **DEBUG_SHOW_QUERY** : S'assurer qu'il est desactive en production
10. **Reseau** : Restreindre l'exposition du port dans docker-compose

### Faible (ameliorations futures)
11. Cache applicatif pour les requetes frequentes
12. Suppression du fichier `app/db/rien.txt`
13. Optimisation mathematique de `calcl_nbr_jours`

---

## 5. Resultats bruts des outils

### Bandit (3 alertes)
| Fichier | Ligne | ID | Severite | Description |
|---------|-------|----|----------|-------------|
| `app/main.py` | 50 | B104 | MEDIUM | Binding sur 0.0.0.0 |
| `app/routes/databricks.py` | 140 | B608 | MEDIUM | Injection SQL par f-string |
| `app/routes/trafics.py` | 202 | B608 | MEDIUM | Injection SQL par f-string |

### pip-audit
```
No known vulnerabilities found
```

---

*Rapport genere automatiquement - revue manuelle complementaire recommandee.*
