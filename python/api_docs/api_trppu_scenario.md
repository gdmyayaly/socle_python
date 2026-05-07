# API `trppu_scenario` — CRUD + workflow de statut

> Module : `app/routes/trppu_scenario/`
> Préfixe HTTP : `/trppu-api/scenarios`
> Tag Swagger : **Scenarios**

Pilotage des scénarios de calcul de trafic. Un scénario porte une période d'analyse,
un PIC version, des coefficients dérivés, et traverse un cycle de vie strict
(`BROUILLON → SIMULATION → VALIDE → PRODUCTION → ARCHIVE`).

---

## 1. Table `trppu_scenario`

| Colonne                 | Type                                                       | Notes                                      |
|-------------------------|------------------------------------------------------------|--------------------------------------------|
| `id_scenario`           | BIGINT AUTO_INCREMENT — **PK**                             | Géré par MySQL                             |
| `co_regate`             | CHAR(6) NOT NULL                                           | FK `trppu_site.co_regate` (RESTRICT)       |
| `lb_scenario`           | VARCHAR(50) NOT NULL                                       |                                            |
| `co_roc`                | CHAR(6) NOT NULL                                           |                                            |
| `statut`                | ENUM(`BROUILLON,SIMULATION,VALIDE,PRODUCTION,ARCHIVE`)     | Défaut serveur : `BROUILLON`               |
| `dt_creation`           | DATETIME DEFAULT NOW()                                     |                                            |
| `dt_validation`         | DATETIME NULL                                              | Posé sur transition `VALIDE`               |
| `dt_mise_en_prod`       | DATETIME NULL                                              | Posé sur transition `PRODUCTION`           |
| `periode_debut/fin`     | DATE NOT NULL                                              | Défaut serveur : `today±1 an`              |
| `periode_realise_*`     | DATE NULL                                                  |                                            |
| `periode_prev_*`        | DATE NULL                                                  |                                            |
| `nb_jours_semaine`      | TINYINT NOT NULL — CHECK IN (5, 6)                         | Défaut serveur : `5`                       |
| `id_pic_version`        | INT NOT NULL                                               | FK `trppu_pic_version` (RESTRICT)          |
| `version_scenario`      | INT NOT NULL DEFAULT 1                                     | Auto-incrémenté à chaque mutation          |
| `id_scenario_parent`    | BIGINT NULL                                                | FK self (`SET NULL`) — versioning          |
| `est_fige`              | TINYINT(1) NOT NULL DEFAULT 0                              | `1` bloque toutes les mutations métier     |

---

## 2. Endpoints

| Méthode | Chemin                                              | Description                                                       |
|---------|-----------------------------------------------------|-------------------------------------------------------------------|
| `GET`    | `/trppu-api/scenarios`                              | Liste paginée + filtres `co_regate`, `co_roc`, `statut`, `est_fige` |
| `GET`    | `/trppu-api/scenarios/{id_scenario}`                | Détail                                                            |
| `POST`   | `/trppu-api/scenarios`                              | Création (BROUILLON, version 1, est_fige=0)                       |
| `DELETE` | `/trppu-api/scenarios/{id_scenario}`                | Soft-delete (transition vers ARCHIVE)                             |
| `PATCH`  | `/trppu-api/scenarios/{id_scenario}/periodes`       | MAJ partielle des bornes de période                               |
| `PATCH`  | `/trppu-api/scenarios/{id_scenario}/nb-jours-semaine` | MAJ `nb_jours_semaine ∈ {5,6}`                                  |
| `PATCH`  | `/trppu-api/scenarios/{id_scenario}/statut`         | Transition de statut (machine à états + effets de bord)           |
| `PATCH`  | `/trppu-api/scenarios/{id_scenario}/est-fige`       | Force `est_fige` (seule porte de sortie après PRODUCTION)         |
| `PATCH`  | `/trppu-api/scenarios/{id_scenario}/lb-scenario`    | Renommage du libellé                                              |
| `POST`   | `/trppu-api/scenarios/{id_scenario}/duplicate`      | Clone en nouveau BROUILLON, `id_scenario_parent = source`         |
| `GET`    | `/trppu-api/scenarios/{id_scenario}/history`        | Lignée complète (ancêtres + descendants)                          |

### 2.1 `GET /trppu-api/scenarios`

| Param        | Type   | Défaut | Description                |
|--------------|--------|--------|----------------------------|
| `co_regate`  | str(6) | —      | Filtre site                |
| `co_roc`     | str(6) | —      | Filtre ROC                 |
| `statut`     | str    | —      | Filtre statut (enum)       |
| `est_fige`   | bool   | —      | Filtre figé / non figé     |
| `limit`      | int    | 100    | 1..1000                    |
| `offset`     | int    | 0      | ≥ 0                        |

### 2.2 `POST /trppu-api/scenarios`

Body minimal :
```json
{
  "co_regate": "ABCDEF",
  "lb_scenario": "Scénario Q1 2026",
  "co_roc": "ABCDEF"
}
```

Body complet :
```json
{
  "co_regate": "ABCDEF",
  "lb_scenario": "Scénario Q1 2026",
  "co_roc": "ABCDEF",
  "nb_jours_semaine": 6,
  "id_pic_version": 12,
  "periode_debut": "2026-01-01",
  "periode_fin": "2026-12-31",
  "periode_realise_debut": "2026-01-01",
  "periode_realise_fin": "2026-06-30",
  "periode_prev_debut": "2026-07-01",
  "periode_prev_fin": "2026-12-31",
  "id_scenario_parent": 42
}
```

Comportement par défaut côté serveur si non fournis :
- `nb_jours_semaine` → `5`
- `id_pic_version` → première ligne `trppu_pic_version` avec `est_par_defaut=1`, sinon `id_pic_version=1`, sinon **422**.
- `periode_debut` / `periode_fin` → `today - 365j` / `today + 365j`
- `statut` = `BROUILLON`, `version_scenario` = `1`, `est_fige` = `false`

Codes :
- `201` créé
- `422` site/pic_version/parent introuvable, ou validations Pydantic
- `500` exception inattendue

### 2.3 `PATCH /trppu-api/scenarios/{id}/periodes`

Tous les champs optionnels — au moins un requis :
```json
{
  "periode_debut": "2026-02-01",
  "periode_fin": "2026-11-30",
  "periode_realise_debut": "2026-02-01",
  "periode_realise_fin": "2026-06-30",
  "periode_prev_debut": "2026-07-01",
  "periode_prev_fin": "2026-11-30"
}
```

Codes :
- `200` MAJ + `version_scenario` incrémenté
- `400` `periode_fin < periode_debut` (ou bornes réalise/prev incohérentes)
- `404` scénario introuvable
- `409` scénario figé (`est_fige=1`)

### 2.4 `PATCH /trppu-api/scenarios/{id}/nb-jours-semaine`

```json
{ "nb_jours_semaine": 6 }
```

- `422` si valeur ∉ {5, 6}
- `409` si figé

### 2.5 `PATCH /trppu-api/scenarios/{id}/statut`

```json
{ "statut": "PRODUCTION" }
```

Effets de bord automatiques :
- `→ VALIDE` : pose `dt_validation = NOW()` (si NULL)
- `→ PRODUCTION` : pose `dt_mise_en_prod = NOW()`, `est_fige = 1` (et `dt_validation` si NULL)
- autres : juste l'UPDATE du statut

Cette route n'est **pas** bloquée par `est_fige` — un scénario PRODUCTION reste archivable.

### 2.6 `PATCH /trppu-api/scenarios/{id}/est-fige`

```json
{ "est_fige": false }
```

Permet manuellement de défiger un scénario sorti de PRODUCTION. À utiliser avec précaution.

### 2.7 `PATCH /trppu-api/scenarios/{id}/lb-scenario`

```json
{ "lb_scenario": "Nouveau libellé" }
```

### 2.8 `POST /trppu-api/scenarios/{id}/duplicate`

Body optionnel :
```json
{ "lb_scenario": "Scénario Q1 2026 (variante)" }
```

Sans body : libellé du clone = `"<source.lb_scenario> (copie)"` (tronqué à 50 caractères).
Le clone copie : `co_regate`, `co_roc`, périodes, `nb_jours_semaine`, `id_pic_version` ;
force `statut=BROUILLON`, `version_scenario=1`, `est_fige=0`,
et pose `id_scenario_parent = id source`.

### 2.9 `GET /trppu-api/scenarios/{id}/history`

Renvoie tous les scénarios de la même lignée :
1. Le serveur remonte de `id` jusqu'à la racine (`id_scenario_parent IS NULL`).
2. Une CTE récursive descend depuis la racine et collecte tous les descendants.

Réponse : `list[ScenarioOut]` triée par `id_scenario`.

---

## 3. Machine à états

```
                      ┌──────────┐
                      │ BROUILLON│
                      └────┬─────┘
                           │
           ┌───────────────┼─────────────────┐
           ▼                                  ▼
     ┌──────────┐                       ┌────────┐
     │SIMULATION│ ◄──────────────┐      │ARCHIVE │
     └────┬─────┘                │      └────────┘
          │                       │           ▲
          ▼                       │           │
     ┌────────┐                   │           │
     │ VALIDE │ ──────────────────┘           │
     └────┬───┘                               │
          │                                    │
          ▼                                    │
     ┌──────────┐                              │
     │PRODUCTION│ ─────────────────────────────┘
     └──────────┘
```

| Depuis      | Vers autorisé                       |
|-------------|-------------------------------------|
| BROUILLON   | SIMULATION, ARCHIVE                 |
| SIMULATION  | VALIDE, BROUILLON, ARCHIVE          |
| VALIDE      | PRODUCTION, SIMULATION, ARCHIVE     |
| PRODUCTION  | ARCHIVE                             |
| ARCHIVE     | (terminal, aucune transition)       |

Une transition non autorisée renvoie **422** avec la liste des cibles valides.

---

## 4. Manœuvres opérationnelles courantes

### 4.1 Création + cycle complet

```bash
# Création (utilise tous les défauts serveur)
SID=$(curl -s -X POST http://localhost:8080/trppu-api/scenarios \
  -H "Content-Type: application/json" \
  -d '{"co_regate":"ABCDEF","lb_scenario":"Demo","co_roc":"ABCDEF"}' \
  | jq -r '.id_scenario')

# BROUILLON → SIMULATION → VALIDE → PRODUCTION
curl -X PATCH http://localhost:8080/trppu-api/scenarios/$SID/statut \
  -H "Content-Type: application/json" -d '{"statut":"SIMULATION"}'
curl -X PATCH http://localhost:8080/trppu-api/scenarios/$SID/statut \
  -H "Content-Type: application/json" -d '{"statut":"VALIDE"}'
curl -X PATCH http://localhost:8080/trppu-api/scenarios/$SID/statut \
  -H "Content-Type: application/json" -d '{"statut":"PRODUCTION"}'
# → est_fige=1, dt_mise_en_prod posé
```

### 4.2 Variante d'un scénario en production

```bash
# Cloner pour repartir d'un BROUILLON, garde id_scenario_parent
NEW=$(curl -s -X POST http://localhost:8080/trppu-api/scenarios/$SID/duplicate \
  -H "Content-Type: application/json" -d '{"lb_scenario":"Demo Q2"}' \
  | jq -r '.id_scenario')

# Modifier les périodes du clone
curl -X PATCH http://localhost:8080/trppu-api/scenarios/$NEW/periodes \
  -H "Content-Type: application/json" \
  -d '{"periode_debut":"2026-04-01","periode_fin":"2026-06-30"}'
```

### 4.3 Retirer un scénario PRODUCTION par erreur

```bash
# Défiger manuellement
curl -X PATCH http://localhost:8080/trppu-api/scenarios/$SID/est-fige \
  -H "Content-Type: application/json" -d '{"est_fige":false}'
# Toujours en statut PRODUCTION : seul ARCHIVE est ouvert,
# il faut donc l'archiver puis recréer un nouveau scénario via duplicate.
```

### 4.4 Lignée d'un scénario

```bash
curl http://localhost:8080/trppu-api/scenarios/$NEW/history
# → liste de tous les scénarios partageant la même racine
```

---

## 5. Structure des fichiers

```
app/routes/trppu_scenario/
├── __init__.py        # exporte router
├── routes.py          # endpoints FastAPI
├── schemas.py         # Pydantic v2 (Create / Out / *Update / Duplicate)
├── helpers.py         # SQL constants, FK checks, défauts métier, version bump
└── statuts.py         # ALLOWED_TRANSITIONS + apply_transition_side_effects
```
