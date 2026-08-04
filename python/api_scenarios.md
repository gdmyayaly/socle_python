# API Scénarios — Documentation

> Module `app/routes/scenarios/` — FastAPI + `aiomysql` + Pydantic v2.

## 1. Vue d'ensemble

Ce module expose 3 ressources liées :

> Toutes les routes sont préfixées par `/trppu-api`.

| Ressource | Table SQL | Endpoints |
|---|---|---|
| **Scénario** | `trppu_scenario` | `POST /trppu-api/scenarios`, `GET /trppu-api/scenarios`, `GET /trppu-api/scenarios/{id}`, `PATCH /trppu-api/scenarios/{id}`, `DELETE /trppu-api/scenarios/{id}`, `PATCH /trppu-api/scenarios/{id}/statut` |
| **Neutralisations** | `trppu_neutralisations` | `GET /trppu-api/scenarios/{id}/neutralisations`, `POST /trppu-api/scenarios/{id}/neutralisations`, `PATCH /trppu-api/neutralisations/{id}`, `DELETE /trppu-api/neutralisations/{id}` |
| **Exclusions** | `trppu_scenario_exclusions` | `GET /trppu-api/scenarios/{id}/exclusions`, `POST /trppu-api/scenarios/{id}/exclusions`, `DELETE /trppu-api/exclusions/{id}` |

Documentation interactive Swagger : `http://localhost:8080/docs`.

## 2. Workflow de statut

```
EN COURS ──► SIMULATION ──► VALIDE ──► VERROUILLE ──► ARCHIVE
   │            │             │             │
   │            ▼             ▼             │
   │         EN COURS     SIMULATION        │
   │                                        │
   └────────────────────────────────────────┘
                  ARCHIVE  (depuis n'importe quel statut sauf ARCHIVE)
```

| De \ Vers | EN COURS | SIMULATION | VALIDE | VERROUILLE | ARCHIVE |
|---|:-:|:-:|:-:|:-:|:-:|
| **EN COURS**   | – | OUI | NON | NON | OUI |
| **SIMULATION** | OUI | – | OUI | NON | OUI |
| **VALIDE**     | NON | OUI | – | OUI | OUI |
| **VERROUILLE** | NON | NON | NON | – | OUI |
| **ARCHIVE**    | NON | NON | NON | NON | – |

### Effets de bord

- **→ VALIDE** : pose `dt_validation = NOW()` si nul.
- **→ VERROUILLE** :
  - `est_fige = TRUE`,
  - `dt_mise_en_prod = NOW()`,
  - **snapshot des coefficients PIC** : copie de `trppu_pic_coefficients` (filtrés sur `id_pic_version` du scénario, dernière `dt_effet` par couple `co_produit`/`jour_semaine`) vers `trppu_scenario_pic_coeffs`.
- **→ ARCHIVE** : aucun effet additionnel.

## 3. Règle de figeage (verrouillage)

Toute mutation (PATCH scénario, ajout/modif/suppr neutralisation, ajout/suppr exclusion) **est refusée** si le scénario est figé, c'est-à-dire :

- `statut IN ('VERROUILLE', 'ARCHIVE')` **ou**
- `est_fige = TRUE`.

→ Réponse : `409 Conflict` avec `{"error": true, "message": "Scénario X figé...", "code": 409}`.

Seule exception : la transition `VERROUILLE → ARCHIVE` reste autorisée via `PATCH /scenarios/{id}/statut` ou `DELETE /scenarios/{id}`.

## 4. Versioning

À **chaque mutation**, `version_scenario` est incrémenté de 1 sur la ligne du scénario parent. La création initialise à `1`. Pas de duplication — un seul enregistrement par scénario.

## 5. Audit

- Header HTTP **`X-User`** lu sur chaque requête (fallback `"system"`).
- Chaque mutation insère une ligne dans `trppu_api_log` (`api_name`, `id_scenario`, `regate`, `dt_appel`, `caller`, `params` JSON) **dans la même transaction** que l'opération métier.

> ⚠️ L'authentification réelle (JWT) reste à câbler. Le header `X-User` est temporaire.

## 6. Format de réponse

### Succès
- `POST` création : `201` + `{"id_scenario|id": ..., "version_scenario": ..., "statut": ...}`.
- `GET` détail : `200` + ligne brute (DictCursor → champs SQL).
- `GET` liste : `{"count": n, "total": N, "limit": ..., "offset": ..., "data": [...]}`.
- `PATCH` / `DELETE` : `{"id_scenario|id": ..., "version_scenario": ...}` éventuellement enrichi.

### Erreur (uniforme avec `app/routes/trppu_trafics/errors.py`)
```json
{
  "detail": {
    "error": true,
    "message": "Description lisible",
    "code": 409
  }
}
```

| Code | Cas |
|---|---|
| `400` | Validation métier post-parse (dates incohérentes, fenêtre hors période) |
| `404` | Scénario / neutralisation / exclusion introuvable |
| `409` | Scénario figé, FK manquante (id_pic_version, co_produit), unicité violée |
| `422` | Pydantic (corps mal formé) ou transition de statut interdite |
| `500` | Erreur serveur |

## 7. Schémas Pydantic v2 (entrée)

| Schéma | Champs requis | Champs optionnels |
|---|---|---|
| `ScenarioCreate` | `co_roc(6)`, `co_regate(6)`, `lb_scenario(1-20)`, `periode_debut`, `periode_fin`, `id_pic_version>0` | `periode_realise_*`, `periode_prev_*` |
| `ScenarioUpdate` | – (au moins 1 champ) | tous ci-dessus sauf `co_roc`/`co_regate` |
| `StatutUpdate` | `statut` ∈ `{EN COURS, SIMULATION, VALIDE, VERROUILLE, ARCHIVE}` | – |
| `NeutralisationCreate` | `dt_debut`, `dt_fin`, `type` ∈ `{FERIE, PEAK, LOCAL}` | – |
| `NeutralisationUpdate` | – (au moins 1 champ) | `dt_debut`, `dt_fin`, `type` |
| `ExclusionCreate` | `co_produit(2)` | `motif(255)` |

Tous les schémas ont `extra="forbid"` : un champ inconnu provoque `422`.

## 8. Exemples `curl`

> Remplacer `:8080` par votre port et `<id>` par les identifiants réels.

### 8.1 Créer un scénario
```bash
curl -X POST http://localhost:8080/trppu-api/scenarios \
  -H "Content-Type: application/json" \
  -H "X-User: alice" \
  -d '{
    "co_roc": "ROC001",
    "co_regate": "REG001",
    "lb_scenario": "Scenario test 1",
    "periode_debut": "2026-05-01",
    "periode_fin": "2026-12-31",
    "id_pic_version": 1
  }'
# → 201 {"id_scenario": 42, "version_scenario": 1, "statut": "EN COURS"}
```

### 8.2 Lister
```bash
curl "http://localhost:8080/trppu-api/scenarios?co_regate=REG001&statut=EN%20COURS&limit=20"
```

### 8.3 Modifier
```bash
curl -X PATCH http://localhost:8080/trppu-api/scenarios/42 \
  -H "Content-Type: application/json" \
  -H "X-User: alice" \
  -d '{"lb_scenario": "Renommé"}'
# → 200 {"id_scenario": 42, "version_scenario": 2}
```

### 8.4 Ajouter une neutralisation
```bash
curl -X POST http://localhost:8080/trppu-api/scenarios/42/neutralisations \
  -H "Content-Type: application/json" \
  -H "X-User: alice" \
  -d '{"dt_debut": "2026-07-14", "dt_fin": "2026-07-14", "type": "FERIE"}'
# → 201 {"id": 7, "id_scenario": 42, "version_scenario": 3, "nb_jour": 1}
```

### 8.5 Ajouter une exclusion
```bash
curl -X POST http://localhost:8080/trppu-api/scenarios/42/exclusions \
  -H "Content-Type: application/json" \
  -H "X-User: alice" \
  -d '{"co_produit": "LR", "motif": "Hors périmètre"}'
```

### 8.6 Faire transiter le statut
```bash
# EN COURS → SIMULATION
curl -X PATCH http://localhost:8080/trppu-api/scenarios/42/statut \
  -H "Content-Type: application/json" \
  -H "X-User: alice" \
  -d '{"statut": "SIMULATION"}'

# SIMULATION → VALIDE
curl -X PATCH http://localhost:8080/trppu-api/scenarios/42/statut \
  -H "Content-Type: application/json" \
  -H "X-User: alice" \
  -d '{"statut": "VALIDE"}'

# VALIDE → VERROUILLE (déclenche le snapshot PIC)
curl -X PATCH http://localhost:8080/trppu-api/scenarios/42/statut \
  -H "Content-Type: application/json" \
  -H "X-User: alice" \
  -d '{"statut": "VERROUILLE"}'
# → {"id_scenario": 42, "version_scenario": 6, "statut": "VERROUILLE",
#    "pic_coeffs_snapshotes": 72}
```

### 8.7 Archivage (soft delete)
```bash
curl -X DELETE http://localhost:8080/trppu-api/scenarios/42 \
  -H "X-User: alice"
# → {"id_scenario": 42, "version_scenario": 7, "statut": "ARCHIVE"}
```

## 9. Architecture des fichiers

```
app/routes/scenarios/
├── __init__.py            # agrège les 3 sous-routers
├── routes.py              # CRUD scénarios + PATCH /statut
├── neutralisations.py     # CRUD neutralisations
├── exclusions.py          # CRUD exclusions
├── schemas.py             # Pydantic v2 (entrée uniquement)
├── statuts.py             # matrice de transitions + snapshot PIC
└── helpers.py             # garde figé, audit, version, validations métier
```

### Responsabilités

- **`helpers.py`** — fonctions communes : `get_caller`, `fetch_scenario`, `assert_not_fige`, `increment_version`, `log_api`, `validate_periode`, `validate_window_inside_scenario`, `assert_pic_version_exists`, `assert_produit_exists`, `last_insert_id`, builder d'`HTTPException` standard (`err`).
- **`statuts.py`** — `ALLOWED_TRANSITIONS`, `assert_transition_allowed`, `apply_transition_side_effects` (qui orchestre le SQL d'`UPDATE` + le snapshot PIC).
- **`schemas.py`** — modèles Pydantic v2 stricts (`extra="forbid"`).
- **`routes.py` / `neutralisations.py` / `exclusions.py`** — endpoints fins, toute mutation enveloppée dans `async with db_write.transaction() as tx`.

## 10. Limites connues / TODO

- **Auth** : seulement le header `X-User`. À remplacer par JWT.
- **FK manquantes côté schéma SQL** (cf. `analyse_db_scenario.md` §3.1) → on compense par des vérifications SQL applicatives (`assert_pic_version_exists`, `assert_produit_exists`).
- **Snapshot PIC** : règle "dernière `dt_effet`" par défaut — à confirmer avec le métier (cf. `analyse_db_scenario.md` §9.3).
- Pas encore de tests automatisés (le projet n'a pas de framework `pytest` installé).
- Endpoints sur `trppu_scenario_variations_prev`, `trppu_scenario_comptages_manuels`, `trppu_tmh`, `trppu_recalcul_log` : hors scope de ce chantier, à traiter ensuite.

## 11. Vérification rapide

```bash
# 1. Installer la nouvelle dépendance
pip install -r requirements.txt

# 2. Lancer l'API
uvicorn app.main:app --reload --port 8080

# 3. Vérifier la doc auto
# → http://localhost:8080/docs (tags Scenarios / Neutralisations / Exclusions)

# 4. Vérifier l'audit après quelques appels
mysql> SELECT api_name, id_scenario, caller, dt_appel
       FROM trppu_api_log
       WHERE id_scenario = 42
       ORDER BY dt_appel;
```
