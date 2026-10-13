# API `trppu_scenario` — CRUD + workflow de statut

> Module : `app/routes/trppu_scenario/`
> Préfixe HTTP : `/trppu-api/scenarios`
> Tag Swagger : **Scenarios**

Table de scénarios TRPPU avec workflow d'état (machine à états), dates de
validation/mise en prod, périodes principales et dérivées (réalisé/prévision),
et flag de figeage (`est_fige`).

---

## 1. Table `trppu_scenario` (extrait)

| Colonne                 | Type                                                       | Notes                                       |
|-------------------------|------------------------------------------------------------|---------------------------------------------|
| `id_scenario`           | BIGINT — **PK** AUTO_INCREMENT                             | Auto-généré                                 |
| `co_regate`             | CHAR(6) NOT NULL                                           | FK → `trppu_site`                           |
| `lb_scenario`           | VARCHAR(50) NOT NULL                                       | Libellé                                     |
| `co_roc`                | CHAR(6) NOT NULL                                           |                                             |
| `statut`                | ENUM('EN COURS','VALIDE','EN PRODUCTION','ARCHIVE') | Voir `GET /enums`                        |
| `dt_creation`           | DATETIME — auto                                            |                                             |
| `dt_validation`         | DATETIME NULL                                              | Posée auto au passage vers VALIDE           |
| `dt_mise_en_prod`       | DATETIME NULL                                              | Posée auto par `POST /mise-en-prod`         |
| `periode_debut/fin`     | DATE NOT NULL                                              | Bornes principales (saisies par l'utilisateur) |
| `dt_pivot`              | DATETIME NULL — exposé `dt_real_prev`                      | Écrit seulement sur pivot transmis (cf. §3.3) |
| `periode_realise_*`     | DATE NULL — **dérivées serveur**                           | [debut, pivot - 1], écrites avec `dt_pivot` |
| `periode_prev_*`        | DATE NULL — **dérivées serveur**                           | [pivot, fin], écrites avec `dt_pivot`       |
| `nb_jours_semaine`      | TINYINT NOT NULL CHECK IN (5,6)                            |                                             |
| `id_pic_version`        | INT NOT NULL                                               | FK → `trppu_pic_version`                    |
| `version_scenario`      | INT NOT NULL DEFAULT 1                                     | Incrémenté à chaque mutation                |
| `est_fige`              | TINYINT(1) NOT NULL DEFAULT 0                              | Bloque `update_periodes`, `lb_scenario`, `nb_jours_semaine` |

> Plus de colonne `id_scenario_parent` (retirée du modèle).

---

## 2. Machine à états

```
EN COURS      -> {VALIDE, ARCHIVE}
VALIDE        -> {EN COURS, ARCHIVE}              (EN PRODUCTION atteignable UNIQUEMENT via /mise-en-prod)
EN PRODUCTION  -> {ARCHIVE}
ARCHIVE    -> {}                               (terminal)
```

**Effets de bord** :
- Vers `VALIDE` : `dt_validation = COALESCE(dt_validation, NOW())`
- Vers `EN PRODUCTION` (via `/mise-en-prod` uniquement) : `dt_mise_en_prod = NOW()`, `est_fige = 1`, `dt_validation = COALESCE(dt_validation, NOW())`

---

## 3. Endpoints

| Méthode | Chemin | Description |
|---|---|---|
| `GET`   | `/trppu-api/scenarios` | Liste paginée + filtres `co_regate`, `co_roc`, `statut`, `est_fige` |
| `GET`   | `/trppu-api/scenarios/enums` | Valeurs autorisées pour `statut` |
| `GET`   | `/trppu-api/scenarios/{id_scenario}` | Récupération par PK |
| `POST`  | `/trppu-api/scenarios` | Création (statut initial = `EN COURS`, version = 1) |
| `DELETE`| `/trppu-api/scenarios/{id_scenario}` | Soft-delete : transition vers `ARCHIVE` |
| `PATCH` | `/trppu-api/scenarios/{id_scenario}/periodes` | MAJ `periode_debut/fin` + `date_pivot` **obligatoire** (pivot et realise/prev recalculés) |
| `PATCH` | `/trppu-api/scenarios/{id_scenario}/nb-jours-semaine` | MAJ 5 ou 6 |
| `PATCH` | `/trppu-api/scenarios/{id_scenario}/statut` | Transition de statut (sauf EN PRODUCTION) |
| `POST`  | `/trppu-api/scenarios/{id_scenario}/mise-en-prod` | **Seule manière d'atteindre EN PRODUCTION** |
| `PATCH` | `/trppu-api/scenarios/{id_scenario}/est-fige` | Force le flag est_fige |
| `PATCH` | `/trppu-api/scenarios/{id_scenario}/lb-scenario` | MAJ libellé |
| `POST`  | `/trppu-api/scenarios/{id_scenario}/duplicate` | Copie profonde (entête + historique TMH/PIC/etc.) en `EN COURS` v1 |

> Plus de `GET /history` (la lignée parent a été retirée du modèle).

### 3.1 `POST /trppu-api/scenarios`

```json
{
  "co_regate": "012345",
  "lb_scenario": "Scénario test",
  "co_roc": "012345",
  "nb_jours_semaine": 6,
  "id_pic_version": 1,
  "periode_debut": "2026-01-01",
  "periode_fin": "2026-12-31"
}
```

- `id_pic_version` : si null, le serveur prend la première `trppu_pic_version` avec `est_par_defaut=1` (sinon id=1, sinon 422).
- `periode_debut`/`fin` : si null, défaut today-1an / today+1an.
- `dt_real_prev` (`dt_pivot`), `periode_realise_*` et `periode_prev_*` ne sont **pas acceptés** dans le body et restent **NULL** à la création (cf. §3.3).
- Statut initial = `EN COURS`, `version_scenario = 1`, `est_fige = false`.

### 3.2 `GET /trppu-api/scenarios/enums`

```json
{ "statut": ["EN COURS", "VALIDE", "EN PRODUCTION", "ARCHIVE"] }
```

### 3.3 `PATCH /trppu-api/scenarios/{id_scenario}/periodes`

```json
{ "periode_debut": "2026-02-01", "periode_fin": "2026-11-30", "date_pivot": "2026-06-01" }
```

`date_pivot` est **obligatoire** ; `periode_debut` / `periode_fin` sont optionnels (absents =
valeurs existantes). `dt_pivot` et les bornes réalisé/prévision ne sont jamais déduits de la
date du jour : ils ne sont écrits que sur un pivot transmis, avec la même règle partout :

- ici (`PATCH /periodes`, pivot obligatoire, sur la période résultante) ;
- au `PUT /scenarios/{id}` si `date_pivot` est présent dans le body (absent = conservés) ;
- à la récupération des trafics Databricks,
  `GET /trppu-api/trafics/get_trafics_pivot?...&id_scenario={id}`, une fois toutes les
  requêtes réussies (période = `date_debut`/`date_fin` de la requête).

La création ne les renseigne pas (NULL).

```
dt_pivot      = date_pivot
realise_debut = debut                realise_fin = min(fin, date_pivot - 1)   (NULL si vide)
prev_debut    = max(debut, date_pivot)   prev_fin = fin                       (NULL si vide)
```

- Ce sont les plages mêmes qui ont servi à interroger Databricks (réalisé < pivot <= prév).
- Période entièrement passée (pivot > fin) : seules `realise_*` sont posées.
- Période entièrement future (pivot <= début) : seules `prev_*` sont posées.
- Rejets côté trafics : scénario introuvable (404), statut hors `EN COURS`/`SIMULATION`
  (409), `co_regate` différent de celui du scénario (400) — tous avant tout appel Databricks.

### 3.4 `PATCH /trppu-api/scenarios/{id_scenario}/statut`

```json
{ "statut": "VALIDE" }
```

- Lève **422** si la transition est interdite (cf. matrice).
- Lève **409** si la transition demandée est `VALIDE -> EN PRODUCTION` (réservée à `/mise-en-prod`).

### 3.5 `POST /trppu-api/scenarios/{id_scenario}/mise-en-prod`

Aucun body. Effets :
- `statut` devient `EN PRODUCTION`
- `dt_mise_en_prod = NOW()`
- `est_fige = 1`
- `dt_validation = COALESCE(dt_validation, NOW())` (filet de sécurité)
- `version_scenario += 1`

Lève **422** si le statut courant n'est pas `VALIDE`.

### 3.6 `POST /trppu-api/scenarios/{id_scenario}/duplicate`

Body requis (`id_rh` obligatoire, `lb_scenario` optionnel — défaut `"<source> (copie)"`,
tronqué à 20) :
```json
{ "id_rh": "U123456", "lb_scenario": "Mon clone" }
```

**Copie profonde** : le clone est créé en `EN COURS`, `version_scenario = 1`,
`est_fige = 0`, avec :
- l'entête complète (périodes, `nb_jours_*`, `dt_pivot`, `dt_mise_en_oeuvre`,
  flags `trafic_*_calcule`) — `dt_validation` / `dt_mise_en_prod` restent NULL ;
- toutes les données filles : `trppu_tmh`, `trppu_neutralisations`,
  `trppu_scenario_comptages_manuels`, `trppu_scenario_exclusions`,
  `trppu_scenario_variations_prev`, `trppu_scenario_pic_coeffs`,
  `trppu_trafic_agrebal`, `trppu_trafic_pdi` (dates d'historique conservées) ;
- le PIC : si la source a une version PIC niveau `SCENARIO`, une **nouvelle**
  `trppu_pic_version` est créée pour le clone avec la copie de ses
  `trppu_pic_coefficients` ; sinon le clone garde l'`id_pic_version` partagé.

L'`id_rh` du body (chiffré) devient l'auteur du clone (`id_rh_creation`) et
remplace l'`id_rh` des lignes filles copiées. Les logs (`trppu_api_log`,
`trppu_recalcul_log`) ne sont pas copiés. La duplication d'un scénario figé ou
archivé est permise. Pas de tracking parent : aucun lien clone↔source persisté.
Tout est exécuté dans une transaction unique (rollback complet en cas d'erreur).

---

## 4. Manœuvres opérationnelles

### 4.1 Mettre un scénario en production
```bash
# 1. Valider
curl -X PATCH .../scenarios/42/statut -d '{"statut":"VALIDE"}'
# 2. Mettre en prod
curl -X POST .../scenarios/42/mise-en-prod
# Le scénario est désormais EN PRODUCTION, est_fige=1, dt_mise_en_prod posée.
```

### 4.2 Modifier un scénario verrouillé
```bash
curl -X PATCH .../scenarios/42/est-fige -d '{"est_fige": false}'
curl -X PATCH .../scenarios/42/lb-scenario -d '{"lb_scenario":"Nouveau"}'
curl -X PATCH .../scenarios/42/est-fige -d '{"est_fige": true}'
```

### 4.3 Archiver
```bash
curl -X DELETE .../scenarios/42
```

---

## 5. Structure des fichiers

```
app/routes/trppu_scenario/
├── __init__.py
├── routes.py
├── schemas.py
├── helpers.py     # SELECT, default_periode, enregistrer_pivot, fetch_or_404, increment_version
└── statuts.py     # STATUTS, ALLOWED_TRANSITIONS, INTERNAL_TRANSITIONS, side-effects
```

Branchement dans `app/main.py` :
```python
from app.routes.trppu_scenario import router as trppu_scenario_router
app.include_router(trppu_scenario_router)
```
