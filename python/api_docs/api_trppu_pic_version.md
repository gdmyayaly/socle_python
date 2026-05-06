# API `trppu_pic_version` — CRUD + Upload Excel

> Module : `app/routes/trppu_pic_version/`
> Préfixe HTTP : `/trppu-api/pic-versions`
> Tag Swagger : **PIC Versions**

Référentiel des versions de courbe PIC, rattachées à un site postal via
`co_regate`. Référencée par FK `ON DELETE CASCADE` depuis
`trppu_pic_coefficients` et `ON DELETE RESTRICT` depuis `trppu_scenario`.
La **soft delete** se fait via `dt_desactivation` (DATETIME).

---

## 1. Table `trppu_pic_version`

| Colonne               | Type                                | Notes                                                            |
|-----------------------|-------------------------------------|------------------------------------------------------------------|
| `id_pic_version`      | INT — **PK** AUTO_INCREMENT         | Auto-généré par la base, **jamais fourni en POST**               |
| `lb_pic_version`      | VARCHAR(80) NULL                    | Libellé court                                                    |
| `niveau`              | ENUM('NATIONAL','DEX','SITE') NOT NULL | Voir `GET /enums`                                             |
| `co_regate`           | CHAR(6) NOT NULL                    | FK → `trppu_site.co_regate` (ON DELETE RESTRICT)                 |
| `dt_activation`       | DATETIME NOT NULL                   | Date/heure d'activation                                          |
| `dt_desactivation`    | DATETIME NULL                       | Si renseignée, doit être **strictement** > dt_activation         |
| `motif_desactivation` | VARCHAR(255) NULL                   | Motif libre                                                      |
| `commentaire`         | VARCHAR(500) NULL                   | Commentaire libre                                                |
| `est_par_defaut`      | TINYINT(1) DEFAULT 0                | Marque la version par défaut pour le site (non unique)           |
| `dt_creation`         | DATETIME — auto                     |                                                                  |
| `dt_maj`              | DATETIME — auto                     |                                                                  |
| `id_rh_creation`      | VARCHAR(40) NULL                    | Tracking utilisateur (futur, non exposé en POST/PUT)             |
| `id_rh_maj`           | VARCHAR(40) NULL                    | Tracking utilisateur (futur, non exposé en POST/PUT)             |

**Contrainte SQL** : `CHECK (dt_desactivation IS NULL OR dt_desactivation > dt_activation)`.

> Le schéma n'enforce **pas** l'unicité de `est_par_defaut=1` par site. Si cette
> règle métier devient nécessaire, la mettre en place via :
> 1. un trigger SQL, ou
> 2. la logique applicative dans `create_pic_version` / `update_pic_version`
> (positionner les autres versions à 0 dans la même transaction).

---

## 2. Endpoints

| Méthode | Chemin                                              | Description                                                  |
|---------|-----------------------------------------------------|--------------------------------------------------------------|
| `GET`   | `/trppu-api/pic-versions`                           | Liste paginée + filtres `co_regate`, `niveau`, `actif_only`, `est_par_defaut` |
| `GET`   | `/trppu-api/pic-versions/enums`                     | Valeurs autorisées des colonnes ENUM (`niveau`)              |
| `GET`   | `/trppu-api/pic-versions/{id_pic_version}`          | Récupération par PK                                          |
| `POST`  | `/trppu-api/pic-versions`                           | Création (422 si site parent absent)                         |
| `PUT`   | `/trppu-api/pic-versions/{id_pic_version}`          | MAJ partielle (422 si dates incohérentes / site absent)      |
| `DELETE`| `/trppu-api/pic-versions/{id_pic_version}?motif=...`| Soft delete (`dt_desactivation = NOW()`, `motif_desactivation`) |
| `POST`  | `/trppu-api/pic-versions/upload-excel`              | Upload massif `.xlsx` — **INSERT-only** (id auto-généré)     |

### 2.1 `GET /trppu-api/pic-versions`

| Param            | Type | Défaut | Description                                          |
|------------------|------|--------|------------------------------------------------------|
| `co_regate`      | str(6) | —    | Filtre par site                                      |
| `niveau`         | enum | —      | NATIONAL / DEX / SITE                                |
| `actif_only`     | bool | false  | Si `true`, exclut les versions désactivées           |
| `est_par_defaut` | bool | —      | Filtre flag par défaut                               |
| `limit`          | int  | 100    | 1..1000                                              |
| `offset`         | int  | 0      | ≥ 0                                                  |

### 2.2 `GET /trppu-api/pic-versions/enums`

```json
{ "niveau": ["NATIONAL", "DEX", "SITE"] }
```

### 2.3 `POST /trppu-api/pic-versions`

```json
{
  "lb_pic_version": "PIC 2026 v1",
  "niveau": "NATIONAL",
  "co_regate": "012345",
  "dt_activation": "2026-01-01T00:00:00",
  "dt_desactivation": null,
  "motif_desactivation": null,
  "commentaire": "Première version",
  "est_par_defaut": true
}
```

- `id_pic_version` est généré côté base et retourné dans la réponse.
- `422` si `co_regate` n'existe pas dans `trppu_site`.

### 2.4 `PUT /trppu-api/pic-versions/{id_pic_version}`

Body partiel. Validation supplémentaire : si `dt_desactivation` est posée, elle
doit rester strictement > `dt_activation` (en tenant compte d'un éventuel
nouveau `dt_activation` envoyé dans le même body).

### 2.5 `DELETE /trppu-api/pic-versions/{id_pic_version}?motif=...`

Soft delete : positionne `dt_desactivation = NOW()` et `motif_desactivation`
(défaut « Désactivé via API »).

> Échoue en `422` si `dt_activation` est dans le futur, à cause du CHECK strict
> SQL. Dans ce cas, utiliser `PUT` pour fixer une date de désactivation
> postérieure à l'activation.

```json
{ "id_pic_version": 42, "dt_desactivation": "2026-05-06T14:32:11",
  "motif_desactivation": "Désactivé via API", "rows_affected": 1 }
```

### 2.6 `POST /trppu-api/pic-versions/upload-excel`

**INSERT-only** : chaque ligne crée une nouvelle version avec un
`id_pic_version` auto-généré.

- Première feuille, ligne 1 = en-têtes.
- Validation Pydantic ligne par ligne, erreurs collectées sans interrompre.
- Pré-vérification que tous les `co_regate` existent dans `trppu_site`.
- Insertion dans une transaction unique.
- `nb_updated` et `nb_unchanged` sont toujours à 0 (pas d'upsert sur PK auto).

---

## 3. Génération du template Excel

```bash
python scripts/generate_trppu_pic_version_template.py
python scripts/generate_trppu_pic_version_template.py --output /tmp/picv.xlsx
```

Inclut des listes déroulantes Excel sur `niveau` et `est_par_defaut`,
un format texte sur `co_regate`, et un format `YYYY-MM-DD HH:MM:SS` sur les
colonnes datetime.

---

## 4. Manœuvres opérationnelles courantes

### 4.1 Ajouter une valeur à `niveau`

1. **SQL** : `ALTER TABLE trppu_pic_version MODIFY niveau ENUM('NATIONAL','DEX','SITE','NOUVEAU') NOT NULL;`
2. **Pydantic** : ajouter le membre dans `NiveauEnum` (`schemas.py`).
3. **Excel** : ajouter la valeur dans `NIVEAU_VALUES` du script template.

### 4.2 Faire pointer la version par défaut sur une autre

```bash
# (1) Dépointer l'ancienne version par défaut
curl -X PUT http://localhost:8080/trppu-api/pic-versions/41 \
  -H "Content-Type: application/json" -d '{"est_par_defaut": false}'
# (2) Pointer la nouvelle
curl -X PUT http://localhost:8080/trppu-api/pic-versions/42 \
  -H "Content-Type: application/json" -d '{"est_par_defaut": true}'
```

> Si l'unicité du flag `est_par_defaut=1` par site doit devenir une garantie,
> implémenter la bascule automatique dans `create_pic_version` /
> `update_pic_version` (un seul endpoint = une transaction).

### 4.3 Réactiver une version désactivée

```bash
curl -X PUT http://localhost:8080/trppu-api/pic-versions/42 \
  -H "Content-Type: application/json" \
  -d '{"dt_desactivation": null, "motif_desactivation": null}'
```

### 4.4 Ajouter une colonne sur `trppu_pic_version`

1. **SQL** : `ALTER TABLE trppu_pic_version ADD COLUMN ...`
2. **Pydantic** : ajouter le champ dans `PicVersionBase` (et `PicVersionUpdate`).
3. **SQL routes** : `SELECT_PICV_SQL`, `INSERT_SQL` (`helpers.py`).
4. **Excel** : `HEADERS` / `COLUMN_WIDTHS` / `EXAMPLE_ROWS` du script + `EXPECTED_HEADERS` du parser.

### 4.5 Purge physique (cas exceptionnel)

```sql
-- Attention : trppu_pic_coefficients est en CASCADE
-- (la suppression efface aussi les coefficients liés).
-- En revanche, fk_scen_picv (trppu_scenario) est en RESTRICT — bloquant.
DELETE FROM trppu_pic_version WHERE id_pic_version = 42;
```

---

## 5. Structure des fichiers

```
app/routes/trppu_pic_version/
├── __init__.py
├── routes.py
├── schemas.py
└── helpers.py
scripts/
└── generate_trppu_pic_version_template.py
```

Branchement dans `app/main.py` :
```python
from app.routes.trppu_pic_version import router as trppu_pic_version_router
app.include_router(trppu_pic_version_router)
```
