# API `trppu_pic_coefficients` — CRUD + Upload Excel

> Module : `app/routes/trppu_pic_coefficients/`
> Préfixe HTTP : `/trppu-api/pic-coefficients`
> Tag Swagger : **PIC Coefficients**

Coefficients PIC (Plan Industriel et Commercial) par version, produit, jour de
semaine et période d'effet. Référencée par FK `ON DELETE CASCADE` depuis
`trppu_pic_version` (la suppression d'une version efface ses coefficients) et
`ON DELETE RESTRICT` depuis `trppu_produit`.

---

## 1. Table `trppu_pic_coefficients`

| Colonne          | Type                                              | Notes                                                          |
|------------------|---------------------------------------------------|----------------------------------------------------------------|
| `id_pic_coef`    | BIGINT — **PK** AUTO_INCREMENT                    | Auto-généré, jamais fourni en POST                             |
| `id_pic_version` | INT NOT NULL                                      | FK → `trppu_pic_version` (ON DELETE CASCADE)                   |
| `co_produit`     | CHAR(2) NOT NULL                                  | FK → `trppu_produit` (ON DELETE RESTRICT)                      |
| `jour_semaine`   | ENUM('LUN','MAR','MER','JEU','VEN','SAM') NOT NULL| Voir `GET /enums`                                              |
| `dt_effet`       | DATE NOT NULL                                     | Début de période                                               |
| `dt_fin_effet`   | DATE NULL                                         | Fin de période ; doit être **strictement** > dt_effet          |
| `coef_dense`     | DECIMAL(7,4) NOT NULL                             | ≥ 0                                                            |
| `coef_faible1`   | DECIMAL(7,4) NOT NULL                             | ≥ 0                                                            |
| `coef_faible2`   | DECIMAL(7,4) NOT NULL                             | ≥ 0                                                            |
| `dt_creation`    | DATETIME — auto                                   |                                                                |
| `dt_maj`         | DATETIME — auto                                   |                                                                |
| `id_rh_creation` | VARCHAR(40) NULL                                  | Tracking utilisateur (futur, non exposé en POST/PUT)           |

**Contraintes SQL** :
- `UNIQUE KEY uq_picc (id_pic_version, co_produit, jour_semaine, dt_effet)` — **clé naturelle utilisée pour l'upsert**.
- `CHECK (dt_fin_effet IS NULL OR dt_fin_effet > dt_effet)`.
- `CHECK (coef_dense >= 0 AND coef_faible1 >= 0 AND coef_faible2 >= 0)`.

> Pas d'`id_rh_maj` ici (contrairement à `trppu_pic_version`) — voir le NB en
> tête de `db3.sql`.

---

## 2. Endpoints

| Méthode | Chemin                                                  | Description                                                       |
|---------|---------------------------------------------------------|-------------------------------------------------------------------|
| `GET`   | `/trppu-api/pic-coefficients`                           | Liste paginée + filtres `id_pic_version`, `co_produit`, `jour_semaine`, `actif_only` |
| `GET`   | `/trppu-api/pic-coefficients/enums`                     | Valeurs autorisées des colonnes ENUM (`jour_semaine`)             |
| `GET`   | `/trppu-api/pic-coefficients/{id_pic_coef}`             | Récupération par PK                                               |
| `POST`  | `/trppu-api/pic-coefficients`                           | Création (422 si FK absente, 409 si natural key déjà présente)    |
| `PUT`   | `/trppu-api/pic-coefficients/{id_pic_coef}`             | MAJ partielle (422 si FK / dates incohérentes, 409 si collision NK) |
| `DELETE`| `/trppu-api/pic-coefficients/{id_pic_coef}`             | Soft delete (`dt_fin_effet = today`)                              |
| `POST`  | `/trppu-api/pic-coefficients/upload-excel`              | Upload massif `.xlsx` (upsert sur natural key)                    |

### 2.1 `GET /trppu-api/pic-coefficients`

| Param            | Type   | Défaut | Description                                          |
|------------------|--------|--------|------------------------------------------------------|
| `id_pic_version` | int    | —      | Filtre par version PIC                               |
| `co_produit`     | str(2) | —      | Filtre par produit                                   |
| `jour_semaine`   | enum   | —      | LUN/MAR/MER/JEU/VEN/SAM                              |
| `actif_only`     | bool   | false  | Si `true`, exclut les coefficients clos              |
| `limit`          | int    | 100    | 1..1000                                              |
| `offset`         | int    | 0      | ≥ 0                                                  |

### 2.2 `POST /trppu-api/pic-coefficients`

```json
{
  "id_pic_version": 1,
  "co_produit": "OO",
  "jour_semaine": "LUN",
  "dt_effet": "2026-01-01",
  "dt_fin_effet": null,
  "coef_dense": 1.0500,
  "coef_faible1": 0.8000,
  "coef_faible2": 0.6000
}
```

Codes de retour :
- `201 Created` → `PicCoefOut` (avec `id_pic_coef` auto-généré).
- `422 Unprocessable Entity` si `id_pic_version` absent de `trppu_pic_version`
  ou si `co_produit` absent de `trppu_produit`.
- `409 Conflict` si la combinaison
  `(id_pic_version, co_produit, jour_semaine, dt_effet)` existe déjà.

### 2.3 `PUT /trppu-api/pic-coefficients/{id_pic_coef}`

Body partiel. Validations supplémentaires :
- Si `id_pic_version` change : doit toujours exister.
- Si `co_produit` change : doit toujours exister.
- Si `dt_fin_effet` est posée : doit rester > `dt_effet` (en tenant compte d'un éventuel nouveau `dt_effet`).
- Si la modification fait entrer la nouvelle clé naturelle en collision avec un autre coefficient → `409`.

### 2.4 `DELETE /trppu-api/pic-coefficients/{id_pic_coef}`

Soft delete : positionne `dt_fin_effet = today`.

> Échoue en `422` si `dt_effet >= aujourd'hui` (le CHECK strict
> `dt_fin_effet > dt_effet` empêche de clôturer dans la même journée). Dans ce
> cas, utiliser PUT pour fixer une `dt_fin_effet` postérieure.

### 2.5 `POST /trppu-api/pic-coefficients/upload-excel`

Upsert transactionnel sur la natural key `uq_picc` :
- Si la combinaison `(id_pic_version, co_produit, jour_semaine, dt_effet)` existe déjà → `UPDATE` de `dt_fin_effet` + 3 coefficients.
- Sinon → `INSERT`.

Pré-vérifications batch :
- `id_pic_version` ∈ `trppu_pic_version`
- `co_produit` ∈ `trppu_produit`

Lignes orphelines collectées dans `errors` sans bloquer le reste.

---

## 3. Template Excel attendu

> Le générateur `scripts/generate_trppu_pic_coefficients_template.py` a été supprimé — le
> fichier se construit à la main, aucune mise en forme n'est imposée par le parseur.

Valeurs de `jour_semaine` conformes à l'énumération, format **texte** sur `co_produit`, format
date `YYYY-MM-DD` sur les colonnes de date et format décimal `0.0000` sur les coefficients.

---

## 4. Manœuvres opérationnelles courantes

### 4.1 Cloner les coefficients d'une version vers une nouvelle version

Pas d'endpoint dédié pour l'instant. Approche recommandée :
1. `GET /trppu-api/pic-coefficients?id_pic_version=N&limit=1000`
2. Modifier `id_pic_version` côté client → uploader via `POST /upload-excel`.

Si le besoin devient récurrent, ajouter un endpoint
`POST /trppu-api/pic-coefficients/clone?from=N&to=M`.

### 4.2 Clore un coefficient à une date future

```bash
curl -X PUT http://localhost:8080/trppu-api/pic-coefficients/123 \
  -H "Content-Type: application/json" \
  -d '{"dt_fin_effet": "2026-12-31"}'
```

### 4.3 Réouvrir un coefficient clos

```bash
curl -X PUT http://localhost:8080/trppu-api/pic-coefficients/123 \
  -H "Content-Type: application/json" -d '{"dt_fin_effet": null}'
```

### 4.4 Ajouter une valeur à `jour_semaine`

1. **SQL** : `ALTER TABLE trppu_pic_coefficients MODIFY jour_semaine ENUM('LUN','MAR','MER','JEU','VEN','SAM','DIM') NOT NULL;`
2. **Pydantic** : ajouter le membre dans `JourSemaineEnum` (`schemas.py`).
3. **Excel** : la nouvelle valeur est acceptée telle quelle dans le fichier uploadé.

### 4.5 Ajouter une colonne sur `trppu_pic_coefficients`

1. **SQL** : `ALTER TABLE trppu_pic_coefficients ADD COLUMN ...`
2. **Pydantic** : `PicCoefBase` (et `PicCoefUpdate` si modifiable).
3. **SQL routes** : `SELECT_PICC_SQL`, INSERT du `create_pic_coef`, et `UPSERT_SQL` (`helpers.py`).
4. **Excel** : ajouter l'en-tête dans `EXPECTED_HEADERS` du parser (`helpers.py`).

### 4.6 Purge physique (cas exceptionnel)

```sql
DELETE FROM trppu_pic_coefficients WHERE id_pic_coef = 123;
```

Pas de FK fille — la suppression physique est sûre. Pour purger en masse les
coefficients d'une version, supprimer la version (CASCADE).

---

## 5. Structure des fichiers

```
app/routes/trppu_pic_coefficients/
├── __init__.py
├── routes.py
├── schemas.py
└── helpers.py
```

Branchement dans `app/main.py` :
```python
from app.routes.trppu_pic_coefficients import router as trppu_pic_coefficients_router
app.include_router(trppu_pic_coefficients_router)
```
