# API `trppu_site` — CRUD + Upload Excel

> Module : `app/routes/trppu_site/`
> Préfixe HTTP : `/trppu-api/sites`
> Tag Swagger : **Sites**

Référentiel des sites postaux autorisés à utiliser TRPPU. Cette table est la
racine du modèle : elle est référencée par FK `ON DELETE RESTRICT` depuis
`trppu_pdi`, `trppu_agrebal`, `trppu_pic_version`, `trppu_scenario`. Toute
suppression physique d'un site déjà référencé sera bloquée par MySQL — d'où la
politique de **soft delete** (`est_actif = 0`).

---

## 1. Table `trppu_site`

| Colonne     | Type                          | Notes                                          |
|-------------|-------------------------------|------------------------------------------------|
| `co_regate` | CHAR(6) — **PK**              | Code Regate, fourni par l'utilisateur          |
| `lb_regate` | VARCHAR(120) NULL             | Libellé associé au code Regate (optionnel)     |
| `lb_site`   | VARCHAR(120) NOT NULL         | Libellé du site                                |
| `type_site` | CHAR(5) NOT NULL              | Type libre (ex : PIC, PDC1, PDC2, PPDC, AUTRE) |
| `co_roc`    | CHAR(6) NOT NULL              | Code ROC                                       |
| `est_actif` | TINYINT(1) NOT NULL DEFAULT 1 | 1 = actif, 0 = soft-deleted                    |
| `dt_maj`    | DATETIME — auto               | `DEFAULT CURRENT_TIMESTAMP ON UPDATE …`        |

---

## 2. Endpoints

| Méthode | Chemin                                | Description                                           |
|---------|---------------------------------------|-------------------------------------------------------|
| `GET`   | `/trppu-api/sites`                    | Liste paginée + filtres `type_site`, `est_actif`, `co_roc` |
| `GET`   | `/trppu-api/sites/{co_regate}`        | Récupération par PK                                   |
| `POST`  | `/trppu-api/sites`                    | Création (409 si déjà existant)                       |
| `PUT`   | `/trppu-api/sites/{co_regate}`        | Mise à jour partielle                                 |
| `DELETE`| `/trppu-api/sites/{co_regate}`        | Soft delete (`est_actif = 0`)                         |
| `POST`  | `/trppu-api/sites/upload-excel`       | Upload massif `.xlsx` (upsert)                        |

> Note : `type_site` étant désormais un `CHAR(5)` (texte libre, plus un ENUM), il
> n'y a plus d'endpoint `/enums` sur ce module.

### 2.1 `GET /trppu-api/sites`

**Query params**

| Param       | Type            | Défaut | Description                            |
|-------------|-----------------|--------|----------------------------------------|
| `type_site` | enum (optionnel)| —      | Filtre par type                        |
| `est_actif` | bool (optionnel)| —      | `true` / `false`                       |
| `co_roc`    | str(6)          | —      | Filtre par code ROC                    |
| `limit`     | int 1..1000     | 100    | Pagination                             |
| `offset`    | int ≥ 0         | 0      | Pagination                             |

**Réponse** : `list[SiteOut]`

### 2.2 `POST /trppu-api/sites`

```json
{
  "co_regate": "012345",
  "lb_regate": "Regate Paris Nord",
  "lb_site": "PIC Paris Nord",
  "type_site": "PIC",
  "co_roc": "012345",
  "est_actif": true
}
```

- `201 Created` → `SiteOut`
- `409 Conflict` si `co_regate` déjà présent (même soft-deleted).

### 2.3 `PUT /trppu-api/sites/{co_regate}`

Body partiel (tous les champs optionnels) :
```json
{ "lb_site": "Nouveau libellé", "est_actif": false }
```

`co_regate` n'est **pas** modifiable (c'est la PK).

### 2.4 `DELETE /trppu-api/sites/{co_regate}`

Soft delete. Réponse :
```json
{ "co_regate": "012345", "est_actif": 0, "rows_affected": 1 }
```

> ⚠️ Pour réactiver un site, faire `PUT { "est_actif": true }`.
> Une suppression physique n'est volontairement pas exposée. Si elle s'avère
> nécessaire (purge), passer par SQL direct après avoir vérifié toutes les FK.

### 2.5 `POST /trppu-api/sites/upload-excel`

Multipart/form-data, champ `file` = fichier `.xlsx` ou `.xlsm`.

Comportement :
- Lecture de la **première feuille**, ligne 1 = en-têtes.
- Validation ligne par ligne via Pydantic (les invalides ne stoppent pas le lot).
- Écriture en transaction unique (`INSERT … ON DUPLICATE KEY UPDATE`).
- Compteurs MySQL : `rowcount=1` → insert, `rowcount=2` → update, `rowcount=0` → inchangé.

Réponse `BulkUploadResult` :
```json
{
  "nb_rows_read": 12,
  "nb_inserted": 8,
  "nb_updated": 3,
  "nb_unchanged": 0,
  "nb_errors": 1,
  "errors": [
    { "row": 7, "error": "type_site invalid…", "raw": { ... } }
  ],
  "execution_time_s": 0.085
}
```

---

## 3. Génération du template Excel

```bash
python scripts/generate_trppu_site_template.py
# ou avec un chemin de sortie spécifique :
python scripts/generate_trppu_site_template.py --output /tmp/sites.xlsx
```

Produit un fichier avec :
- 6 colonnes (`co_regate`, `lb_regate`, `lb_site`, `type_site`, `co_roc`, `est_actif`)
- 3 lignes d'exemple
- Liste déroulante Excel sur `est_actif` (0/1)
- Format texte sur `co_regate`, `co_roc` et `type_site` (préserve les zéros / pas de coercition)
- Une feuille « notice » avec les instructions

Colonnes obligatoires à l'upload : `co_regate`, `lb_site`, `type_site`, `co_roc`. La colonne `lb_regate` est optionnelle (peut être vide).

---

## 4. Manœuvres opérationnelles courantes

### 4.1 Utiliser une nouvelle valeur de `type_site`

`type_site` est désormais un `CHAR(5)` libre — n'importe quelle chaîne de 1 à 5
caractères est acceptée. Aucun changement de code n'est nécessaire pour
introduire un nouveau type ; il suffit de l'utiliser dans les requêtes ou
l'Excel. Si une contrainte de valeurs autorisées devient nécessaire, la
réintroduire au choix : ENUM SQL, table de référence, ou validation Pydantic.

### 4.2 Ajouter une nouvelle colonne sur `trppu_site`

1. **SQL** : `ALTER TABLE trppu_site ADD COLUMN ...`
2. **Pydantic** : ajouter le champ dans `SiteBase` (et `SiteUpdate` si modifiable).
3. **SQL routes** : mettre à jour `SELECT_SITE_SQL`, l'`INSERT` du `create_site`, et `UPSERT_SQL` (`helpers.py`).
4. **Excel** : ajouter la colonne dans `HEADERS` / `COLUMN_WIDTHS` / `EXAMPLE_ROWS` du script template, et dans `EXPECTED_HEADERS` du parser (`helpers.py`).

### 4.3 Réactiver un site soft-deleted

```bash
curl -X PUT http://localhost:8080/trppu-api/sites/012345 \
     -H "Content-Type: application/json" \
     -d '{"est_actif": true}'
```

### 4.4 Re-upload après correction d'erreurs

L'upload est idempotent (upsert) : on peut réuploader le même fichier après
correction des lignes en erreur, les lignes valides déjà importées seront
simplement « unchanged » (rowcount=0) ou « updated » si une valeur a changé.

### 4.5 Purger physiquement un site (cas exceptionnel)

À ne faire qu'après s'être assuré qu'aucune table fille ne le référence :
```sql
DELETE FROM trppu_site WHERE co_regate = '012345';
-- Échouera si fk_pdi_site / fk_agrebal_site / fk_picv_site / fk_scen_site bloquent.
```

---

## 5. Structure des fichiers

```
app/routes/trppu_site/
├── __init__.py        # expose `router`
├── routes.py          # endpoints FastAPI
├── schemas.py         # Pydantic v2 (TypeSiteEnum, SiteCreate, SiteUpdate, SiteOut, BulkUploadResult…)
└── helpers.py         # parsing Excel (openpyxl) + UPSERT_SQL
scripts/
└── generate_trppu_site_template.py
```

Branchement dans `app/main.py` :
```python
from app.routes.trppu_site import router as trppu_site_router
app.include_router(trppu_site_router)
```

Dépendances ajoutées à `requirements.txt` : `openpyxl`, `python-multipart`.

---

## 6. Tests manuels conseillés

1. `GET /trppu-api/sites/enums` → vérifier la liste retournée.
2. `POST /trppu-api/sites` avec un site valide → 201.
3. Re-`POST` même `co_regate` → 409.
4. `GET /trppu-api/sites/{co_regate}` → site retourné.
5. `PUT /trppu-api/sites/{co_regate}` partiel → vérifier que seuls les champs envoyés changent et que `dt_maj` est mise à jour.
6. `DELETE /trppu-api/sites/{co_regate}` → vérifier `est_actif = 0` en base.
7. `POST /trppu-api/sites/upload-excel` avec le template rempli → vérifier compteurs.
8. Re-upload du même fichier → `nb_unchanged` doit être > 0.
9. Upload d'un fichier avec une ligne invalide (ex : `type_site = "XXX"`) → la ligne apparaît dans `errors`, les autres lignes passent.
