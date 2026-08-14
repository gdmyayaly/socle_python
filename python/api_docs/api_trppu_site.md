# API `trppu_site` — CRUD + Upload Excel

> Module : `app/routes/trppu_site/`
> Préfixe HTTP : `/trppu-api/sites`
> Tag Swagger : **Sites**

Référentiel des sites postaux autorisés à utiliser TRPPU. Cette table est la
racine du modèle : elle est référencée par FK `ON DELETE RESTRICT` depuis
`trppu_pic_version` et `trppu_scenario`. Toute suppression physique d'un site
déjà référencé sera bloquée par MySQL.

---

## 1. Table `trppu_site`

| Colonne     | Type                  | Notes                                          |
|-------------|-----------------------|------------------------------------------------|
| `co_regate` | CHAR(6) — **PK**      | Code Regate, fourni par l'utilisateur          |
| `lb_regate` | VARCHAR(120) NULL     | Libellé associé au code Regate (optionnel)     |
| `type_site` | CHAR(5) NOT NULL      | Type libre (ex : PIC, PDC1, PDC2, PPDC, AUTRE) |
| `co_roc`    | CHAR(6) NOT NULL      | Code ROC                                       |
| `dt_maj`    | DATETIME — auto       | `DEFAULT CURRENT_TIMESTAMP ON UPDATE …`        |

> Plus de `lb_site` ni `est_actif` (retirés). Un site existe ou n'existe pas ;
> il n'y a plus de notion d'actif/inactif côté API ni de DELETE soft.

---

## 2. Endpoints

| Méthode | Chemin                           | Description                                                |
|---------|----------------------------------|------------------------------------------------------------|
| `GET`   | `/trppu-api/sites`               | Liste paginée + filtres `type_site`, `co_roc`              |
| `GET`   | `/trppu-api/sites/{co_regate}`   | Récupération par PK                                        |
| `POST`  | `/trppu-api/sites`               | Création (409 si déjà existant)                            |
| `PUT`   | `/trppu-api/sites/{co_regate}`   | Mise à jour partielle                                      |
| `POST`  | `/trppu-api/sites/upload-excel`  | Upload massif `.xlsx` (upsert)                             |

> Pas d'endpoint `DELETE` : la table n'a plus de colonne `est_actif`. Pour
> supprimer physiquement un site, passer par SQL direct (échouera si référencé
> par `trppu_pic_version` ou `trppu_scenario`).

### 2.1 `GET /trppu-api/sites`

| Param       | Type     | Défaut | Description                       |
|-------------|----------|--------|-----------------------------------|
| `type_site` | str(1-5) | —      | Filtre par type                   |
| `co_roc`    | str(6)   | —      | Filtre par code ROC               |
| `limit`     | int      | 100    | 1..1000                           |
| `offset`    | int      | 0      | ≥ 0                               |

### 2.2 `POST /trppu-api/sites`

```json
{
  "co_regate": "012345",
  "lb_regate": "Regate Paris Nord",
  "type_site": "PIC",
  "co_roc": "012345"
}
```

- `201 Created` → `SiteOut`
- `409 Conflict` si `co_regate` déjà présent.

### 2.3 `PUT /trppu-api/sites/{co_regate}`

Body partiel (tous les champs optionnels) :
```json
{ "lb_regate": "Nouveau libellé", "type_site": "PDC1" }
```

`co_regate` n'est **pas** modifiable (c'est la PK).

### 2.4 `POST /trppu-api/sites/upload-excel`

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
    { "row": 7, "error": "...", "raw": { ... } }
  ],
  "execution_time_s": 0.085
}
```

---

## 3. Template Excel attendu

> Le générateur `scripts/generate_trppu_site_template.py` a été supprimé — le fichier se
> construit à la main, aucune mise en forme n'est imposée par le parseur.

Le classeur doit porter :
- 4 colonnes (`co_regate`, `lb_regate`, `type_site`, `co_roc`)
- Format **texte** sur `co_regate`, `co_roc` et `type_site` : un format numérique perdrait les
  zéros de tête des codes régate

Colonnes obligatoires à l'upload : `co_regate`, `type_site`, `co_roc`. La colonne `lb_regate` est optionnelle.

---

## 4. Manœuvres opérationnelles courantes

### 4.1 Utiliser une nouvelle valeur de `type_site`

`type_site` est un `CHAR(5)` libre — n'importe quelle chaîne de 1 à 5 caractères
est acceptée. Aucun changement de code n'est nécessaire.

### 4.2 Ajouter une nouvelle colonne sur `trppu_site`

1. **SQL** : `ALTER TABLE trppu_site ADD COLUMN ...`
2. **Pydantic** : ajouter le champ dans `SiteBase` (et `SiteUpdate` si modifiable).
3. **SQL routes** : mettre à jour `SELECT_SITE_SQL`, l'`INSERT` du `create_site`, et `UPSERT_SQL` (`helpers.py`).
4. **Excel** : ajouter la colonne dans `EXPECTED_HEADERS` du parser (`helpers.py`).

### 4.3 Re-upload après correction d'erreurs

L'upload est idempotent (upsert) : on peut réuploader le même fichier après
correction des lignes en erreur, les lignes valides déjà importées seront
simplement « unchanged » (rowcount=0) ou « updated » si une valeur a changé.

### 4.4 Purger physiquement un site (cas exceptionnel)

À ne faire qu'après s'être assuré qu'aucune table fille ne le référence :
```sql
DELETE FROM trppu_site WHERE co_regate = '012345';
-- Échouera si fk_picv_site / fk_scen_site bloquent.
```

---

## 5. Structure des fichiers

```
app/routes/trppu_site/
├── __init__.py        # expose `router`
├── routes.py          # endpoints FastAPI
├── schemas.py         # Pydantic v2 (SiteCreate, SiteUpdate, SiteOut, BulkUploadResult…)
└── helpers.py         # parsing Excel (openpyxl) + UPSERT_SQL
```

Branchement dans `app/main.py` :
```python
from app.routes.trppu_site import router as trppu_site_router
app.include_router(trppu_site_router)
```

---

## 6. Tests manuels conseillés

1. `POST /trppu-api/sites` avec un site valide → 201.
2. Re-`POST` même `co_regate` → 409.
3. `GET /trppu-api/sites/{co_regate}` → site retourné.
4. `PUT /trppu-api/sites/{co_regate}` partiel → vérifier que seuls les champs envoyés changent et que `dt_maj` est mise à jour.
5. `POST /trppu-api/sites/upload-excel` avec le template rempli → vérifier compteurs.
6. Re-upload du même fichier → `nb_unchanged` doit être > 0.
7. Upload d'un fichier avec une ligne invalide (ex : `co_regate` < 6 chars) → la ligne apparaît dans `errors`, les autres lignes passent.
