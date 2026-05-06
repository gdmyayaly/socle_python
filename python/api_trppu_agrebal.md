# API `trppu_agrebal` — CRUD + Upload Excel

> Module : `app/routes/trppu_agrebal/`
> Préfixe HTTP : `/trppu-api/agrebals`
> Tag Swagger : **Agrebals**

Référentiel des amas (renommage de l'ancien « AMS »). **Définition** : un amas
est un regroupement géographique de PDI desservis de façon continue. Rattaché
à un site postal via `co_regate`. Référencée par FK `ON DELETE RESTRICT`
depuis `trppu_agrebal_pdi` — d'où la **soft delete** via `est_actif = 0`.

---

## 1. Table `trppu_agrebal`

| Colonne      | Type                 | Notes                                                 |
|--------------|----------------------|-------------------------------------------------------|
| `id_agrebal` | BIGINT — **PK**      | Fourni par l'utilisateur                              |
| `co_regate`  | CHAR(6) NOT NULL     | FK → `trppu_site.co_regate` (ON DELETE RESTRICT)      |
| `lb_agrebal` | VARCHAR(120) NULL    | Libellé                                               |
| `est_actif`  | TINYINT(1) DEFAULT 1 | 1 = actif, 0 = soft-deleted                           |
| `dt_maj`     | DATETIME — auto      |                                                       |

---

## 2. Endpoints

| Méthode | Chemin                                  | Description                                                  |
|---------|-----------------------------------------|--------------------------------------------------------------|
| `GET`   | `/trppu-api/agrebals`                   | Liste paginée + filtres `co_regate`, `est_actif`             |
| `GET`   | `/trppu-api/agrebals/{id_agrebal}`      | Récupération par PK                                          |
| `POST`  | `/trppu-api/agrebals`                   | Création (422 si site parent absent, 409 si déjà existant)   |
| `PUT`   | `/trppu-api/agrebals/{id_agrebal}`      | MAJ partielle (422 si nouveau co_regate absent)              |
| `DELETE`| `/trppu-api/agrebals/{id_agrebal}`      | Soft delete (`est_actif = 0`)                                |
| `POST`  | `/trppu-api/agrebals/upload-excel`      | Upload massif `.xlsx` (upsert + pré-check FK)                |

### 2.1 `GET /trppu-api/agrebals`

| Param       | Type     | Défaut | Description                       |
|-------------|----------|--------|-----------------------------------|
| `co_regate` | str(6)   | —      | Filtre par site parent            |
| `est_actif` | bool     | —      | Filtre actif/inactif              |
| `limit`     | int      | 100    | 1..1000                           |
| `offset`    | int      | 0      | ≥ 0                               |

### 2.2 `POST /trppu-api/agrebals`

```json
{
  "id_agrebal": 50001,
  "co_regate": "012345",
  "lb_agrebal": "Amas Paris Nord-Centre",
  "est_actif": true
}
```

- `422` si `co_regate` n'existe pas dans `trppu_site`.
- `409` si `id_agrebal` déjà présent.

### 2.3 `POST /trppu-api/agrebals/upload-excel`

Pré-vérification FK identique à `trppu_pdi` :
1. Parsing Pydantic ligne par ligne.
2. Vérification que tous les `co_regate` existent dans `trppu_site` (une seule requête).
3. Lignes orphelines collectées dans `errors`.
4. Lignes valides upsert dans une transaction unique.

---

## 3. Génération du template Excel

```bash
python scripts/generate_trppu_agrebal_template.py
python scripts/generate_trppu_agrebal_template.py --output /tmp/agrebals.xlsx
```

---

## 4. Manœuvres opérationnelles courantes

### 4.1 Réaffecter un amas à un autre site

```bash
curl -X PUT http://localhost:8080/trppu-api/agrebals/50001 \
  -H "Content-Type: application/json" -d '{"co_regate":"067890"}'
```

### 4.2 Désactiver puis réactiver

```bash
curl -X DELETE http://localhost:8080/trppu-api/agrebals/50001       # est_actif=0
curl -X PUT    http://localhost:8080/trppu-api/agrebals/50001 \
  -H "Content-Type: application/json" -d '{"est_actif": true}'
```

### 4.3 Ajouter une colonne sur `trppu_agrebal`

1. **SQL** : `ALTER TABLE trppu_agrebal ADD COLUMN ...`
2. **Pydantic** : `AgrebalBase` (et `AgrebalUpdate` si modifiable).
3. **SQL routes** : `SELECT_AGREBAL_SQL`, INSERT du `create_agrebal`, `UPSERT_SQL`.
4. **Excel** : `HEADERS` / `COLUMN_WIDTHS` / `EXAMPLE_ROWS` du script + `EXPECTED_HEADERS` du parser.

### 4.4 Purge physique (cas exceptionnel)

```sql
DELETE FROM trppu_agrebal WHERE id_agrebal = 50001;
-- Échoue si fk_agrpdi_agr (trppu_agrebal_pdi) bloque.
```

---

## 5. Structure des fichiers

```
app/routes/trppu_agrebal/
├── __init__.py
├── routes.py
├── schemas.py
└── helpers.py
scripts/
└── generate_trppu_agrebal_template.py
```
