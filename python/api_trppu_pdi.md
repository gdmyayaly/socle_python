# API `trppu_pdi` — CRUD + Upload Excel

> Module : `app/routes/trppu_pdi/`
> Préfixe HTTP : `/trppu-api/pdis`
> Tag Swagger : **PDI**

Référentiel des PDI (Points de Distribution Intermédiaire), rattachés à un
site postal via `co_regate`. Référencée par FK `ON DELETE RESTRICT` depuis
`trppu_agrebal_pdi` et `trppu_cles_repartition` — d'où la **soft delete** via
`est_actif = 0`.

---

## 1. Table `trppu_pdi`

| Colonne     | Type                 | Notes                                                |
|-------------|----------------------|------------------------------------------------------|
| `id_pdi`    | BIGINT — **PK**      | Fourni par l'utilisateur (vient d'une source externe) |
| `co_regate` | CHAR(6) NOT NULL     | FK → `trppu_site.co_regate` (ON DELETE RESTRICT)     |
| `lb_pdi`    | VARCHAR(150) NULL    | Libellé                                              |
| `est_actif` | TINYINT(1) DEFAULT 1 | 1 = actif, 0 = soft-deleted                          |
| `dt_maj`    | DATETIME — auto      |                                                      |

---

## 2. Endpoints

| Méthode | Chemin                          | Description                                                  |
|---------|---------------------------------|--------------------------------------------------------------|
| `GET`   | `/trppu-api/pdis`               | Liste paginée + filtres `co_regate`, `est_actif`             |
| `GET`   | `/trppu-api/pdis/{id_pdi}`      | Récupération par PK                                          |
| `POST`  | `/trppu-api/pdis`               | Création (422 si site parent absent, 409 si déjà existant)   |
| `PUT`   | `/trppu-api/pdis/{id_pdi}`      | MAJ partielle (422 si nouveau co_regate absent)              |
| `DELETE`| `/trppu-api/pdis/{id_pdi}`      | Soft delete (`est_actif = 0`)                                |
| `POST`  | `/trppu-api/pdis/upload-excel`  | Upload massif `.xlsx` (upsert + pré-check FK)                |

### 2.1 `GET /trppu-api/pdis`

| Param       | Type     | Défaut | Description                       |
|-------------|----------|--------|-----------------------------------|
| `co_regate` | str(6)   | —      | Filtre par site parent            |
| `est_actif` | bool     | —      | Filtre actif/inactif              |
| `limit`     | int      | 100    | 1..1000                           |
| `offset`    | int      | 0      | ≥ 0                               |

### 2.2 `POST /trppu-api/pdis`

```json
{
  "id_pdi": 10001,
  "co_regate": "012345",
  "lb_pdi": "PDI Paris Centre",
  "est_actif": true
}
```

- `422` si `co_regate` n'existe pas dans `trppu_site`.
- `409` si `id_pdi` déjà présent.

### 2.3 `POST /trppu-api/pdis/upload-excel`

L'endpoint d'upload effectue une **pré-vérification d'intégrité** :
1. Parsing Pydantic de chaque ligne (erreurs collectées).
2. Récupération des `co_regate` existants en une seule requête.
3. Toute ligne référençant un `co_regate` absent part en erreur (`row`, `error`, `raw`).
4. Les lignes valides sont upsert dans une transaction unique.

---

## 3. Génération du template Excel

```bash
python scripts/generate_trppu_pdi_template.py
python scripts/generate_trppu_pdi_template.py --output /tmp/pdis.xlsx
```

---

## 4. Manœuvres opérationnelles courantes

### 4.1 Créer en masse pour un nouveau site

1. Créer le site (`POST /trppu-api/sites`).
2. Remplir le template PDI avec ce `co_regate`.
3. `POST /trppu-api/pdis/upload-excel` avec le fichier.

### 4.2 Réaffecter un PDI à un autre site

```bash
curl -X PUT http://localhost:8080/trppu-api/pdis/10001 \
  -H "Content-Type: application/json" \
  -d '{"co_regate":"067890"}'
```

### 4.3 Désactiver puis réactiver

```bash
curl -X DELETE http://localhost:8080/trppu-api/pdis/10001       # est_actif=0
curl -X PUT    http://localhost:8080/trppu-api/pdis/10001 \
  -H "Content-Type: application/json" -d '{"est_actif": true}'  # réactivation
```

### 4.4 Ajouter une colonne sur `trppu_pdi`

1. **SQL** : `ALTER TABLE trppu_pdi ADD COLUMN ...`
2. **Pydantic** : ajouter le champ dans `PdiBase` (et `PdiUpdate` si modifiable).
3. **SQL routes** : `SELECT_PDI_SQL`, INSERT du `create_pdi`, `UPSERT_SQL`.
4. **Excel** : `HEADERS` / `COLUMN_WIDTHS` / `EXAMPLE_ROWS` du script + `EXPECTED_HEADERS` du parser.

### 4.5 Purge physique (cas exceptionnel)

```sql
DELETE FROM trppu_pdi WHERE id_pdi = 10001;
-- Échoue si fk_agrpdi_pdi (trppu_agrebal_pdi) ou fk_cles_pdi (trppu_cles_repartition) bloquent.
```

---

## 5. Structure des fichiers

```
app/routes/trppu_pdi/
├── __init__.py
├── routes.py
├── schemas.py
└── helpers.py
scripts/
└── generate_trppu_pdi_template.py
```
