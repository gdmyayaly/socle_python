# API `trppu_produit` — CRUD + Upload Excel

> Module : `app/routes/trppu_produit/`
> Préfixe HTTP : `/trppu-api/produits`
> Tag Swagger : **Produits**

Référentiel des produits postaux (OO, LR, Presse, …). Référencée par FK
`ON DELETE RESTRICT` depuis `trppu_pic_coefficients`,
`trppu_scenario_exclusions`, `trppu_scenario_pic_coeffs`,
`trppu_scenario_variations_prev`, `trppu_scenario_comptages_manuels`, `trppu_tmh`.
La suppression physique est presque toujours bloquée — la **soft delete** se
fait via `dt_desactivation` (et `motif_desactivation`).

---

## 1. Table `trppu_produit`

| Colonne               | Type                  | Notes                                                                |
|-----------------------|-----------------------|----------------------------------------------------------------------|
| `co_produit`          | CHAR(2) — **PK**      | Code produit fourni par l'utilisateur                                |
| `lb_produit`          | VARCHAR(80) NOT NULL  | Libellé                                                              |
| `dt_creation`         | DATETIME — auto       | `DEFAULT CURRENT_TIMESTAMP`, non modifiable via API                  |
| `dt_desactivation`    | DATE NULL             | Si renseignée, doit être >= `DATE(dt_creation)`                      |
| `motif_desactivation` | VARCHAR(255) NULL     | Motif libre                                                          |

**Contrainte SQL** : `CHECK (dt_desactivation IS NULL OR dt_desactivation >= DATE(dt_creation))`.

### 1.1 Création automatique depuis le TMH

Le référentiel des objets traités est piloté par **Databricks** (`co_type_objet` des tables
`g_trppu_trafics_*_3`, restitution dynamique par `get_trafics_pivot`) : un objet peut apparaître
dans les trafics sans exister dans `trppu_produit`. Pour éviter que la FK `fk_tmh_produit` casse
la transaction et remonte une 500 opaque, tout chemin d'écriture TMH crée le produit manquant
avant l'insert :

| Endpoint | Effet |
|----------|-------|
| `POST /trppu-api/scenarios/{id}/tmh` | crée le `co_produit` s'il est absent |
| `PUT /trppu-api/scenarios/{id}/tmh` | idem pour chaque ligne du lot |
| `POST` / `PUT /trppu-api/scenarios` | idem pour les lignes TMH du payload |

- Implémentation : `ensure_produits_exist` (`app/routes/trppu_produit/helpers.py`), en
  `INSERT ... ON DUPLICATE KEY UPDATE` no-op — idempotent, et le libellé d'un produit déjà saisi
  par le métier n'est **jamais** écrasé.
- `lb_produit` est repris de `g_trppu_obj_mapping` (`lb_type_objet`). La colonne étant NOT NULL,
  un objet inconnu du mapping (`PR`, `PPI` — cf. DSR-679) est créé **avec son code pour
  libellé** ; à corriger ensuite via `PUT /trppu-api/produits/{co_produit}`.
- Les codes créés sont tracés dans les logs (`Produits créés automatiquement : …`).

---

## 2. Endpoints

| Méthode | Chemin                                       | Description                                                |
|---------|----------------------------------------------|------------------------------------------------------------|
| `GET`   | `/trppu-api/produits`                        | Liste paginée + filtre `actif_only`                        |
| `GET`   | `/trppu-api/produits/{co_produit}`           | Récupération par PK                                        |
| `POST`  | `/trppu-api/produits`                        | Création (409 si déjà existant)                            |
| `PUT`   | `/trppu-api/produits/{co_produit}`           | MAJ partielle                                              |
| `DELETE`| `/trppu-api/produits/{co_produit}?motif=...` | Soft delete (`dt_desactivation = today`, `motif_desactivation`) |
| `POST`  | `/trppu-api/produits/upload-excel`           | Upload massif `.xlsx` (upsert)                             |

### 2.1 `GET /trppu-api/produits`

| Param        | Type | Défaut | Description                                                       |
|--------------|------|--------|-------------------------------------------------------------------|
| `actif_only` | bool | false  | Si `true`, filtre `dt_desactivation IS NULL OR > CURDATE()`       |
| `limit`      | int  | 100    | 1..1000                                                           |
| `offset`     | int  | 0      | ≥ 0                                                               |

### 2.2 `POST /trppu-api/produits`

```json
{
  "co_produit": "OO",
  "lb_produit": "Ordinaire Ouvert",
  "dt_desactivation": null,
  "motif_desactivation": null
}
```

`dt_creation` est posée automatiquement par la base.

### 2.3 `PUT /trppu-api/produits/{co_produit}`

Body partiel. Champs modifiables : `lb_produit`, `dt_desactivation`, `motif_desactivation`.

### 2.4 `DELETE /trppu-api/produits/{co_produit}?motif=...`

Soft delete : passe `dt_desactivation` à aujourd'hui et fixe
`motif_desactivation` (par défaut « Désactivé via API »).

```json
{ "co_produit": "PR", "dt_desactivation": "2026-05-06",
  "motif_desactivation": "Fin de contrat", "rows_affected": 1 }
```

> Pour réactiver : `PUT { "dt_desactivation": null, "motif_desactivation": null }`.

### 2.5 `POST /trppu-api/produits/upload-excel`

Upsert transactionnel.
- Première feuille, ligne 1 = en-têtes.
- Validation Pydantic ligne par ligne, erreurs collectées.
- Compteurs `nb_inserted` / `nb_updated` / `nb_unchanged`.

**Colonnes obligatoires** : `co_produit`, `lb_produit`.
Optionnelles : `dt_desactivation`, `motif_desactivation`.
`dt_creation` n'est jamais importée (auto en base).

---

## 3. Génération du template Excel

```bash
python scripts/generate_trppu_produit_template.py
python scripts/generate_trppu_produit_template.py --output /tmp/produits.xlsx
```

Inclut un format texte sur `co_produit` et un format date `YYYY-MM-DD`
sur `dt_desactivation`.

---

## 4. Manœuvres opérationnelles courantes

### 4.1 Ajouter un nouveau produit

```bash
curl -X POST http://localhost:8080/trppu-api/produits \
  -H "Content-Type: application/json" \
  -d '{"co_produit":"NV","lb_produit":"Nouveau produit"}'
```

### 4.2 Réactiver un produit désactivé

```bash
curl -X PUT http://localhost:8080/trppu-api/produits/PR \
  -H "Content-Type: application/json" \
  -d '{"dt_desactivation": null, "motif_desactivation": null}'
```

### 4.3 Ajouter une colonne sur `trppu_produit`

1. **SQL** : `ALTER TABLE trppu_produit ADD COLUMN ...`
2. **Pydantic** : ajouter le champ dans `ProduitBase` (et `ProduitUpdate`).
3. **SQL routes** : `SELECT_PRODUIT_SQL`, INSERT du `create_produit`, et `UPSERT_SQL`.
4. **Excel** : `HEADERS` / `COLUMN_WIDTHS` / `EXAMPLE_ROWS` du script + `EXPECTED_HEADERS` du parser.

### 4.4 Purge physique (cas exceptionnel)

```sql
DELETE FROM trppu_produit WHERE co_produit = 'XX';
-- Échoue si fk_picc_produit / fk_excl_produit / fk_spc_produit / fk_var_produit
-- / fk_compt_produit / fk_tmh_produit bloquent.
```

---

## 5. Structure des fichiers

```
app/routes/trppu_produit/
├── __init__.py
├── routes.py
├── schemas.py
└── helpers.py
scripts/
└── generate_trppu_produit_template.py
```
