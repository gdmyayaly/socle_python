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

### 1.1 Produits référencés par le TMH

Le TMH **ne crée pas** de produit : le référentiel `trppu_produit` est alimenté par ailleurs
(création unitaire, import Excel). Tout chemin d'écriture TMH vérifie d'abord que chaque
`co_produit` existe, et rejette sinon la requête **sans rien écrire** :

| Endpoint | Effet si un `co_produit` est absent |
|----------|-------------------------------------|
| `POST /trppu-api/scenarios/{id}/tmh` | 422 |
| `PUT /trppu-api/scenarios/{id}/tmh` | 422 (lot entier rejeté) |
| `POST` / `PUT /trppu-api/scenarios` | 422 si le payload contient des lignes TMH |

- Implémentation : `assert_produits_exist` (`app/routes/trppu_produit/helpers.py`), dans la
  transaction, avant l'insert — remplace la violation de la FK `fk_tmh_produit` (500 opaque).
- Le `detail` liste les codes manquants : `Produit(s) inconnu(s) dans trppu_produit : PR, PPI.`

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

## 3. Template Excel attendu

> Le générateur `scripts/generate_trppu_produit_template.py` a été supprimé — le fichier se
> construit à la main, aucune mise en forme n'est imposée par le parseur.

Format **texte** sur `co_produit` (pas de coercition numérique) et format date `YYYY-MM-DD`
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
4. **Excel** : ajouter l'en-tête dans `EXPECTED_HEADERS` du parser (`helpers.py`).

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
```
