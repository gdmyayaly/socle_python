# DSR-646 — Écriture des variations du trafic prévisionnel

> **User story** : « En tant que TRPPU, je veux que les informations de variation
> du trafic prévisionnel des objets/produits d'un scénario soient mises à jour en
> base afin de sauvegarder les variations des objets liées au scénario en cours
> et ce à chaque modification. »
>
> **Tickets liés** : DSR-651 (lecture des variations), DSR-654 (orchestration).

---

## 1. Contexte & objectif métier

Service (POD yb04) qui **ajoute / modifie / supprime** une variation en % du
trafic prévisionnel d'un produit dans `trppu_scenario_variations_prev`.

- **Ajout** : insère (`id_scenario`, `co_produit`, `variation_pct`, `dt_creation`, `id_rh` crypté).
- **Modification** : met à jour `variation_pct`, `dt_creation` (= date du jour), `id_rh` pour (`id_scenario`, `co_produit`).
- **Suppression** : quand l'utilisateur **repasse à 0 %** (valeur par défaut), la ligne (`id_scenario`, `co_produit`) est **supprimée**.
- Les variations peuvent être **négatives**.

---

## 2. État actuel & analyse des écarts

| Élément | Constat |
| ------- | ------- |
| Module de routes | **Inexistant** → à créer (`app/routes/trppu_variations/`). |
| Table `trppu_scenario_variations_prev` | Existe : `id_variation, id_scenario, co_produit, variation_pct decimal(5,2)`. |
| Colonnes `dt_creation`, `id_rh` | **Absentes** du schéma — exigées → migration. |
| Règle « 0 % ⇒ suppression » | À implémenter côté service. |

---

## 3. Spécification table `trppu_scenario_variations_prev`

| Colonne | Ajout | Modif | Valeur |
| ------- | :---: | :---: | ------ |
| `id_variation` | auto | — | AUTO_INCREMENT |
| `id_scenario` | ✅ | clé | id scénario en cours |
| `co_produit` | ✅ | clé | code produit (ex. `OO`, `IP`) |
| `variation_pct` | ✅ | ✅ | % (decimal(5,2), **peut être négatif**) |
| `dt_creation` *(à ajouter)* | ✅ | ✅ (= date du jour) | datetime |
| `id_rh` *(à ajouter)* | ✅ | ✅ | id RH **crypté** |

> Règle métier : `variation_pct == 0` ⇒ **DELETE** (jamais d'INSERT/UPDATE à 0).
> `decimal(5,2)` borne implicitement à ±999.99 % — borne métier à confirmer.

---

## 4. Contrat d'API proposé

Préfixe : `/trppu-api/scenarios/{id_scenario}/variations`

| Méthode | Chemin | Rôle |
| ------- | ------ | ---- |
| `PUT` | `/trppu-api/scenarios/{id_scenario}/variations/{co_produit}` | Upsert : crée/modifie si `pct != 0`, **supprime** si `pct == 0` |
| `DELETE` | `/.../variations/{co_produit}` | Suppression explicite |

> Le `PUT` idempotent couvre add + update + suppression-par-0, ce qui colle au
> comportement IHM (un curseur de % par produit). Variante possible : `POST` pour
> l'ajout et `PUT` pour la modification (à trancher).

Body `PUT` :
```json
{ "variation_pct": 25.00, "id_rh": "A123456" }
```
Réponses : `200` upsert ; `204` si `pct == 0` (suppression) ; `404` scénario
introuvable ; `409` scénario figé.

---

## 5. Modèles Pydantic proposés

```python
class VariationUpsert(BaseModel):
    model_config = ConfigDict(extra="forbid")
    variation_pct: Decimal = Field(..., max_digits=5, decimal_places=2)  # négatif autorisé
    id_rh: str

class VariationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    co_produit: str
    variation_pct: Decimal
```

---

## 6. DDL de migration

```sql
ALTER TABLE `trppu_scenario_variations_prev`
  ADD COLUMN `dt_creation` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN `id_rh` VARCHAR(40) NULL;
-- Option : ADD UNIQUE KEY `uk_var_scen_prod` (`id_scenario`, `co_produit`);
```

---

## 7. Cryptage & logging

`id_rh` crypté (`app/security/crypto`). En modification, `dt_creation` est
**réécrite à la date du jour** (le ticket l'impose). Logs : `id_scenario`,
`co_produit`, `variation_pct`, action.

---

## 8. Mapping des critères d'acceptance

| Critère | Couverture |
| ------- | ---------- |
| OO +25 % → ligne (id_scenario, OO, 25, dt_creation, id_rh) | §3 + `PUT` |
| IP −15 % (négatif) → ligne | §3 (decimal signé) |
| OO 25→40 % → ligne modifiée (pct, dt_creation, id_rh) | §3 + `PUT` |
| OO 40→0 % → ligne supprimée | §4 (PUT pct=0 / DELETE) |

## 9. Questions ouvertes

`README_incomprehensions.md` : cryptage `id_rh`, colonnes manquantes, bornes de
`variation_pct`, choix `PUT` idempotent vs `POST`/`PUT` séparés.
