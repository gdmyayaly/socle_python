# Résolution — DSR-659 (MAJ des trafics TMH recalculés)

## 1. Statut
**Terminé.** Endpoint d'upsert batch des trafics recalculés (1 entrée par produit) ;
recouvre DSR-649 (qui en est un cas particulier). Helper réutilisé par DSR-634.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_tmh/helpers.py` — `upsert_tmh_rows` / `upsert_tmh_row`.
- `app/routes/trppu_tmh/{schemas,routes}.py` — `TmhUpsert`, `TmhBatchUpdate`, `TmhBatchResult`.

## 3. Endpoint livré
`PUT /trppu-api/scenarios/{id_scenario}/tmh`
```json
{ "id_rh": "1234567",
  "tmh": [ { "co_produit": "OO", "volume_realise": 120000, "volume_previsionnel": 130000,
             "moyenne_journaliere": 4000.00, "moyenne_hebdo": 24000.00,
             "exclusion": false, "manuel": false } ] }
```
Réponse : `{ "id_scenario": 12, "nb_inserted": 0, "nb_updated": 1 }`.
Codes : `200`, `404` scénario inexistant, `409` scénario figé, `422` body invalide.

## 4. Migrations / dépendances
Aucune. **Upsert sans contrainte d'unicité** : UPDATE sur (id_scenario, co_produit),
INSERT si absent (compatible schéma existant, pas de clé unique requise).

## 5. Hypothèses & écarts
- `exclusion` (entrée) → `bl_exclu` (base) ; `manuel` (entrée) → `bl_manuel` (base) ;
  `dt_calcul = NOW()` à chaque écriture.
- `id_rh` reçu **en clair**, crypté serveur (`encrypt_id_rh`, Fernet) avant stockage,
  jamais journalisé — convention transverse partagée avec scénario/neutralisations.
- Si la ligne n'existe pas, elle est **insérée** (upsert) plutôt que rejetée.
- Moyennes reçues telles quelles (calculées IHM) — cf. `README_incomprehensions.md` #13.

## 6. Comment tester
```
PUT /trppu-api/scenarios/12/tmh   body = { "tmh": [ ... ] }   (Swagger)
```
Puis `GET .../tmh` pour vérifier la cohérence.

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| Après actualisation, trppu_tmh correct et en phase IHM | upsert batch transactionnel |

## 8. ➡️ Commentaire Jira (spec v2 — 2026-06-09)

> **✅ Service de MAJ des trafics TMH recalculés — livré (spec v2).**
>
> **Endpoint :** `PUT /trppu-api/scenarios/{id_scenario}/tmh`
> Met à jour (upsert) en transaction les trafics recalculés du tableau TMH, par lot de
> produits. Renvoie le nombre de lignes insérées/modifiées.
>
> **Body :**
> ```json
> {
>   "id_rh": "1234567",
>   "tmh": [
>     { "co_produit": "OO", "volume_realise": 120000, "volume_previsionnel": 130000,
>       "moyenne_journaliere": 4000.00, "moyenne_hebdo": 24000.00,
>       "exclusion": false, "manuel": false }
>   ]
> }
> ```
> **Réponse :** `{ "id_scenario": 12, "nb_inserted": 0, "nb_updated": 1 }`
> **Codes :** `200` OK · `404` scénario inexistant · `409` scénario figé · `422` body invalide.
>
> **Mapping paramètres → colonnes `trppu_tmh` :**
> | Entrée | Colonne | Règle |
> |---|---|---|
> | `volume_realise` | `volume_realise` | tel quel (≥ 0) |
> | `volume_previsionnel` | `volume_previsionnel` | tel quel (≥ 0) |
> | `moyenne_journaliere` | `moyenne_journaliere` | decimal(12,2), calculée IHM |
> | `moyenne_hebdo` | `moyenne_hebdo` | decimal(12,2), calculée IHM |
> | `exclusion` | `bl_exclu` | flag exclusion du calcul PDI |
> | `manuel` | `bl_manuel` | 1 = ajout manuel, 0 = auto |
> | `id_rh` | `id_rh` | **crypté serveur** (Fernet) avant stockage |
> | — | `dt_calcul` | `NOW()` à chaque écriture |
>
> **Évolutions vs version initiale :** ajout des paramètres **`manuel`** (→ `bl_manuel`,
> par produit) et **`id_rh`** (→ colonne `id_rh`, par lot). Le `bl_manuel` n'est plus
> forcé à 0 (ni à l'INSERT ni à l'UPDATE). Le `id_rh` est reçu en clair et chiffré côté
> serveur (`encrypt_id_rh`, Fernet `VARCHAR(255)`, jamais journalisé), conformément à la
> convention transverse (scénario, neutralisations).
>
> **Base de données :** colonnes `bl_manuel` et `id_rh` déjà présentes en prod
> (`db_migrations/db_09_08_2026.sql`) → **aucune migration requise**. L'upsert reste sans
> dépendance à une contrainte d'unicité (UPDATE puis INSERT si absent).
>
> **Réutilisation :** le helper `upsert_tmh_rows` alimente aussi la création (DSR-634) et
> la MAJ de scénario (DSR-656), qui propagent le `id_rh` crypté du scénario aux lignes TMH.
> `ScenarioTmhItem` expose `manuel` (miroir de `TmhUpsert`). Recouvre DSR-649.
>
> **Critère d'acceptance :** après MAJ scénario + actualisation IHM, les trafics persistés
> dans `trppu_tmh` (volumes, moyennes, `bl_exclu`, `bl_manuel`, `id_rh`, `dt_calcul`) sont
> cohérents avec l'IHM — vérifiable via `GET .../tmh` puis contrôle base.
>
> **Tests :** suite complète OK (18/18).
