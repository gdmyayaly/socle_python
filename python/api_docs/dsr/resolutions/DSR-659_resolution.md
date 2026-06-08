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
{ "tmh": [ { "co_produit": "OO", "volume_realise": 120000, "volume_previsionnel": 130000,
             "moyenne_journaliere": 4000.00, "moyenne_hebdo": 24000.00, "exclusion": false } ] }
```
Réponse : `{ "id_scenario": 12, "nb_inserted": 0, "nb_updated": 1 }`.
Codes : `200`, `404` scénario inexistant, `409` scénario figé.

## 4. Migrations / dépendances
Aucune. **Upsert sans contrainte d'unicité** : UPDATE sur (id_scenario, co_produit),
INSERT si absent (compatible schéma existant, pas de clé unique requise).

## 5. Hypothèses & écarts
- `exclusion` (entrée) → `bl_exclu` (base) ; `dt_calcul = NOW()` à chaque écriture.
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

## 8. ➡️ Commentaire Jira
> Endpoint `PUT /trppu-api/scenarios/{id}/tmh` livré : met à jour (upsert) les trafics
> recalculés du tableau TMH, par lot de produits, en transaction. Renvoie le nombre de
> lignes insérées/modifiées. `dt_calcul` repositionné à chaque MAJ. Refus si scénario figé.
> **À noter** : l'upsert ne dépend pas d'une contrainte d'unicité (UPDATE puis INSERT si
> absent), donc aucune migration TMH n'est requise. Recouvre DSR-649.

> **🔄 MAJ 2026-06-08 — Alignement schéma PROD (base de référence) :** la table
> `trppu_tmh` en prod possède une colonne **`bl_manuel` NOT NULL sans valeur par défaut**.
> L'INSERT fournit désormais `bl_manuel = 0` (ligne issue d'un calcul, non saisie
> manuellement) pour éviter l'erreur `Field 'bl_manuel' doesn't have a default value`.
> Cf. `db_analyse/v2/RAPPORT_COMPARAISON_PROD_LOCAL.md`.
