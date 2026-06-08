# Résolution — DSR-656 (MAJ d'un scénario modifié)

## 1. Statut
**Terminé.** `PUT /scenarios/{id}` met à jour un scénario **EN COURS** après recalcul :
périodes réalisé/prév recalculées, nb_jours recalculés (fériés + neutralisations),
`dt_real_prev`/`dt_maj` repositionnées, `id_rh_maj` crypté, TMH mis à jour (DSR-659).

## 2. Fichiers créés / modifiés
- `app/routes/trppu_scenario/schemas.py` — `ScenarioMajRequest`.
- `app/routes/trppu_scenario/routes.py` — handler `update_scenario`.

## 3. Endpoint livré
`PUT /trppu-api/scenarios/{id_scenario}` → `ScenarioOut`
```json
{
  "periode_debut": "2026-01-01", "periode_fin": "2026-12-31",
  "nb_jours_semaine": 6, "dt_mise_en_oeuvre": "2026-06-01", "id_rh": "A123456",
  "tmh": [ { "co_produit": "OO", "volume_realise": 120000, "volume_previsionnel": 130000,
             "moyenne_journaliere": 4000.00, "moyenne_hebdo": 24000.00, "exclusion": false } ]
}
```
Codes : `200`, `404`, `409` (statut ≠ EN COURS), `422` (periode_fin < periode_debut).

## 4. Migrations / dépendances
Migration `001` (id_rh élargi) + `003/004` (fériés). `ID_RH_CRYPTO_KEY`. Module TMH (DSR-659).

## 5. Hypothèses & écarts
- **MAJ autorisée seulement si statut EN COURS** (409 sinon).
- Bornes réalisé/prév **recalculées serveur** (`recompute_realise_prev`), `dt_real_prev=NOW()`.
- `nb_jours_scenario` = (ouvrés/ouvrables) − **SUM(nb_jour) des neutralisations** du scénario.
- `dt_mise_en_oeuvre` : mise à jour si fournie, sinon valeur existante conservée.
- TMH (bloc `tmh[]`) mis à jour **dans la même transaction** (atomicité scénario+TMH).
- « Seules les dates changées sauvegardées » : on réécrit l'ensemble des champs dérivés
  de façon cohérente (optimisation par diff non implémentée) — cf. `README_incomprehensions.md` #7.

## 6. Comment tester
```
PUT /trppu-api/scenarios/{id}  (scénario EN COURS) ; puis GET /{id}/periodes et /{id}/tmh.
```

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| trppu_scenario modifié et conforme IHM | UPDATE recalculé |
| trppu_tmh correct et en phase IHM | bloc `tmh[]` (DSR-659) |
| MAJ seulement si EN COURS | `409` |

## 8. ➡️ Commentaire Jira
> MAJ scénario livrée : `PUT /trppu-api/scenarios/{id}` (autorisée uniquement si statut
> EN COURS). Recalcule les bornes réalisé/prévisionnel et les nombres de jours
> (ouvrés/ouvrables, scénario = base − jours neutralisés), repositionne dt_real_prev/dt_maj,
> crypte id_rh_maj, et met à jour les trafics TMH dans la même transaction.
> **Pré-requis** : migrations 001/003/004 + `ID_RH_CRYPTO_KEY`.

> **🔄 MAJ 2026-06-08 — Alignement schéma PROD (base de référence) :** la colonne
> renommée **`dt_pivot`** en prod (ex-`dt_real_prev`) est désormais utilisée par l'UPDATE
> (`dt_pivot = NOW()`) et la lecture (alias `dt_pivot AS dt_real_prev`, **contrat IHM
> inchangé**). Cf. `db_analyse/v2/RAPPORT_COMPARAISON_PROD_LOCAL.md`.
