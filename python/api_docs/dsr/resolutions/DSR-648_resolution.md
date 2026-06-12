# Résolution — DSR-648 (Sauvegarde en base des trafics moyen hebdo : trppu_tmh)

## 1. Statut
**Terminé + aligné db_10_09_2026.** Service YS04 qui enregistre une ligne par produit
dans `trppu_tmh` (trafics du tableau TMH de l'IHM). Mutualisé avec la création de
scénario (DSR-634) et la MAJ batch (DSR-659). Aligné avec les 2 nouvelles colonnes
`trafic_pdi_calcule` / `trafic_agrebal_calcule` de `trppu_scenario`.

## 2. Fichiers
- `app/routes/trppu_tmh/helpers.py` : `upsert_tmh_row` / `upsert_tmh_rows` (UPDATE si
  (id_scenario, co_produit) existe, sinon INSERT ; `dt_calcul = NOW()`).
- `app/routes/trppu_tmh/{routes,schemas}.py` : endpoints TMH.
- Consommé à la création par `POST /trppu-api/scenarios` (champ `tmh[]`, DSR-634).
- **Alignement db_10_09_2026** : `app/routes/trppu_scenario/helpers.py`
  (`SELECT_SCENARIO_SQL`) + `schemas.py` (`ScenarioOut`) exposent les 2 nouvelles colonnes.

## 3. Service de sauvegarde TMH
Entrées par produit : `co_produit`, `volume_realise`, `volume_previsionnel`,
`moyenne_journaliere`, `moyenne_hebdo`, `exclusion` (→ `bl_exclu`), `manuel` (→ `bl_manuel`).
Champs serveur : `id_tmh` (auto), `dt_calcul = NOW()`, `id_rh` (crypté).
Endpoint batch : `PUT /trppu-api/scenarios/{id_scenario}/tmh`.

## 4. Conformité base de données (db_10_09_2026)
- `trppu_tmh` : colonnes/­types conformes (`bl_exclu`, `bl_manuel` NOT NULL fournis ;
  `volume_*` >= 0 ; unicité (id_scenario, co_produit)). ✅
- **`trppu_scenario` — 2 nouvelles colonnes** `trafic_pdi_calcule`,
  `trafic_agrebal_calcule` (smallint DEFAULT 0 ; 0 = trafic non calculé, 1 = calculé) :
  - **Création / duplication** : INSERT n'énumère pas ces colonnes → DB applique
    **DEFAULT 0** (à la création, PDI et agrebal ne sont pas encore calculés). ✅
  - **Lecture** : ajoutées à `SELECT_SCENARIO_SQL` et `ScenarioOut` (exposées en `bool`). ✅

## 5. Décision (arbitrage)
Les flags `trafic_pdi_calcule` / `trafic_agrebal_calcule` sont gérés **uniquement par les
services de calcul PDI / agrebal**. La sauvegarde ou la MAJ du TMH (DSR-648 / DSR-659 /
DSR-649) **ne les modifie pas** (pas d'invalidation automatique côté TMH).

## 6. Comment tester
Créer un scénario avec un tableau `tmh[]`, puis :
```
GET /trppu-api/scenarios/{id}/tmh        -> 1 ligne par produit, valeurs == IHM
GET /trppu-api/scenarios/{id}            -> trafic_pdi_calcule=false, trafic_agrebal_calcule=false
```
Vérifier en base : `SELECT * FROM trppu_tmh WHERE id_scenario = {id}` et les 2 flags à 0
dans `trppu_scenario`.

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| 1 ligne par produit dans trppu_tmh | `upsert_tmh_rows` (INSERT/UPDATE par produit) |
| Trafics conformes à l'IHM | colonnes mappées 1-1 + `dt_calcul=NOW()` |
| Flags de calcul à 0 à la création | DEFAULT 0 (PDI/agrebal non calculés) |

## 8. ➡️ Commentaire Jira (à coller)

> **Service de sauvegarde TMH** — `PUT /trppu-api/scenarios/{id_scenario}/tmh`
> (également alimenté à la création via `POST /trppu-api/scenarios`, champ `tmh[]`).
>
> **Données d'entrée** (une entrée par produit)
> - `co_produit` | code du produit.
> - `volume_realise` | trafic réalisé (constaté).
> - `volume_previsionnel` | trafic prévisionnel.
> - `moyenne_journaliere` | moyenne du trafic sur une journée.
> - `moyenne_hebdo` | moyenne du trafic sur une semaine.
> - `exclusion` | produit exclu du calcul au PDI ou non (=> bl_exclu).
> - (`id_tmh` auto, `dt_calcul` = date du jour, `id_rh` crypté : posés serveur).
>
> **Données de sortie**
> nombre de lignes insérées / modifiées ; une ligne par produit est présente dans
> trppu_tmh après l'appel.
>
> **Prise en compte db_10_09_2026 (2 nouvelles colonnes trppu_scenario)**
> - `trafic_pdi_calcule` et `trafic_agrebal_calcule` (0 = non calculé, 1 = calculé).
> - À la création d'un scénario, ces deux flags valent 0 par défaut (PDI et agrebal pas
>   encore calculés).
> - Ils sont désormais exposés en lecture du scénario.
> - La sauvegarde / MAJ du TMH ne modifie pas ces flags : ils sont gérés par les services
>   de calcul PDI / agrebal.
