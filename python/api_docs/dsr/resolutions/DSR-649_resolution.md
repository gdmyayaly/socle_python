# Résolution — DSR-649 (MAJ ciblée d'un trafic initial modifié)

## 1. Statut
**Terminé.** Endpoint de MAJ ciblée d'un produit du tableau TMH (volume réalisé +
moyennes). Cas particulier de DSR-659.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_tmh/routes.py` — handler `update_tmh_volume`.
- `app/routes/trppu_tmh/schemas.py` — `TmhVolumeUpdate`.

## 3. Endpoint livré
`PATCH /trppu-api/scenarios/{id_scenario}/tmh/{co_produit}`
```json
{ "volume_realise": 120000, "moyenne_journaliere": 4000.00, "moyenne_hebdo": 24000.00 }
```
Met à jour `volume_realise`, `moyenne_journaliere`, `moyenne_hebdo`, `dt_calcul=NOW()`.
**Ne touche pas** `volume_previsionnel` ni `bl_exclu` (différence avec DSR-659).
Codes : `200` (renvoie la ligne), `404` ligne TMH introuvable, `409` figé.

## 4. Migrations / dépendances
Aucune.

## 5. Hypothèses & écarts
- La ligne TMH doit déjà exister (créée à DSR-634) ; sinon `404` (pas d'upsert ici,
  contrairement à DSR-659 — comportement à confirmer, `README_incomprehensions.md` #13).

## 6. Comment tester
```
PATCH /trppu-api/scenarios/12/tmh/OO   body = { "volume_realise": 120000, ... }
```

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| trppu_tmh correct pour les produits modifiés, en phase IHM | UPDATE ciblé + relecture |

## 8. ➡️ Commentaire Jira
> Endpoint `PATCH /trppu-api/scenarios/{id}/tmh/{co_produit}` livré : met à jour le
> volume réalisé et les moyennes (journalière/hebdo) d'un produit, avec `dt_calcul`
> repositionné. `volume_previsionnel` et `bl_exclu` ne sont pas modifiés (MAJ ciblée).
> 404 si la ligne n'existe pas encore. **À noter** : ce besoin est un sous-ensemble de
> DSR-659 (même module/table) — envisager de mutualiser côté IHM.
