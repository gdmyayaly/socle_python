# Résolution — DSR-634 (Sauvegarde en base d'un scénario à sa création)

## 1. Statut
**Terminé.** La création de scénario enregistre désormais **toutes** les colonnes
attendues (`trppu_scenario`), crée le site si besoin (`trppu_site`), et insère les
lignes de trafic (`trppu_tmh`) — le tout dans une **transaction unique**.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_scenario/schemas.py` — `ScenarioCreate` enrichi (`id_rh`,
  `dt_mise_en_oeuvre`, `tmh[]`, `nb_jours_semaine` défaut **6**), `ScenarioTmhItem`,
  `ScenarioOut` étendu.
- `app/routes/trppu_scenario/helpers.py` — `SELECT_SCENARIO_SQL` étendu.
- `app/routes/trppu_scenario/routes.py` — `create_scenario` complété.
- Dépend du **SOCLE** (crypto, jours_service) et du **module TMH** (`upsert_tmh_rows`).

## 3. Endpoint livré
`POST /trppu-api/scenarios` → `201` + `ScenarioOut`.
Body :
```json
{
  "co_regate": "012345", "co_roc": "012345", "lb_scenario": "Scénario test",
  "lb_regate": "PARIS PDC", "type_site": "PDC",
  "nb_jours_semaine": 6, "periode_debut": "2026-01-01", "periode_fin": "2026-12-31",
  "dt_mise_en_oeuvre": "2026-06-01", "id_rh": "A123456",
  "tmh": [
    { "co_produit": "OO", "volume_realise": 120000, "volume_previsionnel": 130000,
      "moyenne_journaliere": 4000.00, "moyenne_hebdo": 24000.00, "exclusion": false }
  ]
}
```
Colonnes renseignées : `statut='EN COURS'`, `dt_creation=NOW()`, `dt_mise_en_oeuvre`
(défaut today), `dt_real_prev=NOW()`, périodes réalisé/prév (calculées),
`nb_jours_semaine/ouvres/ouvrables/scenario`, `id_pic_version`, `version_scenario=1`,
`est_fige=0`, `id_rh_creation`/`id_rh_maj` (cryptés, identiques à la création).

## 4. Migrations / dépendances
- Migration **001** (élargissement `id_rh*`). Var d'env **`ID_RH_CRYPTO_KEY`**.
- Migrations **003/004** (table fériés) pour le calcul des `nb_jours_*`.

## 5. Hypothèses & écarts
- `nb_jours_semaine` défaut **6** (le code était à 5 ; aligné sur le ticket).
- `nb_jours_scenario` = ouvrés (si 5) / ouvrables (si 6) ; à la création **aucune
  neutralisation** n'est déduite (conforme au ticket).
- Bornes réalisé/prév : logique existante `recompute_realise_prev` **conservée**
  (cf. `README_incomprehensions.md` #7).
- `id_pic_version` : résolution existante (`est_par_defaut`/fallback 1) conservée —
  l'ambiguïté « 0 / national » reste ouverte (#10). Pas de création de ligne
  `trppu_pic_version` à la création (le critère d'acceptance évoquant `trppu_pic_version`
  est couvert par DSR-661 lors d'une modification PIC).
- `id_rh` **jamais loggé en clair**.
- `tmh[]` optionnel : si fourni, 1 ligne `trppu_tmh` par produit (transaction).

## 6. Comment tester
```
POST /trppu-api/scenarios  (Swagger /docs) avec le body ci-dessus.
Puis GET /trppu-api/scenarios/{id} et GET /{id}/tmh et GET /{id}/periodes.
```
Pré-requis : `ID_RH_CRYPTO_KEY` définie, migrations appliquées.

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| `trppu_scenario` renseigné et conforme IHM | INSERT complet |
| `trppu_tmh` correct et en phase IHM | `tmh[]` → `upsert_tmh_rows` |
| Site présent dans `trppu_site` | `ensure_site_exists` |
| `trppu_pic_version` lié / défaut | partiel — voir §5 (#10) + DSR-661 |

## 8. ➡️ Commentaire Jira
> Création de scénario complétée : `POST /trppu-api/scenarios` enregistre en une
> transaction la ligne `trppu_scenario` (toutes les colonnes : périodes réalisé/prév,
> dt_mise_en_oeuvre, dt_real_prev, nb_jours ouvrés/ouvrables/scénario, id_rh créateur
> **crypté**), crée le site si absent (`trppu_site`) et insère les trafics par produit
> (`trppu_tmh`). `nb_jours_semaine` par défaut = **6**.
> **Pré-requis** : variable `ID_RH_CRYPTO_KEY` + migrations 001/003/004 appliquées.
> **À valider PO** : (1) convention `id_pic_version` par défaut (0/national vs version
> existante) et la nécessité de créer une ligne `trppu_pic_version` dès la création ;
> (2) traitement des bornes réalisé/prév le jour-même (logique actuelle conservée).
