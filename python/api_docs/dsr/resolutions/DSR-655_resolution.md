# Résolution — DSR-655 (Lecture des périodes d'un scénario)

## 1. Statut
**Terminé.** Endpoint dédié renvoyant les périodes + nombres de jours d'un scénario
pour actualiser le slider IHM. `SELECT_SCENARIO_SQL` et `ScenarioOut` étendus.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_scenario/helpers.py` — `SELECT_SCENARIO_SQL` étendu
  (`dt_mise_en_oeuvre`, `dt_real_prev`, `nb_jours_ouvres/ouvrables/scenario`).
- `app/routes/trppu_scenario/schemas.py` — `ScenarioPeriodesOut` + `ScenarioOut` étendu.
- `app/routes/trppu_scenario/routes.py` — endpoint `get_scenario_periodes`.

## 3. Endpoint livré
`GET /trppu-api/scenarios/{id_scenario}/periodes` → `ScenarioPeriodesOut`
(option `?id_session_ihm=`).
```json
{
  "periode_debut": "2026-01-01", "periode_fin": "2026-12-31",
  "periode_realise_debut": "2026-01-01", "periode_realise_fin": "2026-05-29",
  "periode_prev_debut": "2026-05-29", "periode_prev_fin": "2026-12-31",
  "nb_jours_semaine": 6, "nb_jours_ouvres": 261,
  "nb_jours_ouvrables": 313, "nb_jours_scenario": 313
}
```
Codes : `200`, `404` scénario inexistant.

## 4. Migrations / dépendances
Aucune (colonnes déjà présentes). Les `nb_jours_*` sont désormais **alimentés** par
DSR-634 (création) et DSR-656 (MAJ) ; sinon `null` pour les scénarios antérieurs.

## 5. Hypothèses & écarts
- Endpoint dédié `/periodes` **en plus** de l'extension du `GET /{id}` (les deux
  exposent les nouveaux champs).
- `id_session_ihm` accepté en query (provenance non spécifiée — #11).

## 6. Comment tester
```
GET /trppu-api/scenarios/{id}/periodes
```

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| Récupérer périodes + nb_jours pour un id existant | endpoint GET |
| Actualiser slider + nb jours/semaine | champs renvoyés |

## 8. ➡️ Commentaire Jira
> Endpoint `GET /trppu-api/scenarios/{id}/periodes` livré : renvoie les 6 bornes de
> périodes (scénario, réalisé, prévisionnel) et les 4 indicateurs de jours
> (nb_jours_semaine/ouvres/ouvrables/scenario) pour actualiser le slider IHM.
> `ScenarioOut` a aussi été étendu (mêmes champs disponibles sur `GET /{id}`).
> **À noter** : pour les scénarios créés avant cette livraison, les `nb_jours_*`
> peuvent être nuls tant qu'aucune MAJ (DSR-656) n'a été effectuée.
