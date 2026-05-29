# Résolution — DSR-654 (Édition d'un scénario — orchestration)

## 1. Statut
**Terminé.** Endpoint agrégateur renvoyant **tous les blocs** d'un scénario en un seul
appel (option B du doc d'intégration), pour l'édition IHM.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_scenario/routes.py` — handler `get_scenario_edition`
  (imports locaux des helpers de lecture pour éviter tout cycle).

## 3. Endpoint livré
`GET /trppu-api/scenarios/{id_scenario}/edition` (option `?id_session_ihm=`)
```json
{
  "scenario": { "...ScenarioOut..." },
  "periodes": { "periode_debut": "...", "nb_jours_semaine": 6, "...": "..." },
  "tmh": [ ... ],
  "comptages": [ ... ],
  "variations": [ ... ],
  "neutralisations": { "feries": {}, "peak": {}, "saison": {} },
  "pic": { "id_pic_version_defaut": 1, "id_pic_version_scenario": null, "coefficients": [ ... ] }
}
```
Codes : `200`, `404` scénario inexistant.

## 4. Migrations / dépendances
Aucune propre ; réutilise les lectures DSR-655/650/653/651/652/660.

## 5. Hypothèses & écarts
- **Option B** retenue (agrégateur serveur) en complément des 6 endpoints unitaires
  (l'IHM peut utiliser l'un ou l'autre) — cf. `README_incomprehensions.md`.
- Échec d'un bloc → `500` global (pas d'affichage partiel) ; à affiner si besoin.
- `id_session_ihm` propagé aux logs.

## 6. Comment tester
```
GET /trppu-api/scenarios/{id}/edition
```
Comparer chaque bloc aux endpoints unitaires correspondants.

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| Tous les blocs affichés correspondent à la base | agrégation des 6 lectures |

## 8. ➡️ Commentaire Jira
> Endpoint d'édition `GET /trppu-api/scenarios/{id}/edition` livré : renvoie en un seul
> appel l'entête scénario, les périodes, le TMH, les comptages, les variations, les
> neutralisations (regroupées) et les coefficients PIC fusionnés. Pratique pour charger
> l'écran d'édition d'un coup ; les endpoints unitaires (DSR-650/651/652/653/655/660)
> restent disponibles.
