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
  "scenario": { "...ScenarioOut (dont est_fige, trafic_pdi_calcule, trafic_agrebal_calcule)..." },
  "periodes": { "periode_debut": "...", "nb_jours_semaine": 6, "...": "..." },
  "tmh": [ ... ],
  "comptages": [ ... ],
  "variations": [ ... ],
  "neutralisations": [ { "id": 1, "dt_debut": "2026-11-11", "dt_fin": "2026-11-11", "nb_jour": 1, "motif": "FERIE" } ],
  "pic": { "id_pic_version_defaut": 1, "id_pic_version_scenario": null, "coefficients": [ ... ] }
}
```
Codes : `200`, `404` scénario inexistant.

> **MAJ 2026-06-10 (alignement db_10_09 + DSR-645)** :
> - `scenario` expose désormais `trafic_pdi_calcule` / `trafic_agrebal_calcule` (cf. DSR-648).
> - `neutralisations` est une **liste à plat** (id, dt_debut, dt_fin, nb_jour, motif) suite
>   au passage de `type` → `motif` (DSR-645) ; le regroupement feries/peak/saison est abandonné.

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
> appel l'entête scénario (dont les indicateurs `trafic_pdi_calcule` /
> `trafic_agrebal_calcule`), les périodes, le TMH, les comptages, les variations, les
> neutralisations (**liste à plat** avec motif libre) et les coefficients PIC fusionnés.
> Pratique pour charger l'écran d'édition d'un coup ; les endpoints unitaires
> (DSR-650/651/652/653/655/660) restent disponibles.
> **À acter IHM** : le bloc `neutralisations` n'est plus regroupé feries/peak/saison mais
> renvoyé à plat (conséquence du passage `motif` texte libre, DSR-645).
