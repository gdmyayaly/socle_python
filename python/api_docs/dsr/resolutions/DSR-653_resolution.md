# Résolution — DSR-653 (Lecture des comptages manuels)

## 1. Statut
**Terminé.** Endpoint de lecture des comptages manuels d'un scénario.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_comptages/{helpers,routes}.py` (module mutualisé avec DSR-644).

## 3. Endpoint livré
`GET /trppu-api/scenarios/{id_scenario}/comptages` → `list[ComptageOut]`
```json
[ { "co_produit": "OO", "dt_comptage": "2026-05-20", "nb_produit": 1500 } ]
```

## 4. Migrations / dépendances
Aucune pour la lecture.

## 5. Hypothèses & écarts
- Table réelle = **`trppu_scenario_comptages_manuels`** (le ticket cite
  `trppu_scenario_comptages`, inexistant — cf. `README_incomprehensions.md` #partie nommage).
- `id_rh` non exposé. `id_session_ihm` accepté en query (#11).

## 6. Comment tester
```
GET /trppu-api/scenarios/{id}/comptages
```

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| Service appelé pour le scénario édité | GET |
| Comptages renseignés dans le tableau IHM | liste renvoyée |
| Log Kibana avec id_scenario | logs |
| Conformité base | SELECT direct |

## 8. ➡️ Commentaire Jira
> Endpoint `GET /trppu-api/scenarios/{id}/comptages` livré : renvoie co_produit,
> dt_comptage, nb_produit. **NB** : la table réelle est `trppu_scenario_comptages_manuels`
> (le ticket mentionne `trppu_scenario_comptages`, qui n'existe pas).
