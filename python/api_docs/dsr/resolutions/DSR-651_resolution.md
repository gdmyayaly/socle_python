# Résolution — DSR-651 (Lecture des variations prévisionnelles)

## 1. Statut
**Terminé.** Endpoint de lecture des variations d'un scénario (produits ≠ 0 % uniquement).

## 2. Fichiers créés / modifiés
- `app/routes/trppu_variations/{helpers,routes}.py` (module mutualisé avec DSR-646).

## 3. Endpoint livré
`GET /trppu-api/scenarios/{id_scenario}/variations` → `list[VariationOut]`
```json
[ { "co_produit": "OO", "variation_pct": 25.00 }, { "co_produit": "IP", "variation_pct": -15.00 } ]
```

## 4. Migrations / dépendances
Aucune pour la lecture.

## 5. Hypothèses & écarts
- Renvoie **uniquement** les variations stockées (≠ 0). Les produits à 0 % par défaut
  ne sont pas en base ; l'IHM applique 0 % (décision §8 du doc d'intégration, #8).
- `id_session_ihm` accepté en query (#11).

## 6. Comment tester
```
GET /trppu-api/scenarios/{id}/variations
```

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| Service appelé pour le scénario édité | GET |
| Produits par défaut (0 %) non stockés | SELECT (≠ 0 uniquement) |
| Log Kibana avec id_scenario | logs |
| Conformité base | SELECT direct |

## 8. ➡️ Commentaire Jira
> Endpoint `GET /trppu-api/scenarios/{id}/variations` livré : renvoie co_produit +
> variation_pct pour les produits ayant une variation ≠ 0 %. Les autres (0 % par défaut)
> ne sont pas stockés et sont gérés par l'IHM. **À valider PO** : faut-il plutôt
> renvoyer la liste complète des produits hydratée à 0 % ?
