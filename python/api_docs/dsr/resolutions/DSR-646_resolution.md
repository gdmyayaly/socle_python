# Résolution — DSR-646 (Écriture des variations prévisionnelles)

## 1. Statut
**Terminé.** Upsert d'une variation par produit ; suppression automatique quand on
repasse à 0 %. `id_rh` crypté.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_variations/{__init__,helpers,schemas,routes}.py`
- `app/main.py` — enregistrement du routeur.
- Migration `002` (`dt_creation`, `id_rh`).

## 3. Endpoints livrés
| Méthode | Chemin | Rôle |
| ------- | ------ | ---- |
| PUT | `/trppu-api/scenarios/{id}/variations/{co_produit}` | Upsert (créé/modifié) ; **supprimé si pct == 0** |
| DELETE | `/trppu-api/scenarios/{id}/variations/{co_produit}` | Suppression explicite (`204`) |

Body PUT : `{ "variation_pct": 25.00, "id_rh": "A123456" }` (négatif autorisé).
Réponse PUT : `{ "co_produit": "OO", "variation_pct": 25.00, "action": "created|updated|deleted|noop" }`.

## 4. Migrations / dépendances
Migration `002` (`dt_creation`, `id_rh`), var d'env `ID_RH_CRYPTO_KEY`.

## 5. Hypothèses & écarts
- `PUT` idempotent couvre ajout + modification + suppression-par-0 (#8/#12).
- `dt_creation` réécrite à la date du jour à chaque modification (conforme ticket).
- `variation_pct` : `decimal(5,2)` (±999.99) ; bornes métier à confirmer.

## 6. Comment tester
```
PUT .../variations/OO {variation_pct:25} ; PUT {40} ; PUT {0} (supprime) ; GET .../variations
```

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| OO +25 % → ligne | PUT (created) |
| IP −15 % (négatif) | PUT (decimal signé) |
| OO 25→40 % → modifiée | PUT (updated) |
| OO 40→0 % → supprimée | PUT (deleted) |

## 8. ➡️ Commentaire Jira
> Service variations prévisionnelles livré : `PUT /trppu-api/scenarios/{id}/variations/{co_produit}`
> (idempotent : crée/modifie, et **supprime la ligne si le pourcentage repasse à 0 %**),
> + `DELETE` explicite. Valeurs négatives acceptées, `dt_creation` repositionnée à chaque
> modif, `id_rh` chiffré. **Pré-requis** : migration `002` + `ID_RH_CRYPTO_KEY`.
