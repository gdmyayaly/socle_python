# Résolution — DSR-646 (Écriture des variations prévisionnelles)

## 1. Statut
**Terminé + traçabilité restaurée (migration 004, 2026-06-10).** Upsert d'une variation
par produit ; suppression automatique quand on repasse à 0 %. `id_rh` **crypté et stocké**,
`dt_creation` renseignée — conformément au ticket et à db_10_09 + migration 004.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_variations/{__init__,helpers,schemas,routes}.py`
- `app/main.py` — enregistrement du routeur.
- **Migration `004_add_variations_tracabilite.sql`** : ajoute `dt_creation` + `id_rh` à
  `trppu_scenario_variations_prev` (enregistrée dans `scripts/run_migrations.py`).

## 3. Endpoints livrés
| Méthode | Chemin | Rôle |
| ------- | ------ | ---- |
| PUT | `/trppu-api/scenarios/{id}/variations/{co_produit}` | Upsert (créé/modifié) ; **supprimé si pct == 0** |
| DELETE | `/trppu-api/scenarios/{id}/variations/{co_produit}` | Suppression explicite (`204`) |

Body PUT : `{ "variation_pct": 25.00, "id_rh": "A123456" }` (négatif autorisé).
Réponse PUT : `{ "co_produit": "OO", "variation_pct": 25.00, "action": "created|updated|deleted|noop" }`.

## 4. Migrations / dépendances
**Migration `004`** (`dt_creation` + `id_rh` sur `trppu_scenario_variations_prev`) — à
appliquer sur la prod. Var d'env `ID_RH_CRYPTO_KEY` (cryptage Fernet de l'id_rh).
Conformité base : INSERT `(id_scenario, co_produit, variation_pct, id_rh)` (dt_creation par
défaut NOW) ; UPDATE `variation_pct, id_rh, dt_creation = NOW()` ; unicité `(id_scenario,
co_produit)`.

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

## 8. ➡️ Commentaire Jira (à coller)
> **URL d'appel**
> `PUT /trppu-api/scenarios/{id_scenario}/variations/{co_produit}` (ajout/modification),
> `DELETE /trppu-api/scenarios/{id_scenario}/variations/{co_produit}` (suppression explicite).
>
> **Données d'entrée** (PUT)
> - `id_scenario` (path), `co_produit` (path).
> - `variation_pct` | variation en % (valeurs négatives acceptées).
> - `id_rh` | id_rh de l'utilisateur, **crypté puis stocké** en base.
>
> **Mise à jour en base (trppu_scenario_variations_prev)**
> - ajout d'une variation => INSERT (`id_scenario`, `co_produit`, `variation_pct`, `id_rh`),
>   `dt_creation` = date du jour ;
> - modification => UPDATE `variation_pct`, `id_rh`, `dt_creation` = date du jour ;
> - repasse à 0 % => DELETE de la ligne (valeur par défaut non stockée).
>
> **Données de sortie**
> `{ co_produit, variation_pct, action: created|updated|deleted|noop }`. DELETE => 204.
>
> **Prise en compte db_10_09 + traçabilité** : la table ne possédait ni `dt_creation` ni
> `id_rh` ; la **migration `004`** les ajoute (à déployer en prod) pour restaurer la
> traçabilité demandée par le ticket. Cryptage via `ID_RH_CRYPTO_KEY`.
