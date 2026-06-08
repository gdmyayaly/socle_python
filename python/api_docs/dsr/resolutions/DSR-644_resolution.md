# Résolution — DSR-644 (Écriture des comptages manuels)

## 1. Statut
**Terminé.** Service d'ajout / modification / suppression des comptages manuels
(`trppu_scenario_comptages_manuels`), `id_rh` crypté.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_comptages/{__init__,helpers,schemas,routes}.py`
- `app/main.py` — enregistrement du routeur.
- Migration `002` (colonne `id_rh`).

## 3. Endpoints livrés
| Méthode | Chemin | Rôle |
| ------- | ------ | ---- |
| POST | `/trppu-api/scenarios/{id}/comptages` | Ajout (`409` si le produit a déjà un comptage) |
| PUT | `/trppu-api/scenarios/{id}/comptages/{co_produit}` | Modification (`404` si absent) |
| DELETE | `/trppu-api/scenarios/{id}/comptages/{co_produit}` | Suppression (`204`) |

Body POST : `{ "co_produit": "OO", "dt_comptage": "2026-05-29", "nb_produit": 1500, "id_rh": "A123456" }`.

## 4. Migrations / dépendances
Migration `002` (`id_rh VARCHAR(255)`), var d'env `ID_RH_CRYPTO_KEY`.

## 5. Hypothèses & écarts
- Clé fonctionnelle retenue : **(id_scenario, co_produit)** ; POST refuse un doublon
  (409), la modification passe par PUT (#12).
- `dt_comptage` défaut = date du jour (ajout et modification).
- `id_rh` crypté (Fernet), jamais loggé en clair.
- Verrou : refus si scénario figé (`assert_not_fige`).

## 6. Comment tester
```
POST .../comptages ; PUT .../comptages/OO ; DELETE .../comptages/OO ; GET .../comptages
```
Vérifier en base la présence/modification/suppression et `id_rh` chiffré.

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| Ajout → ligne (scénario, objet, trafic) | POST |
| Modification → nouveau trafic | PUT |
| Suppression → ligne supprimée | DELETE |
| 2-3 comptages + modif + suppression | endpoints |
| id_rh crypté | Fernet + migration |

## 8. ➡️ Commentaire Jira
> Service comptages manuels livré : `POST/PUT/DELETE /trppu-api/scenarios/{id}/comptages`.
> `id_rh` chiffré (réversible). Clé fonctionnelle (id_scenario, co_produit) : l'ajout
> refuse un doublon (409 → utiliser PUT), `dt_comptage` par défaut = date du jour.
> Écriture refusée si le scénario est figé. **Pré-requis** : migration `002` (colonne
> `id_rh`) + `ID_RH_CRYPTO_KEY`.

> **🔄 MAJ 2026-06-08 — Alignement schéma PROD (base de référence) :** ⚠️ la table
> `trppu_scenario_comptages_manuels` en prod **n'a pas la colonne `id_rh`** (migration
> `002` non déployée en prod sur cette table). L'écriture de `id_rh` a donc été
> **retirée** des INSERT/UPDATE pour que l'endpoint fonctionne contre la prod. Le body
> accepte toujours `id_rh` mais il **n'est plus persisté** → **traçabilité id_rh perdue
> sur cet endpoint**. Pour la rétablir : ajouter `id_rh` à la prod (migration dédiée).
> Cf. `db_analyse/v2/RAPPORT_COMPARAISON_PROD_LOCAL.md`.
