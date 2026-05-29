# Résolution — DSR-661 (Enregistrement d'un coefficient PIC modifié)

## 1. Statut
**Terminé.** Create-or-update transactionnel : crée la version PIC scénario si besoin,
puis insère/met à jour le coefficient. Validation stricte des paramètres.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_scenario_pic/{schemas,routes}.py` (module mutualisé avec DSR-660).

## 3. Endpoint livré
`PUT /trppu-api/scenarios/{id_scenario}/pic-coefficients`
```json
{ "co_produit": "OO", "jour_semaine": "LUNDI", "densite": 0, "coef": 0.8500, "id_rh": "A123456" }
```
Réponse : `{ "action": "update|insert_coef|insert_version_and_coef", "id_pic_version": 57 }`.
Codes : `200`, `404` scénario inexistant, `409` figé, `422` paramètre manquant/invalide.

## 4. Migrations / dépendances
Migration `001` (`id_rh*` élargis pour Fernet), var d'env `ID_RH_CRYPTO_KEY`.

## 5. Hypothèses & écarts
- **Cas 1** (version SCENARIO existante) : UPDATE si (co_produit, jour, densité) existe,
  sinon INSERT du coef. **Cas 2** : INSERT `trppu_pic_version` (niveau SCENARIO,
  `lb=co_regate_idscenario`, `co_regate` **dérivé du scénario**, id_rh cryptés) puis INSERT du coef.
- Clé naturelle retenue : **(id_pic_version, co_produit, jour_semaine, densite)** —
  inclut le jour (acceptance #1), au-delà du texte du cas 1.1 (#9).
- `coef` **absent de la liste des paramètres du ticket** mais requis : rendu obligatoire (#9).
- `422` paramètre manquant assuré par Pydantic (`extra=forbid` + champs requis) (critère #4).
- `id_rh` chiffré ; jamais loggé en clair.

## 6. Comment tester
```
PUT .../pic-coefficients  (produit existant)        -> action "update"
PUT .../pic-coefficients  (produit absent, version) -> "insert_coef"
PUT .../pic-coefficients  (aucune version scénario) -> "insert_version_and_coef"
PUT .../pic-coefficients  (param manquant)          -> 422
```

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| #1 coef existant → UPDATE | Cas 1.1 |
| #2 produit absent, version existe → INSERT coef | Cas 1.2 |
| #3 aucune version → INSERT version + coef | Cas 2 |
| #4 paramètre manquant → rien écrit + message | `422` Pydantic |

## 8. ➡️ Commentaire Jira
> Endpoint `PUT /trppu-api/scenarios/{id}/pic-coefficients` livré : enregistre un
> coefficient PIC modifié. Crée la version PIC niveau SCENARIO si elle n'existe pas
> (lb = `coRegate_idScenario`, co_regate dérivé du scénario, id_rh cryptés) puis
> insère/met à jour le coefficient (clé produit + jour + densité), le tout en transaction.
> `422` si un paramètre manque.
> **À valider PO** : le paramètre `coef` n'apparaît pas dans la liste du ticket alors
> qu'il est indispensable (rendu obligatoire) ; clé naturelle incluant le jour de semaine.
