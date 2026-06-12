# Résolution — DSR-661 (Enregistrement d'un coefficient PIC modifié)

## 1. Statut
**Terminé.** Create-or-update transactionnel : crée la version PIC scénario si besoin,
puis insère/met à jour le coefficient. Validation stricte des paramètres.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_scenario_pic/{schemas,routes}.py` (module mutualisé avec DSR-660).
- `app/main.py` — handler global `RequestValidationError` : **logge** les paramètres
  invalides/manquants (DSR-661 critère 4) avant de renvoyer le 422 standard.
- `schemas.py` — `coef` borné `>= 0` (aligné sur `chk_pic_coefs`).

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
- `422` paramètre manquant/invalide assuré par Pydantic (`extra=forbid` + champs requis) ;
  le handler global **trace les params fautifs dans les logs** ET les remonte à l'IHM
  (critère #4) — `id_session_ihm` (query) repris si présent.
- `coef` validé `>= 0` (conforme `chk_pic_coefs`) ; `densite` ∈ {0,1,2} (conforme `chk_pic_densite`).
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
| #4 paramètre manquant → rien écrit + message tracé + remonté IHM | `422` Pydantic + handler global qui logge |

## 8. ➡️ Commentaire Jira (à coller)
> **URL d'appel** : `PUT /trppu-api/scenarios/{id_scenario}/pic-coefficients`
>
> **Données d'entrée** : `co_produit`, `jour_semaine`, `densite` (0=dense, 1=faible1,
> 2=faible2), `coef` (≥ 0), `id_rh` (crypté). Chaque paramètre est validé avant écriture.
>
> **Comportement** (en transaction) :
> - version PIC scénario (niveau SCENARIO) existante + coef (produit, jour, densité) présent
>   => UPDATE du coef ;
> - version existante, coef absent => INSERT du coef ;
> - aucune version scénario => INSERT `trppu_pic_version` (lb = `coRegate_idScenario`,
>   niveau SCENARIO, co_regate dérivé du scénario, dt_activation = NOW, id_rh cryptés)
>   puis INSERT du coef avec ce nouvel `id_pic_version`.
>
> **Données de sortie** : `{ action: update|insert_coef|insert_version_and_coef, id_pic_version }`.
>
> **Paramètre manquant/invalide** : aucune écriture ; un message indiquant le(s)
> paramètre(s) fautif(s) est **tracé dans les logs** et **remonté à l'IHM** (422).
>
> **À valider PO** : le paramètre `coef` n'apparaît pas dans la liste du ticket alors qu'il
> est indispensable (rendu obligatoire) ; clé naturelle incluant le jour de semaine.
