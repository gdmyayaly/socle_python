# Résolution — DSR-650 (Lecture des trafics TMH d'un scénario)

## 1. Statut
**Terminé.** Endpoint de lecture des lignes TMH d'un scénario (1 par produit),
incluant `bl_exclu` pour le grisage IHM.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_tmh/{__init__,helpers,schemas,routes}.py` (module mutualisé 649/650/659).
- `app/main.py` — enregistrement du routeur TMH.

## 3. Endpoint livré
`GET /trppu-api/scenarios/{id_scenario}/tmh` → `list[TmhOut]`
(option `?id_session_ihm=` pour la traçabilité Kibana).
```json
[ { "co_produit": "OO", "volume_realise": 120000, "volume_previsionnel": 130000,
    "moyenne_journaliere": 4000.00, "moyenne_hebdo": 24000.00, "bl_exclu": false } ]
```
Codes : `200` (liste éventuellement vide), `404` scénario inexistant.

## 4. Migrations / dépendances
Aucune (table `trppu_tmh` complète).

## 5. Hypothèses & écarts
- Validation préalable de l'existence du scénario (`404` si absent) plutôt que `200 []`.
- `id_session_ihm` accepté en query (provenance non spécifiée — cf. `README_incomprehensions.md` #11).

## 6. Comment tester
```
GET /trppu-api/scenarios/12/tmh   (Swagger /docs)
```
Vérifier la cohérence avec la base et le grisage des produits `bl_exclu=true`.

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| Service appelé pour le scénario édité, trafics affichés | endpoint GET |
| Log Kibana avec id_scenario | log d'entrée/sortie + id_session_ihm |
| Données conformes base | SELECT direct |
| Produit exclu → ligne grisée | `bl_exclu` renvoyé |

## 8. ➡️ Commentaire Jira
> Endpoint `GET /trppu-api/scenarios/{id}/tmh` livré : renvoie les trafics moyen
> hebdo du scénario (co_produit, volume réalisé/prévisionnel, moyennes, bl_exclu).
> Le champ `bl_exclu` permet le grisage IHM. Appels tracés (id_scenario + id_session_ihm
> optionnel). 404 si le scénario n'existe pas.
