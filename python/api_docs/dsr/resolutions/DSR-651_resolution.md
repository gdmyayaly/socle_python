# Résolution — DSR-651 (Lecture des variations prévisionnelles)

## 1. Statut
**Terminé.** Endpoint de lecture (service yb04) des variations prévisionnelles d'un
scénario, alimentant le tableau du paramétrage prévisionnel de l'IHM lors de l'édition.
Aligné avec le schéma DB `db_10_09_2026.sql` (table `trppu_scenario_variations_prev`).

## 2. Fichiers créés / modifiés
- `app/routes/trppu_variations/{routes,helpers,schemas}.py` (module mutualisé avec DSR-646 écriture).
- Router monté dans `app/main.py` (`app.include_router(trppu_variations_router)`).

## 3. Endpoint livré

`GET /trppu-api/scenarios/{id_scenario}/variations`

### Entrées
| Emplacement | Paramètre | Type | Obligatoire | Description |
| ----------- | --------- | ---- | ----------- | ----------- |
| Path | `id_scenario` | int | Oui | Id du scénario édité |
| Query | `id_session_ihm` | str | Non | Id de session IHM (traçabilité Kibana) |

Pas de corps de requête (lecture).

### Sortie — `200 OK` → `list[VariationOut]`
Une ligne par produit ayant une variation **≠ 0 %** (les 0 % par défaut ne sont pas stockés).
```json
[
  { "co_produit": "OO", "variation_pct": 25.00 },
  { "co_produit": "IP", "variation_pct": -15.00 }
]
```
Scénario sans variation stockée → `200 OK` avec `[]`.

### Codes d'erreur
| Code | Cas |
| ---- | --- |
| `404` | Scénario inexistant (`fetch_scenario_or_404`) |
| `500` | Erreur de lecture base |

## 4. Migrations / dépendances
Aucune (lecture seule). Schéma DB conforme : `id_scenario` bigint, `co_produit` char(2),
`variation_pct` decimal(5,2), clé unique `(id_scenario, co_produit)`.

## 5. Hypothèses & écarts
- Renvoie **uniquement** les variations stockées (≠ 0 %). Les produits à 0 % par défaut
  ne sont pas en base ; l'IHM applique 0 % (décision §8 du doc d'intégration, #8).
- `id_session_ihm` accepté en query (#11), tracé dans les logs applicatifs.
- ⚠️ **À valider PO** : faut-il renvoyer la liste complète des produits hydratée à 0 %
  plutôt que les seules variations ≠ 0 % ?

## 6. Comment tester
```
GET /trppu-api/scenarios/1/variations
GET /trppu-api/scenarios/1/variations?id_session_ihm=IHM-123
```
Vérifier dans Kibana les logs `Début lecture variations (id_scenario=..., id_session_ihm=...)`
et `Lecture variations terminée (id_scenario=..., count=..., duration_ms=...)`, puis
comparer le `count` et les valeurs avec un `SELECT * FROM trppu_scenario_variations_prev
WHERE id_scenario = 1`.

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| Service appelé pour le scénario édité | `GET /trppu-api/scenarios/{id_scenario}/variations` |
| Récupération co_produit + variation_pct | `SELECT co_produit, variation_pct` |
| Produits par défaut (0 %) non stockés → 0 % IHM | SELECT ≠ 0 uniquement (non stockés en base) |
| Log Kibana avec id_scenario | `logger.info(... id_scenario ...)` début + fin |
| Conformité base | SELECT direct sur `trppu_scenario_variations_prev` |

## 8. ➡️ Commentaire Jira (à coller)

> **URL d'appel**
> `GET /trppu-api/scenarios/{id_scenario}/variations`
> Exemple : `GET /trppu-api/scenarios/1/variations?id_session_ihm=IHM-123`
>
> **Données d'entrée**
> - `id_scenario` | id du scénario édité.
> - `id_session_ihm` | id de session IHM pour la traçabilité (optionnel).
> - Pas de corps de requête.
>
> **Données de sortie**
> tableau JSON (une ligne par produit dont la variation ≠ 0 % ; les produits à 0 % par
> défaut ne sont pas stockés et sont hydratés à 0 % par l'IHM) :
> ```json
> [
>   { "co_produit": "OO", "variation_pct": 25.00 },
>   { "co_produit": "IP", "variation_pct": -15.00 }
> ]
> ```
> - Scénario sans variation stockée => []
> - Scénario inexistant => 404.
> - Erreur base => 500.
>
> **Traçabilité / Kibana**
> chaque appel logue `id_scenario` et `id_session_ihm` à l'entrée et le nombre de lignes
> + durée à la sortie.
>
> **À valider PO**
> renvoyer la liste complète des produits hydratée à 0 %, ou conserver le comportement
> actuel (seules les variations ≠ 0 %, le 0 % étant géré côté IHM) ?
