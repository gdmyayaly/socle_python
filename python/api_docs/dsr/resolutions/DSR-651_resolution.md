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
Une ligne par `co_produit` distinct du **TMH** du scénario ayant au moins une ligne non
exclue (`bl_exclu = 0`), avec la variation stockée ou **0 % par défaut** (matérialisé côté back).
```json
[
  { "co_produit": "OO", "variation_pct": 25.00 },
  { "co_produit": "IP", "variation_pct": -15.00 },
  { "co_produit": "PR", "variation_pct": 0.00 }
]
```
Scénario sans ligne TMH non exclue → `200 OK` avec `[]`.

### Codes d'erreur
| Code | Cas |
| ---- | --- |
| `404` | Scénario inexistant (`fetch_scenario_or_404`) |
| `500` | Erreur de lecture base |

## 4. Migrations / dépendances
Aucune (lecture seule). Schéma DB conforme : `id_scenario` bigint, `co_produit` char(2),
`variation_pct` decimal(5,2), clé unique `(id_scenario, co_produit)`.

## 5. Hypothèses & écarts
- Liste **hydratée depuis le TMH** : un produit par `co_produit` distinct de `trppu_tmh`
  (scénario) ayant ≥ 1 ligne non exclue, variation stockée ou 0 % par défaut (`COALESCE`).
  Les 0 % restent non stockés en base ; le back les matérialise à la lecture.
- Produits entièrement exclus (`bl_exclu = 1` sur toutes leurs lignes) → **masqués**.
- Variation stockée pour un produit absent du TMH (ou entièrement exclu) → **non renvoyée**
  (la liste est pilotée par le TMH).
- `id_session_ihm` accepté en query (#11), tracé dans les logs applicatifs.

## 6. Comment tester
```
GET /trppu-api/scenarios/1/variations
GET /trppu-api/scenarios/1/variations?id_session_ihm=IHM-123
```
Vérifier dans Kibana les logs `Début lecture variations (id_scenario=..., id_session_ihm=...)`
et `Lecture variations terminée (id_scenario=..., count=..., duration_ms=...)`. Le `count`
correspond au nombre de `co_produit` distincts non exclus du TMH (`SELECT COUNT(DISTINCT
co_produit) FROM trppu_tmh WHERE id_scenario = 1 AND bl_exclu = 0`) ; les valeurs ≠ 0
proviennent de `trppu_scenario_variations_prev`, les autres sont à 0 par défaut.

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| Service appelé pour le scénario édité | `GET /trppu-api/scenarios/{id_scenario}/variations` |
| Récupération co_produit + variation_pct | `SELECT t.co_produit, COALESCE(v.variation_pct, 0)` |
| Produits par défaut (0 %) matérialisés | `COALESCE(…, 0)` sur les produits du TMH sans variation stockée |
| Log Kibana avec id_scenario | `logger.info(... id_scenario ...)` début + fin |
| Conformité base | Sous-requête TMH + `LEFT JOIN trppu_scenario_variations_prev` |

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
> tableau JSON piloté par le TMH du scénario : une ligne par `co_produit` distinct ayant
> au moins une ligne TMH non exclue, avec la variation stockée ou 0 % par défaut :
> ```json
> [
>   { "co_produit": "OO", "variation_pct": 25.00 },
>   { "co_produit": "IP", "variation_pct": -15.00 },
>   { "co_produit": "PR", "variation_pct": 0.00 }
> ]
> ```
> - Scénario sans ligne TMH non exclue => []
> - Scénario inexistant => 404.
> - Erreur base => 500.
>
> **Traçabilité / Kibana**
> chaque appel logue `id_scenario` et `id_session_ihm` à l'entrée et le nombre de lignes
> + durée à la sortie.
>
> **Décision (tranchée)**
> la liste est hydratée à partir des produits du TMH du scénario (défaut 0 % matérialisé
> côté back), les produits entièrement exclus étant masqués.
