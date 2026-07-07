# DSR-651 — Lecture du paramétrage prévisionnel (variations) d'un scénario

> **User story** : « En tant que TRPPU, je veux pouvoir récupérer les informations
> du paramétrage prévisionnel d'un scénario afin de les charger dans l'IHM lors de
> l'édition. »
>
> **Tickets liés** : appelé par DSR-654 ; symétrique de l'écriture DSR-646.

---

## 1. Contexte & objectif métier

Service (yb04) qui récupère, pour un `id_scenario`, la liste des variations à charger
dans le tableau du paramétrage prévisionnel : `co_produit`, `variation_pct`.

Règle : **la liste est pilotée par les produits du TMH du scénario**, pas par la table
des variations. Cette dernière ne stocke que les écarts **≠ 0 %** (le 0 % n'est pas
stocké). La réponse renvoie donc **un produit par `co_produit` distinct du TMH ayant
au moins une ligne non exclue** (`bl_exclu = 0`), avec la variation stockée si elle
existe, sinon **0 % par défaut** (matérialisé par le back). Sans cette hydratation, un
scénario sans variation saisie renverrait `[]` et l'IHM n'afficherait aucune ligne.

---

## 2. État actuel & analyse des écarts

| Élément | Constat |
| ------- | ------- |
| Module de routes | **Inexistant** → endpoint à créer (mutualisé avec DSR-646, `app/routes/trppu_variations/`). |
| Table `trppu_scenario_variations_prev` | Existe (`id_variation, id_scenario, co_produit, variation_pct`). |

---

## 3. Spécification (SELECT)

```sql
SELECT t.co_produit, COALESCE(v.variation_pct, 0) AS variation_pct
  FROM (
        SELECT co_produit
          FROM trppu_tmh
         WHERE id_scenario = %s AND bl_exclu = 0
         GROUP BY co_produit
       ) AS t
  LEFT JOIN trppu_scenario_variations_prev v
         ON v.co_produit = t.co_produit AND v.id_scenario = %s
 ORDER BY t.co_produit;
```

> **Décision de design (tranchée)** : la liste est **pilotée par le TMH** du scénario
> (produits réellement présents), pas par le catalogue global `trppu_produit`. Le
> sous-select `bl_exclu = 0 GROUP BY co_produit` ne retient qu'un produit ayant au moins
> une ligne TMH non exclue (produits entièrement exclus → masqués). Le `COALESCE(…, 0)`
> matérialise le défaut 0 % côté back. Deux paramètres `id_scenario` (sous-requête + jointure).

---

## 4. Contrat d'API proposé

`GET /trppu-api/scenarios/{id_scenario}/variations`

Réponse `200` — une ligne par produit du TMH (non entièrement exclu), variation stockée ou 0 :
```json
[
  { "co_produit": "OO", "variation_pct": 25.00 },
  { "co_produit": "IP", "variation_pct": -15.00 },
  { "co_produit": "PR", "variation_pct": 0.00 }
]
```
Codes : `200` (liste vide si le scénario n'a aucune ligne TMH non exclue).

---

## 5. Modèle Pydantic

Réutilise `VariationOut` (cf. DSR-646 §5). `response_model=list[VariationOut]`.

---

## 6. DDL de migration

Aucune pour la lecture (les colonnes `dt_creation`/`id_rh` de DSR-646 ne sont pas
exposées ici).

---

## 7. Logging

Tracer l'`id_scenario` et le nombre de variations renvoyées (critère Kibana) +
id session IHM si fourni.

---

## 8. Mapping des critères d'acceptance

| Critère | Couverture |
| ------- | ---------- |
| Service appelé pour le scénario édité | §4 |
| Liste hydratée depuis le TMH, défaut 0 % matérialisé | §3 (COALESCE) |
| Log Kibana avec id_scenario | §7 |
| Données conformes à la base | SELECT §3 |

## 9. Questions ouvertes (tranchées)

- **Renvoyer seulement les ≠ 0 ou hydrater à 0 % ?** → **Hydrater** : la liste est
  pilotée par les `co_produit` du TMH, défaut 0 % matérialisé côté back (§3).
- **Source de la liste produits ?** → **le TMH du scénario** (`trppu_tmh`), pas le
  catalogue global `trppu_produit`. Produits entièrement exclus (`bl_exclu = 1` sur
  toutes leurs lignes) → masqués.
