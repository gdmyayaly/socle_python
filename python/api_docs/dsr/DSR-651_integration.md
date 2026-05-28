# DSR-651 — Lecture du paramétrage prévisionnel (variations) d'un scénario

> **User story** : « En tant que TRPPU, je veux pouvoir récupérer les informations
> du paramétrage prévisionnel d'un scénario afin de les charger dans l'IHM lors de
> l'édition. »
>
> **Tickets liés** : appelé par DSR-654 ; symétrique de l'écriture DSR-646.

---

## 1. Contexte & objectif métier

Service (yb04) qui récupère, pour un `id_scenario`, **toutes les lignes** de
`trppu_scenario_variations_prev` (1 par produit) : `co_produit`, `variation_pct`.

Règle : **seuls les produits dont la variation ≠ 0 % sont en base**. Les produits
absents sont **à 0 % par défaut** (le défaut n'est pas stocké).

---

## 2. État actuel & analyse des écarts

| Élément | Constat |
| ------- | ------- |
| Module de routes | **Inexistant** → endpoint à créer (mutualisé avec DSR-646, `app/routes/trppu_variations/`). |
| Table `trppu_scenario_variations_prev` | Existe (`id_variation, id_scenario, co_produit, variation_pct`). |

---

## 3. Spécification (SELECT)

```sql
SELECT co_produit, variation_pct
  FROM trppu_scenario_variations_prev
 WHERE id_scenario = %s
 ORDER BY co_produit;
```

> **Décision de design (à valider)** : le service renvoie **uniquement** les
> variations stockées (≠ 0). C'est l'IHM qui applique 0 % par défaut aux autres
> produits — évite de dépendre du catalogue `trppu_produit` côté service. Variante
> possible : « hydrater » la réponse avec tous les produits actifs à 0 %
> (cf. `README_incomprehensions.md`).

---

## 4. Contrat d'API proposé

`GET /trppu-api/scenarios/{id_scenario}/variations`

Réponse `200` :
```json
[
  { "co_produit": "OO", "variation_pct": 25.00 },
  { "co_produit": "IP", "variation_pct": -15.00 }
]
```
Codes : `200` (liste éventuellement vide).

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
| Produits par défaut (0 %) non stockés | §3 (décision de design) |
| Log Kibana avec id_scenario | §7 |
| Données conformes à la base | SELECT §3 |

## 9. Questions ouvertes

`README_incomprehensions.md` : renvoyer seulement les ≠ 0 ou hydrater toute la
liste produits à 0 % ? source de la liste produits (`trppu_produit`) ?
