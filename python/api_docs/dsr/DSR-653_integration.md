# DSR-653 — Lecture des comptages manuels d'un scénario

> **User story** : « En tant que TRPPU, je veux pouvoir récupérer les informations
> d'ajout des comptages d'objets d'un scénario afin de les charger lors de
> l'édition. »
>
> **Tickets liés** : appelé par DSR-654 ; symétrique de l'écriture DSR-644.

---

## 1. Contexte & objectif métier

Service (yb04) qui récupère, pour un `id_scenario`, toutes les lignes de comptages
manuels : `co_produit`, `dt_comptage`, `nb_produit`, pour remplir le tableau
« comptages manuels » de l'édition.

> ⚠️ Le ticket nomme la table **`trppu_scenario_comptages`** ; la table réelle du
> schéma est **`trppu_scenario_comptages_manuels`** (cf. DSR-644). On retient la
> table réelle. À confirmer (cf. `README_incomprehensions.md`).

---

## 2. État actuel & analyse des écarts

| Élément | Constat |
| ------- | ------- |
| Module de routes | **Inexistant** → endpoint à créer (mutualisé avec DSR-644, `app/routes/trppu_comptages/`). |
| Table | `trppu_scenario_comptages_manuels` existe. |

---

## 3. Spécification (SELECT)

```sql
SELECT co_produit, dt_comptage, nb_produit
  FROM trppu_scenario_comptages_manuels
 WHERE id_scenario = %s
 ORDER BY co_produit, dt_comptage;
```

---

## 4. Contrat d'API proposé

`GET /trppu-api/scenarios/{id_scenario}/comptages`

Réponse `200` :
```json
[
  { "co_produit": "OO", "dt_comptage": "2026-05-20", "nb_produit": 1500 },
  { "co_produit": "IP", "dt_comptage": "2026-05-21", "nb_produit": 300 }
]
```
Codes : `200` (liste éventuellement vide).

---

## 5. Modèle Pydantic

Réutilise `ComptageOut` (cf. DSR-644 §5). `response_model=list[ComptageOut]`.

---

## 6. DDL de migration

Aucune pour la lecture.

---

## 7. Logging

Tracer l'`id_scenario` et le nombre de comptages renvoyés (critère Kibana) + id session IHM.

---

## 8. Mapping des critères d'acceptance

| Critère | Couverture |
| ------- | ---------- |
| Service appelé pour le scénario édité | §4 |
| Comptages renseignés dans le tableau IHM | §3-§4 |
| Log Kibana avec id_scenario | §7 |
| Conformité base | SELECT §3 |

## 9. Questions ouvertes

`README_incomprehensions.md` : **nom de table** (`trppu_scenario_comptages` vs
`..._manuels`) ; ordre de tri attendu ; provenance id session IHM.
