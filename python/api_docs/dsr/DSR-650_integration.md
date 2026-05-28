# DSR-650 — Lecture des trafics TMH d'un scénario

> **User story** : « En tant que TRPPU, je veux que les informations de trafic
> d'un scénario soient récupérées de la base afin d'afficher les trafics par
> produit dans le tableau "TMH" de l'IHM. »
>
> **Tickets liés** : appelé par DSR-654 (édition). Symétrique des écritures
> DSR-634 / DSR-649 / DSR-659.

---

## 1. Contexte & objectif métier

Service (yb04) qui, à partir de l'`id_scenario`, récupère **toutes les lignes** de
`trppu_tmh` du scénario (1 ligne par produit) pour alimenter le tableau TMH.

Champs renvoyés : `co_produit`, `volume_realise`, `volume_previsionnel`,
`moyenne_journaliere`, `moyenne_hebdo`, `bl_exclu` (le `bl_exclu` permet à l'IHM de
**griser** la ligne d'un produit exclu).

---

## 2. État actuel & analyse des écarts

| Élément | Constat |
| ------- | ------- |
| Module de routes | **Inexistant** → endpoint à créer dans `app/routes/trppu_tmh/` (mutualisé avec DSR-649/659). |
| Table `trppu_tmh` | Existe, toutes colonnes présentes. |

---

## 3. Spécification (SELECT)

```sql
SELECT co_produit, volume_realise, volume_previsionnel,
       moyenne_journaliere, moyenne_hebdo, bl_exclu
  FROM trppu_tmh
 WHERE id_scenario = %s
 ORDER BY co_produit;
```
Aucune ligne ⇒ tableau vide (`200` + `[]`).

---

## 4. Contrat d'API proposé

`GET /trppu-api/scenarios/{id_scenario}/tmh`

Réponse `200` :
```json
[
  { "co_produit": "OO", "volume_realise": 120000, "volume_previsionnel": 130000,
    "moyenne_journaliere": 4000.00, "moyenne_hebdo": 24000.00, "bl_exclu": false },
  { "co_produit": "IP", "volume_realise": 50000, "volume_previsionnel": 0,
    "moyenne_journaliere": 1666.67, "moyenne_hebdo": 10000.00, "bl_exclu": true }
]
```
Codes : `200` (liste éventuellement vide) ; `404` si l'on choisit de valider
l'existence du scénario au préalable (sinon `200 []`).

---

## 5. Modèle Pydantic

Réutilise `TmhOut` (cf. DSR-649 §5). `response_model=list[TmhOut]`.

---

## 6. DDL de migration

Aucune.

---

## 7. Logging

**Obligatoire (critère Kibana)** : tracer l'`id_scenario` interrogé et le nombre
de lignes renvoyées. Inclure l'**id session IHM** si fourni (cf.
`README_incomprehensions.md`).

---

## 8. Mapping des critères d'acceptance

| Critère | Couverture |
| ------- | ---------- |
| Service appelé pour le scénario édité, trafics affichés | §4 |
| Log Kibana avec id_scenario | §7 |
| Données conformes à la base | SELECT direct §3 |
| Produit exclu (`bl_exclu`) → ligne grisée | `bl_exclu` renvoyé §4 (rendu IHM) |

## 9. Questions ouvertes

`README_incomprehensions.md` : `404` vs `200 []` si scénario inexistant ;
provenance de l'id session IHM ; format exact attendu (clés camelCase ?).
