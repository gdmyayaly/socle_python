# DSR-654 — Édition d'un scénario (orchestration)

> **User story** : « En tant que TRPPU, je veux pouvoir éditer un scénario afin de
> le consulter ou de le modifier. »
>
> **Ticket d'orchestration** — agrège les services de lecture :
> **DSR-655** (périodes/jours), **DSR-650** (TMH), **DSR-653** (comptages),
> **DSR-651** (variations), **DSR-652** (neutralisations : fériés + peak + saison),
> **DSR-660** (rétention PIC).

---

## 1. Contexte & objectif métier

Quand un scénario de la liste est ouvert en **édition**, l'IHM charge **tous** ses
blocs depuis la base :

| Bloc IHM | Source (ticket) |
| -------- | --------------- |
| Slider + dates de période | DSR-655 |
| Nombre de jours / semaine (5 ou 6) | DSR-655 |
| Tableau TMH (trafics moyen hebdo) | DSR-650 |
| Tableau comptages manuels | DSR-653 |
| Tableau variations prévisionnelles | DSR-651 |
| Tableau jours fériés neutralisés | DSR-652 (`FERIE`) |
| Tableau période saisonnière | DSR-652 (`SAISON`) |
| Tableau peak période | DSR-652 (`PEAK`) |
| Tableau rétention PIC | DSR-660 |

---

## 2. État actuel & analyse des écarts

| Élément | Constat |
| ------- | ------- |
| `GET /trppu-api/scenarios/{id}` | Existe (entête scénario : statut, dates principales, pic_version…). |
| Endpoints par bloc | **À créer** (DSR-650/651/652/653/655/660). |
| Endpoint agrégateur | Inexistant — **optionnel** (cf. §3). |

---

## 3. Stratégie d'orchestration

Deux options :

- **A — Orchestration côté IHM (conforme aux tickets)** : l'IHM appelle les 6
  services en parallèle après sélection du scénario. Chaque ticket reste
  autonome ; pas de code serveur supplémentaire.
- **B — Endpoint agrégateur serveur (confort/perf)** : exposer
  `GET /trppu-api/scenarios/{id}/edition` qui appelle en interne les 6 services et
  renvoie un objet composite (1 aller-retour réseau, cohérence d'un instantané).

> Recommandation : livrer d'abord les 6 services unitaires (option A), puis
> proposer l'agrégateur (option B) en amélioration (cf. `README_ameliorations.md`).

### Réponse composite proposée (option B)

```json
{
  "scenario": { "id_scenario": 12, "lb_scenario": "...", "statut": "EN COURS", "...": "..." },
  "periodes": { "...": "DSR-655" },
  "tmh": [ "...DSR-650..." ],
  "comptages": [ "...DSR-653..." ],
  "variations": [ "...DSR-651..." ],
  "neutralisations": { "feries": {}, "peak": {}, "saison": {} },
  "pic": { "...DSR-660..." }
}
```
Codes : `200` ; `404` si le scénario n'existe pas.

---

## 4. Modèle Pydantic proposé (option B)

```python
class ScenarioEditionOut(BaseModel):
    scenario: ScenarioOut                 # existant
    periodes: ScenarioPeriodesOut         # DSR-655
    tmh: list[TmhOut]                     # DSR-650
    comptages: list[ComptageOut]          # DSR-653
    variations: list[VariationOut]        # DSR-651
    neutralisations: NeutralisationsOut   # DSR-652
    pic: PicScenarioOut                   # DSR-660
```

---

## 5. DDL de migration

Aucune propre à ce ticket (hérite des migrations des tickets sources).

---

## 6. Logging

Si endpoint agrégateur : tracer `id_scenario` + id session IHM une fois, et
propager l'id de session aux 6 sous-appels pour regrouper les traces Kibana.

---

## 7. Mapping des critères d'acceptance

| Critère | Couverture |
| ------- | ---------- |
| Tous les blocs affichés correspondent à la base | §1 + délégué aux 6 tickets |

> La validation de DSR-654 dépend de la livraison des 6 tickets sources ; ce
> document sert de **vue d'ensemble d'intégration** et n'introduit pas de nouvelle
> règle métier.

## 8. Questions ouvertes

`README_incomprehensions.md` : orchestration IHM (A) vs agrégateur serveur (B) ;
appels parallèles/séquentiels ; comportement en cas d'échec partiel d'un bloc ;
propagation de l'id session IHM.
