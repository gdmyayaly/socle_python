# DSR-652 — Lecture des périodes neutralisées d'un scénario

> **User story** : « En tant que TRPPU, je veux pouvoir récupérer les périodes qui
> ont été neutralisées pour un scénario afin de les charger dans l'IHM lors de
> l'édition. »
>
> **Tickets liés** : appelé par DSR-654 ; symétrique de l'écriture DSR-645.

---

## 1. Contexte & objectif métier

Service (yb04) qui récupère, pour un `id_scenario`, toutes les lignes de
`trppu_neutralisations` (`dt_debut`, `dt_fin`, `nb_jour`, `type`) et les **regroupe
par type** pour alimenter les 3 widgets IHM :

| `type` | Rendu IHM |
| ------ | --------- |
| `FERIE` | Liste des jours exclus ; `dt_debut == dt_fin`, `nb_jour = 1` par ligne ; le compteur affiché = **somme des FERIE**. |
| `PEAK` | **1 seule** ligne ; tableau peak rempli (dates + nb_jour) ; bouton « activé/vert ». |
| `SAISON` | **1 seule** ligne ; tableau saisonnier rempli (dates + nb_jour) ; bouton « activé/vert ». |

Type absent ⇒ **valeur par défaut** : liste fériés vide / bouton PEAK désactivé /
bouton SAISON désactivé.

---

## 2. État actuel & analyse des écarts

| Élément | Constat |
| ------- | ------- |
| Module de routes | **Inexistant** → endpoint à créer (mutualisé avec DSR-645, `app/routes/trppu_neutralisations/`). |
| Table `trppu_neutralisations` | Existe ; **enum = `LOCAL`** alors que le service doit renvoyer `SAISON` (cf. DSR-645, migration enum). |

---

## 3. Spécification (SELECT + regroupement)

```sql
SELECT dt_debut, dt_fin, nb_jour, type
  FROM trppu_neutralisations
 WHERE id_scenario = %s
 ORDER BY type, dt_debut;
```
Post-traitement serveur : agréger en 3 blocs (`feries[]`, `peak`, `saison`),
calculer `feries.nb_jours_total = Σ nb_jour`, et `actif=false` pour peak/saison
absents.

---

## 4. Contrat d'API proposé

`GET /trppu-api/scenarios/{id_scenario}/neutralisations`

Réponse `200` (structure regroupée, directement exploitable par l'IHM) :
```json
{
  "feries": {
    "actif": true,
    "nb_jours_total": 2,
    "jours": [
      { "dt": "2026-11-11", "nb_jour": 1 },
      { "dt": "2026-12-25", "nb_jour": 1 }
    ]
  },
  "peak":   { "actif": true,  "dt_debut": "2026-11-10", "dt_fin": "2026-12-19", "nb_jour": 28 },
  "saison": { "actif": false, "dt_debut": null, "dt_fin": null, "nb_jour": 0 }
}
```
Codes : `200` (toujours, avec défauts si aucune neutralisation).

---

## 5. Modèles Pydantic proposés

```python
class FerieJour(BaseModel):
    dt: date
    nb_jour: int = 1

class FeriesBloc(BaseModel):
    actif: bool
    nb_jours_total: int
    jours: list[FerieJour]

class PeriodeBloc(BaseModel):
    actif: bool
    dt_debut: date | None = None
    dt_fin: date | None = None
    nb_jour: int = 0

class NeutralisationsOut(BaseModel):
    feries: FeriesBloc
    peak: PeriodeBloc
    saison: PeriodeBloc
```

---

## 6. DDL de migration

Aucune propre à ce ticket (dépend de la migration enum `LOCAL→SAISON` portée par
DSR-645).

---

## 7. Logging

Tracer l'`id_scenario`, le nombre de lignes par type (critère Kibana) + id session IHM.

---

## 8. Mapping des critères d'acceptance

| Critère | Couverture |
| ------- | ---------- |
| Service appelé pour le scénario édité | §4 |
| Paramètres renseignés selon le type | §3-§4 (regroupement) |
| Type absent ⇒ défaut (rien neutralisé) | §4 (`actif:false`, listes vides) |
| Log Kibana avec id_scenario | §7 |
| Conformité base | SELECT §3 |

## 9. Questions ouvertes

`README_incomprehensions.md` : **SAISON vs LOCAL** ; structure plate vs regroupée
(proposée ici) ; provenance id session IHM.
