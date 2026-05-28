# DSR-655 — Lecture des périodes & nombres de jours d'un scénario

> **User story** : « En tant que TRPPU, je veux pouvoir récupérer les différentes
> périodes définies à la création d'un scénario afin d'actualiser le slider lors
> de l'édition. »
>
> **Tickets liés** : appelé par DSR-654 ; alimenté par DSR-634 (création) et
> DSR-656 (mise à jour). Dépend des `nb_jours_*` (DSR-613).

---

## 1. Contexte & objectif métier

Service (yb04) qui récupère, pour un `id_scenario`, les **périodes** et **nombres
de jours** depuis `trppu_scenario`, pour actualiser le slider et le sélecteur de
jours de l'IHM :

`periode_debut`, `periode_fin`, `periode_realise_debut`, `periode_realise_fin`,
`periode_prev_debut`, `periode_prev_fin`, `nb_jours_semaine`, `nb_jours_ouvres`,
`nb_jours_ouvrables`, `nb_jours_scenario`.

---

## 2. État actuel & analyse des écarts

| Élément | Constat |
| ------- | ------- |
| `GET /trppu-api/scenarios/{id}` | Existe (`fetch_scenario_or_404`) mais via `SELECT_SCENARIO_SQL`. |
| `SELECT_SCENARIO_SQL` (`helpers.py`) | Sélectionne `periode_*` et `nb_jours_semaine` **mais PAS** `nb_jours_ouvres`, `nb_jours_ouvrables`, `nb_jours_scenario`. |
| `ScenarioOut` (`schemas.py`) | N'expose pas ces trois `nb_jours_*` (ni `dt_mise_en_oeuvre`, `dt_real_prev`). |
| Insertion à la création | Ces colonnes **ne sont pas alimentées** aujourd'hui (cf. DSR-634). |

> Donc deux choix d'implémentation : (a) **étendre** `SELECT_SCENARIO_SQL` +
> `ScenarioOut` et exposer ces champs sur le `GET /{id}` existant ; ou (b) créer un
> **endpoint dédié** `GET /{id}/periodes`. Recommandé : **(a)** + un endpoint
> dédié léger qui projette uniquement le sous-ensemble « périodes ».

---

## 3. Spécification (SELECT)

```sql
SELECT periode_debut, periode_fin,
       periode_realise_debut, periode_realise_fin,
       periode_prev_debut, periode_prev_fin,
       nb_jours_semaine, nb_jours_ouvres, nb_jours_ouvrables, nb_jours_scenario
  FROM trppu_scenario
 WHERE id_scenario = %s;
```

---

## 4. Contrat d'API proposé

`GET /trppu-api/scenarios/{id_scenario}/periodes`

Réponse `200` :
```json
{
  "periode_debut": "2026-01-01", "periode_fin": "2026-12-31",
  "periode_realise_debut": "2026-01-01", "periode_realise_fin": "2026-05-28",
  "periode_prev_debut": "2026-05-28", "periode_prev_fin": "2026-12-31",
  "nb_jours_semaine": 6,
  "nb_jours_ouvres": 261, "nb_jours_ouvrables": 313, "nb_jours_scenario": 305
}
```
Codes : `200` ; `404` scénario introuvable (`fetch_scenario_or_404`).

---

## 5. Modèle Pydantic proposé

```python
class ScenarioPeriodesOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    periode_debut: date
    periode_fin: date
    periode_realise_debut: date | None = None
    periode_realise_fin: date | None = None
    periode_prev_debut: date | None = None
    periode_prev_fin: date | None = None
    nb_jours_semaine: int
    nb_jours_ouvres: int | None = None
    nb_jours_ouvrables: int | None = None
    nb_jours_scenario: int | None = None
```

---

## 6. DDL de migration

Aucune (colonnes présentes dans `trppu_scenario`). **Pré-requis fonctionnel** :
ces `nb_jours_*` doivent être **alimentés** à la création (DSR-634) / MAJ
(DSR-656) pour ne pas renvoyer `NULL`.

---

## 7. Logging

Tracer l'`id_scenario` (cohérent avec le reste de la lecture d'édition).

---

## 8. Mapping des critères d'acceptance

| Critère | Couverture |
| ------- | ---------- |
| Récupérer les 10 champs de période/jours pour un id existant | §3-§4 |
| Actualiser slider + nb jours de la semaine | §4 |

## 9. Questions ouvertes

`README_incomprehensions.md` : endpoint dédié vs extension du `GET /{id}` ;
dépendance forte avec l'alimentation des `nb_jours_*` (DSR-634/656).
