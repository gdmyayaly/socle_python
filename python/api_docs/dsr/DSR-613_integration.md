# DSR-613 — `RecupererTrafics` renvoie le nb de jours ouvrés / ouvrables

> **User story** : « En tant que TRPPU, je veux que le service "RecupererTrafics"
> du POD YS04 renvoie le nombre de jours ouvrés et ouvrables pour la période
> interrogée afin de transmettre l'information à l'IHM TRPPU. »
>
> **Tickets liés** : DSR-645 (réutilise la même mécanique fériés/jours pour le
> calcul de `nb_jour` des neutralisations), DSR-634 / DSR-655 / DSR-656 (les
> `nb_jours_*` du scénario s'appuient sur ce calcul).

---

## 1. Contexte & objectif métier

Lors de l'appel IHM au web service **`RecupererTrafics`** (YS04), le service doit
appeler **`CalculerNbJours`** en lui transmettant les dates début/fin de la
période, calculer le **nombre de jours ouvrés** (lun→ven, 5/sem) et le **nombre
de jours ouvrables** (lun→sam, 6/sem), **déduire les jours fériés nationaux**,
puis renvoyer les deux résultats à l'IHM.

Règle de déduction des fériés (citée verbatim au ticket) :
- férié **hors week-end** → déduit des ouvrés **et** des ouvrables ;
- férié tombant un **samedi** → déduit des **ouvrables uniquement** ;
- férié tombant un **dimanche** → **non déduit** (déjà exclu des deux comptages).

**Exemple de référence** — période `01/03/2025 → 31/03/2026` :

| Donnée                          | Valeur |
| ------------------------------- | -----: |
| Jours totaux                    |    396 |
| Jours ouvrés (lun–ven)          |    272 |
| Jours ouvrables (lun–sam)       |    339 |
| Fériés sur la période           |     11 |
| dont hors week-end              |     10 |
| dont samedi                     |      1 |
| **nbJoursOuvres** = 272 − 10    | **262** |
| **nbJoursOuvrables** = 339 − 10 − 1 | **328** |

---

## 2. État actuel & analyse des écarts

| Élément existant | Fichier | Écart vs DSR-613 |
| ---------------- | ------- | ---------------- |
| `RecupererTrafics` ≈ `GET /trppu-api/trafics/get_trafics` | `app/routes/trafics.py` | Ne renvoie **pas** `nbJoursOuvres` / `nbJoursOuvrables` ; n'appelle pas `CalculerNbJours`. |
| `CalculerNbJours` ≈ `GET /calcl_nbr_jours/get_nbr_jours` | `app/routes/calcl_nbr_jours.py` | Compte les « jours ouverts » par semaine en **excluant uniquement le dimanche** (= ouvrables lun–sam). **Ne calcule pas** la variante ouvrés (lun–ven) et **ne déduit pas** les jours fériés. |
| Source de jours fériés | — | **Inexistante** (aucune table ni dépendance). |

> Le calcul actuel (`isoweekday() != 7`) correspond aux **jours ouvrables** bruts,
> sans fériés. Il faut : (a) ajouter la variante **ouvrés**, (b) introduire une
> **source de jours fériés**, (c) appliquer la règle de déduction ci-dessus,
> (d) brancher l'appel dans `RecupererTrafics`.

---

## 3. Spécification du calcul

Pour un intervalle `[date_debut, date_fin]` (bornes incluses) :

```
ouvrables_bruts  = nb de jours dont weekday ∈ {lun..sam}        # dimanche exclu
ouvres_bruts     = nb de jours dont weekday ∈ {lun..ven}        # sam + dim exclus
feries           = liste des jours fériés nationaux ∈ [debut, fin]
feries_hors_we   = fériés dont weekday ∈ {lun..ven}
feries_samedi    = fériés dont weekday == samedi
# (les fériés tombant un dimanche sont ignorés)

nb_jours_ouvres     = ouvres_bruts    - len(feries_hors_we)
nb_jours_ouvrables  = ouvrables_bruts - len(feries_hors_we) - len(feries_samedi)
```

> **Source des jours fériés** : API jours-fermes des tournées
> (`GET {host}/tournees/jours-fermes/v1/get?annee=AAAA`), consommée via
> `app/services/jours_fermes_client.py` (cache par année + TTL). L'ancienne table
> `trppu_jours_feries` n'est plus utilisée.

---

## 4. Contrat d'API proposé

### 4.1 `CalculerNbJours` enrichi

`GET /calcl_nbr_jours/get_nbr_jours?date_debut=AAAAMMJJ&date_fin=AAAAMMJJ`

Réponse (champs ajoutés en **gras**) :

```json
{
  "date_debut": "20250301",
  "date_fin": "20260331",
  "nb_jours_total": 396,
  "nb_jours_ouvres_bruts": 272,
  "nb_jours_ouvrables_bruts": 339,
  "nb_feries_hors_weekend": 10,
  "nb_feries_samedi": 1,
  "nbJoursOuvres": 262,
  "nbJoursOuvrables": 328
}
```

### 4.2 `RecupererTrafics` enrichi

`GET /trppu-api/trafics/get_trafics?co_regate=...&date_debut=...&date_fin=...`

La réponse existante est complétée par un bloc `nb_jours` :

```json
{
  "co_regate": "400300",
  "date_debut": "2025-03-01",
  "date_fin": "2026-03-31",
  "count": 1234,
  "data": [ "... trafics bruts inchangés ..." ],
  "nb_jours": { "nbJoursOuvres": 262, "nbJoursOuvrables": 328 }
}
```

Codes : `200` nominal ; `400` dates invalides / écart > `MAX_DATE_RANGE_DAYS` (réutilise `validate_params`).

---

## 5. Modèles Pydantic proposés

```python
class NbJoursResult(BaseModel):
    nb_jours_total: int
    nb_jours_ouvres_bruts: int
    nb_jours_ouvrables_bruts: int
    nb_feries_hors_weekend: int
    nb_feries_samedi: int
    nbJoursOuvres: int       # ouvrés - fériés hors week-end
    nbJoursOuvrables: int    # ouvrables - fériés hors week-end - fériés samedi
```

`get_trafics` renvoie aujourd'hui un `dict` libre : ajouter la clé `nb_jours`
typée par `NbJoursResult` (ou un sous-dict `{nbJoursOuvres, nbJoursOuvrables}`).

---

## 6. Implémentation suggérée (réutilisation)

- Factoriser le calcul dans un helper partagé `app/services/jours_service.py`
  (réutilisé par DSR-645 et le calcul des `nb_jours_*` de DSR-634/655/656) :
  `def compute_nb_jours(debut: date, fin: date) -> NbJoursResult`.
- Réutiliser la boucle de parcours de jours déjà présente dans
  `calcl_nbr_jours.py` (passer de « dimanche exclu » à deux compteurs).
- `RecupererTrafics` (`trafics.py`) appelle ce helper après `validate_params`.
- Pas de DDL **sauf** si l'option « table jours fériés » est retenue (voir
  `README_ameliorations.md`).

---

## 7. Mapping des critères d'acceptance

| Élément du ticket | Couverture |
| ----------------- | ---------- |
| `RecupererTrafics` appelle `CalculerNbJours` avec début/fin | §4.2 + §6 |
| Calcul ouvrés (lun–ven) et ouvrables (lun–sam) | §3 |
| Déduction fériés (hors WE des 2 ; samedi des ouvrables ; dimanche ignoré) | §3 |
| Exemple 262 / 328 sur 01/03/2025–31/03/2026 | §1 (table) — sert de test de non-régression |
| Les 2 résultats renvoyés à l'IHM | §4.2 |

---

## 8. Questions ouvertes

Voir `README_incomprehensions.md` : **source des jours fériés**, **format exact
attendu par l'IHM** (noms de champs `nbJoursOuvres`/`nbJoursOuvrables` vs
snake_case), périmètre « national » uniquement ou fériés locaux à terme.
