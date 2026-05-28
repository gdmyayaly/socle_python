# DSR-660 — Lecture du paramétrage de rétention en PIC d'un scénario

> **User story** : « En tant que TRPPU, je veux que les informations de
> paramétrage de rétention en PIC soient récupérées de la base afin d'afficher les
> paramétrages de rétention en PIC propres au scénario édité. »
>
> **Tickets liés** : appelé par DSR-654 ; symétrique de l'écriture DSR-661.

---

## 1. Contexte & objectif métier

Service (yb04) qui, pour un `id_scenario`, construit le paramétrage PIC à afficher
en **fusionnant** :

1. les **coefficients par défaut** = lignes de `trppu_pic_coefficients` avec
   `id_pic_version = 1` (niveau national) ;
2. **surchargés** par les coefficients spécifiques au scénario, **s'il existe** une
   ligne dans `trppu_pic_version` pour cet `id_scenario` (on récupère alors son
   `id_pic_version` et les `trppu_pic_coefficients` correspondants).

La surcharge **remplace** le défaut sur la clé **(`co_produit`, `jour_semaine`,
`densite`)**. `densite` : `0=dense`, `1=faible 1`, `2=faible 2`.

---

## 2. État actuel & analyse des écarts

| Élément existant | Constat |
| ---------------- | ------- |
| `GET /trppu-api/pic-coefficients` (CRUD) | Liste/filtre brut par colonnes ; **pas** de logique de merge défaut↔scénario. |
| `GET /trppu-api/pic-versions` (CRUD) | Liste brute ; pas de résolution par `id_scenario`. |
| Logique de surcharge | **Inexistante** → endpoint dédié à créer (sous `trppu_scenario` ou `trppu_pic_coefficients`). |
| Table `trppu_scenario_pic_coeffs` | Override « legacy » par scénario présent au schéma — **non utilisé** par ce ticket (qui passe par `trppu_pic_version`). À clarifier (cf. `README_incomprehensions.md`). |

---

## 3. Spécification (merge)

```sql
-- 1) coefficients par défaut (national)
SELECT co_produit, jour_semaine, densite, coef
  FROM trppu_pic_coefficients
 WHERE id_pic_version = 1;

-- 2) id_pic_version spécifique au scénario (si présent)
SELECT id_pic_version, niveau
  FROM trppu_pic_version
 WHERE id_scenario = %s
 ORDER BY id_pic_version DESC LIMIT 1;     -- ou règle d'activation à préciser

-- 3) coefficients du scénario (si id_pic_version trouvé)
SELECT co_produit, jour_semaine, densite, coef
  FROM trppu_pic_coefficients
 WHERE id_pic_version = %s;
```
Fusion en mémoire : dict clé `(co_produit, jour_semaine, densite)` ; on part du
défaut puis on écrase avec les valeurs scénario, en marquant `modifie=true` pour
celles surchargées (pour le rendu « couleur différente » de l'IHM).

Attendu : **3 densités × 6 jours (lun→sam) = 18 lignes par produit**.

---

## 4. Contrat d'API proposé

`GET /trppu-api/scenarios/{id_scenario}/pic-coefficients`

Réponse `200` :
```json
{
  "id_pic_version_defaut": 1,
  "id_pic_version_scenario": 57,
  "niveau_scenario": "SCENARIO",
  "coefficients": [
    { "co_produit": "OO", "jour_semaine": "LUNDI", "densite": 0, "coef": 0.8500, "modifie": true },
    { "co_produit": "OO", "jour_semaine": "LUNDI", "densite": 1, "coef": 0.7000, "modifie": false }
  ]
}
```
`id_pic_version_scenario = null` si le scénario n'a jamais été surchargé (tout
vient du national). Codes : `200` ; `404` scénario introuvable.

---

## 5. Modèles Pydantic proposés

```python
JourSemaine = Literal["LUNDI","MARDI","MERCREDI","JEUDI","VENDREDI","SAMEDI"]

class PicCoefItem(BaseModel):
    co_produit: str
    jour_semaine: JourSemaine
    densite: int = Field(..., ge=0, le=2)   # 0 dense / 1 faible1 / 2 faible2
    coef: Decimal = Field(..., max_digits=7, decimal_places=4)
    modifie: bool

class PicScenarioOut(BaseModel):
    id_pic_version_defaut: int
    id_pic_version_scenario: int | None
    niveau_scenario: str | None
    coefficients: list[PicCoefItem]
```

---

## 6. DDL de migration

Aucune (tables et colonnes existantes).

---

## 7. Logging (exigence forte du ticket)

Tracer : `id_scenario`, **`id_pic_version` utilisé** pour la surcharge, nombre de
coefficients défaut/surchargés, et **id session IHM** (NB explicite du ticket pour
regrouper les traces par session). Cf. `README_incomprehensions.md` (provenance de
l'id session).

---

## 8. Mapping des critères d'acceptance

| Critère | Couverture |
| ------- | ---------- |
| Pour chaque produit : 3 densités × 6 jours = 18 lignes | §3 |
| Scénario jamais modifié → tous les coefs nationaux | §3 (id_pic_version_scenario null) |
| Scénario modifié → national non modifiés + surcharges scénario | §3 (`modifie`) |
| Distinction visuelle des modifiés | champ `modifie` §4 (rendu IHM) |
| Logs id_pic_version + id session | §7 |

## 9. Questions ouvertes

`README_incomprehensions.md` : rôle de `trppu_scenario_pic_coeffs` (legacy) vs
`trppu_pic_version`+`trppu_pic_coefficients` ; sélection de la bonne `id_pic_version`
si plusieurs lignes pour le scénario (activation/désactivation) ; niveaux
`DEX`/`SITE` non traités ; provenance id session IHM.
