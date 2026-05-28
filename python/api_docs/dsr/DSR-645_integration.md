# DSR-645 — Écriture des périodes neutralisées d'un scénario

> **User story** : « En tant que TRPPU, je veux que les informations concernant
> les différents jours à neutraliser d'une période d'un scénario soient mises à
> jour en base afin de sauvegarder l'information liée au scénario en cours et ce
> à chaque modification. »
>
> **Tickets liés** : DSR-652 (lecture des neutralisations), DSR-613 (mécanique
> jours fériés / week-end réutilisée pour `nb_jour`), DSR-656 (`nb_jours_scenario`
> = jours ouvrés/ouvrables **déduits des périodes neutralisées**).

---

## 1. Contexte & objectif métier

Service (POD yb04) qui **ajoute** ou **supprime** des périodes à neutraliser dans
`trppu_neutralisations`. Trois types, correspondant à trois widgets IHM :

| Widget IHM | `type` |
| ---------- | ------ |
| « période neutralisée » (jours fériés cochés) | `FERIE` |
| « Neutralisation de la peak période »         | `PEAK` |
| « Neutralisation saisonnière »                | `SAISON` |

- **Ajout** : insère (`id_scenario`, `dt_debut`, `dt_fin`, `nb_jour`, `type`, `dt_creation`, `id_rh` crypté).
- **Suppression** : supprime la/les ligne(s) du couple (`id_scenario`, `type`) concerné(s).

---

## 2. État actuel & analyse des écarts

| Élément | Constat |
| ------- | ------- |
| Module de routes | **Inexistant** → à créer (`app/routes/trppu_neutralisations/`). |
| Table `trppu_neutralisations` | Existe : `id, id_scenario, dt_debut, dt_fin, nb_jour, type`. |
| Enum `type` | Schéma = `('FERIE','PEAK','LOCAL')` — le ticket impose **`SAISON`** (pas `LOCAL`). **Incohérence bloquante** → migration enum. |
| Colonnes `dt_creation`, `id_rh` | **Absentes** du schéma — exigées → migration. |
| Calcul `nb_jour` | Aucun helper → réutiliser la mécanique fériés/week-end de DSR-613. |

---

## 3. Règle de calcul de `nb_jour`

`nb_jour` = nombre de jours **réellement déduits** de la période du scénario.

### Type `FERIE`
Chaque jour férié coché crée **une ligne** : `dt_debut = dt_fin`, `nb_jour = 1`, `type = FERIE`.

### Types `PEAK` et `SAISON` (même formule)
```
nb_jour = (jours totaux de [dt_debut, dt_fin])
          - (fériés de la période)
          - (samedis)   si nb_jours_semaine == 5   # semaine ouvrée
          - (dimanches)                            # toujours
```
Autrement dit :
- **semaine 5 jours (ouvrés)** : retirer fériés + **samedis** + **dimanches** ;
- **semaine 6 jours (ouvrables)** : retirer fériés + **dimanches** uniquement
  (un férié tombant un samedi compte alors comme férié, pas comme samedi).

**Exemple PEAK** — `mar 10/11/2026 → sam 19/12/2026`, 40 jours, 1 férié (11/11) :
- 5 j : `40 − 1 − 10(sam) − 11(dim) = 18`
- 6 j : `40 − 1 − 11(dim) = 28`

**Exemple SAISON** — `sam 08/08/2026 → dim 23/08/2026`, 16 jours, 1 férié samedi (15/08) :
- 5 j : `16 − 3(sam) − 3(dim) = 10`
- 6 j : `16 − 3(dim) − 1(samedi 15/08 férié) = 12`

> Le `nb_jours_semaine` du scénario (table `trppu_scenario`) pilote la formule.
> Le helper de calcul des jours/fériés (DSR-613, `jours_service`) est réutilisé ici.

---

## 4. Contrat d'API proposé

Préfixe : `/trppu-api/scenarios/{id_scenario}/neutralisations`

| Méthode | Chemin | Rôle |
| ------- | ------ | ---- |
| `POST` | `/trppu-api/scenarios/{id_scenario}/neutralisations` | Ajout d'une neutralisation (le `nb_jour` est **calculé serveur**) |
| `DELETE` | `/.../neutralisations?type=FERIE&dt=2026-11-11` | Suppression d'un jour férié précis |
| `DELETE` | `/.../neutralisations?type=PEAK` | Désactivation de la peak période (supprime la ligne PEAK) |
| `DELETE` | `/.../neutralisations?type=SAISON` | Désactivation de la période saisonnière |

Body `POST` :
```json
{ "type": "PEAK", "dt_debut": "2026-11-10", "dt_fin": "2026-12-19", "id_rh": "A123456" }
```
Réponse `201` :
```json
{ "id": 42, "type": "PEAK", "dt_debut": "2026-11-10", "dt_fin": "2026-12-19", "nb_jour": 28 }
```
Codes : `201` ; `204` suppression ; `404` scénario introuvable ; `409` scénario
figé ; `422` `dt_fin < dt_debut` ou type inconnu.

> Pour `FERIE`, l'IHM peut poster plusieurs jours ; prévoir soit N appels `POST`,
> soit un `POST` batch `{ "type":"FERIE", "jours": ["2026-11-11", ...] }`.

---

## 5. Modèles Pydantic proposés

```python
TypeNeutralisation = Literal["FERIE", "PEAK", "SAISON"]

class NeutralisationCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: TypeNeutralisation
    dt_debut: date
    dt_fin: date
    id_rh: str
    @model_validator(mode="after")
    def _check(self):
        if self.dt_fin < self.dt_debut:
            raise ValueError("dt_fin doit être >= dt_debut")
        if self.type == "FERIE" and self.dt_debut != self.dt_fin:
            raise ValueError("Un FERIE couvre un seul jour (dt_debut == dt_fin)")
        return self

class NeutralisationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    type: TypeNeutralisation
    dt_debut: date
    dt_fin: date
    nb_jour: int
```

---

## 6. DDL de migration

```sql
ALTER TABLE `trppu_neutralisations`
  MODIFY COLUMN `type` ENUM('FERIE','PEAK','SAISON') NOT NULL,   -- LOCAL -> SAISON
  ADD COLUMN `dt_creation` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN `id_rh` VARCHAR(40) NULL;
```
> ⚠️ Le passage `LOCAL → SAISON` doit être validé par le PO (données existantes ?).
> Voir `README_incomprehensions.md`.

---

## 7. Cryptage & logging

`id_rh` crypté (`app/security/crypto`). Logs : tracer `id_scenario`, `type`,
`dt_debut`, `dt_fin`, `nb_jour` calculé ; jamais d'id_rh en clair.

---

## 8. Mapping des critères d'acceptance

| Critère | Couverture |
| ------- | ---------- |
| FERIE : 1 ligne/jour, dt_debut=dt_fin, nb_jour=1, type=FERIE | §3 + §4 |
| PEAK : 1 ligne, dates IHM, nb_jour calculé selon 5/6 j | §3 (ex. 18/28) |
| SAISON : 1 ligne, dates IHM, nb_jour calculé selon 5/6 j | §3 (ex. 10/12) |
| `dt_creation` = date du jour | §6 (défaut SQL) |
| `id_rh` crypté | §7 |
| Suppression FERIE/PEAK/SAISON → ligne supprimée | `DELETE` §4 |

## 9. Questions ouvertes

`README_incomprehensions.md` : **enum SAISON vs LOCAL**, colonnes manquantes,
source des jours fériés, unicité (PEAK/SAISON = 1 seule ligne ⇒ contrainte ?),
batch FERIE vs N appels, cryptage `id_rh`.
