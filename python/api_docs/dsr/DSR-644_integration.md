# DSR-644 — Écriture des comptages manuels d'objets d'un scénario

> **User story** : « En tant que TRPPU, je veux que les informations d'ajout ou
> de suppression des comptages d'objets soient mises à jour en base afin de
> sauvegarder l'information liée au scénario en cours à chaque modification. »
>
> **Tickets liés** : DSR-653 (lecture des mêmes comptages pour l'édition),
> DSR-654 (orchestration édition).

---

## 1. Contexte & objectif métier

Créer un service (POD yb04) qui **ajoute / modifie / supprime** des comptages
manuels saisis par l'utilisateur dans la table `trppu_scenario_comptages_manuels`.

- **Ajout** : insère une ligne (`id_scenario`, `dt_comptage`, `co_produit`, `nb_produit`, `id_rh` crypté).
- **Modification** : met à jour la ligne du couple (`id_scenario`, `co_produit`).
- **Suppression** : supprime la ligne du couple (`id_scenario`, `co_produit`).

---

## 2. État actuel & analyse des écarts

| Élément | Constat |
| ------- | ------- |
| Module de routes | **Inexistant** — aucun endpoint pour `trppu_scenario_comptages_manuels`. À créer (`app/routes/trppu_comptages/`). |
| Table `trppu_scenario_comptages_manuels` | Existe : `id_comptage, id_scenario, dt_comptage, co_produit, nb_produit`. |
| Colonne `id_rh` | **Absente du schéma** — exigée par le ticket → migration nécessaire (cf. §6). |
| Cryptage `id_rh` | **Aucun utilitaire** dans le code → module transverse à créer (`README_ameliorations.md`). |

---

## 3. Spécification table `trppu_scenario_comptages_manuels`

| Colonne | Ajout | Modification | Valeur |
| ------- | :---: | :---: | ------ |
| `id_comptage` | auto | — | AUTO_INCREMENT |
| `id_scenario` | ✅ | clé | id du scénario en cours |
| `co_produit` | ✅ | clé | code produit |
| `dt_comptage` | ✅ | ✅ | date d'ajout/modification |
| `nb_produit` | ✅ | ✅ | trafic saisi |
| `id_rh` *(à ajouter)* | ✅ | ✅ | id RH **crypté** |

> **Clé fonctionnelle** : (`id_scenario`, `co_produit`). À confirmer si plusieurs
> comptages d'un même produit sont possibles (sinon contrainte d'unicité utile —
> cf. `README_incomprehensions.md`).

---

## 4. Contrat d'API proposé

Préfixe : `/trppu-api/scenarios/{id_scenario}/comptages`

| Méthode | Chemin | Rôle |
| ------- | ------ | ---- |
| `POST` | `/trppu-api/scenarios/{id_scenario}/comptages` | Ajout (ou upsert) d'un comptage |
| `PUT` | `/trppu-api/scenarios/{id_scenario}/comptages/{co_produit}` | Modification |
| `DELETE` | `/trppu-api/scenarios/{id_scenario}/comptages/{co_produit}` | Suppression |

Body `POST` :
```json
{ "co_produit": "OO", "dt_comptage": "2026-05-28", "nb_produit": 1500, "id_rh": "A123456" }
```
Body `PUT` :
```json
{ "dt_comptage": "2026-05-28", "nb_produit": 1800, "id_rh": "A123456" }
```

Codes : `201` création ; `200` modification ; `204` suppression ; `404`
scénario / comptage introuvable ; `409` scénario figé (réutiliser `assert_not_fige`).

---

## 5. Modèles Pydantic proposés

```python
class ComptageCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    co_produit: str = Field(..., min_length=2, max_length=2)
    dt_comptage: date
    nb_produit: int = Field(..., ge=0)
    id_rh: str = Field(..., min_length=1)          # crypté côté service avant INSERT

class ComptageUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    dt_comptage: date | None = None
    nb_produit: int | None = Field(None, ge=0)
    id_rh: str

class ComptageOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    co_produit: str
    dt_comptage: date
    nb_produit: int
```
> `id_rh` n'est jamais renvoyé en sortie.

---

## 6. DDL de migration

```sql
ALTER TABLE `trppu_scenario_comptages_manuels`
  ADD COLUMN `id_rh` VARCHAR(40) NULL AFTER `nb_produit`;
-- Option (à valider) : unicité fonctionnelle
-- ADD UNIQUE KEY `uk_comptage_scen_prod` (`id_scenario`, `co_produit`);
```

---

## 7. Cryptage & logging

- `id_rh` crypté via le module transverse `app/security/crypto.encrypt_id_rh()`
  (cf. `README_ameliorations.md`). Garantir une longueur ≤ 40.
- Logs `safe_preview` : tracer `id_scenario`, `co_produit`, action (add/update/delete) ; **ne jamais logger l'id_rh en clair**.

---

## 8. Mapping des critères d'acceptance

| Critère | Couverture |
| ------- | ---------- |
| Ajout → 1 ligne avec id_scenario, objet, trafic | §3 + `POST` §4 |
| Modification → nouveau trafic en base | §3 + `PUT` §4 |
| Suppression → ligne supprimée | `DELETE` §4 |
| 2-3 comptages, puis modif + suppression | couvert par les 3 endpoints |
| `id_rh` crypté en base | §7 + migration §6 |

## 9. Questions ouvertes

Voir `README_incomprehensions.md` : cryptage `id_rh`, unicité (id_scenario,
co_produit), upsert vs insert pur, longueur de `co_produit` (char(2)).
