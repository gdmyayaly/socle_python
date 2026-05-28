# DSR-661 — Enregistrement d'un coefficient de rétention PIC modifié

> **User story** : « En tant que TRPPU, je veux que les informations de
> paramétrage de rétention en PIC soient enregistrées en base à chaque
> modification afin de conserver les changements de rétention en PIC d'un produit
> pour un scénario donné. »
>
> **Tickets liés** : symétrique de la lecture DSR-660 ; réutilise le module
> `trppu_pic_version` / `trppu_pic_coefficients`.

---

## 1. Contexte & objectif métier

Service (yb04) qui enregistre la modification d'**un** coefficient PIC pour un
scénario. Paramètres reçus (ordre fixe, **chaque paramètre validé avant écriture**) :
`id_scenario`, `co_produit`, `jour_semaine`, `densite` (0/1/2), `coef`, `id_rh` (crypté).

> ⚠️ Le `coef` (valeur du coefficient) est **utilisé** dans tous les INSERT/UPDATE
> mais **n'apparaît pas** dans la liste des paramètres en tête du ticket. On le
> considère **obligatoire**. À confirmer (cf. `README_incomprehensions.md`).
> De même, `co_regate` (nécessaire pour créer la `trppu_pic_version`) n'est pas
> dans les paramètres : il est **dérivé du scénario** (`trppu_scenario.co_regate`).

---

## 2. Logique de traitement

```
v = SELECT id_pic_version FROM trppu_pic_version WHERE id_scenario = :id_scenario

SI v existe (Cas 1) :
    SI une ligne trppu_pic_coefficients (v.id_pic_version, co_produit, jour_semaine, densite) existe :
        UPDATE coef                      # acceptance #1
    SINON :
        INSERT trppu_pic_coefficients(...)   # acceptance #2

SINON (Cas 2 — aucune version scénario) :
    co_regate = SELECT co_regate FROM trppu_scenario WHERE id_scenario = :id_scenario
    new_id = INSERT trppu_pic_version(
                lb_pic_version = f"{co_regate}_{id_scenario}",
                niveau = 'SCENARIO', co_regate, id_scenario,
                dt_activation = NOW(), id_rh_creation, id_rh_maj = id_rh_creation)
    INSERT trppu_pic_coefficients(id_pic_version = new_id, ...)   # acceptance #3
```

> Clé naturelle d'unicité du coefficient retenue : **(`id_pic_version`,
> `co_produit`, `jour_semaine`, `densite`)** — conforme à l'acceptance #1 (« même
> jour et même densité »). Le texte du cas 1.1 ne mentionne que produit+densité ;
> on retient la clé complète (incluant le jour), à confirmer.

Colonnes d'INSERT `trppu_pic_coefficients` : `id_pic_version, co_produit,
jour_semaine, dt_effet = NOW(), coef, densite, id_rh` (crypté).

---

## 3. État actuel & analyse des écarts

| Élément | Constat |
| ------- | ------- |
| CRUD `trppu_pic_version` / `trppu_pic_coefficients` | Existent (POST/PUT bruts) mais **pas** la logique conditionnelle create-version-then-coef par scénario. |
| `niveau = 'SCENARIO'` | Valeur déjà supportée par l'enum `trppu_pic_version.niveau`. |
| Validation stricte des paramètres + message d'erreur IHM | À implémenter (acceptance #4). |

---

## 4. Contrat d'API proposé

`PUT /trppu-api/scenarios/{id_scenario}/pic-coefficients`

Body :
```json
{ "co_produit": "OO", "jour_semaine": "LUNDI", "densite": 0, "coef": 0.8500, "id_rh": "A123456" }
```
Réponses :
```json
{ "action": "update", "id_pic_version": 57 }
{ "action": "insert_coef", "id_pic_version": 57 }
{ "action": "insert_version_and_coef", "id_pic_version": 58 }
```
Codes : `200` (update/insert) ; `404` scénario introuvable ; `409` figé ;
`422` paramètre manquant/invalide (message précisant **quel** paramètre — acceptance #4).

---

## 5. Modèle Pydantic proposé

```python
class PicCoefUpsert(BaseModel):
    model_config = ConfigDict(extra="forbid")
    co_produit: str = Field(..., min_length=2, max_length=2)
    jour_semaine: JourSemaine                              # LUNDI..SAMEDI
    densite: int = Field(..., ge=0, le=2)
    coef: Decimal = Field(..., max_digits=7, decimal_places=4)
    id_rh: str = Field(..., min_length=1)
```
> La validation Pydantic + `extra="forbid"` réalise l'« ordre/présence de chaque
> paramètre vérifié avant insertion » exigé par le ticket ; le `422` renvoie le
> détail du champ fautif (acceptance #4).

---

## 6. DDL de migration

Aucune (toutes les colonnes existent : `trppu_pic_version`,
`trppu_pic_coefficients.{dt_effet, coef, densite, id_rh}`).

---

## 7. Transaction, cryptage & logging

- **Transaction unique** (`db_write.transaction()`) pour le Cas 2 (version + coef
  atomiques).
- `id_rh` / `id_rh_creation` / `id_rh_maj` cryptés (`app/security/crypto`).
- Logs : paramètres (sans id_rh clair), **action réalisée** (update / insert_coef /
  insert_version_and_coef), `id_pic_version`, **id session IHM**.

---

## 8. Mapping des critères d'acceptance

| Critère | Couverture |
| ------- | ---------- |
| #1 coef existant (version+produit+jour+densité) → UPDATE seul | §2 Cas 1.1 |
| #2 version scénario existe mais produit absent → INSERT coef | §2 Cas 1.2 |
| #3 aucune version scénario → INSERT version + INSERT coef | §2 Cas 2 |
| #4 paramètre manquant → rien écrit + log + message IHM | §4 (`422`) + §5 |

## 9. Questions ouvertes

`README_incomprehensions.md` : **`coef` absent de la liste des paramètres** ;
`co_regate` dérivé du scénario ; clé naturelle (jour inclus ?) ; cryptage `id_rh` ;
provenance id session IHM ; gestion de plusieurs versions scénario.
