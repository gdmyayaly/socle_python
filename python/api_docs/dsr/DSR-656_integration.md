# DSR-656 — Mise à jour d'un scénario modifié (EN COURS)

> **User story** : « En tant que TRPPU, je veux que les informations d'un scénario
> soient mises à jour quand elles sont modifiées par l'utilisateur afin de
> sauvegarder les changements du scénario. »
>
> **Tickets liés** : **DSR-659** (MAJ TMH appelée par ce service), DSR-634
> (création — mêmes règles de périodes), DSR-655 (lecture des périodes),
> DSR-613 (calcul `nb_jours_*`).

---

## 1. Contexte & objectif métier

Modification possible **uniquement** si `statut == 'EN COURS'`. Un changement de
dates **n'est sauvegardé qu'après actualisation/recalcul des trafics** (pour que
dates et trafics restent cohérents). À l'actualisation : **toutes les périodes**
sont recalculées et **le TMH** est réécrit (via DSR-659). **Seules les dates ayant
réellement changé sont sauvegardées.**

---

## 2. État actuel & analyse des écarts

| Élément existant | Constat |
| ---------------- | ------- |
| `PATCH /trppu-api/scenarios/{id}/periodes` (`routes.py`) | Met à jour `periode_*` et recalcule realise/prev via `recompute_realise_prev()` + `increment_version()`. **N'écrit pas** `nb_jours_ouvres/ouvrables/scenario`, `dt_mise_en_oeuvre`, `dt_real_prev`, `id_rh_maj`, `dt_maj`. |
| `recompute_realise_prev()` (`helpers.py`) | Logique réalisé/prév déjà présente — **à réviser** sur les bornes « today » (cf. DSR-634, identique ici). |
| `assert_not_fige()` | Réutilisable pour refuser la MAJ hors EN COURS (ici : interdire si statut ≠ EN COURS). |
| MAJ TMH | **Inexistante** → déléguée à DSR-659. |

---

## 3. Spécification table `trppu_scenario` (UPDATE)

Champs mis à jour (paramètres reçus) :

| Colonne | Règle |
| ------- | ----- |
| `periode_debut`, `periode_fin` | dates du scénario |
| `dt_mise_en_oeuvre` | date saisie (à remettre à jour même si inchangée) |
| `dt_real_prev` | date **recalculée** séparant réalisé / prévisionnel |
| `periode_realise_debut/fin`, `periode_prev_debut/fin` | recalculées (cf. §4) |
| `nb_jours_semaine` | 5 (ouvrés) ou 6 (ouvrables) |
| `nb_jours_ouvres`, `nb_jours_ouvrables` | recalculés sur la période (DSR-613) |
| `nb_jours_scenario` | ouvrés/ouvrables **− jours neutralisés** (DSR-645) |
| `dt_maj` | date courante |
| `id_rh_maj` | id RH **crypté** de l'utilisateur qui modifie |

> « Seules les dates ayant réellement changé sont à sauvegarder » : construire
> l'`UPDATE` dynamiquement à partir du diff entre l'état courant et les valeurs
> reçues (cf. patron `set_parts` déjà utilisé dans `trppu_site`/`trppu_pic_version`).

---

## 4. Règles de recalcul réalisé / prévisionnel

Identiques à DSR-634 (`recompute_realise_prev`), avec `today` = date de la MAJ :

| Champ | Règle |
| ----- | ----- |
| `periode_realise_debut` | passé → `periode_debut` ; futur intégral → `NULL` |
| `periode_realise_fin` | fin < today → `periode_fin` ; fin ≥ today → **today (date de MAJ)** ; futur intégral → `NULL` |
| `periode_prev_debut` | futur incluant today → today ; futur intégral → `periode_debut` ; fin < today → `NULL` |
| `periode_prev_fin` | couvre le futur → `periode_fin` ; s'arrête ≤ today → `NULL` |

> ⚠️ Cas limite « today » à arbitrer (cf. `README_incomprehensions.md`, item bornes today).

---

## 5. Contrat d'API proposé

`PUT /trppu-api/scenarios/{id_scenario}` (mise à jour « post-actualisation »)

Body :
```json
{
  "periode_debut": "2026-01-01", "periode_fin": "2026-12-31",
  "dt_mise_en_oeuvre": "2026-06-01",
  "dt_real_prev": "2026-05-28",
  "nb_jours_semaine": 6,
  "nb_jours_ouvres": 261, "nb_jours_ouvrables": 313, "nb_jours_scenario": 305,
  "id_rh_maj": "A123456",
  "tmh": [
    { "co_produit": "OO", "volume_realise": 120000, "volume_previsionnel": 130000,
      "moyenne_journaliere": 4000.00, "moyenne_hebdo": 24000.00, "exclusion": false }
  ]
}
```
> Le bloc `tmh[]` est traité par le service DSR-659 **dans la même transaction**
> (atomicité scénario + TMH). Variante : appels séparés orchestrés côté IHM.

Codes : `200` ; `404` introuvable ; `409` statut ≠ EN COURS (ou figé) ;
`422` `periode_fin < periode_debut`.

---

## 6. Modèles Pydantic proposés

```python
class ScenarioMajRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    periode_debut: date
    periode_fin: date
    dt_mise_en_oeuvre: date
    dt_real_prev: date
    nb_jours_semaine: Literal[5, 6]
    nb_jours_ouvres: int = Field(..., ge=0)
    nb_jours_ouvrables: int = Field(..., ge=0)
    nb_jours_scenario: int = Field(..., ge=0)
    id_rh_maj: str
    tmh: list[TmhUpsert] = []          # cf. DSR-659
    @model_validator(mode="after")
    def _check(self):
        if self.periode_fin < self.periode_debut:
            raise ValueError("periode_fin doit être >= periode_debut")
        return self
```

---

## 7. DDL de migration

Aucune (colonnes présentes dans `trppu_scenario`). Pré-requis : alimentation
effective de `nb_jours_*`, `dt_mise_en_oeuvre`, `dt_real_prev`, `id_rh_maj`
(harmonisation avec DSR-634 — cf. `README_ameliorations.md`).

---

## 8. Cryptage & logging

`id_rh_maj` crypté (`app/security/crypto`). Logs : `id_scenario`, champs modifiés
(diff via `safe_preview`), nb de produits TMH mis à jour.

---

## 9. Mapping des critères d'acceptance

| Critère | Couverture |
| ------- | ---------- |
| Après actualisation, `trppu_scenario` modifié et conforme IHM | §3-§5 |
| `trppu_tmh` correct et en phase IHM | §5 (bloc `tmh`) + DSR-659 |
| Modif possible seulement si EN COURS | §5 (`409`) |
| Seules dates changées sauvegardées | §3 (UPDATE dynamique) |

## 10. Questions ouvertes

`README_incomprehensions.md` : transaction unique scénario+TMH vs appels séparés ;
bornes « today » ; `dt_mise_en_oeuvre` toujours réécrite ? cryptage `id_rh`.
