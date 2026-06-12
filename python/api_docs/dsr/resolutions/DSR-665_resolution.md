# Résolution — DSR-665 (Ajout du paramètre bl_manuel au service TMH)

## 1. Statut
**Terminé (déjà couvert).** Le service YS04 de mise à jour de `trppu_tmh` reçoit et
enregistre déjà l'indicateur **`bl_manuel`** (ligne de trafic ajoutée manuellement), en
plus des paramètres existants. Aucun changement de code nécessaire.

> **Doublon** : DSR-665 a un énoncé **identique à DSR-652** (v2) — même demande
> (ajout de `lb_manuel`/`bl_manuel`). La même implémentation couvre les deux.
> ⚠️ Le fichier `DSR-652_resolution.md` est **obsolète** (il décrit encore la lecture des
> neutralisations, contenu antérieur du ticket avant repurposing).

## 2. Fichiers (existants)
- `app/routes/trppu_tmh/schemas.py` : `TmhUpsert.manuel` (→ `bl_manuel`).
- `app/routes/trppu_scenario/schemas.py` : `ScenarioTmhItem.manuel` (→ `bl_manuel`, création DSR-634).
- `app/routes/trppu_tmh/helpers.py` : `upsert_tmh_row` / `upsert_tmh_rows` écrivent
  `bl_manuel` en **INSERT et UPDATE** (`dt_calcul = NOW()`).

## 3. Service
`PUT /trppu-api/scenarios/{id_scenario}/tmh` (batch, DSR-659) et création via
`POST /trppu-api/scenarios` (champ `tmh[]`, DSR-634).

Paramètres par produit : `co_produit`, `volume_realise`, `volume_previsionnel`,
`moyenne_journaliere`, `moyenne_hebdo`, `exclusion` (→ `bl_exclu`), **`manuel` (→ `bl_manuel`)**.
Upsert : si la ligne (id_scenario, co_produit) n'existe pas → INSERT ; sinon UPDATE des
champs modifiés.

## 4. Conformité base de données (db_10_09_2026)
- `trppu_tmh.bl_manuel` = `tinyint(1) NOT NULL` (sans défaut) → l'INSERT fournit
  toujours `0`/`1` ; jamais NULL. ✅
- `bl_exclu` idem ; `volume_*` >= 0 ; unicité (id_scenario, co_produit). ✅

## 5. Hypothèses & écarts
- Le ticket écrit `lb_manuel` : interprété comme **`bl_manuel`** (colonne DB) ; exposé en
  API sous le nom `manuel` (booléen).
- La **lecture** TMH (`TmhOut`, DSR-650) n'expose pas `bl_manuel` — non requis par DSR-665
  (paramètre d'écriture). À ajouter si l'IHM doit afficher l'origine manuelle.
- La MAJ ciblée d'un volume (`PATCH .../tmh/{co_produit}`, DSR-649) ne porte pas sur
  `bl_manuel` (hors périmètre : édition de volume, pas de l'origine de la ligne).

## 6. Comment tester
Créer/éditer un scénario avec une ligne `tmh` marquée `manuel: true`, puis :
```
SELECT co_produit, bl_manuel FROM trppu_tmh WHERE id_scenario = {id};
```
Vérifier `bl_manuel = 1` pour la ligne saisie manuellement, `0` pour les lignes calculées.

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| Service reçoit `bl_manuel` en plus des paramètres existants | `TmhUpsert.manuel` |
| Ligne absente → INSERT ; existante → UPDATE | `upsert_tmh_row` |
| Trafics (dont origine manuelle) conformes à l'IHM | colonnes mappées 1-1 |

## 8. ➡️ Commentaire Jira (à coller)

> **Service** — `PUT /trppu-api/scenarios/{id_scenario}/tmh` (et création via
> `POST /trppu-api/scenarios`, champ `tmh[]`).
>
> **Paramètre ajouté**
> - `manuel` (booléen, → colonne `bl_manuel`) | indique que la ligne de trafic a été
>   ajoutée manuellement. Le ticket le nomme `lb_manuel` ; la colonne en base est
>   `bl_manuel`.
>
> **Paramètres déjà pris en charge** (rappel)
> - `id_scenario`, `co_produit`, `volume_realise`, `volume_previsionnel`,
>   `moyenne_journaliere`, `moyenne_hebdo`, `exclusion` (→ `bl_exclu`).
> - posés serveur : `id_tmh` (auto), `dt_calcul` (date du jour), `id_rh` (crypté).
>
> **Comportement**
> une ligne par produit ; si elle n'existe pas pour le scénario => insertion, sinon mise
> à jour des informations modifiées. `bl_manuel` vaut 1 pour une ligne saisie
> manuellement, 0 pour une ligne issue du calcul automatique.
>
> **Note** : DSR-665 a le même énoncé que DSR-652 ; la même implémentation couvre les deux.
