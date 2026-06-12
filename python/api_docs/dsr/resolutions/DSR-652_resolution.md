# Résolution — DSR-652 (Enregistrement exclusion + saisie manuelle dans trppu_tmh)

> **⚠️ Ticket repurposé** : l'énoncé v2 de DSR-652 porte désormais sur l'ajout du
> paramètre `bl_manuel` (et `bl_exclu`) au service TMH — **objectif identique à DSR-665**.
> Le contenu antérieur de ce ticket (« lecture des périodes neutralisées ») n'est plus
> rattaché ici ; cet endpoint existe toujours mais renvoie désormais une **liste à plat**
> (cf. `DSR-645_resolution.md`, suite au passage `type` → `motif`).

## 1. Statut
**Terminé (déjà couvert).** Le service YS04 de mise à jour de `trppu_tmh` reçoit et
enregistre l'origine **manuelle** d'une ligne (`bl_manuel`) en plus de l'**exclusion**
(`bl_exclu`) et des autres paramètres. Aucun changement de code (même implémentation que
DSR-665).

## 2. Objectif
Disposer, par produit et par scénario, de toutes les infos de trafic nécessaires au calcul
du trafic au PDI (yb05) : volumes, moyennes, **exclusion du calcul** (`bl_exclu`) et
**origine manuelle** de la ligne (`bl_manuel`).

## 3. Fichiers (existants)
- `app/routes/trppu_tmh/schemas.py` : `TmhUpsert.exclusion` (→ `bl_exclu`),
  `TmhUpsert.manuel` (→ `bl_manuel`) ; `TmhOut` expose `bl_exclu` + `bl_manuel`.
- `app/routes/trppu_scenario/schemas.py` : `ScenarioTmhItem.exclusion` / `.manuel` (création DSR-634).
- `app/routes/trppu_tmh/helpers.py` : `upsert_tmh_row` écrit `bl_exclu` + `bl_manuel`
  en **INSERT et UPDATE** (`dt_calcul = NOW()`).

## 4. Service
`PUT /trppu-api/scenarios/{id_scenario}/tmh` (batch DSR-659) et création via
`POST /trppu-api/scenarios` (champ `tmh[]`, DSR-634).

Paramètres par produit : `co_produit`, `volume_realise`, `volume_previsionnel`,
`moyenne_journaliere`, `moyenne_hebdo`, **`exclusion` (→ `bl_exclu`)**,
**`manuel` (→ `bl_manuel`)**.
Upsert : ligne absente → INSERT ; existante → UPDATE des champs modifiés.

## 5. Conformité base de données (db_10_09_2026)
- `trppu_tmh.bl_exclu` et `trppu_tmh.bl_manuel` = `tinyint(1) NOT NULL` (sans défaut) →
  l'INSERT fournit toujours `0`/`1` ; jamais NULL. ✅
- Lecture : `bl_exclu` + `bl_manuel` exposés par `TmhOut` (DSR-650). ✅

## 6. Liens
- **DSR-665** : énoncé identique (même implémentation).
- **DSR-649** : la MAJ ciblée d'un trafic force `bl_manuel = 1` (édition manuelle).
- **DSR-650** : lecture TMH expose `bl_exclu` + `bl_manuel`.
- Le `lb_manuel` du ticket est interprété comme `bl_manuel` (colonne DB).

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| Service reçoit `bl_manuel` en plus des paramètres existants | `TmhUpsert.manuel` |
| Exclusion enregistrée | `TmhUpsert.exclusion` → `bl_exclu` |
| Ligne absente → INSERT ; existante → UPDATE | `upsert_tmh_row` |
| Trafics (exclusion + origine) conformes à l'IHM | colonnes mappées 1-1 |

## 8. ➡️ Commentaire Jira (à coller)

> **Service** — `PUT /trppu-api/scenarios/{id_scenario}/tmh` (et création via
> `POST /trppu-api/scenarios`, champ `tmh[]`).
>
> **Paramètres pris en compte** (par produit)
> - `exclusion` (→ `bl_exclu`) | produit exclu du calcul au PDI ou non.
> - `manuel` (→ `bl_manuel`) | ligne de trafic ajoutée/saisie manuellement. Le ticket le
>   nomme `lb_manuel` ; la colonne en base est `bl_manuel`.
> - rappel : `co_produit`, `volume_realise`, `volume_previsionnel`, `moyenne_journaliere`,
>   `moyenne_hebdo` ; posés serveur : `id_tmh` (auto), `dt_calcul` (date du jour).
>
> **Comportement** : une ligne par produit ; insertion si absente, sinon mise à jour des
> informations modifiées. `bl_exclu` et `bl_manuel` valent 1/0 selon le cas.
>
> **Lecture** : `GET /trppu-api/scenarios/{id_scenario}/tmh` renvoie `bl_exclu` et
> `bl_manuel` (DSR-650).
>
> **Note** : objectif identique à DSR-665 (même implémentation). L'ancien intitulé de ce
> ticket (lecture des neutralisations) n'est plus rattaché ici.
