# TRPPU — Synthèse d'intégration des tickets DSR (API ys04 / yb04)

Ce dossier regroupe les **fiches d'intégration technique** des tickets DSR liés à
la persistance et à la restitution des données d'un scénario TRPPU, ainsi que les
documents transverses (incompréhensions, améliorations).

- **Périmètre** : 16 tickets (DSR-634 déjà documenté + 15 nouveaux).
- **Pile** : FastAPI (async), MySQL via `aiomysql` (`app/db/mysql.py`), Pydantic v2.
- **Livrable de ce dossier** : documentation. L'implémentation est un chantier de suivi.

> Lire d'abord ce README, puis `README_incomprehensions.md` (à arbitrer avec le PO
> **avant** de coder), `README_ameliorations.md` (briques transverses & nouveaux
> tickets proposés) et `README_ordre_implementation.md` (séquencement des phases).

---

## 1. Thèmes & fiches

| Thème | Tickets | Fiche(s) |
| ----- | ------- | -------- |
| Création de scénario | DSR-634 | [DSR-634_implementation.md](DSR-634_implementation.md) |
| Calcul transverse (jours ouvrés/ouvrables + fériés) | DSR-613 | [DSR-613](DSR-613_integration.md) |
| Écriture des paramètres | DSR-644, 645, 646, 649 | [644](DSR-644_integration.md) · [645](DSR-645_integration.md) · [646](DSR-646_integration.md) · [649](DSR-649_integration.md) |
| Lecture pour l'édition | DSR-650, 651, 652, 653, 655 | [650](DSR-650_integration.md) · [651](DSR-651_integration.md) · [652](DSR-652_integration.md) · [653](DSR-653_integration.md) · [655](DSR-655_integration.md) |
| Mise à jour & orchestration | DSR-656, 659, 654 | [656](DSR-656_integration.md) · [659](DSR-659_integration.md) · [654](DSR-654_integration.md) |
| Rétention PIC par scénario | DSR-660, 661 | [660](DSR-660_integration.md) · [661](DSR-661_integration.md) |

---

## 2. Tableau récapitulatif (ticket → table → endpoint → statut)

| Ticket | Sens | Table(s) cible | Endpoint proposé | Existant ? |
| ------ | ---- | -------------- | ---------------- | ---------- |
| 613 | calcul | (aucune / jours fériés) | `GET /calcl_nbr_jours/get_nbr_jours` + `GET /trppu-api/trafics/get_trafics` (enrichis) | **à étendre** |
| 634 | écriture | `trppu_scenario`, `trppu_tmh`, `trppu_site` | `POST /trppu-api/scenarios` | **à étendre** |
| 644 | écriture | `trppu_scenario_comptages_manuels` | `POST/PUT/DELETE /scenarios/{id}/comptages` | **à créer** |
| 645 | écriture | `trppu_neutralisations` | `POST/DELETE /scenarios/{id}/neutralisations` | **à créer** |
| 646 | écriture | `trppu_scenario_variations_prev` | `PUT/DELETE /scenarios/{id}/variations/{co_produit}` | **à créer** |
| 649 | écriture | `trppu_tmh` | `PATCH /scenarios/{id}/tmh/{co_produit}` | **à créer** |
| 650 | lecture | `trppu_tmh` | `GET /scenarios/{id}/tmh` | **à créer** |
| 651 | lecture | `trppu_scenario_variations_prev` | `GET /scenarios/{id}/variations` | **à créer** |
| 652 | lecture | `trppu_neutralisations` | `GET /scenarios/{id}/neutralisations` | **à créer** |
| 653 | lecture | `trppu_scenario_comptages_manuels` | `GET /scenarios/{id}/comptages` | **à créer** |
| 655 | lecture | `trppu_scenario` | `GET /scenarios/{id}/periodes` | **à créer / étendre** |
| 656 | écriture | `trppu_scenario` (+ `trppu_tmh` via 659) | `PUT /scenarios/{id}` | **à étendre** (`PATCH /periodes`) |
| 659 | écriture | `trppu_tmh` | `PUT /scenarios/{id}/tmh` | **à créer** |
| 654 | lecture | agrégation 655/650/653/651/652/660 | `GET /scenarios/{id}/edition` (optionnel) | **à créer** (option B) |
| 660 | lecture | `trppu_pic_version` + `trppu_pic_coefficients` | `GET /scenarios/{id}/pic-coefficients` | **à créer** |
| 661 | écriture | `trppu_pic_version` + `trppu_pic_coefficients` | `PUT /scenarios/{id}/pic-coefficients` | **à créer** |

---

## 3. Graphe de dépendances

```
                          DSR-654 (Édition — orchestration)
                ┌───────────┬───────────┬──────────┬──────────┬───────────┐
              DSR-655     DSR-650     DSR-653    DSR-651     DSR-652     DSR-660
            (périodes)    (TMH)     (comptages)(variations)(neutralis.) (PIC lecture)
                                                                            │ symétrique
                                                                          DSR-661 (PIC écriture)

   Écriture des paramètres (symétriques des lectures ci-dessus) :
     DSR-644 ⇄ DSR-653   |   DSR-645 ⇄ DSR-652   |   DSR-646 ⇄ DSR-651

   Mise à jour scénario :
     DSR-656 ──appelle──► DSR-659 (MAJ TMH)        [DSR-659 recouvre DSR-649]

   Calcul transverse réutilisé :
     DSR-613 (jours ouvrés/ouvrables + fériés) ──► DSR-634, DSR-645, DSR-655, DSR-656
```

---

## 4. Cartographie de l'existant (rappel)

Modules `app/routes/` suivant le patron `routes.py` / `helpers.py` / `schemas.py` :
`trppu_scenario`, `trppu_site`, `trppu_produit`, `trppu_pic_version`,
`trppu_pic_coefficients`, plus `trafics.py`, `calcl_nbr_jours.py`, `logs.py`,
`databricks.py`, `health.py`, `mysql_debug.py`.

Briques réutilisables :
- `app/db/mysql.py` : `db_read`, `db_write`, `db_write.transaction()` (commit/rollback auto).
- `app/log_utils.py` : `safe_preview()`.
- `app/routes/trppu_scenario/helpers.py` : `recompute_realise_prev()`, `ensure_site_exists()`, `increment_version()`, `fetch_scenario_or_404()`, `SELECT_SCENARIO_SQL`.
- `app/routes/calcl_nbr_jours.py` : comptage de jours par semaine (à étendre).
- Enregistrement des routeurs : `app/main.py`.

**Modules à créer** : `trppu_tmh` (649/650/659), `trppu_neutralisations` (645/652),
`trppu_variations` (646/651), `trppu_comptages` (644/653), plus `app/services/`
(jours, crypto) — cf. `README_ameliorations.md`.

---

## 5. Conventions communes (toutes fiches)

- Préfixe REST `/trppu-api/...`, ressources imbriquées sous `/scenarios/{id}/...`.
- Pydantic v2 : `ConfigDict(extra="forbid", str_strip_whitespace=True)`, `id_rh` jamais renvoyé en sortie.
- Écriture transactionnelle quand plusieurs tables (`db_write.transaction()`).
- Logs JSON via `safe_preview`, **sans flèches**, avec `id_scenario` (et id session IHM pour les lectures d'édition / PIC).
- Deux points transverses signalés dans **chaque** fiche concernée : **SAISON ↔ LOCAL** et **cryptage `id_rh`** (décisions centralisées, pas locales).

---

## 6. Index des fichiers

- Fiches : `DSR-613/644/645/646/649/650/651/652/653/654/655/656/659/660/661_integration.md`
- `DSR-634_implementation.md` (création — antérieur)
- `README.md` (ce fichier) · `README_incomprehensions.md` · `README_ameliorations.md` · `README_ordre_implementation.md`
