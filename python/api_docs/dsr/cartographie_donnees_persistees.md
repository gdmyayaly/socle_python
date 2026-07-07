# Cartographie des fonctionnalités — Données reçues ↔ Données persistées

**Objet** : documenter, pour chaque fonctionnalité de l'API TRPPU, les **données reçues** (corps,
query/path params) et les **données réellement écrites en base** (table + colonnes), afin
d'**identifier les données non persistées qui devraient l'être** : champs reçus puis ignorés, colonnes
existantes jamais alimentées, tables jamais peuplées.

**Référentiel schéma** : `db_migrations/db_10_09_2026.sql` (19 tables) + migration `004`.
**Méthode** : analyse des 76 endpoints (`app/main.py`), confrontation input (Pydantic) ↔ SQL
(`INSERT`/`UPDATE`) ↔ schéma. Document complémentaire : `audit_concordance_db_10_09_2026.md`.

## Légende des statuts

| Symbole | Signification |
| --- | --- |
| ✅ | reçu **et** persisté tel quel |
| 🧮 | **dérivé serveur** (recalculé, non saisi : périodes, nb_jours, nb_jour…) |
| 🔒 | reçu en clair puis **crypté (Fernet)** avant stockage (`id_rh`) |
| ⚙️ | **auto-DB** (PK auto-incrément, `dt_creation` DEFAULT, `dt_maj` ON UPDATE) |
| ❌ | reçu mais **ignoré** (jamais écrit) |
| 🚫 | colonne/table **jamais écrite** par aucun code |

---

## 1. Cartographie par fonctionnalité

### 1.1 Scénarios — `trppu_scenario` (+ `trppu_site`, `trppu_tmh`)
Fichiers : `app/routes/trppu_scenario/{routes,helpers,statuts}.py`.

| Endpoint | DSR | Données reçues | Persisté (table.colonnes) |
| --- | --- | --- | --- |
| `POST /scenarios` | 634 | co_regate, lb_scenario, co_roc, lb_regate, type_site, nb_jours_semaine(=6), id_pic_version?, periode_debut?, periode_fin?, dt_mise_en_oeuvre?, id_rh, tmh[] | `trppu_scenario`: co_regate, lb_scenario, co_roc, statut(='EN COURS'), dt_creation⚙️, dt_mise_en_oeuvre, dt_pivot🧮(NOW), periode_debut, periode_fin, periode_realise_*🧮, periode_prev_*🧮, nb_jours_semaine, nb_jours_ouvres🧮, nb_jours_ouvrables🧮, nb_jours_scenario🧮, id_pic_version, version_scenario(=1), est_fige(=0), id_rh_creation🔒, id_rh_maj🔒 · `trppu_site`(si absent) · `trppu_tmh`(par produit) |
| `PUT /scenarios/{id}` | 656 | periode_debut, periode_fin, nb_jours_semaine, dt_mise_en_oeuvre?, id_rh, tmh[] | `trppu_scenario`: periode_*, periode_realise_*🧮, periode_prev_*🧮, dt_pivot🧮, dt_mise_en_oeuvre?, nb_jours_*🧮, dt_maj⚙️, id_rh_maj🔒, version_scenario(+1) · upsert `trppu_tmh` |
| `PATCH /{id}/periodes` | — | periode_debut?, periode_fin? | periode_*, periode_realise_*🧮, periode_prev_*🧮, version_scenario(+1) |
| `PATCH /{id}/nb-jours-semaine` | — | nb_jours_semaine | nb_jours_semaine, version_scenario(+1) |
| `PATCH /{id}/statut` | — | statut | statut + effets de transition, version_scenario(+1) |
| `POST /{id}/mise-en-prod` | — | — | statut(='EN PRODUCTION'), est_fige(=1), dt_mise_en_prod🧮(NOW), version_scenario(+1) |
| `PATCH /{id}/est-fige` | — | est_fige | est_fige, version_scenario(+1) |
| `PATCH /{id}/figement` | 669 | statut("validé"/"simulation"/"en cours") | est_fige🧮(mappé), version_scenario(+1) — **statut DB non modifié** |
| `PATCH /{id}/lb-scenario` | — | lb_scenario | lb_scenario, version_scenario(+1) |
| `POST /{id}/archive` | — | — | statut(='ARCHIVE'), version_scenario(+1) |
| `POST /{id}/duplicate` | — | lb_scenario? | nouveau `trppu_scenario` (périodes, nb_jours_semaine, id_pic_version copiés ; **sous-ressources non copiées**) |
| `DELETE /scenarios/{id}` | — | — | DELETE cascade enfants + `trppu_scenario` |
| `GET /scenarios`, `/{id}`, `/{id}/periodes`(655), `/{id}/edition`(654), `/enums` | 654/655 | — | 🔎 lecture seule |

> ⚠️ `dt_validation` n'est positionnée que via `COALESCE(dt_validation, NOW())` à la transition VALIDE
> (`statuts.py`) — jamais en saisie directe. Flags `trafic_pdi_calcule`/`trafic_agrebal_calcule` : voir §3.

### 1.2 TMH — `trppu_tmh`
Fichiers : `app/routes/trppu_tmh/{routes,helpers}.py`.

| Endpoint | DSR | Données reçues | Persisté |
| --- | --- | --- | --- |
| `PUT /scenarios/{id}/tmh` | 648/659 | tmh[]{co_produit, volume_realise?, volume_previsionnel?, moyenne_journaliere, moyenne_hebdo, exclusion, manuel}, id_rh | upsert `trppu_tmh`: volume_realise, volume_previsionnel, moyenne_journaliere, moyenne_hebdo, bl_exclu, bl_manuel, id_rh🔒, dt_calcul🧮(NOW) |
| `PATCH /scenarios/{id}/tmh/{co_produit}` | 649 | volume_realise, moyenne_journaliere, moyenne_hebdo | volume_realise, moyenne_journaliere, moyenne_hebdo, **bl_manuel=1**🧮, dt_calcul🧮(NOW) — ne touche pas volume_previsionnel/bl_exclu |
| `GET /scenarios/{id}/tmh` | 650 | id_session_ihm? | 🔎 lecture seule |

### 1.3 Comptages manuels — `trppu_scenario_comptages_manuels`
Fichiers : `app/routes/trppu_comptages/routes.py`.

| Endpoint | DSR | Données reçues | Persisté |
| --- | --- | --- | --- |
| `POST /scenarios/{id}/comptages` | 644 | co_produit, dt_comptage?(=today), nb_produit, **id_rh** | id_scenario, dt_comptage, co_produit, nb_produit — **`id_rh` reçu mais ❌ non écrit** (voir §3) |
| `PUT /…/comptages/{co_produit}` | 644 | dt_comptage?, nb_produit, **id_rh** | dt_comptage, nb_produit — **`id_rh` ❌ non écrit** |
| `DELETE /…/comptages/{co_produit}` | 644 | — | DELETE par (id_scenario, co_produit) |
| `GET /scenarios/{id}/comptages` | 653 | id_session_ihm? | 🔎 lecture seule |

### 1.4 Variations prévisionnelles — `trppu_scenario_variations_prev`
Fichiers : `app/routes/trppu_variations/routes.py`. *(colonnes `dt_creation`/`id_rh` via migration 004)*

| Endpoint | DSR | Données reçues | Persisté |
| --- | --- | --- | --- |
| `PUT /…/variations/{co_produit}` | 646 | variation_pct, id_rh | upsert : variation_pct, id_rh🔒, dt_creation🧮(NOW à chaque modif) · **suppression si variation_pct=0** |
| `DELETE /…/variations/{co_produit}` | 646 | — | DELETE par (id_scenario, co_produit) |
| `GET /scenarios/{id}/variations` | 651 | id_session_ihm? | 🔎 lecture seule (liste pilotée par le TMH : co_produit distincts non exclus, variation stockée ou 0 par défaut) |

### 1.5 Neutralisations — `trppu_neutralisations`
Fichiers : `app/routes/trppu_neutralisations/routes.py`.

| Endpoint | DSR | Données reçues | Persisté |
| --- | --- | --- | --- |
| `POST /scenarios/{id}/neutralisations` | 645 | dt_debut, dt_fin, motif, id_rh | id_scenario, dt_debut, dt_fin, **nb_jour🧮** (1 si jour unique, sinon ouvrés/ouvrables), motif, id_rh🔒, dt_creation⚙️, id_neutralisation⚙️ |
| `DELETE /…/neutralisations` | 645 | dt_debut, dt_fin (query) | DELETE par période (id_scenario, dt_debut, dt_fin) |
| `GET /scenarios/{id}/neutralisations` | 652 | id_session_ihm? | 🔎 lecture seule (liste à plat) |

### 1.6 Coefficients PIC d'un scénario — `trppu_pic_version` + `trppu_pic_coefficients`
Fichiers : `app/routes/trppu_scenario_pic/routes.py`. **Module conforme au schéma.**

| Endpoint | DSR | Données reçues | Persisté |
| --- | --- | --- | --- |
| `PUT /scenarios/{id}/pic-coefficients` | 661 | co_produit, jour_semaine(LUNDI…SAMEDI), densite(0-2), coef, id_rh | si version absente → `trppu_pic_version`(niveau='SCENARIO', co_regate, id_scenario, dt_activation🧮, id_rh_creation🔒, id_rh_maj🔒) puis `trppu_pic_coefficients`(id_pic_version, co_produit, jour_semaine, dt_effet🧮, coef, densite, id_rh🔒) ; sinon UPDATE coef, dt_maj⚙️, id_rh🔒 |
| `GET /scenarios/{id}/pic-coefficients` | 660 | id_session_ihm? | 🔎 lecture seule (fusion défaut national + surcharge scénario) |

### 1.7 Référentiels CRUD — `trppu_site`, `trppu_produit`, `trppu_pic_version`
Fichiers : `app/routes/trppu_site/`, `trppu_produit/`, `trppu_pic_version/`.

| Endpoint | Données reçues | Persisté |
| --- | --- | --- |
| `POST/PUT /sites` (+upload-excel) | co_regate, lb_regate?, type_site, co_roc | `trppu_site`: co_regate, lb_regate, type_site, co_roc (dt_maj⚙️) |
| `POST/PUT /produits` (+upload, +DELETE soft) | co_produit, lb_produit, dt_desactivation?, motif_desactivation? | `trppu_produit`: co_produit, lb_produit, dt_desactivation, motif_desactivation (dt_creation⚙️) |
| `POST/PUT /pic-versions` (+upload, +DELETE soft) | lb_pic_version?, niveau(NATIONAL/DEX/SITE), co_regate, dt_activation, dt_desactivation?, motif_desactivation?, commentaire?, est_par_defaut? | `trppu_pic_version`: ces colonnes — **mais `id_rh_creation`/`id_rh_maj` ❌ non écrits** + `id_scenario` non fourni (voir §3) |

### 1.8 Coefficients PIC nationaux (standalone) — `trppu_pic_coefficients`
Fichiers : `app/routes/trppu_pic_coefficients/`. **⚠️ Module cassé** (écrit des colonnes inexistantes
`coef_dense/faible*`, `dt_fin_effet`, `id_rh_creation` + enum `LUN…SAM`). Toutes les écritures
échouent à l'exécution. Détail : `audit_concordance_db_10_09_2026.md` §5.1. *(7 endpoints non listés ici.)*

### 1.9 Modules en lecture seule / hors persistance MySQL
| Module | Endpoints | Persistance |
| --- | --- | --- |
| Trafics (613/666) | `GET /trafics/get_trafics`, `/get_trafics_pivot` | 🔎 lecture **Databricks** (aucune écriture MySQL) |
| Calcul jours (613) | `GET /calcl_nbr_jours/get_nb_jours` | 🔎 API jours-fermes, aucune écriture |
| Audit id_rh | `POST /audit/actions-id-rh` | 🔎 SELECT multi-tables + déchiffrement |
| Health / Databricks / MySQL-debug / Logs | ~15 endpoints | 🔎 introspection / fichiers (`POST /mysql/import` écrit en dynamique, hors métier) |

---

## 2. Tables jamais alimentées par l'API

| Table | Référencée comment | Nature |
| --- | --- | --- |
| `trppu_trafic_pdi` 🚫 | DELETE cascade seul | trafic PDI calculé par **batch non présent** dans ce repo |
| `trppu_trafic_agrebal` 🚫 | DELETE cascade seul | trafic agrebal calculé par **batch non présent** |
| `trppu_agrebal_pdi` 🚫 | non référencée | table de rattachement, jamais peuplée |
| `trppu_cles_repartition` 🚫 | non référencée | clés de répartition, jamais peuplées |
| `trppu_scenario_exclusions` 🚫 | DELETE cascade seul | exclusions produit **non exposées en écriture** |
| `trppu_scenario_pic_coeffs` 🚫 | DELETE cascade seul | table **legacy** (remplacée par `trppu_pic_coefficients`) |
| `trppu_pic_coefficients_ko` 🚫 | non référencée | variante « ko » jamais utilisée |
| `trppu_api_log` 🚫 | exclue **volontairement** (`trppu_scenario/helpers.py:135`) | **aucun log d'appel API persisté en base** (logs → fichiers/Kibana) |
| `trppu_recalcul_log` 🚫 | exclue **volontairement** | **aucun log de recalcul persisté** |
| `demande_dsr` 🚫 | non référencée | hors périmètre API (orchestrateur/système externe) |

→ **10 tables sur 19 ne reçoivent aucune écriture** de l'API. La majorité relève d'un **domaine
calcul/batch (PDI, agrebal, répartition) qui n'existe pas dans ce dépôt**, ou de tables de log/legacy.

---

## 3. Colonnes non écrites dans les tables actives

| Table.colonne | Statut | Commentaire |
| --- | --- | --- |
| `*.id_*` (PK), `*.dt_creation`, `trppu_scenario.dt_maj`, `trppu_site.dt_maj`, `trppu_pic_version.dt_maj`, `trppu_tmh.dt_calcul` | ⚙️ | auto-générées / DEFAULT / ON UPDATE — normal |
| **`trppu_scenario.trafic_pdi_calcule`** | 🚫 gap | insérée à 0, **jamais passée à 1** : le service de calcul PDI (DSR-648) censé la gérer n'existe pas ici → reste 0 en permanence |
| **`trppu_scenario.trafic_agrebal_calcule`** | 🚫 gap | idem pour le calcul agrebal |
| `trppu_scenario.dt_validation` | 🟡 partiel | seulement via `COALESCE(dt_validation, NOW())` à la transition VALIDE — jamais en saisie directe (à confirmer voulu) |
| **`trppu_pic_version.id_rh_creation` / `id_rh_maj`** | 🟡 partiel | **écrites** par le flux scénario-PIC (`trppu_scenario_pic/routes.py:123-132`) mais **PAS** par la CRUD `POST/PUT /pic-versions` (absentes de l'INSERT) → versions créées par cette voie **sans traçabilité** |
| `trppu_pic_version.id_scenario` | 🟡 | NOT NULL en schéma, alimentée uniquement par le flux scénario-PIC ; la CRUD nationale ne la fournit pas |
| `trppu_scenario_comptages_manuels.id_rh` *(post-migration 002)* | 🚫 gap | reçu en entrée (POST/PUT) mais **jamais inséré** → traçabilité DSR-644 perdue |
| `trppu_pic_coefficients.coef` / `densite` / `dt_fin` / `id_rh` | 🚫 | le module standalone écrit `coef_dense/dt_fin_effet/…` à la place → colonnes réelles jamais touchées (cf. `audit_concordance` §5.1) |

---

## 4. Manquements de persistance — synthèse priorisée

| # | Manquement | Impact | Arbitrage |
| --- | --- | --- | --- |
| 1 | `trppu_pic_coefficients` (CRUD national) écrit des colonnes inexistantes | écritures **impossibles** en prod | réécrire sur `coef/densite/dt_fin/id_rh` (cf. audit §5.1) |
| 2 | `id_rh` des **comptages** reçu mais non inséré | traçabilité utilisateur DSR-644 **perdue** | écrire `id_rh` (colonne via migr. 002) **ou** retirer le champ de l'API |
| 3 | `id_rh_creation`/`id_rh_maj` non écrits par la CRUD **pic-versions nationale** | versions nationales **sans auteur** | aligner l'INSERT sur le flux scénario-PIC (crypter + écrire) |
| 4 | `trafic_pdi_calcule` / `trafic_agrebal_calcule` jamais passés à 1 | flags **toujours 0** → consommateurs ne savent jamais qu'un calcul a eu lieu | implémenter (ou brancher) le service de calcul qui les positionne ; sinon documenter qu'ils sont gérés hors-API |
| 5 | Tables `trppu_trafic_pdi/agrebal`, `agrebal_pdi`, `cles_repartition`, `scenario_exclusions` non alimentées | fonctionnalités calcul/exclusion **absentes** | confirmer qu'elles relèvent d'un batch externe (hors périmètre de ce dépôt) |
| 6 | `trppu_api_log` / `trppu_recalcul_log` jamais écrites | **aucun audit d'appels/recalculs en base** | confirmer que les logs fichiers/Kibana suffisent, sinon brancher l'écriture |
| 7 | `dt_validation` jamais saisie directement | date de validation **implicite** | confirmer que le `COALESCE` au passage VALIDE est l'intention |

> Les points 1 à 3 sont des **vrais défauts de réalisation** (donnée attendue, non écrite).
> Les points 4 à 7 sont des **gaps de périmètre/architecture** à confirmer avec le PO / l'équipe.

---

## 5. Renvois
- Défauts de **schéma** et de **migrations** : `api_docs/dsr/audit_concordance_db_10_09_2026.md`.
- Statut déclaré par ticket : `api_docs/dsr/resolutions/DSR-*_resolution.md`.
