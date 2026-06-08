# Rapport de comparaison — schéma PROD (`dbV2.sql`) ↔ schéma LOCAL (run_migrations)

> Date : 2026-06-08 · Base : `dsr_mercure_aa` / `trppu` (19 tables) · Préfixe `trppu_` + `demande_dsr`

---

## 1. Résumé exécutif

Le schéma **production** (`db_analyse/v2/dbV2.sql`) et le schéma **local** (sortie de
`scripts/run_migrations.py`) dérivent de la même base introspectée : ils sont donc **quasi
identiques**. Mais **5 divergences chirurgicales** portent sur des colonnes / clés primaires que le
code lit ou écrit **en dur**, plus **1 incohérence préexistante** indépendante de la prod.

L'impact est largement amplifié par une seule divergence (**A — `dt_real_prev` vs `dt_pivot`**) :
la requête de lecture d'un scénario est appelée par presque toutes les routes du domaine, si bien
que **l'essentiel de l'API « scénario » tombe en production**.

| Gravité | Constat |
|---------|---------|
| 🔴 Bloquant | Tout le domaine `/trppu-api/scenarios` + sous-ressources (lecture ET écriture) échoue sur prod |
| 🔴 Bloquant | Neutralisations, écritures TMH / comptages / variations échouent sur prod |
| 🟠 Préexistant | Le CRUD `/trppu-api/pic-coefficients` est déjà cassé (local ET prod) |
| 🟢 Sain | `sites`, `produits`, `pic-versions` et les routes utilitaires fonctionnent sur prod |

---

## 2. Périmètre & méthode

- **PROD** = DDL réel de `db_analyse/v2/dbV2.sql` (dump du schéma de production).
- **LOCAL** = schéma reconstruit par `scripts/run_migrations.py`, c.-à-d. :
  `db_analyse/schema_trppu.sql` + `db_migrations/001_widen_id_rh_columns.sql`
  + `db_migrations/002_add_param_columns.sql`.
- **Contrat applicatif** = le SQL en dur dans `app/routes/**/helpers.py` et `routes.py`. C'est lui
  qui détermine si une route casse : une route échoue dès que son SQL référence une colonne/clé
  absente du schéma cible (erreur MySQL `Unknown column …`), ou viole une contrainte `NOT NULL`/FK/CHECK.

> ⚠️ Les anciens fichiers `db.sql`, `db2.sql`, `db3.sql` (racine) sont **obsolètes** et ne reflètent
> ni la prod ni le local. Ils ont été supprimés dans le cadre de ce travail (ils induisaient en
> erreur, p.ex. `db3.sql` proposait `jour_semaine` = `LUNDI…SAMEDIE` et `type` = `…,LOCAL`).

---

## 3. Tableau des divergences (LOCAL post-migration vs PROD)

| # | Table | LOCAL (code + migrations) | PROD (`dbV2.sql`) | Erreur déclenchée |
|---|-------|---------------------------|-------------------|-------------------|
| **A** | `trppu_scenario` | colonne `dt_real_prev` | colonne `dt_pivot` (**pas** de `dt_real_prev`) | `Unknown column 'dt_real_prev'` |
| **B** | `trppu_neutralisations` | PK = `id` | PK = `id_neutralisation` (**pas** de `id`) | `Unknown column 'id'` |
| **C** | `trppu_tmh` | pas de `bl_manuel` | `bl_manuel` TINYINT(1) **NOT NULL sans défaut** (+ `id_rh`) | `Field 'bl_manuel' doesn't have a default value` |
| **D** | `trppu_scenario_comptages_manuels` | `id_rh` VARCHAR(255) (migr. 002) | **pas** de `id_rh` | `Unknown column 'id_rh'` |
| **E** | `trppu_scenario_variations_prev` | `dt_creation` + `id_rh` (migr. 002) | **aucune** des deux | `Unknown column 'id_rh'` / `'dt_creation'` |
| **F** | `trppu_pic_coefficients` | *(le module CRUD écrit)* `coef_dense`, `coef_faible1`, `coef_faible2`, `dt_fin_effet` | colonnes réelles `coef`, `densite`, `dt_fin` | `Unknown column …` — **cassé sur PROD *et* LOCAL** |

### Détail DDL des cas A–C (les plus impactants)

**A — `trppu_scenario`**
```
LOCAL : … dt_mise_en_oeuvre, dt_mise_en_prod, dt_real_prev, periode_debut, …
PROD  : … dt_mise_en_oeuvre, dt_mise_en_prod, dt_pivot,     periode_debut, …
```
La colonne a été renommée `dt_real_prev` → `dt_pivot` côté prod. Le code ne connaît que `dt_real_prev`.

**B — `trppu_neutralisations`**
```
LOCAL : `id` bigint AUTO_INCREMENT  PRIMARY KEY
PROD  : `id_neutralisation` bigint AUTO_INCREMENT PRIMARY KEY  (+ colonne `motif`)
```
Le type d'enum est aligné des deux côtés (`'FERIE','PEAK','SAISON'`, grâce à la migration 002).
Seul le nom de la PK diffère, mais le code fait `SELECT id` / `WHERE id = …`.

**C — `trppu_tmh`**
```
LOCAL : …, dt_calcul, bl_exclu
PROD  : …, dt_calcul, bl_exclu, bl_manuel (NOT NULL, sans DEFAULT), id_rh
```
La prod a deux colonnes en plus ; `bl_manuel` est obligatoire sans valeur par défaut, donc tout
`INSERT` qui ne la renseigne pas est rejeté en mode SQL strict.

---

## 4. Impact par route

> **Cause A = rayon de blast maximal.** `SELECT_SCENARIO_SQL` (qui liste `dt_real_prev`) est utilisée
> par `fetch_scenario_or_404()` — `app/routes/trppu_scenario/helpers.py:10` — appelée par quasiment
> **toutes** les routes de scénario et de sous-ressources. À elle seule, A fait tomber le domaine.

### 🔴 Tier 1 — cassent par leur propre SQL

| Route(s) | Cause | Emplacement |
|----------|-------|-------------|
| `GET/POST/PUT /trppu-api/scenarios` + `/{id}`, `/periodes`, `/statut`, `/mise-en-prod`, `/duplicate`, `/edition`, `/archive`, `/est-fige`, `/lb-scenario`, `/nb-jours-semaine` | A | `trppu_scenario/helpers.py:10-17` ; INSERT `routes.py:225` ; UPDATE `routes.py:325` |
| `GET /scenarios/{id}/neutralisations` | B | `trppu_neutralisations/helpers.py:6` |
| `POST /scenarios/{id}/neutralisations` | B | `trppu_neutralisations/routes.py:90,108,116` (`SELECT id`, `WHERE id`) |
| `PUT/PATCH /scenarios/{id}/tmh` + TMH via create/update scénario | C | `trppu_tmh/helpers.py:60-63` (INSERT sans `bl_manuel`) |
| `POST/PUT /scenarios/{id}/comptages` | D | `trppu_comptages/routes.py:80-83, 131-134` (écrit `id_rh`) |
| `PUT /scenarios/{id}/variations/{co_produit}` | E | `trppu_variations/routes.py:83, 91` (écrit `id_rh`/`dt_creation`) |

### 🔴 Tier 2 — cassent transitivement via `fetch_scenario_or_404` (A)

Ces routes ont (souvent) un SQL propre correct, mais valident d'abord l'existence du scénario via la
requête A, donc échouent malgré tout :

- `GET /scenarios/{id}/comptages` · `GET /scenarios/{id}/variations`
- `GET /scenarios/{id}/pic-coefficients` (module `trppu_scenario_pic`)
- `DELETE /scenarios/{id}/comptages|variations|neutralisations`
- `GET /scenarios/{id}/edition` (agrégateur)

> Remarque : le module `trppu_scenario_pic` a pourtant un SQL **correct** vis-à-vis de la prod
> (design `coef`/`densite`, cf. `trppu_scenario_pic/helpers.py:10` et `routes.py:160-173`). Il ne
> tombe qu'à cause de A.

### 🟠 Tier 3 — préexistant, indépendant de la prod (cas F)

`/trppu-api/pic-coefficients` (GET liste/`{id}`, POST, PUT, DELETE, `upload-excel`) écrit et lit
`coef_dense/coef_faible1/coef_faible2/dt_fin_effet` (`trppu_pic_coefficients/helpers.py:36-46` et
`routes.py:31-36, 174-176`). Ces colonnes n'existent **ni en local ni en prod** (la table utilise
`coef` + `densite` + `dt_fin`). **Ce module est donc déjà cassé en local** : ce n'est pas une
régression introduite par la prod, mais une incohérence à corriger indépendamment.

### 🟢 Routes saines sur prod

- `GET/POST/PUT /trppu-api/sites` — `trppu_site` identique.
- `GET/POST/PUT/DELETE /trppu-api/produits` — `trppu_produit` identique.
- `GET/POST/PUT/DELETE /trppu-api/pic-versions` — `trppu_pic_version` identique.
- Utilitaires : `health`, `databricks`, `logs`, `trafics` (Databricks, hors MySQL),
  `calcl_nbr_jours` (API externe jours fermés), `mysql_debug`.

---

## 5. Contraintes PROD plus strictes (nouveaux modes d'échec)

Le schéma local (`schema_trppu.sql`) ne déclare **aucune** clé étrangère, contrainte d'unicité ni
CHECK (uniquement les PK). La prod, elle, en ajoute. Même quand les colonnes correspondent, ces
contraintes peuvent faire échouer des écritures qui passaient en local :

- **FK `fk_tmh_produit`, `fk_picc_produit` → `trppu_produit(co_produit)`** : insérer un TMH ou un
  coefficient PIC avec un `co_produit` absent de `trppu_produit` déclenche une violation de FK en
  prod (silencieux en local).
- **CHECK `densite IN (0,1,2)` et `coef >= 0`** sur `trppu_pic_coefficients`.
- **UNIQUE `uq_picc (id_pic_version, co_produit, jour_semaine, densite)`** : la clé naturelle inclut
  `densite` côté prod.
- **FK `fk_neutre_scen`, `fk_tmh_scen` (ON DELETE CASCADE)** : sans effet négatif a priori, mais
  modifie le comportement de suppression.

---

## 6. Recommandations de réconciliation

> À décider selon la référence qui fait foi. **Non implémenté dans ce rapport.**

### Option 1 — Aligner le code (+ migrations) sur la PROD (si `dbV2.sql` fait foi)
- A : remplacer `dt_real_prev` par `dt_pivot` partout (`SELECT_SCENARIO_SQL`, INSERT/UPDATE scénario).
- B : lire/filtrer sur `id_neutralisation` (alias `AS id` si on veut garder le contrat de sortie).
- C : renseigner `bl_manuel` dans les INSERT TMH (+ éventuellement `id_rh`).
- D / E : retirer `id_rh` (et `dt_creation`) des écritures comptages/variations.
- F : réécrire le module `trppu_pic_coefficients` sur le design réel `coef` + `densite` + `dt_fin`.

### Option 2 — Faire évoluer la PROD pour matcher le LOCAL
- Nouvelle migration `db_migrations/003_align_prod.sql` : renommer `dt_pivot` → `dt_real_prev` et la
  PK `id_neutralisation` → `id`, rendre `bl_manuel` nullable / avec défaut, ajouter `id_rh` aux
  comptages et `dt_creation`/`id_rh` aux variations. ⚠️ Renommer une PK référencée par des FK exige
  de gérer les contraintes.

**Recommandation pragmatique** : traiter en priorité **A** (déblocage de ~tout le domaine scénario),
puis **B, C, D, E**, et enfin **F** (bug isolé). L'option 1 est généralement la moins risquée si la
prod est la base de vérité.

---

## 6 bis. Cas G — `nb_jours_*` en TINYINT (crash runtime, prod ET local)

Découvert à l'exécution (création de scénario) : `trppu_scenario.nb_jours_ouvres`,
`nb_jours_ouvrables` et `nb_jours_scenario` sont en **`TINYINT`** (max 127) **en prod comme
en local**, alors qu'ils comptent les jours sur **toute la période** du scénario. Avec la
période par défaut (today−1an / today+1an, jusqu'à 730 j), le nombre de jours ouvrés (~520)
dépasse 127 →

```
pymysql.err.DataError: (1264, "Out of range value for column 'nb_jours_ouvres' at row 1")
```

Ce n'est pas une divergence prod/local mais un **type trop petit** : impossible d'« aligner
le code » (la valeur est légitimement grande). **Correctif = migration** `db_migrations/003_widen_nb_jours.sql`
(TINYINT → SMALLINT pour les 3 colonnes ; `nb_jours_semaine` reste TINYINT). À appliquer
**sur la prod** (et intégrée à `run_migrations.py` pour le local).

## 7. Annexe — points mineurs

- **`lb_scenario` VARCHAR(20)** en local **et** en prod, alors que le code autorise jusqu'à 50
  caractères (`duplicate_scenario` tronque à 50 — `trppu_scenario/routes.py:817`). Un libellé de
  21–50 caractères est tronqué/rejeté sur les **deux** schémas : à corriger indépendamment si besoin.
- **`trppu_recalcul_log.raison`** : enum `'TOPIC_agrebal','MANUEL','SYSTEME'` identique local/prod
  (le `agrebal` minuscule est cohérent des deux côtés).
- **`trppu_api_log`** : colonne `regate` (et non `co_regate`) identique local/prod — pas d'impact.
- Tables **identiques** local/prod (hors index/FK) : `demande_dsr`, `trppu_agrebal_pdi`,
  `trppu_cles_repartition`, `trppu_pic_version`, `trppu_produit`, `trppu_site`,
  `trppu_scenario_exclusions`, `trppu_scenario_pic_coeffs`, `trppu_trafic_agrebal`,
  `trppu_trafic_pdi`, `trppu_pic_coefficients_ko`.
