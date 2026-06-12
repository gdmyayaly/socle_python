# Audit de concordance — Réalisation des tickets DSR ↔ schéma de base

**Référentiels** : `db_migrations/db_10_09_2026.sql` (dump prod, source de vérité) +
`db_migrations/004_add_variations_tracabilite.sql` (dernière migration) — recoupés avec
`db_analyse/schema_trppu.sql` (schéma généré, second référentiel) et le code applicatif.

**Périmètre** : ~20 tickets (DSR-613 → DSR-669 + briques socle). Date d'audit : 2026-06-10.

---

## 1. Synthèse

Le cœur fonctionnel (scénarios, neutralisations, TMH, variations, périodes, PIC scénario) est
**globalement concordant** avec le schéma de prod `db_10_09_2026.sql`. Mais l'audit révèle **deux
anomalies critiques** : un module CRUD (`trppu_pic_coefficients`) écrit contre des colonnes qui
n'existent pas, et la **chaîne de migrations 002/004 est en conflit** avec le schéma de prod (elle
échouerait à l'exécution).

| Sévérité | Nb | Constats |
| --- | --- | --- |
| 🔴 Critique | 2 | Module `trppu_pic_coefficients` désaligné ; migrations 002 & 004 en conflit avec le dump prod |
| 🟠 Manquement | 1 | DSR-644 : `id_rh` des comptages manuels non persisté côté code |
| 🟡 Vigilance | 3 | DSR-646 concordant *seulement si* migration appliquée ; tooling `run_migrations.py` ; mismatch nom de base |
| ✅ Concordant | 7+ | `trppu_scenario`, `trppu_neutralisations`, `trppu_tmh`, `trppu_pic_version`, `trppu_produit`, `trppu_site`, `trppu_scenario_pic` |

**Verdict** : la majorité des tickets est correctement réalisée vis-à-vis de la base, mais le module
`trppu_pic_coefficients` est **non fonctionnel en l'état** et la procédure de migration **doit être
corrigée** avant tout déploiement contre `db_10_09_2026`.

---

## 2. Méthodologie & périmètre

- **Comparaison colonnes** : chaque requête SQL du code (`INSERT`/`UPDATE`/`SELECT`/`UPSERT`) a été
  confrontée aux définitions `CREATE TABLE` des deux dumps.
- **Référentiels croisés** : `db_10_09_2026.sql` (prod, DB `dsr_mercure_aa`) **et**
  `db_analyse/schema_trppu.sql` (généré depuis JSON par `scripts/gen_schema_sql.py`, DB `trppu`).
  Les deux ont été utilisés pour distinguer un simple décalage de version d'un vrai défaut.
- **Migrations** : analyse de `001` → `004` + `README.md` + `scripts/run_migrations.py`.
- **Hors périmètre DB** : DSR-666 (Databricks) n'écrit aucune table MySQL — non audité ici sur la
  concordance schéma (ses 4 points data restent à confirmer, cf. sa résolution).

---

## 3. Concordance par table

| Table | Colonnes attendues (schéma prod) | Colonnes utilisées (code) | Fichiers code | Verdict |
| --- | --- | --- | --- | --- |
| `trppu_scenario` | `dt_pivot, id_rh_creation, id_rh_maj, est_fige, trafic_pdi_calcule, trafic_agrebal_calcule, periode_*, nb_jours_*` | identiques (lecture `dt_pivot AS dt_real_prev`) | `trppu_scenario/{helpers,routes,statuts}.py` | ✅ |
| `trppu_neutralisations` | `id_neutralisation, id_scenario, dt_debut, dt_fin, nb_jour, motif, dt_creation, id_rh` | identiques | `trppu_neutralisations/{helpers,routes}.py` | ✅ |
| `trppu_tmh` | `volume_realise, volume_previsionnel, moyenne_journaliere, moyenne_hebdo, dt_calcul, bl_exclu, bl_manuel, id_rh` | identiques | `trppu_tmh/helpers.py` | ✅ |
| `trppu_pic_version` | `lb_pic_version, niveau, co_regate, id_scenario, dt_activation, dt_desactivation, id_rh_creation, id_rh_maj, …` | identiques | `trppu_pic_version/*`, `trppu_scenario_pic/*` | ✅ |
| `trppu_produit` | `co_produit, lb_produit, dt_desactivation, motif_desactivation` | identiques | `trppu_produit/helpers.py` | ✅ |
| `trppu_site` | `co_regate, lb_regate, type_site, co_roc` | identiques | `trppu_scenario/helpers.py` (`ensure_site_exists`) | ✅ |
| `trppu_scenario_pic_coeffs` *(legacy)* | — | non utilisée (DELETE cascade seul) | `trppu_scenario/helpers.py` | ✅ |
| `trppu_scenario_pic` (lit `trppu_pic_coefficients`) | `id_pic_version, co_produit, jour_semaine(LUNDI…SAMEDI), densite, coef` | identiques | `trppu_scenario_pic/{helpers,routes,schemas}.py` | ✅ |
| `trppu_scenario_comptages_manuels` | `id_scenario, dt_comptage, co_produit, nb_produit` (+`id_rh` via migr. 002) | sans `id_rh` | `trppu_comptages/{helpers,routes}.py` | 🟠 |
| `trppu_scenario_variations_prev` | `id_scenario, co_produit, variation_pct` (+`dt_creation,id_rh` via migr.) | `+ dt_creation, id_rh` | `trppu_variations/{helpers,routes}.py` | 🟡 |
| **`trppu_pic_coefficients`** (module CRUD) | `dt_fin, coef, densite, id_rh` | **`dt_fin_effet, coef_dense, coef_faible1, coef_faible2, id_rh_creation`** | `trppu_pic_coefficients/{routes,helpers,schemas}.py` | 🔴 |

---

## 4. Concordance par ticket

| Ticket | Objet | Table(s) | Concordance | Manquement |
| --- | --- | --- | --- | --- |
| DSR-613 | nb jours ouvrés/ouvrables | (API jours-fermes) | ✅ | — |
| DSR-634 | Création scénario en base | `trppu_scenario`, `trppu_site`, `trppu_tmh` | ✅ | nécessite migr. 001/003 |
| DSR-644 | Écriture comptages manuels | `trppu_scenario_comptages_manuels` | 🟠 | `id_rh` non inséré (traçabilité perdue) |
| DSR-645 | Écriture neutralisations | `trppu_neutralisations` | ✅ | aligné `motif` (plus de `type`) |
| DSR-646 | Écriture variations prév. | `trppu_scenario_variations_prev` | 🟡 | OK **si** migr. 002 *ou* 004 appliquée (pas les deux) |
| DSR-648 | Sauvegarde TMH | `trppu_tmh`, `trppu_scenario` | ✅ | — |
| DSR-649 | MAJ ciblée trafic (bl_manuel=1) | `trppu_tmh` | ✅ | — |
| DSR-650 | Lecture TMH | `trppu_tmh` | ✅ | — |
| DSR-651 | Lecture variations | `trppu_scenario_variations_prev` | ✅ | — |
| DSR-652 / 665 | bl_manuel / bl_exclu TMH | `trppu_tmh` | ✅ | doublon assumé |
| DSR-653 | Lecture comptages | `trppu_scenario_comptages_manuels` | ✅ | — |
| DSR-654 | Orchestration édition scénario | (agrégation) | ✅ | — |
| DSR-655 | Lecture périodes | `trppu_scenario` | ✅ | — |
| DSR-656 | MAJ scénario | `trppu_scenario`, `trppu_tmh` | ✅ | — |
| DSR-659 | MAJ TMH recalculés | `trppu_tmh` | ✅ | — |
| DSR-660 | Lecture PIC (fusion) | `trppu_pic_version`, `trppu_pic_coefficients` | ✅ | via `trppu_scenario_pic` (correct) |
| DSR-661 | Écriture coef PIC scénario | `trppu_pic_version`, `trppu_pic_coefficients` | ✅ | via `trppu_scenario_pic` (correct) |
| DSR-666 | Trafics Databricks pivot | (Databricks, pas MySQL) | n/a | hors concordance DB ; 4 points data à confirmer |
| DSR-669 | Figement (est_fige) | `trppu_scenario` | ✅ | — |
| **— (sans ticket)** | **CRUD coef PIC nationaux + Excel** | **`trppu_pic_coefficients`** | 🔴 | **module désaligné — voir §5.1** |

---

## 5. Manquements & incohérences critiques

### 5.1 🔴 Module `trppu_pic_coefficients` (CRUD + upload Excel) entièrement désaligné

**Fichiers** : `app/routes/trppu_pic_coefficients/routes.py`, `helpers.py`, `schemas.py`
(monté dans `app/main.py:172`).

Le module lit et écrit la table **`trppu_pic_coefficients`** avec un jeu de colonnes qui **n'existe
pas** sur cette table, dans **les deux** référentiels (`db_10_09_2026.sql` et `schema_trppu.sql`) :

| Colonne utilisée par le code | Existe sur `trppu_pic_coefficients` ? | Réalité schéma |
| --- | --- | --- |
| `dt_fin_effet` | ❌ | la colonne est `dt_fin` |
| `coef_dense`, `coef_faible1`, `coef_faible2` | ❌ | la colonne est `coef` (+ `densite`) |
| `id_rh_creation` | ❌ | la colonne est `id_rh` |
| `jour_semaine` = `LUN/MAR/MER/JEU/VEN/SAM` | ❌ | ENUM `LUNDI,MARDI,MERCREDI,JEUDI,VENDREDI,SAMEDI` |

- Les colonnes `coef_dense/faible1/faible2` appartiennent en réalité à la table **`trppu_pic_coefficients_ko`**
  — mais même là, la date est `dt_fin` (pas `dt_fin_effet`) et la traçabilité est `id_rh` (pas `id_rh_creation`).
  Le module mélange donc deux tables et invente des noms de colonnes.
- Le module **n'écrit jamais `densite`**, alors que la clé unique `uq_picc` =
  `(id_pic_version, co_produit, jour_semaine, densite)` l'inclut → l'`ON DUPLICATE KEY UPDATE`
  (`helpers.py:UPSERT_SQL`) ne déclenche pas l'upsert attendu.

**Points de code** :
- `helpers.py:36-46` `UPSERT_SQL` (Excel) et `pic_coef_to_upsert_params` → `dt_fin_effet, coef_dense, coef_faible1, coef_faible2`.
- `routes.py:32-35` `SELECT … dt_fin_effet, coef_dense, coef_faible1, coef_faible2, … id_rh_creation`.
- `routes.py:174-176` `INSERT INTO trppu_pic_coefficients (… coef_dense, coef_faible1, coef_faible2)`.
- `routes.py:276` `UPDATE …`, `routes.py:333` soft-delete `SET dt_fin_effet = …`.
- `schemas.py:11-17` `JourSemaineEnum = LUN…SAM` ; `schemas.py:26-28,69` `coef_dense/faible*`, `id_rh_creation`.

**Impact runtime** : **toutes** les opérations (POST, PUT, DELETE/soft-delete, GET liste, upload Excel)
lèvent une erreur SQL « Unknown column » contre la base réelle. Les endpoints sont exposés mais cassés.

> ⚠️ Ce module ne correspond à **aucun ticket DSR résolu**. La gestion des coefficients PIC d'un
> scénario (DSR-660/661) est assurée par le module **`trppu_scenario_pic`**, qui lui est **correct**
> (`coef`, `densite`, enum `LUNDI…SAMEDI`). Le module `trppu_pic_coefficients` semble être une brique
> de gestion des coefficients **nationaux** restée sur un ancien modèle de données.

### 5.2 🔴 Migrations 002 & 004 en conflit avec `db_10_09_2026.sql`

**`002_add_param_columns.sql`** suppose un schéma `trppu_neutralisations` **antérieur** :
```sql
ALTER TABLE trppu_neutralisations
  MODIFY COLUMN type ENUM('FERIE','PEAK','SAISON') NOT NULL,
  ADD COLUMN dt_creation … , ADD COLUMN id_rh … ;
```
Or dans `db_10_09_2026.sql`, `trppu_neutralisations` :
- **n'a plus de colonne `type`** (remplacée par `motif varchar(255)`, cf. `impact_migration_db_09_08_2026.md`),
- possède **déjà** `dt_creation` et `id_rh`.

→ La migration 002 **échoue** contre le dump prod : `Unknown column 'type'` puis `Duplicate column 'dt_creation'`.

**Double-ajout 002 / 004** : les deux migrations ajoutent `dt_creation` + `id_rh` à
`trppu_scenario_variations_prev`.
- `002_add_param_columns.sql:12-14`
- `004_add_variations_tracabilite.sql:11-13`

→ Exécuter la chaîne complète provoque `Duplicate column` sur 004. Le `README.md` des migrations ne
liste que **001-003** ; 004 a été ajoutée après coup, **redondante** avec la partie variations de 002.

### 5.3 🟠 DSR-644 — `id_rh` des comptages manuels non persisté

- `db_10_09_2026.sql` `trppu_scenario_comptages_manuels` **n'a pas** de colonne `id_rh`
  (la migration 002 l'ajoute).
- Le code (`trppu_comptages/{helpers,routes}.py`) **n'insère ni ne lit `id_rh`**.

→ La traçabilité utilisateur annoncée par la résolution DSR-644 (« `id_rh` crypté ») n'est **pas
réalisée côté code**. Soit la colonne est inutile (migration 002 à corriger), soit le code doit
écrire `id_rh` — **à arbitrer**.

---

## 6. Migrations — état & conflits

| # | Fichier | Cible | Statut vs `db_10_09_2026` |
| --- | --- | --- | --- |
| 001 | `001_widen_id_rh_columns.sql` | `trppu` | ✅ cohérent (élargit `id_rh*` → VARCHAR(255)) |
| 002 | `002_add_param_columns.sql` | `trppu` | 🔴 conflit : `type` inexistant + `dt_creation/id_rh` déjà présents sur neutralisations ; double-ajout variations |
| 003 | `003_widen_nb_jours.sql` | `trppu` | ✅ cohérent (TINYINT → SMALLINT) |
| 004 | `004_add_variations_tracabilite.sql` | `trppu` | 🔴 redondant avec 002 (mêmes colonnes variations) ; absent du `README.md` |

**Autres points** :
- **Nom de base incohérent** : les migrations et `run_migrations.py` ciblent `trppu` ; les dumps
  (`db_09_08_2026.sql`, `db_10_09_2026.sql`) créent `dsr_mercure_aa`. Deux lignées de schéma coexistent.
- **Pas de suivi de migrations** : `scripts/run_migrations.py` ne tient aucune table
  `schema_migrations` ; il rejoue une liste codée en dur (reconstruction destructive) et dépend de
  `db_analyse/schema_trppu.sql` (hors `db_migrations/`).
- **Source de vérité ambiguë** : `schema_trppu.sql` est *généré* (depuis JSON) ; les dumps sont des
  *snapshots prod*. Sur les tables auditées, les deux **coïncident** pour `trppu_pic_coefficients`
  (`coef/densite/dt_fin/id_rh`) — le défaut §5.1 est donc réel quel que soit le référentiel.

---

## 7. Recommandations (priorisées)

> Cette étape est un **audit** : aucune correction de code n'est appliquée ici. Actions proposées :

1. **🔴 Réécrire le module `trppu_pic_coefficients`** sur le vrai modèle de la table :
   colonnes `coef`, `densite`, `dt_fin`, `id_rh` ; enum `jour_semaine` = `LUNDI…SAMEDI` ;
   intégrer `densite` dans l'upsert (clé `uq_picc`). *Alternative* : si la cible voulue est bien la
   table `trppu_pic_coefficients_ko` (modèle dense/faible1/faible2), pointer explicitement dessus et
   corriger `dt_fin_effet`→`dt_fin`, `id_rh_creation`→`id_rh`. **À arbitrer** : quelle table ce module
   doit-il réellement gérer ?
2. **🔴 Corriger la chaîne de migrations** :
   - retirer de **002** le bloc `trppu_neutralisations` (obsolète : `type` n'existe plus, `dt_creation/id_rh`
     déjà en base) ;
   - dé-dupliquer l'ajout `dt_creation/id_rh` sur `trppu_scenario_variations_prev` (le garder dans **un seul**
     fichier, idéalement 004) ;
   - mettre le `README.md` à jour pour lister 004 et l'ordre réel.
3. **🟠 DSR-644** : décider si `id_rh` doit être persisté pour les comptages → soit écrire `id_rh`
   dans `trppu_comptages`, soit supprimer la colonne/ligne de migration correspondante.
4. **🟡 Aligner le nom de base** entre dumps (`dsr_mercure_aa`) et migrations/tooling (`trppu`).
5. **🟡 Introduire un suivi de migrations** (table `schema_migrations` + application idempotente) pour
   éviter les rejeux destructifs et les doubles-ajouts.

---

## 8. Vérification de l'audit

- Chaque constat cite un fichier + une (table/colonne) traçable aux deux dumps.
- Recoupement : `grep` des colonnes incriminées dans le schéma cible →
  `coef_dense` / `dt_fin_effet` / `id_rh_creation` **absents** de `trppu_pic_coefficients` ;
  `type` **absent** de `trppu_neutralisations` (db_10_09_2026).
- Optionnel : `python scripts/run_migrations.py --dry-run` pour confirmer l'ordre des migrations jouées.
