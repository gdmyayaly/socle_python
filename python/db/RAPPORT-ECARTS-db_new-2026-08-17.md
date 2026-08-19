# Rapport d'écarts — `app/routes/` ↔ schéma `db/db_new.sql`

> **Schéma analysé :** `python/db/db_new.sql` — dump du schéma `dsr_mercure_aa`, **25 tables**,
> re-généré le 17/08/2026.
> **Code analysé :** `python/app/routes/` (17 packages `trppu_*` + modules plats), `app/services/`,
> `app/db/`, `scripts/`.
> **Date :** 2026-08-17.
> **Remplace** `RAPPORT-ECARTS-db_new.md` (23/07/2026, schéma à 24 tables), supprimé.

Le schéma est considéré comme **source de vérité** : il s'agit d'un dump de production, et il sert
de fixture à `tests/test_duplicate_scenario.py`. Toutes les corrections proposées sont donc côté
Python.

---

## 1. Synthèse

| # | Sévérité | Écart | Impact |
|---|----------|-------|--------|
| 1 | 🔴 P0 | `SELECT_VARIATIONS_SQL` appelé avec 1 paramètre au lieu de 2 depuis `/edition` | `GET /scenarios/{id}/edition` → **500 systématique** |
| 2 | 🔴 P0 | Statut `SIMULATION` présent en base, absent du `Literal` de l'API | GET scénarios → **500** dès qu'un scénario est en simulation |
| 3 | 🔴 P0 | `NiveauEnum` sans `SCENARIO`, alors que le code insère ce niveau | `GET /pic-versions` → **500** dès qu'une surcharge PIC existe |
| 4 | 🟠 P1 | `chk_tmh_volumes` interdit `volume_realise < 0`, l'API l'autorise | POST/PUT/PATCH TMH → MySQL 3819 → **500** |
| 5 | 🟠 P1 | `chk_var_borne` borne `variation_pct` à ±100, l'API accepte ±999,99 | PUT variation → MySQL 3819 → **500** |
| 6 | 🟠 P1 | `trppu_site.lb_regate` `varchar(40) NOT NULL` vs API nullable/120 | POST /sites & POST /scenarios → 1048/1406 → **500** |
| 7 | 🟠 P1 | FK `co_produit` / `id_pic_version` non pré-validées sur les coefficients PIC et les variations | MySQL 1452 → **500** au lieu d'un 422 explicite |
| 8 | 🟠 P1 | Bornes `co_produit` hétérogènes (base 3, API 2 ou 3 selon le module) | Produit à 3 caractères rejeté en **422** sur 4 routes |
| 9 | 🟡 P2 | `id_referentiel` / `id_version_cle` / `Calcul_trafic_en_cours` jamais lus ni écrits | Scénarios orphelins de référentiel ; **perte au clonage** |
| 10 | 🟡 P2 | Hard-delete scénario exposé aux FK `trppu_api_log` / `trppu_recalcul_log` | 1451 → **500** (risque latent : tables vides) |
| 11 | 🟡 P2 | `id_rh` non persisté sur les modules CRUD PIC ; exigé mais inexistant sur les comptages | Traçabilité absente de l'audit ; champ d'API trompeur |
| 12 | 🟡 P2 | `PicVersionOut.id_scenario` toujours `0` (colonne non sélectionnée) | Réponse API trompeuse pour les versions SCENARIO |
| 13-16 | 🟢 P3 | Vigilance : clé naturelle des comptages, résolveurs PIC divergents, soft-delete PIC, 12 tables non couvertes | Pas de correction de code |

**Verdict** : le cœur métier (scénarios, TMH, neutralisations, variations, PIC scénario) est aligné
sur le schéma, colonne par colonne. Mais **trois défauts provoquent des 500 en l'état**, dont un
totalement indépendant du schéma (écart n°1, endpoint d'édition cassé), et **cinq contraintes de la
base ne sont pas répercutées dans les schémas Pydantic**, ce qui transforme des saisies légitimes du
point de vue de l'API en erreurs 500.

---

## 2. Delta depuis le rapport du 23/07

### 2.1 Le schéma a changé

`git diff e72abf0 -- python/db/db_new.sql`, plus le diff du working tree (le dump du jour n'est pas
encore commité) :

| Changement | Impact code |
|---|---|
| `trppu_site_trafic` **renommée** `trppu_trafic_site` (`db_new.sql:448`) | aucun — table jamais référencée |
| **Nouvelle table** `trppu_suivi_batch` (`db_new.sql:368`) | aucun — jamais référencée |
| `trppu_tmh` **+** `volume_brut` (`db_new.sql:397`) | ✅ géré (DSR-689) |
| `trppu_trafic_agrebal` **+** `agrebal_uuid` NOT NULL (`:414`), `jour_semaine` `LUN…` → `LUNDI…` (`:416`) | ✅ duplication à jour |
| `trppu_trafic_pdi` **+** `agrebal_uuid` NOT NULL (`:432`), **−** `id_calcul_batch` | ✅ duplication à jour |
| `trppu_version_cle` **+** `co_regate`, `date_creation` → `date_debut_validite`/`date_fin_validite` | aucun — jamais référencée |
| `trppu_referentiel` **+** `co_regate` (`:252`) | aucun — jamais référencée |
| `trppu_recalcul_log` **−** `id_agrebal`, `raison` → `enum('AGREBAL','CLE_REPARTITION','MANUEL','INITIAL')` (`:241`) | aucun — jamais alimentée |
| `trppu_cles_repartition_calcule.co_regate_site` `varchar(10)` → `char(6)` | aucun |

Aucun de ces changements ne casse le code : les colonnes `NOT NULL` ajoutées sur les tables trafic
(`agrebal_uuid`) sont déjà copiées par la duplication de scénario
(`trppu_scenario/helpers.py:219` et `:232`), et les tables nouvelles ou renommées ne sont référencées
nulle part.

### 2.2 Écarts du rapport précédent désormais fermés

- **`volume_previsionnel_recalcule` ignorée** → corrigé : lue par `_TMH_COLS`
  (`trppu_tmh/helpers.py:17-21`) et copiée à la duplication (`trppu_scenario/helpers.py:182`).
  La nouvelle colonne `volume_brut` est en plus calculée serveur et persistée
  (`compute_volume_brut`, `trppu_tmh/helpers.py:45`).
- **FK produit non anticipée sur le TMH** → corrigé pour le TMH et la création de scénario :
  `ensure_produits_exist` (`trppu_produit/helpers.py:53`) crée le produit manquant avant l'INSERT
  (`trppu_tmh/routes.py:88`, `:139` ; `trppu_scenario/routes.py:281`, `:384`). **Reste ouvert** pour
  les coefficients PIC et les variations : cf. écart n°7.

### 2.3 Rapport à ne plus suivre

`api_docs/dsr/audit_concordance_db_10_09_2026.md` §5.1 déclare le module `trppu_pic_coefficients`
« non fonctionnel », écrivant contre des colonnes inexistantes (`dt_fin_effet`, `coef_dense`,
`coef_faible1`, `coef_faible2`, `id_rh_creation`, jours `LUN…SAM`). **C'est périmé** : le module est
aujourd'hui aligné sur le vrai modèle de la table — `coef` + `densite` (0/1/2), `dt_effet`/`dt_fin`,
`id_rh`, `jour_semaine` en `LUNDI…SAMEDI` (`trppu_pic_coefficients/helpers.py:38-46`,
`routes.py:31-36`, `schemas.py:17-33`). Les colonnes `coef_dense/faible*` n'existent plus que sur
`trppu_pic_coefficients_ko` (jamais lue) et `trppu_scenario_pic_coeffs` (uniquement dupliquée).

---

## 3. Écarts P0 — 500 garantis

### 3.1 🔴 Écart n°1 — `GET /scenarios/{id}/edition` est cassé

**Ce n'est pas un écart de schéma, mais un défaut d'appel**, découvert en confrontant les requêtes à
leurs paramètres.

`SELECT_VARIATIONS_SQL` (`app/routes/trppu_variations/helpers.py:11-21`) contient **deux**
placeholders — un pour la sous-requête TMH, un pour la jointure des variations ; sa docstring le dit
explicitement (« Deux paramètres id_scenario attendus ») :

```sql
SELECT t.co_produit, COALESCE(v.variation_pct, 0) AS variation_pct
FROM (SELECT co_produit FROM trppu_tmh WHERE id_scenario = %s AND bl_exclu = 0
      GROUP BY co_produit) AS t
LEFT JOIN trppu_scenario_variations_prev v
       ON v.co_produit = t.co_produit AND v.id_scenario = %s
ORDER BY t.co_produit
```

| Appelant | Paramètres | Verdict |
|---|---|---|
| `app/routes/trppu_variations/routes.py:42` | `(id_scenario, id_scenario)` | ✅ |
| `app/routes/trppu_scenario/routes.py:446` | `(id_scenario,)` | ❌ |

**Erreur** : le driver interpole `query % args` → `TypeError: not enough arguments for format
string`, capturé par le `try/except` de l'endpoint → **500** sur `GET /trppu-api/scenarios/{id}/edition`,
l'orchestration d'édition du scénario (DSR-654).

**Correction** : passer `(id_scenario, id_scenario)` en `trppu_scenario/routes.py:446`.

### 3.2 🔴 Écart n°2 — statut `SIMULATION` inconnu de l'API

**Base** (`db_new.sql:265`) :

```sql
`statut` enum('EN COURS','SIMULATION','VALIDE','EN PRODUCTION','ARCHIVE') NOT NULL
```

**Code** :

- `trppu_scenario/statuts.py:8` → `STATUTS = ("EN COURS", "VALIDE", "EN PRODUCTION", "ARCHIVE")`
- `trppu_scenario/schemas.py:12` → `Statut = Literal["EN COURS", "VALIDE", "EN PRODUCTION", "ARCHIVE"]`,
  utilisé par `ScenarioOut.statut` (`:105`) et `StatutUpdate.statut` (`:191`)
- `statuts.py:46-51` → `ALLOWED_TRANSITIONS` sans entrée `SIMULATION`
- `trppu_scenario/routes.py:330` → `if scenario["statut"] != "EN COURS": 409`

**Erreurs** :

1. **500 en lecture.** Un scénario passé en `SIMULATION` (batch, script SQL, autre application du SI)
   fait échouer la validation du `response_model` sur `GET /scenarios`, `GET /scenarios/{id}`,
   `/periodes`, `/edition` → `ResponseValidationError` → 500. Le module OPTIPACC contourne déjà le
   problème avec un SELECT dédié qui ne passe pas par `ScenarioOut`
   (`trppu_optipacc/helpers.py:46-52`, commentaire explicite).
2. **État verrouillé.** `PATCH /statut` vers `SIMULATION` → 422 ; depuis `SIMULATION`, aucune
   transition (`ALLOWED_TRANSITIONS.get("SIMULATION")` = ∅).
3. **MAJ scénario refusée.** `routes.py:330` renvoie 409 pour un scénario en simulation.
4. **Incohérence interne.** `FIGE_PAR_STATUT` (`statuts.py:13-17`, DSR-669) connaît déjà
   `"SIMULATION"` (→ `est_fige=True`) ; le reste du module l'ignore.
5. `GET /scenarios/enums` n'expose pas la valeur.

**Correction retenue** : `SIMULATION` se comporte comme `EN COURS` dans la machine à états —
ajout au `Literal`, à `STATUTS`, transitions `{"VALIDE", "ARCHIVE"}` identiques à `EN COURS` (et
`SIMULATION` accessible depuis `EN COURS`/`VALIDE`), et `routes.py:330` qui accepte les deux
statuts.

> **Point à arbitrer** : `FIGE_PAR_STATUT` mappe le libellé IHM « simulation » vers `est_fige=True`
> (DSR-669), alors que « en cours » vaut `False`. L'équivalence porte sur le `statut` ; le flag
> `est_fige` reste indépendant et continue de bloquer l'édition via `assert_editable`
> (`trppu_scenario/helpers.py:127`). À confirmer avant correction.

### 3.3 🔴 Écart n°3 — `niveau = 'SCENARIO'` écrit, mais absent de `NiveauEnum`

**Base** (`db_new.sql:206`) : `niveau enum('NATIONAL','DEX','SITE','SCENARIO') NOT NULL`

**Code** : `NiveauEnum` (`trppu_pic_version/schemas.py:11-15`) ne déclare que `NATIONAL`, `DEX`,
`SITE`. Or le niveau `SCENARIO` est **écrit en dur** par deux modules :

- `trppu_scenario_pic/routes.py:136` — `INSERT INTO trppu_pic_version (…) VALUES (%s, 'SCENARIO', …)`
  (surcharge d'un coefficient PIC, DSR-661)
- `trppu_scenario/helpers.py:296` — même INSERT lors de la duplication d'un scénario

et relu par `trppu_scenario_pic/helpers.py:40` et `routes.py:108`.

**Erreurs** : dès qu'une surcharge PIC existe, `PicVersionOut.niveau: NiveauEnum` échoue →
`GET /trppu-api/pic-versions` (listing complet) et `GET /pic-versions/{id}` → **500** ; la version
est aussi impossible à modifier/désactiver. `GET /pic-versions/enums` (`routes.py:100`) n'expose pas
la valeur.

**Correction** : ajouter `SCENARIO` à `NiveauEnum` (indispensable en lecture), et garder le POST
fermé à ce niveau via un enum de création distinct — les versions SCENARIO sont créées par
`trppu_scenario_pic`, pas par le CRUD.

---

## 4. Écarts P1 — la base refuse ce que l'API accepte

### 4.1 🟠 Écart n°4 — `chk_tmh_volumes` vs volumes négatifs

**Base** (`db_new.sql:404`) :

```sql
CONSTRAINT `chk_tmh_volumes` CHECK ((((`volume_realise` is null) or (`volume_realise` >= 0))
  and ((`volume_previsionnel` is null) or (`volume_previsionnel` >= 0))))
```

**Code** — le négatif est autorisé *volontairement*, commentaire à l'appui :

- `trppu_tmh/schemas.py:45` → `volume_realise: int | None = None  # valeurs négatives autorisées`
- `trppu_tmh/schemas.py:106` → `volume_realise: int  # valeurs négatives autorisées` (PATCH DSR-649)

`volume_previsionnel` et `volume_previsionnel_recalcule` sont, eux, bien bornés (`ge=0`, `:46-49`).

**Erreur** : un constaté négatif passe la validation Pydantic puis viole la contrainte → MySQL
**3819 (Check constraint violated)** → 500 sur `POST /tmh`, `PUT /tmh` (lot) et
`PATCH /tmh/{id}/volume`.

**Correction** : `ge=0` sur les deux champs, et retirer les commentaires devenus faux. À noter :
`tests/test_tmh_volume_brut.py:112` (`test_volume_brut_accepte_un_constate_negatif`) teste
l'arithmétique de `compute_volume_brut`, pas la validation d'entrée — il reste valide, mais mérite
un commentaire précisant que l'API n'accepte plus de négatif.

### 4.2 🟠 Écart n°5 — `chk_var_borne` vs `variation_pct`

**Base** (`db_new.sql:343` et `:351`) : `variation_pct decimal(6,2) NOT NULL` +
`CHECK (variation_pct between -100 and 100)`.

**Code** (`trppu_variations/schemas.py:14`) :

```python
variation_pct: Decimal = Field(..., max_digits=5, decimal_places=2)
```

`max_digits=5, decimal_places=2` autorise jusqu'à **±999,99** : aucune borne à ±100.

**Erreur** : `PUT /scenarios/{id}/variations/{co_produit}` avec 150 % → 3819 → 500.

**Correction** : `ge=-100, le=100`.

### 4.3 🟠 Écart n°6 — `trppu_site.lb_regate`

**Base** (`db_new.sql:358`) : `lb_regate varchar(40) NOT NULL`

**Code** :

- `trppu_site/schemas.py:12` → `lb_regate: str | None = Field(None, max_length=120)` — **nullable**
  et jusqu'à 120 caractères
- `trppu_scenario/schemas.py` (`ScenarioCreate.lb_regate`) → `max_length=120`

**Erreurs** :

- `POST /sites` sans `lb_regate` → INSERT `NULL` sur colonne NOT NULL → MySQL **1048** → 500
- libellé de 41 à 120 caractères → MySQL **1406 (Data too long)** en mode strict → 500, y compris
  pendant `POST /scenarios` via `ensure_site_exists` (`trppu_scenario/helpers.py:316-338`), ce qui
  annule toute la création du scénario

**Correction** : `max_length=40` et champ obligatoire côté site ; `max_length=40` aussi sur
`ScenarioCreate.lb_regate`.

### 4.4 🟠 Écart n°7 — FK non pré-validées : 1452 masqué en 500

Le schéma impose trois FK `ON DELETE RESTRICT` vers `trppu_produit` et une FK `CASCADE` vers
`trppu_pic_version` (`db_new.sql:175-176`, `:349-350`, `:402-403`). Les points d'écriture suivants
n'effectuent **aucun contrôle d'existence** :

| Point d'entrée | Fichier | FK non vérifiée |
|---|---|---|
| `POST /pic-coefficients` | `trppu_pic_coefficients/routes.py:134-187` | `fk_picc_produit`, `fk_picc_version` |
| Upload Excel coefficients PIC | `trppu_pic_coefficients/helpers.py:38-46` (`UPSERT_SQL`) | idem, sur chaque ligne |
| `PUT /scenarios/{id}/pic-coefficients` | `trppu_scenario_pic/routes.py:173-176` | `fk_picc_produit` |
| `PUT /scenarios/{id}/variations/{co_produit}` | `trppu_variations/routes.py:97-102` | `fk_var_produit` |

Le POST coefficient ne valide que la **clé naturelle** `uq_picc` (`routes.py:145-155`, → 409) ; toute
autre erreur tombe dans le `except Exception` générique et devient un 500 opaque
(« Erreur création coefficient. », `routes.py:187`). L'upload Excel étant transactionnel, une seule
ligne au produit inconnu fait échouer **tout le lot**.

**Correction** : réutiliser `ensure_produits_exist` (`trppu_produit/helpers.py:53`) — déjà employé
par le TMH — ou un `SELECT co_produit` renvoyant un **422** explicite ; plus un contrôle d'existence
de `id_pic_version` avant l'INSERT de coefficient.

### 4.5 🟠 Écart n°8 — bornes `co_produit` hétérogènes

**Base** : `co_produit varchar(3)` partout (`trppu_produit:227`, `trppu_pic_coefficients:163`,
`trppu_tmh:386`, `trppu_scenario_comptages_manuels:303`, `trppu_scenario_exclusions:315`,
`trppu_scenario_pic_coeffs:327`, `trppu_scenario_variations_prev:342`, `trppu_trafic_*`).

| Module | Contrainte | Verdict |
|---|---|---|
| `trppu_produit/schemas.py:11` | 2 à **3** | ✅ |
| `trppu_tmh/schemas.py:44` | 1 à **3** | ✅ |
| `trppu_pic_coefficients/schemas.py:28`, `:59` | exactement **2** (+ filtre `routes.py:42`, `zfill(2)` Excel) | ❌ |
| `trppu_scenario_pic/schemas.py:37` | 1 à **2** | ❌ |
| `trppu_scenario/schemas.py:32` (TMH de création) | 1 à **2** | ❌ |
| `trppu_comptages/schemas.py:16` | 1 à **2** | ❌ |

**Erreur fonctionnelle** : un produit à 3 caractères, légitime (créé par `POST /produits`, accepté
par la base et par le TMH direct), est rejeté en **422** sur le coefficient PIC, la surcharge PIC de
scénario, le comptage manuel et le TMH passé dans `POST /scenarios`. Croisé avec l'écart n°7 : la FK
ne pourrait de toute façon jamais être satisfaite sur ces routes.

**Correction** : harmoniser sur **1..3** (`pattern ^[A-Za-z0-9]{1,3}$`).

---

## 5. Écarts P2 — pertes de données et incohérences silencieuses

### 5.1 🟡 Écart n°9 — `id_referentiel`, `id_version_cle`, `Calcul_trafic_en_cours` ignorés

**Base** (`db_new.sql:289-291`) :

```sql
`Calcul_trafic_en_cours` smallint DEFAULT '0',
`id_referentiel` int NOT NULL DEFAULT '0',
`id_version_cle` int NOT NULL DEFAULT '0',
```

**Code** : ces trois colonnes n'apparaissent **nulle part** dans `app/` (grep négatif).

- `SELECT_SCENARIO_SQL` (`trppu_scenario/helpers.py:10-19`) ne les sélectionne pas → jamais exposées
  (ni `ScenarioOut`, ni `/edition`)
- l'INSERT de création (`trppu_scenario/routes.py:242-272`) ne les renseigne pas → tout scénario naît
  à `0`, valeur qui ne correspond à aucune ligne de `trppu_referentiel` / `trppu_version_cle`
  (pas de FK, donc pas d'erreur SQL — juste une incohérence référentielle silencieuse)
- l'INSERT…SELECT de duplication (`trppu_scenario/routes.py:921-943`) ne les copie pas → **le clone
  perd le rattachement référentiel de sa source** et retombe à `0`, alors que
  `trafic_pdi_calcule` / `trafic_agrebal_calcule` sont, eux, bien recopiés

**Conséquence** : tout traitement aval joignant `trppu_scenario.id_referentiel` /
`id_version_cle` vers `trppu_cles_repartition*` / `trppu_trafic_site` (structure visiblement prévue
pour cela) travaillera sur des scénarios orphelins.

**Correction** : exposer les deux identifiants en lecture et les copier à la duplication. Leur
alimentation à la création relève d'un arbitrage métier (quel référentiel par défaut ?), hors
périmètre de cet alignement.

### 5.2 🟡 Écart n°10 — hard-delete de scénario exposé aux FK des tables de logs

**Base** (`db_new.sql:95` et `:245`) — deux FK **sans** `ON DELETE CASCADE` (donc RESTRICT) :

```sql
CONSTRAINT `trppu_api_log_ibfk_1`      FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
CONSTRAINT `trppu_recalcul_log_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
```

**Code** : `SCENARIO_CHILD_TABLES` (`trppu_scenario/helpers.py:141-151`) exclut *volontairement* ces
deux tables « pour préserver la traçabilité » (commentaire `:139-140`), puis
`delete_scenario_cascade` (`:154-163`) supprime le scénario parent.

**Erreur** : dès qu'une ligne de log référence le scénario, `DELETE /trppu-api/scenarios/{id}` échoue
en MySQL **1451 (Cannot delete or update a parent row)** → 500. La traçabilité voulue est
incompatible avec le hard-delete.

**Risque réel : latent.** `db/count.json` donne 0 ligne pour les deux tables, et **aucune route
n'écrit dedans** — l'API ne les alimente jamais, malgré leur vocation manifeste. Le défaut ne se
déclenchera que si un autre composant commence à les remplir.

**Correction** : détacher `trppu_api_log.id_scenario = NULL` (colonne nullable) et supprimer les
lignes `trppu_recalcul_log` (colonne NOT NULL, pas de détachement possible), en assumant la perte de
traçabilité par un commentaire.

### 5.3 🟡 Écart n°11 — `id_rh` : colonnes non alimentées d'un côté, champ fantôme de l'autre

| Table | Colonne base | Écrite par le code ? |
|---|---|---|
| `trppu_pic_coefficients` | `id_rh varchar(255)` (`:171`) | **Non** par le CRUD (`helpers.py:38-46` UPSERT, `routes.py:173-177` INSERT, `routes.py:281` UPDATE) ; oui par `trppu_scenario_pic` (`routes.py:123`, `:175`) |
| `trppu_pic_version` | `id_rh_creation`, `id_rh_maj` (`:216-217`) | **Non** par le CRUD (`helpers.py:27-32`, UPDATE dynamique `routes.py:212`) ; oui par `trppu_scenario_pic/routes.py:134-136` |
| `trppu_scenario_comptages_manuels` | **aucune colonne `id_rh`** | Le code n'écrit rien — mais `ComptageCreate.id_rh` et `ComptageUpdate.id_rh` sont **obligatoires** (`trppu_comptages/schemas.py:19`, `:29`) |

**Conséquences** : le module d'audit (`trppu_audit/helpers.py`, qui balaie `id_rh` sur 6 tables) ne
voit aucune création/modification de coefficient ou de version PIC faite via le CRUD. Et l'API des
comptages réclame un `id_rh` qu'elle jette silencieusement (traçabilité annoncée par DSR-644, non
réalisée : la colonne n'existe pas dans ce schéma).

**Correction** : écrire `id_rh` / `id_rh_creation` / `id_rh_maj` dans les modules CRUD PIC ; pour les
comptages, arbitrer entre retirer le champ des schémas et le documenter comme non persisté.

### 5.4 🟡 Écart n°12 — `PicVersionOut.id_scenario` toujours `0`

`SELECT_PICV_SQL` (`trppu_pic_version/routes.py:31-36`) sélectionne 13 colonnes mais **pas**
`id_scenario` ; le champ Pydantic retombe sur son défaut `0` (`schemas.py:25`). Les consommateurs de
`GET /pic-versions` ne peuvent donc pas savoir à quel scénario une version est rattachée — point qui
deviendra visible dès que l'écart n°3 sera corrigé et que les versions `SCENARIO` apparaîtront dans
le listing.

---

## 6. P3 — vigilance, sans correction de code

13. **Clé naturelle des comptages manuels.** L'API traite `(id_scenario, co_produit)` comme clé
    (`trppu_comptages/helpers.py:12-16`, UPDATE `routes.py:127-132`, DELETE `routes.py:165-169`),
    alors que l'index base est `idx_scm (id_scenario, dt_comptage, co_produit)` (`db_new.sql:306`)
    et qu'**aucune contrainte UNIQUE** n'existe. Deux conséquences : un seul comptage par produit est
    représentable côté API (le schéma suggère un comptage par date), et si des doublons existent
    (autre composant, concurrence), les UPDATE/DELETE portent sur toutes les lignes du produit.
14. **Deux résolveurs de version PIC par défaut divergents** :
    `trppu_scenario/helpers.py:61-83` (tout `est_par_defaut=1`, repli `id=1`, sinon 422) contre
    `trppu_scenario_pic/helpers.py:19-23` (`niveau='NATIONAL'` **et** `est_par_defaut=1`, repli
    silencieux `DEFAULT_PIC_VERSION = 1`). Selon la route, un même scénario peut se voir attribuer un
    défaut différent.
15. **Soft-delete d'un coefficient PIC** : `dt_fin = date.today()` (`trppu_pic_coefficients/routes.py:340`)
    sur une colonne `datetime` → `00:00`, alors que le filtre `actif_only` est `dt_fin > NOW()`
    (`routes.py:71`) : le coefficient est inactif le jour même de sa clôture. Comportement à
    confirmer.
16. **12 tables sans aucune couverture API** (cf. §7).

---

## 7. Couverture du schéma par l'API

**9 tables pilotées par des routes** (lecture et/ou écriture) : `trppu_scenario`, `trppu_tmh`,
`trppu_neutralisations`, `trppu_scenario_comptages_manuels`, `trppu_scenario_variations_prev`,
`trppu_site`, `trppu_produit`, `trppu_pic_version`, `trppu_pic_coefficients`.

**4 tables touchées uniquement par la suppression / duplication de scénario**
(`trppu_scenario/helpers.py:141-151` et `:175-243`) : `trppu_scenario_exclusions`,
`trppu_scenario_pic_coeffs`, `trppu_trafic_agrebal`, `trppu_trafic_pdi`.

**12 tables jamais nommées dans le code Python** : `demande_dsr`, `trafic_staging`,
`trppu_agrebal_pdi`, `trppu_api_log`, `trppu_cles_repartition`, `trppu_cles_repartition_calcule`,
`trppu_pic_coefficients_ko`, `trppu_recalcul_log`, `trppu_referentiel`, `trppu_suivi_batch`,
`trppu_trafic_site`, `trppu_version_cle`.

Normal si elles sont alimentées par d'autres composants (batch AA, Kafka `TOPIC_agrebal`, calcul des
clés de répartition) — `trppu_cles_repartition` porte d'ailleurs 22,4 M de lignes et
`trppu_agrebal_pdi` 9 505 (`db/count.json`). Deux points restent à documenter : `trppu_api_log` et
`trppu_recalcul_log` semblent conçues pour tracer l'API mais ne le sont jamais (cf. écart n°10), et
`trppu_pic_coefficients_ko` conserve l'ancien modèle `coef_dense/faible*` sans lecteur.

Hors MySQL : les routes `trppu_trafics/` et `scripts/controle_trafics_679.py` n'attaquent que
Databricks (tables `gold.*` / `g_trppu_trafics_*_3`) — hors périmètre de ce dump.

---

## 8. Recommandations, par ordre de priorité

**P0 — à corriger en premier (500 actifs)**

1. `trppu_scenario/routes.py:446` → passer `(id_scenario, id_scenario)`.
2. Ajouter `SIMULATION` au `Literal Statut`, à `STATUTS`, aux transitions (aligné sur `EN COURS`), à
   `/enums`, et au contrôle `routes.py:330` — après arbitrage sur `FIGE_PAR_STATUT`.
3. Ajouter `SCENARIO` à `NiveauEnum` (lecture), en gardant le POST fermé à ce niveau.

**P1 — répercuter les contraintes de la base dans les schémas**

4. `ge=0` sur `volume_realise` (`trppu_tmh/schemas.py:45`, `:106`).
5. `ge=-100, le=100` sur `variation_pct` (`trppu_variations/schemas.py:14`).
6. `lb_regate` obligatoire et `max_length=40` (`trppu_site/schemas.py:12`,
   `trppu_scenario/schemas.py`).
7. Pré-valider produit et version PIC (422 explicite) sur les 4 points d'écriture de l'écart n°7,
   en réutilisant `ensure_produits_exist`.
8. Harmoniser `co_produit` sur 1..3 dans les 4 modules concernés.

**P2 — cohérence référentielle et traçabilité**

9. Exposer `id_referentiel` / `id_version_cle` et les copier à la duplication.
10. Traiter les tables de logs dans `delete_scenario_cascade`.
11. Écrire `id_rh` dans les CRUD PIC ; arbitrer le sort de `ComptageCreate.id_rh`.
12. Ajouter `id_scenario` à `SELECT_PICV_SQL`.

**Tests de garde à ajouter** — dans l'esprit de `tests/test_duplicate_scenario.py`, qui parse déjà
`db/db_new.sql` (fixture, `len(SCHEMA) == 25`) et vérifie sans base que les colonnes copiées existent
et que toute colonne `NOT NULL` sans défaut est couverte :

- `Literal Statut` ⊆ enum `trppu_scenario.statut` du dump, et réciproquement ;
- `NiveauEnum` ⊆ enum `trppu_pic_version.niveau` ;
- bornes Pydantic vs `chk_tmh_volumes`, `chk_var_borne`, `varchar(40)` de `lb_regate` ;
- nombre de placeholders de `SELECT_VARIATIONS_SQL` vs paramètres passés par `/edition`.

---

## 9. Méthode et vérifiabilité

- Chaque requête SQL de `app/routes/`, `app/services/`, `app/db/` et `scripts/` a été relevée
  (table, colonnes lues/écrites, valeurs d'enum en dur) puis confrontée aux `CREATE TABLE` du dump.
- Chaque écart cite un `fichier:ligne` de code **et** une ligne de `db/db_new.sql`.
- Évolution du schéma reconstituée par `git diff e72abf0 -- python/db/db_new.sql` complété du diff du
  working tree (le dump du jour n'est pas commité).
- Volumétrie tirée de `db/count.json` (25 tables, relevé du 13/08 — antérieur au renommage
  `trppu_site_trafic` → `trppu_trafic_site`).
- Les écarts n°1, 4, 5 et 7 sont **nouveaux** par rapport au rapport du 23/07.
