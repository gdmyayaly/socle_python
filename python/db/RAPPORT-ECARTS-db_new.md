# Rapport d'analyse — écarts entre `app/routes/` et le nouveau schéma `db_new.sql`

> **Base analysée :** `python/db/db_new.sql` (dump schéma `dsr_mercure_aa`, 24 objets)
> **Code analysé :** `python/app/routes/` (tous les packages `trppu_*` + modules plats)
> **Date :** 2026-07-23

---

## Synthèse

| # | Sévérité | Écart | Impact |
|---|----------|-------|--------|
| 1 | 🔴 Critique | FK `trppu_api_log` / `trppu_recalcul_log` → `trppu_scenario` sans CASCADE, non gérées par le hard-delete | `DELETE /scenarios/{id}` → erreur MySQL 1451 → **500** dès qu'un log existe |
| 2 | 🔴 Critique | Statut `SIMULATION` présent en base, inconnu de l'API | GET scénarios → **500** (validation Pydantic) ; transitions impossibles |
| 3 | 🔴 Critique | `NiveauEnum` sans valeur `SCENARIO` alors que le code en insère | `GET /pic-versions` → **500** dès qu'une version SCENARIO existe |
| 4 | 🔴 Critique | Nouvelles FK produit (`fk_picc_produit`, `fk_tmh_produit`, `fk_var_produit`) non anticipées | INSERT TMH / coef PIC / variation avec produit inconnu → erreur 1452 → **500** |
| 5 | 🟠 Majeur | `trppu_site.lb_regate` varchar(**40**) NOT NULL vs API max_length=**120** et nullable | POST /sites & création scénario → erreur 1406/1048 → **500** |
| 6 | 🟠 Majeur | Longueurs `co_produit` incohérentes (DB varchar(3) vs API bornée à 2) | Produits à 3 caractères inutilisables sur PIC/comptages/variations (**422**) |
| 7 | 🟡 Moyen | Nouvelles colonnes scénario `id_referentiel` / `id_version_cle` ignorées (création, duplication, SELECT) | Scénarios créés avec référentiel « 0 » inexistant ; perte au clonage |
| 8 | 🟡 Moyen | `trppu_tmh.volume_previsionnel_recalcule` ignorée partout | Donnée perdue à la duplication, jamais exposée |
| 9 | 🟢 Mineur | `PicVersionOut.id_scenario` toujours 0 (non sélectionné en SQL) | Réponse API trompeuse pour les versions SCENARIO |
| 10 | 🟢 Info | 11 tables du nouveau schéma sans aucune couverture API | `demande_dsr`, `trafic_staging`, `trppu_agrebal_pdi`, clés de répartition… |

---

## 1. 🔴 Hard-delete de scénario bloqué par les FK des tables de logs

**Fichier :** `app/routes/trppu_scenario/helpers.py:141-163` (`SCENARIO_CHILD_TABLES`, `delete_scenario_cascade`)

Le nouveau schéma définit :

```sql
CONSTRAINT `trppu_api_log_ibfk_1`     FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
CONSTRAINT `trppu_recalcul_log_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
```

Ces deux FK sont **sans `ON DELETE CASCADE`** (donc RESTRICT par défaut). Or le commentaire du code (helpers.py:139-140) exclut *volontairement* ces tables du nettoyage « pour préserver la traçabilité » :

```python
SCENARIO_CHILD_TABLES = (
    "trppu_neutralisations", "trppu_tmh", ... , "trppu_pic_version",
)  # trppu_api_log et trppu_recalcul_log absentes
await tx.execute("DELETE FROM trppu_scenario WHERE id_scenario = %s", ...)
```

**Erreur à venir :** dès qu'une ligne `trppu_api_log` ou `trppu_recalcul_log` référence le scénario, `DELETE /trppu-api/scenarios/{id}` échoue avec MySQL **1451 (Cannot delete or update a parent row)** → HTTP 500 « Erreur suppression scenario ». La traçabilité voulue est incompatible avec le hard-delete : il faut soit détacher les logs (`SET id_scenario = NULL`, la colonne est nullable dans `trppu_api_log` mais **pas** dans `trppu_recalcul_log`), soit les supprimer, soit basculer les FK en `ON DELETE SET NULL/CASCADE`.

---

## 2. 🔴 Statut `SIMULATION` : la base l'autorise, l'API l'ignore

**Base :** `trppu_scenario.statut enum('EN COURS','SIMULATION','VALIDE','EN PRODUCTION','ARCHIVE')`

**Code :**
- `app/routes/trppu_scenario/statuts.py:8` → `STATUTS = ("EN COURS", "VALIDE", "EN PRODUCTION", "ARCHIVE")`
- `app/routes/trppu_scenario/schemas.py:12` → `Statut = Literal["EN COURS", "VALIDE", "EN PRODUCTION", "ARCHIVE"]`
- `statuts.py:46-51` → `ALLOWED_TRANSITIONS` sans entrée `SIMULATION`

**Erreurs à venir :**
1. **500 en lecture** : un scénario passé en `SIMULATION` (par un batch, un script SQL, ou une autre appli du SI) fait échouer la validation du `response_model` (`ScenarioOut.statut: Statut`) sur `GET /scenarios`, `GET /scenarios/{id}`, `/periodes`, `/edition`… → `ResponseValidationError` → 500.
2. **Machine à états incohérente** : `PATCH /statut` vers `SIMULATION` → 422 « Statut inconnu » ; depuis `SIMULATION` → aucune transition possible (`ALLOWED_TRANSITIONS.get("SIMULATION")` = ∅). Le scénario est verrouillé côté API.
3. **Incohérence interne** : `FIGE_PAR_STATUT` (statuts.py:13, DSR-669) accepte déjà `"SIMULATION"` pour le figement — le reste du module ne le connaît pas.
4. `GET /scenarios/enums` n'expose pas la valeur → l'IHM ne peut pas la proposer.

---

## 3. 🔴 `trppu_pic_version.niveau = 'SCENARIO'` : inséré par le code, rejeté par les schémas

**Base :** `niveau enum('NATIONAL','DEX','SITE','SCENARIO')`

**Code :**
- `app/routes/trppu_pic_version/schemas.py:11-14` → `NiveauEnum` ne contient que `NATIONAL / DEX / SITE`.
- Or `app/routes/trppu_scenario_pic/routes.py:133-136` et `app/routes/trppu_scenario/helpers.py:280-284` **insèrent** des lignes `niveau='SCENARIO'` (rétention PIC DSR-661, duplication de scénario).

**Erreurs à venir :** dès qu'un utilisateur a surchargé un coefficient PIC (donc créé une version SCENARIO) :
- `GET /trppu-api/pic-versions` (listing sans filtre) → la ligne SCENARIO fait échouer `PicVersionOut.niveau: NiveauEnum` → **500** sur tout le listing ;
- `GET /trppu-api/pic-versions/{id}` sur cette version → **500** ;
- impossible de la modifier/désactiver via `PUT`/`DELETE` (déjà le retour est invalide).

---

## 4. 🔴 Nouvelles FK vers `trppu_produit` : aucun contrôle préalable dans le code

Le nouveau schéma ajoute trois contraintes `ON DELETE RESTRICT` :

```sql
trppu_pic_coefficients      : fk_picc_produit (co_produit → trppu_produit)
trppu_tmh                   : fk_tmh_produit  (co_produit → trppu_produit)
trppu_scenario_variations_prev : fk_var_produit (co_produit → trppu_produit)
```

Aucun des points d'écriture ne vérifie l'existence du produit :

| Point d'entrée | Fichier |
|---|---|
| `POST /pic-coefficients` + upload Excel (UPSERT ligne à ligne) | `trppu_pic_coefficients/routes.py:174`, `helpers.py:38` |
| `PUT /scenarios/{id}/pic-coefficients` (`_insert_coef`) | `trppu_scenario_pic/routes.py:172-185` |
| Création / upsert TMH — y compris le lot `payload.tmh` de `POST /scenarios` | `trppu_tmh/helpers.py:48-67`, `trppu_scenario/routes.py:261-271` |
| `PUT /scenarios/{id}/variations/{co_produit}` | `trppu_variations/routes.py:97-102` |

**Erreurs à venir :**
- Tout `co_produit` absent de `trppu_produit` → MySQL **1452 (foreign key constraint fails)** → HTTP 500 générique (« Erreur création TMH », etc.) au lieu d'un 422 explicite. Cas concret : la création de scénario (`POST /scenarios`) échoue **entièrement** (rollback transaction) si une seule ligne TMH porte un produit non référencé.
- L'upload Excel PIC échoue en bloc (transaction unique) sur la première ligne au produit inconnu, avec un message SQL brut dans le detail (`Échec de l'écriture du lot en base : ...`).
- Effet croisé avec l'écart n°6 : les produits sont normalisés sur **2** caractères côté PIC (`zfill(2)`, pattern `{2}`) — si le référentiel produit contient des codes à 3 caractères, la FK ne matchera jamais.
- Sens inverse (RESTRICT) : un `DELETE` physique de `trppu_produit` serait bloqué — sans impact aujourd'hui, la route produit fait du soft-delete (`dt_desactivation`).

---

## 5. 🟠 `trppu_site.lb_regate` : varchar(40) NOT NULL vs API 120 caractères et nullable

**Base :** `lb_regate varchar(40) NOT NULL`

**Code :**
- `app/routes/trppu_site/schemas.py:12` → `lb_regate: str | None = Field(None, max_length=120)` (**nullable** et jusqu'à 120 caractères)
- `app/routes/trppu_scenario/schemas.py:68` → `lb_regate: str = Field(..., min_length=1, max_length=120)`

**Erreurs à venir :**
- `POST /sites` sans `lb_regate` → INSERT `NULL` dans une colonne NOT NULL → MySQL **1048** → 500.
- `POST /sites`, upload Excel sites, ou `POST /scenarios` (via `ensure_site_exists`, `trppu_scenario/helpers.py:321-325`) avec un libellé de 41 à 120 caractères → MySQL **1406 (Data too long)** en mode strict → 500 ; la création du scénario est annulée.

À aligner : `max_length=40` + rendre le champ obligatoire (ou tronquer serveur).

---

## 6. 🟠 Longueurs `co_produit` : la base dit 3, l'API impose 2 (selon les modules)

**Base :** `co_produit varchar(3)` dans `trppu_produit`, `trppu_tmh`, `trppu_pic_coefficients`, `trppu_scenario_comptages_manuels`, `trppu_scenario_exclusions`, `trppu_scenario_pic_coeffs`, `trppu_scenario_variations_prev`, `trppu_trafic_*`.

**Code — bornes hétérogènes :**

| Module | Contrainte co_produit |
|---|---|
| `trppu_produit/schemas.py:11` | 2 à **3** ✅ |
| `trppu_tmh/schemas.py:37` | 1 à **3** ✅ |
| `trppu_pic_coefficients/schemas.py:28,59` | exactement **2** ❌ (+ `zfill(2)` dans l'Excel, `helpers.py:66-71`) |
| `trppu_pic_coefficients/routes.py:42` (filtre GET) | exactement **2** ❌ |
| `trppu_scenario_pic/schemas.py:37` | 1 à **2** ❌ |
| `trppu_scenario/schemas.py:32` (TMH de création) | 1 à **2** ❌ |
| `trppu_comptages/schemas.py:16` | 1 à **2** ❌ |

**Écart fonctionnel à venir :** un produit légitime à 3 caractères (créé via `POST /produits`, accepté par la base et par le TMH direct) est **rejeté en 422** partout ailleurs : coefficient PIC, rétention PIC scénario, comptage manuel, TMH passé dans `POST /scenarios`. Incohérence de surface d'API + risque de FK jamais satisfaite (cf. n°4).

---

## 7. 🟡 Nouvelles colonnes `trppu_scenario` non prises en compte

**Base :** `id_referentiel int NOT NULL DEFAULT '0'`, `id_version_cle int NOT NULL DEFAULT '0'`, `Calcul_trafic_en_cours smallint DEFAULT '0'` + tables associées `trppu_referentiel`, `trppu_version_cle`.

**Code :**
- `SELECT_SCENARIO_SQL` (`trppu_scenario/helpers.py:10-19`) ne sélectionne aucune des trois → jamais exposées par l'API (ni `ScenarioOut`, ni `/edition`).
- `POST /scenarios` (routes.py:226-256) ne les renseigne pas → tout scénario naît avec `id_referentiel=0` / `id_version_cle=0`, valeurs qui ne correspondent à **aucune** ligne de `trppu_referentiel` / `trppu_version_cle` (pas de FK donc pas d'erreur SQL, mais une incohérence référentielle silencieuse pour les traitements de calcul de clés).
- `POST /scenarios/{id}/duplicate` (routes.py:889-910) : l'INSERT…SELECT ne copie ni `id_referentiel` ni `id_version_cle` → **le clone perd le rattachement référentiel de la source** (retombe à 0). Idem `Calcul_trafic_en_cours` (acceptable) — alors que `trafic_pdi_calcule` / `trafic_agrebal_calcule` sont, eux, copiés.

**Erreur à venir :** pas de plantage SQL, mais tout traitement aval qui joint `trppu_scenario.id_referentiel → trppu_cles_repartition/trppu_site_trafic` (structure visiblement prévue pour ça) tombera sur des scénarios orphelins.

---

## 8. 🟡 `trppu_tmh.volume_previsionnel_recalcule` ignorée

**Base :** nouvelle colonne `volume_previsionnel_recalcule int DEFAULT NULL`.

**Code :**
- `_TMH_COLS` (`trppu_tmh/helpers.py:12-15`) ne la sélectionne pas → jamais restituée (GET TMH, `/edition`).
- `DUPLICATE_CHILD_SPECS` (`trppu_scenario/helpers.py:172-187`) ne la copie pas → **perte de la valeur au clonage** d'un scénario.
- Aucun endpoint ne l'écrit.

Si cette colonne est alimentée par un batch de recalcul, la duplication produit des clones incomplets sans erreur visible.

---

## 9. 🟢 Points mineurs / vigilance

1. **`PicVersionOut.id_scenario` toujours 0** — `SELECT_PICV_SQL` (`trppu_pic_version/routes.py:31-35`) n'inclut pas `id_scenario` ; le champ Pydantic retombe sur son défaut `0` (`schemas.py:25`). Les consommateurs de `GET /pic-versions` ne peuvent pas savoir à quel scénario une version est rattachée (dès que l'écart n°3 sera corrigé, ce point deviendra visible).
2. **`trppu_pic_coefficients.dt_effet/dt_fin` sont des `datetime`**, l'API manipule des `date` (create/update/soft-delete, `trppu_pic_coefficients/schemas.py:31-32`). MySQL caste sans erreur (`00:00:00`), et le code lit défensivement (`isinstance(..., datetime)`) — OK, mais le filtre `actif_only` (`dt_fin > NOW()`) rend un coefficient clôturé « aujourd'hui » immédiatement inactif alors que le soft-delete pose `dt_fin = today 00:00` : comportement voulu à confirmer.
3. **`trppu_scenario_pic_coeffs` garde l'ancienne structure** (`coef_dense/coef_faible1/coef_faible2`, jours `LUN..SAM`) alors que `trppu_pic_coefficients` est passée au modèle `coef + densite` (jours `LUNDI..SAMEDI`). Le code n'y touche que par la duplication (copie conforme, OK), mais la coexistence des deux modèles + `trppu_pic_coefficients_ko` (ancienne structure conservée) est un piège pour les évolutions futures.
4. **Deux résolveurs de version PIC par défaut divergents** : `trppu_scenario/helpers.py:61-83` (tout `est_par_defaut=1`, fallback id=1, sinon **422**) vs `trppu_scenario_pic/helpers.py:13-26` (`niveau='NATIONAL'` + `est_par_defaut=1`, fallback silencieux `DEFAULT_PIC_VERSION=1`). Avec `AUTO_INCREMENT=3` sur `trppu_pic_version` dans le dump, un id 1 absent ou non-national donnera des défauts différents selon la route.
5. **Commentaires obsolètes** : `trppu_scenario/helpers.py:134` référence des FK « de db.sql » qui ne correspondent plus (ex. comptages/exclusions décrites RESTRICT — vrai, mais neutralisations/tmh/variations sont bien CASCADE) ; `statuts.py:137` mentionne `chk_scen_prod` qui n'existe pas dans `db_new.sql` (sans impact).
6. **Audit id_rh** (`trppu_audit/helpers.py`) : balaie les 6 tables historiques — cohérent avec le schéma. Les nouvelles tables porteuses d'identité (`demande_dsr.idrh` en clair char(7), `trppu_pic_coefficients_ko.id_rh`) ne sont pas balayées ; à confirmer si hors périmètre.
7. **Tables trafics Databricks** (`trafics.py`, `trppu_trafics/`) : hors périmètre de ce dump MySQL (tables `gold.*` / `g_trppu_trafics_*_3`) — aucun écart évaluable ici.

---

## 10. 🟢 Tables du nouveau schéma sans couverture API

Aucune route ne lit/écrit : `demande_dsr`, `trafic_staging`, `trppu_agrebal_pdi`, `trppu_cles_repartition`, `trppu_cles_repartition_calcule`, `trppu_referentiel`, `trppu_version_cle`, `trppu_site_trafic`, `trppu_pic_coefficients_ko`, `trppu_api_log`, `trppu_recalcul_log` (les deux dernières ne sont même jamais alimentées par l'API alors qu'elles semblent conçues pour la tracer). `trppu_scenario_exclusions`, `trppu_trafic_agrebal` et `trppu_trafic_pdi` ne sont touchées que par la duplication/suppression de scénario. Normal si ces tables sont alimentées par d'autres composants (batch AA / Kafka `TOPIC_agrebal`), mais à documenter.

---

## Recommandations (ordre de priorité)

1. **Hard-delete** : traiter `trppu_api_log` / `trppu_recalcul_log` dans `delete_scenario_cascade` (détacher `trppu_api_log.id_scenario = NULL` + décider du sort de `trppu_recalcul_log`, colonne NOT NULL) — ou faire évoluer les FK.
2. **Ajouter `SIMULATION`** à `STATUTS`, au `Literal Statut`, aux transitions (`ALLOWED_TRANSITIONS`) et à `/enums` — ou au minimum élargir le `Literal` de `ScenarioOut` pour éviter les 500 en lecture.
3. **Ajouter `SCENARIO`** à `NiveauEnum` (lecture au minimum ; à exclure éventuellement du `POST /pic-versions` via un enum de création distinct).
4. **Pré-valider l'existence du produit** (SELECT `trppu_produit`) avant les INSERT TMH / PIC / variations pour transformer les 1452 en 422 explicites ; harmoniser les bornes `co_produit` sur 1..3.
5. **Aligner `lb_regate`** sur varchar(40) NOT NULL (schemas site + scenario).
6. **Décider du sort de `id_referentiel` / `id_version_cle`** à la création et duplication de scénario, et ajouter `volume_previsionnel_recalcule` à `_TMH_COLS` + `DUPLICATE_CHILD_SPECS`.
