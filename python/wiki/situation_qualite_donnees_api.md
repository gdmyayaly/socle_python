# API TRPPU — Situation & qualité des données (5 modules)

> **Objet** : exposer le fonctionnement de 5 modules de l'API et aider à repérer les soucis de
> qualité de données (colonnes laissées vides, données envoyées mais non enregistrées, valeurs par
> défaut qui écrasent l'existant, migrations désalignées du schéma réel).
>
> **Périmètre** : `trppu_scenario`, `trppu_tmh`, `trppu_produit`, `trppu_neutralisations`,
> `trafics.py`. Les modules **comptages** et **variations** ne sont cités qu'en notes transverses
> (hors périmètre demandé), car certains soucis les concernent aussi.
>
> **Source de vérité** : code applicatif actuel + schéma `db_migrations/03_db_12_06_2026.sql`
> (dernière version de la structure, 12/06/2026). Les exemples de payload/SQL sont **illustratifs**
> (valeurs inventées). `id_rh` est reçu en clair et stocké **chiffré (Fernet)** ; aucune clé ni
> identifiant réel n'apparaît ici.

---

## Partie A — Synthèse exécutive

L'API couvre cinq briques : le **cycle de vie des scénarios** de simulation (création, mise à jour,
validation, mise en production, archivage, duplication), le **trafic moyen hebdomadaire (TMH)** par
produit, le **référentiel des produits**, les **neutralisations** de périodes (jours non travaillés
d'un scénario), et la **lecture des trafics** depuis Databricks. Les quatre premières écrivent en
base MySQL ; la cinquième est en **lecture seule**.

Globalement le socle fonctionne, mais l'analyse a relevé **plusieurs points qui dégradent la
qualité ou la traçabilité des données**. Les plus importants : un scénario **dupliqué arrive avec
ses compteurs de jours vides**, certaines mises à jour **ne recodent pas les jours** (incohérence
période ↔ nombre de jours), la table **comptages est la seule à ne pas avoir de colonne `id_rh`**
(traçabilité absente alors que ses tables sœurs en ont une), et de la **traçabilité utilisateur
(`id_rh`) non exploitée** sur plusieurs actions.

> **Mise à jour 12/06/2026** : analyse réalignée sur le dernier dump `03_db_12_06_2026.sql`.
> Par rapport à la version précédente, les tables `trppu_neutralisations` et
> `trppu_scenario_variations_prev` **ont désormais bien leurs colonnes `id_rh`/`dt_creation`**
> (traçabilité en place). En revanche `trppu_scenario_comptages_manuels` **n'a toujours pas de
> colonne `id_rh`** — c'est le point dur restant.

### A.1 — Tableau des constats (feux tricolores)

| # | Constat | Module / Route | Impact données | Gravité |
|---|---------|----------------|----------------|:---:|
| 1 | Duplication : `nb_jours_ouvres/ouvrables/scenario` et `id_rh_creation/maj` non renseignés | Scénario · `POST /duplicate` | Clone avec compteurs **NULL**, inexploitable sans PUT | 🔴 |
| 2 | `trppu_scenario_comptages_manuels` **n'a pas de colonne `id_rh`** (contrairement à neutralisations/variations) ; le code exige pourtant `id_rh` et le **jette** | Comptages *(transverse)* | Comptages **non traçables** ; auditing incomplet | 🔴 |
| 3 | Scripts de migration `db_migrations/` **désynchronisés** du dump réel (ex. `002` fait `MODIFY COLUMN type` sur une table sans `type`) | DB · migrations | Rejouer les migrations en l'état **échoue** | 🟠 |
| 4 | `PATCH /periodes` et `/nb-jours-semaine` ne recalculent pas les jours | Scénario | `nb_jours_*` **incohérents** avec la nouvelle période | 🟠 |
| 5 | `PUT /variations` écrase `dt_creation = NOW()` à chaque modif | *Variations (transverse)* | Perte de la **date de création** d'origine | 🟠 |
| 6 | Site existant **jamais mis à jour** (`lb_regate`/`type_site` ignorés) | Scénario · `POST /scenarios` | Libellés envoyés **silencieusement ignorés** | 🟠 |
| 7 | Longueurs désalignées : `lb_scenario` 20 (DB) vs 50 (API) ; `lb_regate` 40 (DB) vs 120 (API) | Scénario / Site | Risque d'**erreur SQL** ou troncature | 🟠 |
| 8 | Flags `trafic_pdi_calcule` / `trafic_agrebal_calcule` exposés mais **jamais écrits** | Scénario | Toujours `0` → information **non fiable** | 🟡 |
| 9 | `nb_jour` (neutralisation) dépend d'une **API externe** sans repli | Neutralisations · `POST` | **503** si jours-fermés indisponible (pas de fallback) | 🟡 |
| 10 | Tables legacy / non alimentées | DB · `trppu_pic_coefficients_ko`, `trppu_api_log`, `trppu_recalcul_log` | Stockage **mort** / logs jamais écrits par ces modules | 🟡 |
| 11 | `id_rh` jamais relu/exposé après écriture | Scénario / TMH | Le client ne peut **pas afficher** qui a créé/modifié | 🟡 |

### A.2 — Colonnes qui restent vides (NULL) en pratique

| Table | Colonne(s) | Pourquoi vide | Conséquence |
|-------|-----------|---------------|-------------|
| `trppu_scenario` | `nb_jours_ouvres`, `nb_jours_ouvrables`, `nb_jours_scenario` | Non renseignées par `POST /duplicate` | Scénario dupliqué sans volumétrie de jours |
| `trppu_scenario` | `id_rh_creation`, `id_rh_maj` | Non renseignées par `POST /duplicate` | Créateur du clone inconnu |
| `trppu_scenario` | `trafic_pdi_calcule`, `trafic_agrebal_calcule` | Aucune route ne les met à `1` | Flag de calcul jamais positionné (toujours `0`) |
| `trppu_scenario` | `dt_validation`, `dt_mise_en_prod` | Posées seulement aux transitions VALIDE / EN PRODUCTION | Normal tant que le scénario n'a pas avancé |
| `trppu_scenario_comptages_manuels` | `id_rh` | **Colonne inexistante** dans le dump 12/06/2026 + non écrite par le code | Aucune traçabilité des comptages 🔴 |

> Note : dans le dump 12/06/2026, `trppu_neutralisations.id_rh`/`dt_creation` et
> `trppu_scenario_variations_prev.id_rh`/`dt_creation` **existent et sont alimentés** par le code —
> ces deux tables ne sont donc plus des « colonnes vides ». Seule la table **comptages** reste sans
> colonne `id_rh`.

### A.3 — Migrations vs schéma réel (dump 12/06/2026)

Le dump de référence est désormais `db_migrations/03_db_12_06_2026.sql`. État constaté des
colonnes de traçabilité dans **ce** dump, comparé aux scripts de migration du dossier :

| Table | `id_rh` / traçabilité dans le dump 12/06 | Cohérence avec les scripts |
|-------|------------------------------------------|----------------------------|
| `trppu_scenario` | `id_rh_creation`/`id_rh_maj` en **varchar(255)** ✓ | OK (migration `001` appliquée) |
| `trppu_neutralisations` | `id_rh` + `dt_creation` **présents** ✓ ; **pas** de colonne `type` | OK côté colonnes ; la ligne `MODIFY COLUMN type` de `002` ne correspond à rien |
| `trppu_scenario_variations_prev` | `id_rh` + `dt_creation` **présents** ✓ (+ `variation_pct` en decimal(6,2), CHECK −100..100, FK produit) | OK (migrations `002`/`004` reflétées) |
| `trppu_scenario_comptages_manuels` | **Aucune** colonne `id_rh` ✗ | ✗ La colonne prévue par `002` **n'est pas** dans le dump |
| `trppu_pic_version` | `id_rh_creation`/`id_rh_maj` encore en **varchar(40)** | `001` non appliquée à cette table (mineur) |

> **Point d'attention** : la base a évolué (neutralisations et variations ont gagné leur
> traçabilité), mais **les scripts `db_migrations/` ne reflètent pas fidèlement cet état** —
> rejouer `002` tel quel échouerait (`MODIFY COLUMN type` sur une colonne inexistante). Surtout,
> **les comptages n'ont jamais reçu leur colonne `id_rh`** : c'est le seul vrai manque de
> traçabilité restant (souci #2). 🔴

---

## Partie B — Vue par module

### B.1 — Scénarios (`trppu_scenario`)

Table maître du chantier. Machine à états stricte : `EN COURS → VALIDE → EN PRODUCTION → ARCHIVE`.
Chaque écriture incrémente `version_scenario`. La duplication et le `DELETE` ont des effets de bord
notables (voir annexe).

| Route | Entrée (résumé) | Écrit en base | Souci |
|-------|-----------------|---------------|-------|
| `GET /scenarios` (+ `/{id}`, `/enums`, `/periodes`, `/edition`) | filtres / id | — (lecture) | `trafic_*_calcule` exposés mais toujours 0 |
| `POST /scenarios` (DSR-634) | scénario + TMH + `id_rh` | `trppu_scenario` (création), `trppu_site` (si absent), `trppu_tmh` | Site existant non mis à jour (#6) ; longueurs (#7) |
| `PUT /scenarios/{id}` (DSR-656) | périodes, jours, TMH, `id_rh` | `trppu_scenario` (recalcul jours, `id_rh_maj`), `trppu_tmh` | Seule route qui recode les jours correctement |
| `PATCH /periodes` | périodes | `trppu_scenario` (périodes seules) | **Jours non recalculés** (#4) |
| `PATCH /nb-jours-semaine` | 5 ou 6 | `trppu_scenario` (`nb_jours_semaine` seul) | **Jours non recalculés** (#4) |
| `PATCH /statut` · `POST /mise-en-prod` · `POST /archive` | statut cible | `trppu_scenario` (statut + `dt_validation`/`dt_mise_en_prod`/`est_fige`) | Pas d'`id_rh` (#11) |
| `PATCH /est-fige` · `/figement` (DSR-669) · `/lb-scenario` | flag / libellé | `trppu_scenario` | `/figement` ne change **pas** le statut, juste `est_fige` |
| `POST /duplicate` | libellé optionnel | `trppu_scenario` (clone partiel) | **Compteurs jours + id_rh NULL** (#1) |
| `DELETE /scenarios/{id}` | id | Supprime 9 tables enfants + le scénario | Cascade **applicative** (pas de FK) — risque d'oubli |

### B.2 — TMH (`trppu_tmh`)

Trafic moyen hebdomadaire, 1 ligne par produit et par scénario (clé unique
`(id_scenario, co_produit)`). Pas de table de traçabilité dédiée : seul `dt_calcul` et `id_rh`
gardent une trace.

| Route | Entrée | Écrit en base | Souci |
|-------|--------|---------------|-------|
| `GET /scenarios/{id}/tmh` (DSR-650) | id | — (lecture) | `id_rh`, `dt_calcul`, `id_tmh` non exposés |
| `PUT /scenarios/{id}/tmh` (DSR-659) | lot TMH + `id_rh` | `trppu_tmh` (upsert : update sinon insert) | `id_rh` écrit mais jamais relu (#11) |
| `PATCH /scenarios/{id}/tmh/{co_produit}` (DSR-649) | volume + moyennes | `trppu_tmh` (`bl_manuel=1`, `dt_calcul=NOW()`) | **Pas d'`id_rh`** : modification manuelle non attribuée |

### B.3 — Produits (`trppu_produit`)

Référentiel simple (PK `co_produit`, 2 caractères). La suppression est un **soft-delete**
(désactivation réversible), pas un effacement.

| Route | Entrée | Écrit en base | Souci |
|-------|--------|---------------|-------|
| `GET /produits` (+ `/{co_produit}`) | filtres | — (lecture) | — |
| `POST /produits` | produit | `trppu_produit` (insert) | 409 si existe ; `dt_creation` auto |
| `PUT /produits/{co_produit}` | champs partiels | `trppu_produit` (update dynamique) | 400 si aucun champ |
| `DELETE /produits/{co_produit}` | `motif?` | `trppu_produit` (`dt_desactivation=today`, `motif`) | Réversible via PUT ; pas de contrôle date passée |
| `POST /produits/upload-excel` | fichier `.xlsx/.xlsm` | `trppu_produit` (`INSERT … ON DUPLICATE KEY UPDATE`) | Une ligne en erreur → **rollback du lot entier** |

### B.4 — Neutralisations (`trppu_neutralisations`)

Périodes neutralisées d'un scénario (jours non comptés). Unicité `(id_scenario, dt_debut, dt_fin)`.
Le nombre de jours est **calculé côté serveur**.

| Route | Entrée | Écrit en base | Souci |
|-------|--------|---------------|-------|
| `GET /scenarios/{id}/neutralisations` (DSR-652) | id | — (lecture) | `id_rh`, `dt_creation` non exposés |
| `POST /scenarios/{id}/neutralisations` (DSR-645) | dates + motif + `id_rh` | `trppu_neutralisations` (insert, `nb_jour` calculé, `id_rh` chiffré) | Dépend de l'API jours-fermés (#9) ; `id_rh`/`dt_creation` désormais bien en base ✓ |
| `DELETE /scenarios/{id}/neutralisations` (DSR-645) | `dt_debut`, `dt_fin` (query) | `trppu_neutralisations` (delete) | 404 si rien supprimé |

### B.5 — Trafics (`trafics.py`)

**Lecture seule Databricks — aucune écriture MySQL.** L'intervalle est découpé automatiquement en
segments mois / semaines / jours, une requête par grain.

| Route | Entrée | Source lue | Souci |
|-------|--------|------------|-------|
| `GET /trafics/get_trafics` (DSR-613) | `co_regate`, `date_debut`, `date_fin`, `limit?` | `gold.default.g_trppu_trafics_{jour,semaine,mois}` | Bloc `nb_jours` à `null` si calcul KO (résilient) |
| `GET /trafics/get_trafics_pivot` (DSR-666) | `co_regate`, `date_debut`, `date_fin`, `date_pivot` | idem, split réel/prév autour du pivot | 6 produits hydratés à 0 ; mapping libellé→produit par config |

---

## Partie C — Annexe technique (exemples payload → SQL)

> Valeurs illustratives. `ENCRYPTED(...)` = token chiffré Fernet (l'`id_rh` n'est jamais stocké en clair
> si la clé est configurée).

### C.1 — `POST /scenarios` (création)

**Payload :**
```json
{
  "co_regate": "400300", "lb_scenario": "Scénario T1", "co_roc": "750001",
  "lb_regate": "PARIS 15 PDC", "type_site": "PDC",
  "nb_jours_semaine": 5, "periode_debut": "2026-01-01", "periode_fin": "2026-12-31",
  "id_rh": "P123456",
  "tmh": [ { "co_produit": "OS", "volume_realise": 1000, "volume_previsionnel": 1200,
             "moyenne_journaliere": 120.50, "moyenne_hebdo": 602.50,
             "exclusion": false, "manuel": false } ]
}
```
**SQL (simplifié) :**
```sql
-- 1) Site créé seulement s'il n'existe pas (sinon lb_regate/type_site IGNORÉS — souci #6)
INSERT INTO trppu_site (co_regate, lb_regate, type_site, co_roc)
VALUES ('400300', 'PARIS 15 PDC', 'PDC', '750001');   -- si absent

-- 2) Scénario : périodes réalisé/prév + nb_jours calculés serveur
INSERT INTO trppu_scenario
 (co_regate, lb_scenario, co_roc, statut, dt_creation, dt_mise_en_oeuvre, dt_pivot,
  periode_debut, periode_fin, periode_realise_debut, periode_realise_fin,
  periode_prev_debut, periode_prev_fin, nb_jours_semaine, nb_jours_ouvres,
  nb_jours_ouvrables, nb_jours_scenario, id_pic_version, version_scenario, est_fige,
  id_rh_creation, id_rh_maj)
VALUES ('400300','Scénario T1','750001','EN COURS',NOW(),'2026-06-12',NOW(),
        '2026-01-01','2026-12-31','2026-01-01','2026-06-11','2026-06-12','2026-12-31',
        5, 252, 302, 252, 1, 1, 0, ENCRYPTED('P123456'), ENCRYPTED('P123456'));

-- 3) TMH (1 upsert par produit)
INSERT INTO trppu_tmh (id_scenario, co_produit, volume_realise, volume_previsionnel,
        moyenne_journaliere, moyenne_hebdo, dt_calcul, bl_exclu, bl_manuel, id_rh)
VALUES (42,'OS',1000,1200,120.50,602.50,NOW(),0,0,ENCRYPTED('P123456'));
```
**Colonnes non renseignées** : `dt_validation`, `dt_mise_en_prod`, `trafic_pdi_calcule`,
`trafic_agrebal_calcule` (restent NULL/0).

### C.2 — `POST /duplicate` (le souci #1, illustré)

**SQL réellement exécuté** (`trppu_scenario/routes.py`, fonction `duplicate_scenario`) :
```sql
INSERT INTO trppu_scenario
 (co_regate, lb_scenario, co_roc, statut, dt_creation,
  periode_debut, periode_fin, periode_realise_debut, periode_realise_fin,
  periode_prev_debut, periode_prev_fin, nb_jours_semaine, id_pic_version,
  version_scenario, est_fige)
VALUES ('400300','Scénario T1 (copie)','750001','EN COURS',NOW(),
        '2026-01-01','2026-12-31','2026-01-01','2026-06-11','2026-06-12','2026-12-31',
        5, 1, 1, 0);
```
**Colonnes laissées vides (NULL) — c'est le problème :**
`nb_jours_ouvres`, `nb_jours_ouvrables`, `nb_jours_scenario`, `id_rh_creation`, `id_rh_maj`,
`dt_mise_en_oeuvre`, `dt_pivot`. → Le clone n'a **aucune volumétrie de jours** et **aucun
créateur** tant qu'on n'a pas lancé un `PUT /scenarios/{id}`.

### C.3 — Transition statut (effets de bord)

`POST /mise-en-prod` (seul chemin VALIDE → EN PRODUCTION) :
```sql
UPDATE trppu_scenario
   SET statut = 'EN PRODUCTION',
       dt_validation  = COALESCE(dt_validation, NOW()),
       dt_mise_en_prod = NOW(),
       est_fige = 1
 WHERE id_scenario = 42;
```
`PATCH /statut` vers `VALIDE` pose `dt_validation` si NULL. Les autres cibles ne touchent que
`statut`. **Aucune** de ces routes n'enregistre l'`id_rh` de l'auteur (souci #11).

### C.4 — `PATCH /tmh/{co_produit}` (modification manuelle)

```sql
UPDATE trppu_tmh
   SET volume_realise = 1050, moyenne_journaliere = 122.50, moyenne_hebdo = 612.50,
       bl_manuel = 1, dt_calcul = NOW()
 WHERE id_scenario = 42 AND co_produit = 'OS';
```
`bl_manuel` passe à 1 (la ligne devient « saisie manuelle »), mais **aucun `id_rh`** n'est demandé
ni écrit : on sait que la ligne a été modifiée à la main, pas **par qui**.

### C.5 — `POST /neutralisations` (calcul de `nb_jour`)

**Jour unique** (`dt_debut == dt_fin`) → `nb_jour = 1`.
**Période** → appel au service jours-fermés (fériés/week-ends déduits selon `nb_jours_semaine`).

```json
{ "dt_debut": "2026-08-03", "dt_fin": "2026-08-07", "motif": "Travaux", "id_rh": "P123456" }
```
```sql
-- nb_jour calculé serveur (ex. 5 jours ouvrés, 0 férié) ; 409 si période déjà neutralisée
INSERT INTO trppu_neutralisations (id_scenario, dt_debut, dt_fin, nb_jour, motif, id_rh)
VALUES (42, '2026-08-03', '2026-08-07', 5, 'Travaux', ENCRYPTED('P123456'));
```
Erreurs possibles : **422** (aucun jour ouvré), **409** (doublon), **503** (API jours-fermés
indisponible — pas de repli, souci #9).

### C.6 — `POST /produits/upload-excel` (upsert)

Colonnes Excel : `co_produit`, `lb_produit` (obligatoires), `dt_desactivation`,
`motif_desactivation` (optionnelles). `co_produit` normalisé (2 car., majuscules, padding).
```sql
INSERT INTO trppu_produit (co_produit, lb_produit, dt_desactivation, motif_desactivation)
VALUES ('OS', 'Objets suivis', NULL, NULL)
ON DUPLICATE KEY UPDATE lb_produit=VALUES(lb_produit),
  dt_desactivation=VALUES(dt_desactivation), motif_desactivation=VALUES(motif_desactivation);
```
Comportement : insert si nouveau, update sinon. **Limite** : une ligne en erreur d'UPSERT annule la
transaction → le lot complet est rejeté (les lignes valides aussi).

### C.7 — `GET /trafics` (lecture seule)

Aucune écriture. L'intervalle `[date_debut, date_fin]` est découpé en mois pleins / semaines
pleines / jours, une requête Databricks par grain :
```sql
SELECT * FROM gold.default.g_trppu_trafics_mois
WHERE co_regate = :co_regate AND (co_mois_comptage BETWEEN :d0 AND :f0);
-- + g_trppu_trafics_semaine, + g_trppu_trafics_jour selon le découpage
```
Le bloc `nb_jours` (appel jours-fermés) est **résilient** : en cas d'échec, il vaut `null` sans
faire échouer la réponse trafics.

---

## Partie D — Annexe schéma (tables touchées)

### `trppu_scenario`
| Colonne | Type | Null | Défaut |
|---|---|:--:|---|
| id_scenario | bigint (PK, AI) | non | — |
| co_roc / co_regate | char(6) | non | — |
| lb_scenario | **varchar(20)** | non | — *(API accepte 50 → #7)* |
| statut | enum(EN COURS,VALIDE,EN PRODUCTION,ARCHIVE) | non | — |
| dt_creation | datetime | non | — |
| dt_validation / dt_mise_en_oeuvre / dt_mise_en_prod / dt_pivot | datetime | oui | NULL |
| periode_debut / periode_fin | date | non | — |
| periode_realise_debut/fin · periode_prev_debut/fin | date | oui | NULL |
| nb_jours_semaine | smallint | oui | NULL *(CHECK 5/6)* |
| nb_jours_ouvres / ouvrables / scenario | smallint | oui | NULL |
| id_pic_version | int | non | — |
| version_scenario | int | non | — |
| est_fige | smallint | oui | 0 |
| dt_maj | datetime | non | CURRENT_TIMESTAMP ON UPDATE |
| id_rh_creation / id_rh_maj | varchar(255) | oui | NULL |
| trafic_pdi_calcule / trafic_agrebal_calcule | smallint | oui | 0 *(jamais écrits → #8)* |

### `trppu_tmh`
| Colonne | Type | Null | Défaut |
|---|---|:--:|---|
| id_tmh | bigint (PK, AI) | non | — |
| id_scenario | bigint (FK→scenario, CASCADE) | non | — |
| co_produit | char(2) (FK→produit, RESTRICT) | non | — |
| volume_realise / volume_previsionnel | int | oui | NULL *(CHECK ≥ 0)* |
| moyenne_journaliere / moyenne_hebdo | decimal(12,2) | oui | NULL |
| dt_calcul | datetime | non | CURRENT_TIMESTAMP |
| bl_exclu / bl_manuel | tinyint(1) | non | — |
| id_rh | varchar(255) | oui | NULL |
| | | | UNIQUE `(id_scenario, co_produit)` |

### `trppu_produit`
| Colonne | Type | Null | Défaut |
|---|---|:--:|---|
| co_produit | char(2) (PK) | non | — |
| lb_produit | varchar(80) | non | — |
| dt_creation | datetime | non | CURRENT_TIMESTAMP |
| dt_desactivation | date | oui | NULL |
| motif_desactivation | varchar(255) | oui | NULL |

### `trppu_neutralisations`
| Colonne | Type | Null | Défaut |
|---|---|:--:|---|
| id_neutralisation | bigint (PK, AI) | non | — |
| id_scenario | bigint (FK→scenario, CASCADE) | non | — |
| dt_debut / dt_fin | date | non | — *(CHECK dt_debut ≤ dt_fin)* |
| nb_jour | int | non | — *(CHECK > 0)* |
| motif | varchar(255) | oui | NULL |
| dt_creation | datetime | non | CURRENT_TIMESTAMP |
| id_rh | varchar(255) | oui | NULL |
| | | | UNIQUE `(id_scenario, dt_debut, dt_fin)` · FK→scenario **ON DELETE CASCADE** |

> Note : `id_rh` et `dt_creation` sont **bien présents** dans le dump 12/06/2026 et alimentés par
> le code. Il n'y a **pas** de colonne `type` (la ligne `MODIFY COLUMN type` du script `002` ne
> correspond donc à rien — souci #3).

### `trppu_scenario_comptages_manuels` — table sœur **sans `id_rh`** (souci #2 🔴)
| Colonne | Type | Null | Défaut |
|---|---|:--:|---|
| id_comptage | int (PK, AI) | non | — |
| id_scenario | bigint (FK→scenario) | non | — |
| dt_comptage | date | non | — |
| co_produit | char(2) | non | — |
| nb_produit | int | non | — |

> Aucune colonne `id_rh` ici, alors que `trppu_tmh`, `trppu_neutralisations` et
> `trppu_scenario_variations_prev` en ont une. Le code des comptages réclame pourtant `id_rh` à
> l'entrée → la valeur est **jetée** faute de colonne où l'écrire.

### `trppu_site`
| Colonne | Type | Null | Défaut |
|---|---|:--:|---|
| co_regate | char(6) (PK) | non | — |
| lb_regate | **varchar(40)** | non | — *(API accepte 120 → #7)* |
| type_site | char(5) | non | — |
| co_roc | char(6) | non | — |
| dt_maj | datetime | non | CURRENT_TIMESTAMP ON UPDATE |

### Tables enfants supprimées par `DELETE /scenarios/{id}` (cascade applicative)

Dans l'ordre : `trppu_neutralisations`, `trppu_tmh`, `trppu_scenario_comptages_manuels`,
`trppu_scenario_exclusions`, `trppu_scenario_pic_coeffs`, `trppu_scenario_variations_prev`,
`trppu_trafic_agrebal`, `trppu_trafic_pdi`, `trppu_pic_version`, puis `trppu_scenario`.

Dans le dump 12/06/2026, seules **trois** FK sont `ON DELETE CASCADE` (`trppu_tmh`,
`trppu_neutralisations`, `trppu_scenario_variations_prev`). Pour les autres (comptages,
exclusions, pic_coeffs, trafic_agrebal…), il **n'y a pas de cascade SQL** : la suppression
**applicative** dans le code reste donc **indispensable** — et toute nouvelle table enfant devra y
être ajoutée manuellement, sous peine de lignes orphelines ou d'erreur de contrainte.

---

## Partie E — Recommandations (priorisées)

**🔴 Prioritaire**
1. **Duplication** : recalculer `nb_jours_ouvres/ouvrables/scenario` et renseigner
   `id_rh_creation/maj` lors du `POST /duplicate` (ou exécuter le même calcul que `POST /scenarios`),
   afin que le clone soit immédiatement exploitable.
2. **Traçabilité comptages** : ajouter la colonne `id_rh` à `trppu_scenario_comptages_manuels`
   (alignée sur ses tables sœurs), l'écrire à l'INSERT/UPDATE et l'inclure dans l'audit `id_rh`.
   C'est le seul manque de traçabilité restant au niveau base.
3. **Scripts de migration** : remettre `db_migrations/` en phase avec le dump 12/06/2026 — retirer
   la ligne `MODIFY COLUMN type …` (colonne inexistante) et matérialiser l'ajout de `id_rh` aux
   comptages, pour qu'un rejeu des migrations soit fiable.

**🟠 Important**
4. **Cohérence des jours** : recalculer les `nb_jours_*` dans `PATCH /periodes` et
   `PATCH /nb-jours-semaine` (aujourd'hui seul `PUT /scenarios` le fait).
5. **Site existant** : décider si `POST /scenarios` doit mettre à jour un site déjà présent, ou
   au minimum **signaler** dans la réponse que `lb_regate`/`type_site` ont été ignorés.
6. **Longueurs varchar** : aligner base et validation (`lb_scenario` 20↔50, `lb_regate` 40↔120).
7. **`id_rh` sur les modifications manuelles** : ajouter l'auteur au `PATCH /tmh/{co_produit}` et
   aux actions de cycle de vie (statut, archive, mise-en-prod).
8. **`dt_creation` variations** : ne pas l'écraser à chaque `PUT` (utiliser une colonne `dt_maj`).

**🟡 À arbitrer**
9. **Flags `trafic_*_calcule`** : soit les alimenter (processus de calcul), soit les retirer de la
   réponse API pour ne pas exposer une information toujours à 0.
10. **Neutralisations** : prévoir un repli (cache/retry) si l'API jours-fermés est indisponible.
11. **Tables legacy** : statuer sur `trppu_pic_coefficients_ko` (supprimer ou documenter) et sur
    `trppu_api_log` / `trppu_recalcul_log` (les alimenter ou les retirer du périmètre).

---

*Document de travail — à valider avec l'équipe avant publication définitive sur le wiki. Les
constats 🔴 ont été vérifiés directement dans le code et le schéma `03_db_12_06_2026.sql`
(dernière version connue de la structure de la base).*
