# Cartographie complète du système TRPPU — API, flux de données, base

> Générée le 12/06/2026 depuis le **code actuel** (working tree, branche `develop`).
> Schéma de référence : `db_migrations/db_10_09_2026.sql`. Conventions : `id_rh` est reçu **en clair**
> dans les payloads et stocké **crypté (Fernet, clé `ID_RH_CRYPTO_KEY`)** ; les champs *dérivés serveur*
> (périodes réalisé/prév, nb_jours, dt_pivot) sont calculés et jamais acceptés en entrée.

---

## Sommaire

1. [Architecture générale](#1-architecture-générale)
2. [Routes — Scénarios (`trppu_scenario`)](#2-scénarios)
3. [Routes — TMH (`trppu_tmh`)](#3-tmh)
4. [Routes — Comptages manuels](#4-comptages-manuels)
5. [Routes — Variations prévisionnelles](#5-variations-prévisionnelles)
6. [Routes — Neutralisations](#6-neutralisations)
7. [Routes — Coefficients PIC scénario](#7-coefficients-pic-scénario)
8. [Routes — Audit id_rh](#8-audit-id_rh)
9. [Routes — Trafics Databricks](#9-trafics-databricks)
10. [Routes — Calcul nombre de jours](#10-calcul-nombre-de-jours)
11. [Routes — Référentiel Sites](#11-référentiel-sites)
12. [Routes — Référentiel Produits](#12-référentiel-produits)
13. [Routes — Référentiel Versions PIC](#13-référentiel-versions-pic)
14. [Routes — Coefficients PIC nationaux (⚠️ module cassé)](#14-coefficients-pic-nationaux)
15. [Routes techniques (health, databricks, mysql, logs)](#15-routes-techniques)
16. [Schéma MySQL complet](#16-schéma-mysql-complet)
17. [Sources Databricks](#17-sources-databricks)
18. [Matrice routes → tables](#18-matrice-routes--tables)
19. [Correspondance routes ↔ tickets Jira](#19-correspondance-routes--tickets-jira)
20. [Données envoyées jamais persistées & axes d'amélioration](#20-données-envoyées-jamais-persistées--axes-damélioration)

---

## 1. Architecture générale

- **Framework** : FastAPI (async, uvicorn, port 8080). Docs : `/docs` (Swagger local), `/redoc`, `/openapi.json`.
- **Base MySQL** : 2 pools `aiomysql` distincts — `db_read` et `db_write` (hôtes/credentials séparés via env
  `SGBD_SERVER_READ/WRITE`, `SGBD_APP_USER_READ/WRITE`…), retry exponentiel (3 tentatives).
- **Databricks** : connexion OAuth M2M (service principal, `DATABRICKS_CLIENT_ID/SECRET`), catalogue/schéma
  configurables (`DATABRICKS_CATALOG`/`DATABRICKS_SCHEMA`), lecture seule.
- **API externe** : `JOURS_FERMES_API_BASE_URL` (jours fériés/fermés, basic auth, cache 24h) — utilisée pour
  tous les calculs de jours ouvrés/ouvrables. Indisponibilité → HTTP 503.
- **Middlewares** : CORS (paramétrable env), logging HTTP JSON (méthode, path, statut, durée).
- **Authentification** : aucune sur les routes. `id_session_ihm` (query optionnel) sert uniquement de tag
  de traçabilité Kibana.
- **Cryptage** : `id_rh` chiffré Fernet avant écriture (`ID_RH_CRYPTO_KEY` ; si vide → pass-through en clair).

### Routers montés (`app/main.py`)

| Préfixe | Module | Rôle |
|---|---|---|
| `/` , `/health` | health | Santé |
| `/databricks` | databricks | Debug Databricks |
| `/mysql` | mysql_debug | Debug/admin MySQL |
| `/trppu-api/trafics` | trafics | Trafics Databricks |
| `/trppu-api/calcl_nbr_jours` | calcl_nbr_jours | Calcul jours |
| `/logs` | logs | Gestion fichiers logs |
| `/trppu-api/sites` | trppu_site | Référentiel sites |
| `/trppu-api/produits` | trppu_produit | Référentiel produits |
| `/trppu-api/pic-versions` | trppu_pic_version | Référentiel versions PIC |
| `/trppu-api/pic-coefficients` | trppu_pic_coefficients | Référentiel coefficients PIC (⚠️ cassé) |
| `/trppu-api/scenarios` | trppu_scenario, trppu_tmh, trppu_comptages, trppu_variations, trppu_neutralisations, trppu_scenario_pic | Cœur métier |
| `/trppu-api/audit` | trppu_audit | Traçabilité id_rh |

---

## 2. Scénarios

Table maître : `trppu_scenario`. Statuts : `EN COURS → VALIDE → EN PRODUCTION → ARCHIVE` (machine à états stricte).
Chaque écriture incrémente `version_scenario`.

### `GET /trppu-api/scenarios`
- **Entrée** (query) : `co_regate?` (6 alphanum), `co_roc?` (6 alphanum), `statut?`, `est_fige?` (bool), `limit=100` (1–1000), `offset=0`.
- **Sortie 200** : `list[ScenarioOut]` :
```json
{
  "id_scenario": 42, "co_regate": "400300", "lb_scenario": "Scénario 2026 T1", "co_roc": "750001",
  "statut": "EN COURS", "dt_creation": "2026-06-10T09:15:00", "dt_validation": null,
  "dt_mise_en_oeuvre": "2026-06-10T00:00:00", "dt_mise_en_prod": null, "dt_real_prev": "2026-06-10T09:15:00",
  "periode_debut": "2025-06-10", "periode_fin": "2027-06-10",
  "periode_realise_debut": "2025-06-10", "periode_realise_fin": "2026-06-09",
  "periode_prev_debut": "2026-06-10", "periode_prev_fin": "2027-06-10",
  "nb_jours_semaine": 6, "nb_jours_ouvres": 521, "nb_jours_ouvrables": 626, "nb_jours_scenario": 626,
  "id_pic_version": 1, "version_scenario": 1, "est_fige": false,
  "trafic_pdi_calcule": false, "trafic_agrebal_calcule": false
}
```
- **SELECT** : `trppu_scenario` (25 colonnes : `id_scenario, co_regate, lb_scenario, co_roc, statut, dt_creation, dt_validation, dt_mise_en_oeuvre, dt_mise_en_prod, dt_pivot AS dt_real_prev, periode_debut, periode_fin, periode_realise_debut, periode_realise_fin, periode_prev_debut, periode_prev_fin, nb_jours_semaine, nb_jours_ouvres, nb_jours_ouvrables, nb_jours_scenario, id_pic_version, version_scenario, est_fige, trafic_pdi_calcule, trafic_agrebal_calcule`).
- **Écriture** : aucune.

### `GET /trppu-api/scenarios/enums`
- **Entrée** : aucune. **Sortie 200** : `{"statut": ["EN COURS","VALIDE","EN PRODUCTION","ARCHIVE"]}`.
- **Lu/Écrit** : rien (liste en dur, `statuts.py`).

### `GET /trppu-api/scenarios/{id_scenario}`
- **Entrée** : path `id_scenario` (int). **Sortie 200** : `ScenarioOut` (cf. ci-dessus). 404 si inexistant.
- **SELECT** : `trppu_scenario WHERE id_scenario` (25 colonnes). **Écriture** : aucune.

### `GET /trppu-api/scenarios/{id_scenario}/periodes` (DSR-655)
- **Entrée** : path `id_scenario`, query `id_session_ihm?`.
- **Sortie 200** : `ScenarioPeriodesOut` — `periode_debut, periode_fin, periode_realise_debut, periode_realise_fin, periode_prev_debut, periode_prev_fin, nb_jours_semaine, nb_jours_ouvres, nb_jours_ouvrables, nb_jours_scenario`.
- **SELECT** : `trppu_scenario`. **Écriture** : aucune.

### `GET /trppu-api/scenarios/{id_scenario}/edition` (DSR-654 — agrégateur)
- **Entrée** : path `id_scenario`, query `id_session_ihm?`.
- **Sortie 200** :
```json
{
  "scenario": { "...ScenarioOut..." },
  "periodes": { "periode_debut": "...", "periode_fin": "...", "nb_jours_scenario": 626, "..." : "..." },
  "tmh": [ { "co_produit": "OS", "volume_realise": 1000, "volume_previsionnel": 1100,
             "moyenne_journaliere": 38.46, "moyenne_hebdo": 230.77, "bl_exclu": false, "bl_manuel": false } ],
  "comptages": [ { "co_produit": "OS", "dt_comptage": "2026-06-01", "nb_produit": 120 } ],
  "variations": [ { "co_produit": "OS", "variation_pct": -2.50 } ],
  "neutralisations": [ { "id": 7, "dt_debut": "2026-07-14", "dt_fin": "2026-07-14", "nb_jour": 1, "motif": "Férié" } ],
  "pic": { "id_pic_version_defaut": 1, "id_pic_version_scenario": 12, "niveau_scenario": "SCENARIO",
           "coefficients": [ { "id_pic_version": 12, "co_produit": "OS", "jour_semaine": "LUNDI",
                               "densite": 0, "coef": 1.2000, "modifie": true } ] }
}
```
- **SELECT** : `trppu_scenario` · `trppu_tmh` (`co_produit, volume_realise, volume_previsionnel, moyenne_journaliere, moyenne_hebdo, bl_exclu, bl_manuel`) · `trppu_scenario_comptages_manuels` (`co_produit, dt_comptage, nb_produit`) · `trppu_scenario_variations_prev` (`co_produit, variation_pct`) · `trppu_neutralisations` (`id_neutralisation, dt_debut, dt_fin, nb_jour, motif`) · `trppu_pic_version` (`id_pic_version, niveau`) · `trppu_pic_coefficients` (`id_pic_version, co_produit, jour_semaine, densite, coef` — défaut national v1 + surcharge scénario, fusionnés, `modifie=true` si surchargé).
- **Écriture** : aucune.

### `POST /trppu-api/scenarios` (DSR-634) — **création**
- **Entrée** (body `ScenarioCreate`) :
```json
{
  "co_regate": "400300", "lb_scenario": "Mon scénario", "co_roc": "750001",
  "lb_regate": "PARIS 15 PDC", "type_site": "PDC",
  "nb_jours_semaine": 6, "id_pic_version": null,
  "periode_debut": null, "periode_fin": null, "dt_mise_en_oeuvre": null,
  "id_rh": "P123456",
  "tmh": [ { "co_produit": "OS", "volume_realise": 1000, "volume_previsionnel": 1100,
             "moyenne_journaliere": 38.46, "moyenne_hebdo": 230.77, "exclusion": false, "manuel": false } ]
}
```
  Contraintes : `co_regate`/`co_roc` 6 alphanum ; `lb_scenario` 1–50 ; `lb_regate` 1–120 ; `type_site` 1–5 ;
  `nb_jours_semaine` ∈ {5,6} ; `co_produit` 1–2 alphanum ; volumes ≥ 0 ; `id_rh` obligatoire.
- **Sortie 201** : `ScenarioOut`.
- **Calculs serveur** : `periode_debut/fin` défaut = aujourd'hui ±1 an ; bornes réalisé/prév dérivées du pivot ;
  `nb_jours_ouvres/ouvrables` via service jours fermés ; `nb_jours_scenario` = ouvrés (5j) ou ouvrables (6j) ;
  `id_pic_version` résolu par défaut si null ; `dt_mise_en_oeuvre` défaut = aujourd'hui ; `dt_pivot = NOW()`.
- **INSERT** : `trppu_scenario` (`co_regate, lb_scenario, co_roc, statut='EN COURS', dt_creation=NOW(), dt_mise_en_oeuvre, dt_pivot, periode_debut, periode_fin, periode_realise_debut, periode_realise_fin, periode_prev_debut, periode_prev_fin, nb_jours_semaine, nb_jours_ouvres, nb_jours_ouvrables, nb_jours_scenario, id_pic_version, version_scenario=1, est_fige=0, id_rh_creation*, id_rh_maj*`)
  · `trppu_site` (INSERT si absent : `co_regate, lb_regate, type_site, co_roc`)
  · `trppu_tmh` (upsert : `id_scenario, co_produit, volume_realise, volume_previsionnel, moyenne_journaliere, moyenne_hebdo, dt_calcul=NOW(), bl_exclu, bl_manuel, id_rh*`). `*` = crypté Fernet. Transaction unique.

### `PUT /trppu-api/scenarios/{id_scenario}` (DSR-656) — **mise à jour globale**
- **Entrée** (body `ScenarioMajRequest`) : `periode_debut` (date, oblig.), `periode_fin` (date ≥ debut, oblig.),
  `nb_jours_semaine` (5|6, oblig.), `dt_mise_en_oeuvre?`, `id_rh` (oblig.), `tmh[]` (optionnel, même forme que POST).
- **Précondition** : statut `EN COURS`, sinon 409.
- **Sortie 200** : `ScenarioOut`.
- **SELECT** : `trppu_scenario` · `trppu_neutralisations` (`SUM(nb_jour) WHERE id_scenario`) pour le net.
- **UPDATE** : `trppu_scenario` (`periode_debut, periode_fin, periode_realise_*, periode_prev_*, dt_pivot=NOW(), nb_jours_semaine, nb_jours_ouvres, nb_jours_ouvrables, nb_jours_scenario` (= brut − Σ neutralisations), `dt_maj=NOW(), id_rh_maj*`, `dt_mise_en_oeuvre?`, `version_scenario+1`)
  · `trppu_tmh` upsert si `tmh` fourni.

### `PATCH /trppu-api/scenarios/{id_scenario}/periodes`
- **Entrée** : `periode_debut?`, `periode_fin?` (au moins un ; fin ≥ début). Précondition : modifiable (ni archivé ni figé).
- **Sortie 200** : `ScenarioOut`.
- **UPDATE** `trppu_scenario` : `periode_debut, periode_fin` + bornes réalisé/prév recalculées + `version_scenario+1`.

### `PATCH /trppu-api/scenarios/{id_scenario}/nb-jours-semaine`
- **Entrée** : `{"nb_jours_semaine": 5|6}`. **Sortie 200** : `ScenarioOut`.
- **UPDATE** `trppu_scenario` : `nb_jours_semaine`, `version_scenario+1`.

### `PATCH /trppu-api/scenarios/{id_scenario}/statut`
- **Entrée** : `{"statut": "EN COURS"|"VALIDE"|"EN PRODUCTION"|"ARCHIVE"}`. Transition validée (409 si interdite ;
  `VALIDE → EN PRODUCTION` interdit ici → passer par `/mise-en-prod`).
- **Sortie 200** : `ScenarioOut`.
- **UPDATE** `trppu_scenario` : `statut` (+ si `VALIDE` : `dt_validation=COALESCE(dt_validation,NOW())`), `version_scenario+1`.

### `POST /trppu-api/scenarios/{id_scenario}/mise-en-prod`
- **Entrée** : path uniquement. Précondition : statut `VALIDE`.
- **Sortie 200** : `ScenarioOut`.
- **UPDATE** `trppu_scenario` : `statut='EN PRODUCTION', dt_validation=COALESCE(...,NOW()), dt_mise_en_prod=NOW(), est_fige=1`, `version_scenario+1`. Seule voie vers `EN PRODUCTION` ; fige automatiquement.

### `PATCH /trppu-api/scenarios/{id_scenario}/est-fige`
- **Entrée** : `{"est_fige": true|false}`. Précondition : non archivé. **Sortie 200** : `ScenarioOut`.
- **UPDATE** `trppu_scenario` : `est_fige`, `version_scenario+1`. Seul moyen de défiger après mise en prod.

### `PATCH /trppu-api/scenarios/{id_scenario}/figement` (DSR-669)
- **Entrée** : `{"statut": "validé"|"simulation"|"en cours"}` (libre, insensible casse/accents).
  Mapping : VALIDE→figé, SIMULATION→figé, EN COURS→défigé ; statut inconnu → 422.
- **Sortie 200** : `ScenarioOut`.
- **UPDATE** `trppu_scenario` : `est_fige` uniquement (le statut du scénario n'est PAS modifié), `version_scenario+1`.

### `PATCH /trppu-api/scenarios/{id_scenario}/lb-scenario`
- **Entrée** : `{"lb_scenario": "..."}` (1–50). **Sortie 200** : `ScenarioOut`.
- **UPDATE** `trppu_scenario` : `lb_scenario`, `version_scenario+1`.

### `POST /trppu-api/scenarios/{id_scenario}/archive`
- **Entrée** : path. Transition autorisée depuis EN COURS/VALIDE/EN PRODUCTION.
- **Sortie 200** : `ScenarioOut`. **UPDATE** `trppu_scenario` : `statut='ARCHIVE'`, `version_scenario+1`.

### `POST /trppu-api/scenarios/{id_scenario}/duplicate` (sans ticket DSR)
- **Entrée** : path + body optionnel `{"lb_scenario": "..."}`.
- **Sortie 201** : `ScenarioOut` du **nouveau** scénario.
- **SELECT** : `trppu_scenario` (source). **INSERT** `trppu_scenario` : copie de `co_regate, co_roc, periode_debut, periode_fin, periode_realise_*, periode_prev_*, nb_jours_semaine, id_pic_version` ; forcés : `statut='EN COURS', dt_creation=NOW(), version_scenario=1, est_fige=0` ; libellé = body ou `"<source> (copie)"` (tronqué à 50). **Les données enfants (TMH, comptages…) ne sont PAS dupliquées.**

### `DELETE /trppu-api/scenarios/{id_scenario}`
- **Entrée** : path. **Sortie** : 204 sans corps.
- **DELETE en cascade applicative** (transaction, dans l'ordre) : `trppu_neutralisations`, `trppu_tmh`, `trppu_scenario_comptages_manuels`, `trppu_scenario_exclusions`, `trppu_scenario_pic_coeffs`, `trppu_scenario_variations_prev`, `trppu_trafic_agrebal`, `trppu_trafic_pdi`, `trppu_pic_version` (versions du scénario), puis `trppu_scenario` — toutes `WHERE id_scenario = ?`. Hard delete.

---

## 3. TMH

Table : `trppu_tmh` (1 ligne par produit et par scénario, UNIQUE `(id_scenario, co_produit)`).

### `GET /trppu-api/scenarios/{id_scenario}/tmh` (DSR-650)
- **Entrée** : path `id_scenario`, query `id_session_ihm?`.
- **Sortie 200** : `list[TmhOut]`, trié par `co_produit` :
```json
[ { "co_produit": "OS", "volume_realise": 12000, "volume_previsionnel": 12500,
    "moyenne_journaliere": 46.15, "moyenne_hebdo": 276.92, "bl_exclu": false, "bl_manuel": false } ]
```
- **SELECT** : `trppu_tmh` (`co_produit, volume_realise, volume_previsionnel, moyenne_journaliere, moyenne_hebdo, bl_exclu, bl_manuel WHERE id_scenario`). **Écriture** : aucune.

### `PUT /trppu-api/scenarios/{id_scenario}/tmh` (DSR-648/659) — **upsert batch**
- **Entrée** (body `TmhBatchUpdate`) :
```json
{ "tmh": [ { "co_produit": "OS", "volume_realise": 12000, "volume_previsionnel": 12500,
             "moyenne_journaliere": 46.15, "moyenne_hebdo": 276.92,
             "exclusion": false, "manuel": false } ],
  "id_rh": "P123456" }
```
  (`tmh` min 1 élément ; volumes ≥ 0 ; décimales 12,2). Précondition : scénario modifiable.
- **Sortie 200** : `{"id_scenario": 42, "nb_inserted": 2, "nb_updated": 4}`.
- **INSERT/UPDATE** : `trppu_tmh` par couple `(id_scenario, co_produit)` —
  UPDATE : `volume_realise, volume_previsionnel, moyenne_journaliere, moyenne_hebdo, bl_exclu, bl_manuel, id_rh*, dt_calcul=NOW()` ;
  INSERT : mêmes colonnes + `id_scenario, co_produit`. Mapping `exclusion→bl_exclu`, `manuel→bl_manuel`. Transaction.

### `PATCH /trppu-api/scenarios/{id_scenario}/tmh/{co_produit}` (DSR-649)
- **Entrée** : path `id_scenario`, `co_produit` ; body : `volume_realise` (≥0, oblig.), `moyenne_journaliere`, `moyenne_hebdo` (oblig.). Précondition : modifiable ; 404 si ligne absente.
- **Sortie 200** : `TmhOut`.
- **UPDATE** : `trppu_tmh` (`volume_realise, moyenne_journaliere, moyenne_hebdo, bl_manuel=1, dt_calcul=NOW() WHERE id_scenario AND co_produit`). Marque la ligne comme modifiée manuellement.

---

## 4. Comptages manuels

Table : `trppu_scenario_comptages_manuels` (unicité applicative `(id_scenario, co_produit)`).

### `GET /trppu-api/scenarios/{id_scenario}/comptages` (DSR-653)
- **Entrée** : path `id_scenario`, query `id_session_ihm?`.
- **Sortie 200** : `[ { "co_produit": "OS", "dt_comptage": "2026-06-01", "nb_produit": 120 } ]` (tri produit, date).
- **SELECT** : `trppu_scenario_comptages_manuels` (`co_produit, dt_comptage, nb_produit WHERE id_scenario`) + `trppu_scenario` (contrôle 404). **Écriture** : aucune. `id_rh` jamais exposé en lecture.

### `POST /trppu-api/scenarios/{id_scenario}/comptages` (DSR-644)
- **Entrée** (body `ComptageCreate`) : `co_produit` (1–2 alphanum, oblig.), `dt_comptage?` (défaut serveur = aujourd'hui), `nb_produit` (int ≥ 0, oblig.), `id_rh` (oblig.).
- **Sortie 201** : `{ "co_produit": "OS", "dt_comptage": "2026-06-12", "nb_produit": 120 }`. 409 si un comptage existe déjà pour ce produit. Précondition : scénario modifiable.
- **SELECT** : `trppu_scenario_comptages_manuels` (`id_comptage` — contrôle doublon) + `trppu_scenario`.
- **INSERT** : `trppu_scenario_comptages_manuels` (`id_scenario, dt_comptage, co_produit, nb_produit, id_rh*` — id_rh si migration 002 appliquée).

### `PUT /trppu-api/scenarios/{id_scenario}/comptages/{co_produit}` (DSR-644)
- **Entrée** : `dt_comptage?` (défaut aujourd'hui), `nb_produit` (≥0, oblig.), `id_rh` (oblig.). 404 si absent.
- **Sortie 200** : même forme que POST.
- **UPDATE** : `trppu_scenario_comptages_manuels` (`dt_comptage, nb_produit WHERE id_scenario AND co_produit`). ⚠️ `id_rh` non remis à jour à l'UPDATE.

### `DELETE /trppu-api/scenarios/{id_scenario}/comptages/{co_produit}` (DSR-644)
- **Entrée** : path. **Sortie** : 204. 404 si rien supprimé.
- **DELETE** : `trppu_scenario_comptages_manuels WHERE id_scenario AND co_produit`.

---

## 5. Variations prévisionnelles

Table : `trppu_scenario_variations_prev` (UNIQUE `(id_scenario, co_produit)`). Convention : **0 % n'est jamais stocké**.

### `GET /trppu-api/scenarios/{id_scenario}/variations` (DSR-651)
- **Entrée** : path `id_scenario`, query `id_session_ihm?`.
- **Sortie 200** : `[ { "co_produit": "OS", "variation_pct": -2.50 } ]` (tri produit).
- **SELECT** : `trppu_scenario_variations_prev` (`co_produit, variation_pct WHERE id_scenario`) + `trppu_scenario`. **Écriture** : aucune.

### `PUT /trppu-api/scenarios/{id_scenario}/variations/{co_produit}` (DSR-646) — **upsert intelligent**
- **Entrée** : `variation_pct` (Decimal 5,2 — peut être négatif, oblig.), `id_rh` (oblig.). Précondition : modifiable.
- **Sortie 200** : `{ "co_produit": "OS", "variation_pct": -2.50, "action": "created"|"updated"|"deleted"|"noop" }`.
- **Logique / écritures** sur `trppu_scenario_variations_prev` :
  - `variation_pct == 0` → **DELETE** `WHERE id_scenario AND co_produit` (action `deleted`, ou `noop` si rien) ;
  - existe → **UPDATE** `variation_pct, id_rh*, dt_creation=NOW()` ;
  - absent → **INSERT** `id_scenario, co_produit, variation_pct, id_rh*` (`dt_creation` auto).

### `DELETE /trppu-api/scenarios/{id_scenario}/variations/{co_produit}` (DSR-646)
- **Entrée** : path. **Sortie** : 204. 404 si rien supprimé.
- **DELETE** : `trppu_scenario_variations_prev WHERE id_scenario AND co_produit`.

---

## 6. Neutralisations

Table : `trppu_neutralisations` (UNIQUE `(id_scenario, dt_debut, dt_fin)`).

### `GET /trppu-api/scenarios/{id_scenario}/neutralisations` (DSR-652)
- **Entrée** : path `id_scenario`, query `id_session_ihm?`.
- **Sortie 200** : `[ { "id": 7, "dt_debut": "2026-07-13", "dt_fin": "2026-07-17", "nb_jour": 4, "motif": "Travaux" } ]` (tri dates).
- **SELECT** : `trppu_neutralisations` (`id_neutralisation AS id, dt_debut, dt_fin, nb_jour, motif WHERE id_scenario`) + `trppu_scenario`. **Écriture** : aucune.

### `POST /trppu-api/scenarios/{id_scenario}/neutralisations` (DSR-645)
- **Entrée** (body `NeutralisationCreate`) : `dt_debut` (oblig.), `dt_fin` (≥ dt_debut, oblig.), `motif` (1–255, oblig.), `id_rh` (oblig.). Précondition : modifiable.
- **Sortie 201** : `{ "id": 8, "dt_debut": "...", "dt_fin": "...", "nb_jour": 4, "motif": "...", "action": "created" }`.
  Erreurs : 409 (période déjà existante), 422 (`nb_jour < 1` — aucune journée ouvrée), 503 (API jours fermés HS).
- **Calcul serveur** : `nb_jour` = 1 si jour unique, sinon nombre de jours ouvrés/ouvrables (selon `nb_jours_semaine` du scénario) hors fériés via API jours fermés.
- **SELECT** : `trppu_neutralisations` (contrôle doublon) + `trppu_scenario`.
- **INSERT** : `trppu_neutralisations` (`id_scenario, dt_debut, dt_fin, nb_jour, motif, id_rh*` ; `dt_creation` auto).

### `DELETE /trppu-api/scenarios/{id_scenario}/neutralisations` (DSR-645)
- **Entrée** : path `id_scenario` + query **obligatoires** `dt_debut`, `dt_fin` (jour unique : début = fin).
- **Sortie** : 204. 404 si rien supprimé.
- **DELETE** : `trppu_neutralisations WHERE id_scenario AND dt_debut AND dt_fin`.

---

## 7. Coefficients PIC scénario

Tables : `trppu_pic_version` (version par scénario, `niveau='SCENARIO'`) + `trppu_pic_coefficients`.

### `GET /trppu-api/scenarios/{id_scenario}/pic-coefficients` (DSR-660)
- **Entrée** : path `id_scenario`, query `id_session_ihm?`.
- **Sortie 200** :
```json
{ "id_pic_version_defaut": 1, "id_pic_version_scenario": 12, "niveau_scenario": "SCENARIO",
  "coefficients": [ { "id_pic_version": 12, "co_produit": "OS", "jour_semaine": "LUNDI",
                      "densite": 0, "coef": 1.2000, "modifie": true } ] }
```
  `jour_semaine` ∈ LUNDI…SAMEDI ; `densite` ∈ {0,1,2} ; `modifie=true` ssi surcharge scénario.
- **SELECT** : `trppu_pic_coefficients` (`id_pic_version, co_produit, jour_semaine, densite, coef` — version défaut 1 puis version scénario) · `trppu_pic_version` (`id_pic_version, niveau WHERE niveau='SCENARIO' AND id_scenario`). Fusion défaut + surcharges. **Écriture** : aucune.

### `PUT /trppu-api/scenarios/{id_scenario}/pic-coefficients` (DSR-661)
- **Entrée** (body `PicCoefUpsert`) : `co_produit` (1–2 alphanum), `jour_semaine` (LUNDI…SAMEDI), `densite` (0|1|2), `coef` (Decimal 7,4 ≥ 0), `id_rh` — tous obligatoires. Précondition : modifiable.
- **Sortie 200** : `{ "action": "update"|"insert_coef"|"insert_version_and_coef", "id_pic_version": 12 }`.
- **Écritures** (transaction) :
  - version scénario existante + coef existant `(id_pic_version, co_produit, jour_semaine, densite)` → **UPDATE** `trppu_pic_coefficients` (`coef, dt_maj=NOW(), id_rh*`) ;
  - version existante, coef absent → **INSERT** `trppu_pic_coefficients` (`id_pic_version, co_produit, jour_semaine, dt_effet=NOW(), coef, densite, id_rh*`) ;
  - pas de version scénario → **INSERT** `trppu_pic_version` (`lb_pic_version="{co_regate}_{id_scenario}", niveau='SCENARIO', co_regate, id_scenario, dt_activation=NOW(), id_rh_creation*, id_rh_maj*`) puis INSERT du coefficient.

---

## 8. Audit id_rh

### `POST /trppu-api/audit/actions-id-rh`
- **Entrée** : `{ "id_rh": "<token Fernet ou clair>", "cle": "<clé de déchiffrement>" }` (les deux obligatoires).
- **Sortie 200** :
```json
{ "id_rh": "P123456", "nb_actions": 3,
  "actions": [ { "ressource": "trppu_scenario", "action": "CREATION_SCENARIO", "id": 42,
                 "id_scenario": 42, "date": "2026-06-10T09:15:00", "details": { "co_regate": "400300" } } ] }
```
  Actions possibles : `CREATION_SCENARIO`, `MAJ_SCENARIO`, `CREATION_PIC_VERSION`, `MAJ_PIC_VERSION`,
  `ECRITURE_PIC_COEFFICIENT`, `NEUTRALISATION`, `ECRITURE_TMH`. Tri chronologique décroissant.
  Erreurs : 400 (clé/token invalide), 500.
- **SELECT (balayage exhaustif, déchiffrement ligne à ligne — Fernet non déterministe)** :
  - `trppu_scenario` : `id_scenario, co_regate, lb_scenario, statut, dt_creation, dt_maj, id_rh_creation, id_rh_maj` ;
  - `trppu_pic_version` : `id_pic_version, co_regate, id_scenario, niveau, dt_creation, dt_maj, id_rh_creation, id_rh_maj` ;
  - `trppu_pic_coefficients` : `id_pic_coef, id_pic_version, co_produit, jour_semaine, dt_maj, id_rh` ;
  - `trppu_neutralisations` : `id_neutralisation, id_scenario, motif, dt_debut, dt_fin, dt_creation, id_rh` ;
  - `trppu_tmh` : `id_tmh, id_scenario, co_produit, dt_calcul, id_rh`.
- **Écriture** : aucune (lecture seule, endpoint d'administration ; clé et id_rh jamais journalisés).

---

## 9. Trafics Databricks

Aucune écriture MySQL — lecture Databricks uniquement.

### `GET /trppu-api/trafics/get_trafics` (DSR-613)
- **Entrée** (query) : `co_regate` (oblig.), `date_debut`, `date_fin` (oblig., `AAAAMMJJ` ou `AAAA-MM-JJ`, plage max `MAX_DATE_RANGE_DAYS`=730 j), `limit?`.
- **Sortie 200** :
```json
{ "execution_time_s": 1.85, "co_regate": "400300", "date_debut": "2026-01-01", "date_fin": "2026-03-31",
  "count": 246,
  "data": [ { "co_regate": "400300", "lb_type_objet": "Objets suivis", "da_comptage": "2026-01-05",
              "co_mois_comptage": null, "co_semaine_comptage": null, "co_annee_comptage": 2026,
              "trafic_constate": 1200, "trafic_prevu": 1300, "...": "autres colonnes référentiel site" } ],
  "nb_jours": { "nbJoursOuvres": 63, "nbJoursOuvrables": 76 },
  "queries": ["..."] }
```
  (`queries` seulement si `DEBUG_SHOW_QUERY=true`.)
- **Lu (Databricks)** : découpage automatique de la plage en mois pleins / semaines pleines / jours :
  `gold.default.g_trppu_trafics_mois` (`co_mois_comptage, co_regate, lb_type_objet, nb_objet_retenu, nb_objet_prevu_recadre_bu` + référentiel site) ·
  `…_semaine` (`co_semaine_comptage, da_lundi_semaine_comptage`, idem) ·
  `…_jour` (`da_comptage`, idem). + API jours fermés pour `nb_jours`.

### `GET /trppu-api/trafics/get_trafics_pivot` (DSR-666)
- **Entrée** (query, tous requis) : `co_regate`, `date_debut`, `date_fin`, `date_pivot` (`AAAAMMJJ`).
- **Sortie 200** :
```json
{ "execution_time_s": 2.1, "co_regate": "400300",
  "date_debut": "2025-06-10", "date_fin": "2027-06-10", "date_pivot": "2026-06-10",
  "count": 6,
  "trafics": [ { "co_produit": "OO", "trafic_brut": 152000, "trafic_previsionnel": 148500 } ],
  "nb_jours": { "nbJoursOuvres": 521, "nbJoursOuvrables": 626 } }
```
  6 produits fixes : `OO, OS, PRESSE, PPI, COLIS, IP` (env `TRAFIC_PRODUITS`).
- **Lu (Databricks)** : mêmes 3 tables ; avant pivot → colonne `nb_objet_retenu` (constaté), à partir du pivot →
  `nb_objet_prevu_recadre_bu` (prévisionnel). Mapping `lb_type_objet → co_produit` via env `TRAFIC_PRODUIT_MAPPING`.

---

## 10. Calcul nombre de jours

### `GET /trppu-api/calcl_nbr_jours/get_nb_jours` (DSR-613)
- **Entrée** (query) : `date_debut`, `date_fin` (oblig., `AAAAMMJJ` ou `AAAA-MM-JJ`).
- **Sortie 200** :
```json
{ "date_debut": "2026-01-01", "date_fin": "2026-03-31", "nb_jours_total": 90,
  "nb_jours_ouvres_bruts": 64, "nb_jours_ouvrables_bruts": 77,
  "nb_feries_hors_weekend": 1, "nb_feries_samedi": 1,
  "nbJoursOuvres": 63, "nbJoursOuvrables": 76, "execution_time_ms": 12.4 }
```
- **Lu** : API externe jours fermés uniquement (cache 24h). **Aucune base** lue ni écrite.

---

## 11. Référentiel Sites

Table : `trppu_site` (PK `co_regate`).

### `GET /trppu-api/sites` · `GET /trppu-api/sites/{co_regate}`
- **Entrée** : liste — query `type_site?` (1–5), `co_roc?` (6), `limit=100`, `offset=0` ; détail — path `co_regate` (6 alphanum).
- **Sortie 200** : `{ "co_regate": "400300", "lb_regate": "PARIS 15 PDC", "type_site": "PDC", "co_roc": "750001", "dt_maj": "..." }` (liste : tableau). 404 si détail introuvable.
- **SELECT** : `trppu_site` (`co_regate, lb_regate, type_site, co_roc, dt_maj`). **Écriture** : aucune.

### `POST /trppu-api/sites`
- **Entrée** : `co_regate` (6, oblig.), `lb_regate?` (≤120), `type_site` (1–5, oblig.), `co_roc` (6, oblig.).
- **Sortie 201** : `SiteOut`. 409 si existe.
- **INSERT** : `trppu_site` (`co_regate, lb_regate, type_site, co_roc`) — `dt_maj` auto.

### `PUT /trppu-api/sites/{co_regate}`
- **Entrée** : au moins un de `lb_regate?, type_site?, co_roc?`. **Sortie 200** : `SiteOut`. 400/404.
- **UPDATE** : `trppu_site` (champs fournis parmi `lb_regate, type_site, co_roc`).

### `POST /trppu-api/sites/upload-excel`
- **Entrée** : fichier `.xlsx/.xlsm` (multipart). Colonnes : `co_regate, type_site, co_roc` (oblig.), `lb_regate` (opt.). Normalisation : padding 6 chars.
- **Sortie 200** : `{ "nb_rows_read", "nb_inserted", "nb_updated", "nb_unchanged", "nb_errors", "errors": [{"row", "error", "raw"}], "execution_time_s" }`.
- **Écriture** : `trppu_site` — `INSERT … ON DUPLICATE KEY UPDATE lb_regate, type_site, co_roc`.

---

## 12. Référentiel Produits

Table : `trppu_produit` (PK `co_produit`). Suppression = **soft delete** (désactivation).

### `GET /trppu-api/produits` · `GET /trppu-api/produits/{co_produit}`
- **Entrée** : liste — `actif_only=false`, `limit=100`, `offset=0` ; détail — path `co_produit` (2 alphanum).
- **Sortie 200** : `{ "co_produit": "OS", "lb_produit": "Objets suivis", "dt_creation": "...", "dt_desactivation": null, "motif_desactivation": null }`.
- **SELECT** : `trppu_produit` (`co_produit, lb_produit, dt_creation, dt_desactivation, motif_desactivation` ; `actif_only` → `dt_desactivation IS NULL OR > CURDATE()`).

### `POST /trppu-api/produits`
- **Entrée** : `co_produit` (2, oblig.), `lb_produit` (1–80, oblig.), `dt_desactivation?`, `motif_desactivation?` (≤255).
- **Sortie 201**. 409 si existe. **INSERT** : `trppu_produit` (`co_produit, lb_produit, dt_desactivation, motif_desactivation`).

### `PUT /trppu-api/produits/{co_produit}`
- **Entrée** : au moins un de `lb_produit?, dt_desactivation?, motif_desactivation?`. **Sortie 200**.
- **UPDATE** : `trppu_produit` (champs fournis).

### `DELETE /trppu-api/produits/{co_produit}` (soft delete)
- **Entrée** : path + query `motif` (défaut `"Désactivé via API"`).
- **Sortie 200** : `{ "co_produit", "dt_desactivation": "<today>", "motif_desactivation", "rows_affected" }`.
- **UPDATE** : `trppu_produit` (`dt_desactivation=today, motif_desactivation`). Pas de DELETE physique.

### `POST /trppu-api/produits/upload-excel`
- **Entrée** : Excel — colonnes `co_produit, lb_produit` (oblig.), `dt_desactivation, motif_desactivation` (opt.).
- **Sortie** : même rapport que sites. **Écriture** : `trppu_produit` — `INSERT … ON DUPLICATE KEY UPDATE lb_produit, dt_desactivation, motif_desactivation`.

---

## 13. Référentiel Versions PIC

Table : `trppu_pic_version`.

### `GET /trppu-api/pic-versions` · `/{id_pic_version}` · `/enums`
- **Entrée** : liste — `co_regate?` (6), `niveau?` (NATIONAL|DEX|SITE), `actif_only=false`, `est_par_defaut?`, `limit=100`, `offset=0` ; enums — aucune (`{"niveau": ["NATIONAL","DEX","SITE"]}`).
- **Sortie 200** :
```json
{ "id_pic_version": 1, "lb_pic_version": "PIC nationale 2026", "niveau": "NATIONAL", "co_regate": "000000",
  "dt_activation": "2026-01-01T00:00:00", "dt_desactivation": null, "motif_desactivation": null,
  "commentaire": null, "est_par_defaut": true, "dt_creation": "...", "dt_maj": "...",
  "id_rh_creation": null, "id_rh_maj": null }
```
- **SELECT** : `trppu_pic_version` (13 colonnes ci-dessus). ⚠️ L'API n'expose pas le niveau `SCENARIO` ni `id_scenario` pourtant présents en base.

### `POST /trppu-api/pic-versions`
- **Entrée** : `niveau` (oblig.), `co_regate` (6, oblig.), `dt_activation` (oblig.), `lb_pic_version?` (≤80), `dt_desactivation?` (> activation), `motif_desactivation?`, `commentaire?` (≤500), `est_par_defaut=false`.
- **Sortie 201**. **INSERT** : `trppu_pic_version` (`lb_pic_version, niveau, co_regate, dt_activation, dt_desactivation, motif_desactivation, commentaire, est_par_defaut`).

### `PUT /trppu-api/pic-versions/{id_pic_version}`
- **Entrée** : tous champs optionnels (au moins un) ; validation dates. **Sortie 200**. 400/404/422.
- **UPDATE** : `trppu_pic_version` (champs fournis).

### `DELETE /trppu-api/pic-versions/{id_pic_version}` (soft delete)
- **Entrée** : path + query `motif` (défaut `"Désactivé via API"`). Refus 422 si `NOW() <= dt_activation`.
- **Sortie 200** : `{ "id_pic_version", "dt_desactivation": "<now>", "motif_desactivation", "rows_affected" }`.
- **UPDATE** : `trppu_pic_version` (`dt_desactivation, motif_desactivation`).

### `POST /trppu-api/pic-versions/upload-excel`
- **Entrée** : Excel — `niveau, co_regate, dt_activation` (oblig.) ; `lb_pic_version, dt_desactivation, motif_desactivation, commentaire, est_par_defaut` (opt.).
- **Sortie** : rapport (INSERT-only : `nb_updated`/`nb_unchanged` toujours 0). **INSERT** : `trppu_pic_version` (8 colonnes).

---

## 14. Coefficients PIC nationaux

### ⚠️ MODULE CASSÉ — `app/routes/trppu_pic_coefficients/`

Le code de ce module CRUD utilise un **ancien schéma** qui ne correspond plus à la table `trppu_pic_coefficients` actuelle :

| | Code API | Base réelle (`db_10_09_2026.sql`) |
|---|---|---|
| Coefs | `coef_dense, coef_faible1, coef_faible2` (3 colonnes) | `coef` (1 colonne) + `densite` (0/1/2) |
| Fin d'effet | `dt_fin_effet` (date) | `dt_fin` (datetime) |
| `jour_semaine` | `LUN…SAM` | `LUNDI…SAMEDI` |
| Clé unique | `(id_pic_version, co_produit, jour_semaine, dt_effet)` | `(id_pic_version, co_produit, jour_semaine, densite)` |

**Conséquence : tous les SELECT/INSERT/UPDATE de ce module échouent en SQL** (colonnes inexistantes). L'ancien
schéma survit dans la table legacy `trppu_pic_coefficients_ko`. Routes déclarées (théoriques) :
`GET /trppu-api/pic-coefficients` (+ `/{id}`, `/enums`), `POST`, `PUT /{id}`, `DELETE /{id}` (soft delete via `dt_fin_effet`), `POST /upload-excel`.
La gestion **fonctionnelle** des coefficients passe par les routes scénario (§7).

---

## 15. Routes techniques

| Route | Entrée | Sortie | Lu | Écrit |
|---|---|---|---|---|
| `GET /` | — | `{"message": "Bienvenue sur l'API trppu"}` | — | — |
| `GET /health` | — | `{"status", "mysql_config", "databricks_config"}` | env vars | — |
| `GET /health/resources` | — | `{"status", "mysql_read", "mysql_write", "databricks"}` | `SELECT 1` sur les 3 pools | — |
| `GET /databricks/test` | — | `{"test": "ok", "execution_time_s", "result"}` | Databricks `SELECT 1` | — |
| `GET /mysql/test` | — | idem | MySQL `SELECT 1` | — |
| `GET /mysql/tables` | — | liste tables + types + nb lignes | `information_schema.tables` | — |
| `GET /mysql/columns` | `table` | colonnes (type, nullable, clé, défaut) | `information_schema.columns` | — |
| `GET /mysql/indexes` | `table` | index | `SHOW INDEX` | — |
| `GET /mysql/sample` | `table`, `limit=10` (≤100) | lignes brutes | `SELECT * LIMIT` | — |
| `GET /mysql/schema` | — | schéma complet (tables + colonnes) | `information_schema` | — |
| `GET /mysql/dump` | `fmt=sql\|json`, `drop=true` | DDL complet | `SHOW CREATE TABLE/VIEW` | — |
| `GET /mysql/export` | `table`, `fmt=json\|sql`, `truncate` | données complètes | `SELECT *` | — |
| `POST /mysql/import` ⚠️ | `{table, rows[], columns?, truncate=true}` | `{inserted, truncated, ...}` | `information_schema.columns` | **TRUNCATE + INSERT par lots de 500** sur table arbitraire, `FOREIGN_KEY_CHECKS=0` |
| `GET /logs/latest` | — | fichier .log (download) | FS local | — |
| `DELETE /logs` | `keep_today=false` | `{deleted[], truncated[], errors[]}` | FS local | suppression fichiers logs |

⚠️ Les routes `/mysql/*` (surtout `import`, `dump`, `export`) sont des routes de **debug/admin sans authentification** — à ne pas exposer en production.

---

## 16. Schéma MySQL complet

Base : `dsr_mercure_aa` (env `SGBD_DB_NAME`). Source : `db_migrations/db_10_09_2026.sql`.

### Tables cœur métier

**`trppu_scenario`** (PK `id_scenario` bigint AI) — scénario de simulation par site :
`co_roc` char(6), `co_regate` char(6), `lb_scenario` varchar(20)⚠️(API accepte 50), `statut` enum(EN COURS,VALIDE,EN PRODUCTION,ARCHIVE), `dt_creation`, `dt_validation`, `dt_mise_en_oeuvre`, `dt_mise_en_prod`, `dt_pivot`, `periode_debut/fin` date, `periode_realise_debut/fin` date, `periode_prev_debut/fin` date, `nb_jours_semaine` smallint(5|6), `nb_jours_ouvres/ouvrables/scenario` smallint, `id_pic_version` int, `version_scenario` int, `est_fige` smallint, `dt_maj`, `id_rh_creation` varchar(255) (crypté), `id_rh_maj` varchar(255) (crypté), `trafic_pdi_calcule`, `trafic_agrebal_calcule` smallint. Index `(co_regate, statut)`.

**`trppu_tmh`** (PK `id_tmh` bigint AI ; UNIQUE `(id_scenario, co_produit)` ; FK → scenario CASCADE, produit RESTRICT) :
`id_scenario`, `co_produit` char(2), `volume_realise` int, `volume_previsionnel` int, `moyenne_journaliere` decimal(12,2), `moyenne_hebdo` decimal(12,2), `dt_calcul`, `bl_exclu` tinyint, `bl_manuel` tinyint, `id_rh` varchar(255) (crypté).

**`trppu_scenario_comptages_manuels`** (PK `id_comptage` int AI ; FK → scenario) :
`id_scenario`, `dt_comptage` date, `co_produit` char(2), `nb_produit` int. (⚠️ `id_rh` ajouté par migration applicative, absent du dump.)

**`trppu_scenario_variations_prev`** (PK `id_variation` int AI ; UNIQUE `(id_scenario, co_produit)` ; FK → scenario) :
`id_scenario`, `co_produit` char(2), `variation_pct` decimal(5,2). (⚠️ `id_rh`/`dt_creation` : migration `004_add_variations_tracabilite.sql`.)

**`trppu_neutralisations`** (PK `id_neutralisation` bigint AI ; UNIQUE `(id_scenario, dt_debut, dt_fin)` ; FK → scenario CASCADE) :
`id_scenario`, `dt_debut` date, `dt_fin` date, `nb_jour` int CHECK>0, `motif` varchar(255), `dt_creation`, `id_rh` varchar(255) (crypté).

**`trppu_pic_version`** (PK `id_pic_version` int AI) :
`lb_pic_version` varchar(80), `niveau` enum(NATIONAL,DEX,SITE,**SCENARIO**), `co_regate` char(6), `id_scenario` bigint, `dt_activation`, `dt_desactivation`, `motif_desactivation` varchar(255), `commentaire` varchar(500), `est_par_defaut` tinyint, `dt_creation`, `dt_maj`, `id_rh_creation` varchar(40) (crypté), `id_rh_maj` varchar(40) (crypté).

**`trppu_pic_coefficients`** (PK `id_pic_coef` bigint AI ; UNIQUE `(id_pic_version, co_produit, jour_semaine, densite)` ; FK → pic_version CASCADE, produit RESTRICT) :
`id_pic_version`, `co_produit` char(2), `jour_semaine` enum(LUNDI…SAMEDI), `dt_effet` datetime, `dt_fin` datetime, `coef` decimal(7,4) CHECK≥0, `densite` tinyint CHECK∈(0,1,2), `dt_creation`, `dt_maj`, `id_rh` varchar(40) (crypté).

### Référentiels

**`trppu_site`** (PK `co_regate` char(6)) : `lb_regate` varchar(40)⚠️(API accepte 120), `type_site` char(5), `co_roc` char(6), `dt_maj`. Index `co_roc`.

**`trppu_produit`** (PK `co_produit` char(2)) : `lb_produit` varchar(80), `dt_creation`, `dt_desactivation` date, `motif_desactivation` varchar(255).

### Tables de calcul / restitution (alimentées par batchs, non par l'API)

**`trppu_trafic_agrebal`** (PK `id` bigint AI ; FK → scenario) : `id_scenario, co_regate, id_agrebal, co_produit, jour_semaine` enum(LUN…SAM), `couleur_pic` enum(DENSE,FAIBLE1,FAIBLE2), `volume` decimal(12,4).

**`trppu_trafic_pdi`** (PK composite `(id_trafic_pdi, id_scenario)`) : `co_regate, id_agrebal, id_pdi, co_produit, jour_semaine, couleur_pic, volume` int CHECK≥0, `dt_calcul`, `id_calcul_batch`.

**`trppu_agrebal_pdi`** (PK `(id_agrebal, id_pdi)`) : `co_regate` — table de liaison.

**`trppu_cles_repartition`** (PK `id_pdi` bigint) : `pdi_rattache`, 9 colonnes `trafic_*` decimal(10,8), `nature` char(3), `co_regate_site`, `type_site`, `lb_regate`, `co_regate_etablissement`, `lb_etablissement`, `co_regate_dex`, `lb_dex`.

**`trppu_scenario_exclusions`** (PK `id` ; UNIQUE `(id_scenario, co_produit)`) : `motif` — touchée seulement par le DELETE scénario.

**`trppu_scenario_pic_coeffs`** (PK `id` ; UNIQUE `(id_scenario, co_produit, jour_semaine)`) : `coef_dense, coef_faible1, coef_faible2` decimal(8,5) — legacy, touchée seulement par le DELETE scénario.

### Tables de log / hors API

**`trppu_api_log`** (PK `id_log` ; FK → scenario) : `api_name, id_scenario, regate, dt_appel, caller, params` json.
**`trppu_recalcul_log`** (PK `id_log` ; FK → scenario) : `id_agrebal, dt_recalcul, raison` enum, `commentaire`.
**`trppu_pic_coefficients_ko`** : legacy (ancien schéma coefficients — cf. §14).
**`demande_dsr`** (PK `id`) : workflow de demandes DSR (`nomFichier, statut` enum, `idrh` char(7), `codeRegate`, `bassins` json, flags simulation…) — hors périmètre API actuelle.

---

## 17. Sources Databricks

Catalogue/schéma : env `DATABRICKS_CATALOG`/`DATABRICKS_SCHEMA` (défauts `gold`.`default` ; prod : `ppd_dd_kairos_int`.`03_gold`). **Lecture seule.**

| Table | Granularité | Colonnes clés utilisées |
|---|---|---|
| `g_trppu_trafics_jour` | jour | `da_comptage`, `co_regate`, `lb_type_objet`, `nb_objet_retenu` (constaté), `nb_objet_prevu_recadre_bu` (prévisionnel), `co_annee_comptage` + ~15 colonnes référentiel site (`lb_entite`, `co_departement`, `lb_zone_ferie`, `lb_zone_vacances`, `co_regate_pic`…) |
| `g_trppu_trafics_semaine` | semaine | `co_semaine_comptage`, `da_lundi_semaine_comptage` + idem |
| `g_trppu_trafics_mois` | mois | `co_mois_comptage` + idem |

Les colonnes trafic sont configurables : `TRAFIC_COL_OBJET`, `TRAFIC_COL_CONSTATE`, `TRAFIC_COL_PREVISIONNEL`.

---

## 18. Matrice routes → tables

| Route | trppu_scenario | trppu_tmh | comptages | variations | neutralisations | pic_version | pic_coefficients | trppu_site | trppu_produit | Databricks |
|---|---|---|---|---|---|---|---|---|---|---|
| GET scenarios (+/{id}, /periodes) | **R** | | | | | | | | | |
| GET /edition | R | R | R | R | R | R | R | | | |
| POST scenarios | **W** | W | | | | | | W(si absent) | | |
| PUT scenario | **W** | W | | | R | | | | | |
| PATCH periodes/nb-jours/statut/lb/est-fige/figement | **W** | | | | | | | | | |
| POST mise-en-prod / archive | **W** | | | | | | | | | |
| POST duplicate | R+**W** | | | | | | | | | |
| DELETE scenario | **D** | D | D | D | D | D | (via version) | | | + exclusions, scenario_pic_coeffs, trafic_agrebal, trafic_pdi |
| GET/PUT/PATCH tmh | R | **R/W** | | | | | | | | |
| GET/POST/PUT/DELETE comptages | R | | **R/W/D** | | | | | | | |
| GET/PUT/DELETE variations | R | | | **R/W/D** | | | | | | |
| GET/POST/DELETE neutralisations | R | | | | **R/W/D** | | | | | |
| GET/PUT pic-coefficients (scénario) | R | | | | | R/**W** | R/**W** | | | |
| POST audit | R | R | | | R | R | R | | | |
| GET trafics / trafics_pivot | | | | | | | | | | **R** |
| CRUD sites (+excel) | | | | | | | | **R/W** | | |
| CRUD produits (+excel) | | | | | | | | | **R/W** | |
| CRUD pic-versions (+excel) | | | | | | **R/W** | | | | |
| CRUD pic-coefficients nationaux | | | | | | | ⚠️ cassé | | | |

R = SELECT, W = INSERT/UPDATE, D = DELETE.

---

## 19. Correspondance routes ↔ tickets Jira

Références extraites des docstrings du code (`app/routes/**`) et des fiches `jira/v2/*.md`.
`—` = aucun ticket DSR identifié (socle technique ou route hors chantier DSR).

### Tickets et routes associées

| Ticket | Objet (résumé) | Routes |
|---|---|---|
| **DSR-613** | RecupererTrafics renvoie aussi le nb de jours ouvrés/ouvrables | `GET /trafics/get_trafics`, `GET /calcl_nbr_jours/get_nb_jours` (+ réutilisé par POST/PUT scénario) |
| **DSR-634** | Création d'un scénario (enregistrement en base, TMH inclus) | `POST /scenarios` |
| **DSR-644** | Écriture des comptages manuels | `POST`, `PUT`, `DELETE /scenarios/{id}/comptages[/{co_produit}]` |
| **DSR-645** | MAJ en base des jours à neutraliser d'une période | `POST`, `DELETE /scenarios/{id}/neutralisations` |
| **DSR-646** | MAJ des variations du trafic prévisionnel | `PUT`, `DELETE /scenarios/{id}/variations/{co_produit}` |
| **DSR-648** | Enregistrement des informations de trafic d'un scénario | `PUT /scenarios/{id}/tmh` (écriture initiale ; flag `bl_manuel` cf. DSR-665) |
| **DSR-649** | MAJ des trafics en cas de modification des trafics initiaux | `PATCH /scenarios/{id}/tmh/{co_produit}` |
| **DSR-650** | Lecture des trafics TMH d'un scénario | `GET /scenarios/{id}/tmh` |
| **DSR-651** | Lecture du paramétrage prévisionnel d'un scénario | `GET /scenarios/{id}/variations` |
| **DSR-652** | Lecture des neutralisations (liste à plat) | `GET /scenarios/{id}/neutralisations` |
| **DSR-653** | Lecture des comptages manuels | `GET /scenarios/{id}/comptages` |
| **DSR-654** | Agrégateur d'édition (tous les blocs en un appel) | `GET /scenarios/{id}/edition` |
| **DSR-655** | Récupération des périodes définies à la création | `GET /scenarios/{id}/periodes` |
| **DSR-656** | MAJ d'un scénario modifié par l'utilisateur | `PUT /scenarios/{id}` |
| **DSR-659** | MAJ des trafics d'un scénario modifié (upsert batch) | `PUT /scenarios/{id}/tmh` (réutilisé par DSR-634/656) |
| **DSR-660** | Lecture du paramétrage de rétention PIC | `GET /scenarios/{id}/pic-coefficients` |
| **DSR-661** | Enregistrement de la rétention PIC à chaque modification | `PUT /scenarios/{id}/pic-coefficients` |
| **DSR-665** | Marquage exclu / ajouté manuellement (`bl_exclu`/`bl_manuel`) | porté par `PUT`/`PATCH /scenarios/{id}/tmh` |
| **DSR-666** | Trafics Databricks avec date pivot | `GET /trafics/get_trafics_pivot` |
| **DSR-669** | Activer/désactiver les changements d'un scénario (figement) | `PATCH /scenarios/{id}/figement` |

### Routes sans ticket DSR identifié

| Route | Statut |
|---|---|
| `GET /scenarios`, `GET /scenarios/{id}`, `GET /scenarios/enums` | — (socle lecture scénario) |
| `PATCH /scenarios/{id}/periodes`, `/nb-jours-semaine`, `/statut`, `/lb-scenario`, `/est-fige` | — (cycle de vie, antérieur au chantier) |
| `POST /scenarios/{id}/mise-en-prod`, `/archive`, `/duplicate`, `DELETE /scenarios/{id}` | — |
| `POST /audit/actions-id-rh` | — (transverse traçabilité/RGPD) |
| CRUD `/sites`, `/produits`, `/pic-versions`, `/pic-coefficients` (+ upload-excel) | — (socle référentiels) |
| `/health*`, `/databricks/test`, `/mysql/*`, `/logs*` | — (technique) |

---

## 20. Données envoyées jamais persistées & axes d'amélioration

> Analyse du 12/06/2026, vérifiée directement dans le code (pas seulement déduite de la doc).

### 20.1 Données envoyées et JAMAIS persistées (confirmé code)

| # | Route(s) | Donnée perdue | Détail | Gravité |
|---|---|---|---|---|
| 1 | `POST` / `PUT /scenarios/{id}/comptages` (DSR-644) | **`id_rh`** | Champ **obligatoire** dans `ComptageCreate`/`ComptageUpdate` (annoté « crypté en base », `trppu_comptages/schemas.py:19`), mais l'INSERT n'écrit que `id_scenario, dt_comptage, co_produit, nb_produit` (`routes.py:77-82`) et l'UPDATE que `dt_comptage, nb_produit` (`routes.py:127-132`). Cause racine : colonne `id_rh` absente de `trppu_scenario_comptages_manuels` dans le schéma prod. Conséquence : les saisies de comptages sont **intraçables** — l'audit `POST /audit/actions-id-rh` ne balaye d'ailleurs pas cette table. | 🔴 |
| 2 | `POST`, `PUT`, `upload-excel /pic-coefficients` (national) | **Tout le payload** | Module cassé (cf. §14) : les colonnes ciblées (`coef_dense/faible1/faible2`, `dt_fin_effet`) n'existent plus → toute écriture échoue en 500. Données envoyées, jamais persistées, par construction. | 🔴 |
| 3 | `POST /scenarios` (DSR-634) | `lb_regate`, `type_site` (si le site existe) | Utilisés uniquement pour créer `trppu_site` s'il est absent (`helpers.py:161-184`) ; si le site existe, ignorés **silencieusement** (documenté dans le schéma mais aucune indication dans la réponse : l'appelant ne sait pas si ses libellés divergent de la base). | 🟠 |
| 4 | `PUT /scenarios/{id}/comptages/{co_produit}` (DSR-644) | `dt_comptage` d'origine | `dt_comptage` optionnel avec défaut serveur `date.today()` (`routes.py:118`) : un PUT qui ne veut modifier que `nb_produit` **écrase la date de comptage réelle** par la date du jour. Le défaut devrait être « conserver la valeur existante ». | 🟠 |
| 5 | `PATCH /scenarios/{id}/figement` (DSR-669) | `statut` (en tant que statut) | Le statut reçu ne sert qu'à dériver `est_fige` ; le statut du scénario n'est jamais modifié. By design, mais source de confusion probable côté IHM (un PATCH « statut » qui ne change pas le statut). | 🟡 |
| 6 | Toutes routes scénario | `id_session_ihm` | Uniquement loggé (traçabilité Kibana), jamais stocké — alors que la table `trppu_api_log` existe en base et n'est alimentée par **aucune** route. Soit alimenter `trppu_api_log`, soit supprimer la table. | 🟡 |

### 20.2 L'inverse : actions NON traçables (id_rh jamais demandé alors que la base le prévoit)

| Route(s) | Problème |
|---|---|
| `PATCH /scenarios/{id}/tmh/{co_produit}` (DSR-649) | Pas d'`id_rh` dans `TmhVolumeUpdate` (`trppu_tmh/schemas.py:56-62`) alors que c'est une **modification manuelle** (la ligne passe `bl_manuel=1` sans auteur) et que le PUT batch, lui, l'exige. Incohérent. |
| `POST /duplicate`, `/archive`, `/mise-en-prod`, `PATCH /statut`, `/periodes`, `/nb-jours-semaine`, `/lb-scenario`, `/est-fige`, `/figement`, `DELETE /scenarios/{id}` | Aucun `id_rh` : tout le cycle de vie scénario est anonyme. `duplicate` crée même un scénario avec `id_rh_creation=NULL`. Incohérent avec les créations DSR-634/645/646 qui sont tracées. |
| `DELETE` comptages / variations / neutralisations | Suppressions anonymes — aucune trace de qui a supprimé quoi. |
| CRUD `/pic-versions` | La table a `id_rh_creation`/`id_rh_maj` mais l'API référentiel ne les accepte jamais → toujours NULL par ce canal. |
| `PUT variations` (DSR-646) | L'UPDATE écrase `dt_creation = NOW()` : la date de création réelle est perdue à chaque modification. Il faudrait une colonne `dt_maj` distincte (cf. migration `004_add_variations_tracabilite.sql`). |

### 20.3 Autres améliorations recommandées

1. **Unicité comptages non garantie en base** : pas de contrainte UNIQUE sur `(id_scenario, co_produit)` — seulement un SELECT-puis-INSERT applicatif → doublon possible en cas d'appels concurrents. Ajouter la contrainte UNIQUE (comme variations/neutralisations l'ont déjà).
2. **Écarts de longueurs base/API** : `trppu_scenario.lb_scenario` varchar(20) vs 50 accepté par l'API ; `trppu_site.lb_regate` varchar(40) vs 120 → un libellé valide côté Pydantic casse en SQL strict mode. Aligner (élargir les colonnes ou resserrer les schémas).
3. **`/mysql/*` et `/logs` sans authentification**, avec écriture destructive (`POST /mysql/import` = TRUNCATE + INSERT avec `FOREIGN_KEY_CHECKS=0` sur table arbitraire). À désactiver/protéger hors dev.
4. **Aucune authentification** sur l'API ; `id_rh` transite en clair dans les payloads (chiffré uniquement au repos).
5. **DELETE scénario** : cascade applicative manuelle sur 9 tables enfants — toute nouvelle table enfant devra y être ajoutée à la main (risque d'oubli ; envisager des FK `ON DELETE CASCADE`).
6. **`trppu_pic_version`** : l'API référentiel ignore le niveau `SCENARIO` et la colonne `id_scenario` (gérés uniquement par les routes scénario §7) — les listes référentiel peuvent donc remonter des versions scénario sans contexte.
7. **Dépendance dure à l'API jours fermés** : `POST`/`PUT /scenarios` et `POST /neutralisations` tombent en 503 si l'API externe est indisponible (pas de fallback ni retry différé).
8. **Défauts silencieux du `POST /scenarios`** : périodes par défaut ±1 an et `dt_mise_en_oeuvre=today` appliqués sans que la réponse signale qu'il s'agit de valeurs par défaut — à documenter côté IHM.

### Priorités proposées

1. 🔴 Ajouter `id_rh` (colonne + INSERT/UPDATE + balayage audit) sur `trppu_scenario_comptages_manuels` — aligné avec l'écart déjà identifié au chantier DSR.
2. 🔴 Réparer ou retirer le module `pic-coefficients` national (§14).
3. 🟠 Corriger le défaut `dt_comptage` du PUT comptage (conserver l'existant si omis).
4. 🟠 Ajouter `id_rh` au `PATCH tmh/{co_produit}` et aux actions de cycle de vie scénario.
5. 🟠 Contrainte UNIQUE comptages + alignement des longueurs varchar.
