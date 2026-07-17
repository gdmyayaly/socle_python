# Spécifications détaillées des routes — Entrée / Sortie / Lecture DB / Écriture DB

Pour **chaque route** : le **JSON d'entrée** (si applicable), le **JSON de sortie**, les **données
récupérées en base** (SELECT) et les **données altérées en base** (INSERT/UPDATE/DELETE).

> Conventions : les exemples de valeurs sont illustratifs. `id_rh` est reçu **en clair** et stocké
> **crypté (Fernet)**. Les champs *dérivés serveur* (périodes réel/prév, nb_jours, nb_jour, dt_pivot)
> sont calculés et non acceptés en entrée. Schéma de référence : `db_migrations/db_10_09_2026.sql`.

---

# 1. SCÉNARIOS (`trppu_scenario`)

## `GET /trppu-api/scenarios`
Liste paginée. **Entrée** : query `co_regate?`, `co_roc?`, `statut?`, `est_fige?`, `limit=100`, `offset=0`.
**Sortie** : `list[ScenarioOut]`.
**Lu** : `trppu_scenario` (25 colonnes, `dt_pivot AS dt_real_prev`). **Altéré** : aucun.

## `GET /trppu-api/scenarios/{id_scenario}`
**Entrée** : path `id_scenario`. **Sortie** : `ScenarioOut` :
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
**Lu** : `trppu_scenario WHERE id_scenario`. **Altéré** : aucun.

## `GET /trppu-api/scenarios/enums`
**Entrée** : aucune. **Sortie** : `{"statut": ["EN COURS","VALIDE","EN PRODUCTION","ARCHIVE"]}`.
**Lu/Altéré** : aucun.

## `GET /trppu-api/scenarios/{id_scenario}/periodes` (DSR-655)
**Entrée** : path `id_scenario`, query `id_session_ihm?`. **Sortie** : `ScenarioPeriodesOut` :
```json
{
  "periode_debut": "2025-06-10", "periode_fin": "2027-06-10",
  "periode_realise_debut": "2025-06-10", "periode_realise_fin": "2026-06-09",
  "periode_prev_debut": "2026-06-10", "periode_prev_fin": "2027-06-10",
  "nb_jours_semaine": 6, "nb_jours_ouvres": 521, "nb_jours_ouvrables": 626, "nb_jours_scenario": 626
}
```
**Lu** : `trppu_scenario`. **Altéré** : aucun.

## `GET /trppu-api/scenarios/{id_scenario}/edition` (DSR-654 — agrégateur)
**Entrée** : path `id_scenario`, query `id_session_ihm?`. **Sortie** (dict agrégé) :
```json
{
  "scenario": { "...ScenarioOut..." },
  "periodes": { "...ScenarioPeriodesOut..." },
  "tmh": [ { "co_produit": "OO", "volume_realise": 3500, "volume_previsionnel": 2435,
             "moyenne_journaliere": 120.50, "moyenne_hebdo": 723.00, "bl_exclu": false, "bl_manuel": false } ],
  "comptages": [ { "co_produit": "OO", "dt_comptage": "2026-05-12", "nb_produit": 1200 } ],
  "variations": [ { "co_produit": "OO", "variation_pct": 5.00 } ],
  "neutralisations": [ { "id": 7, "dt_debut": "2026-08-01", "dt_fin": "2026-08-15", "nb_jour": 12, "motif": "Congés" } ],
  "pic": {
    "id_pic_version_defaut": 1, "id_pic_version_scenario": 5, "niveau_scenario": "SCENARIO",
    "coefficients": [ { "id_pic_version": 5, "co_produit": "OO", "jour_semaine": "LUNDI",
                        "densite": 0, "coef": 1.2500, "modifie": true } ]
  }
}
```
**Lu** : `trppu_scenario`, `trppu_tmh`, `trppu_scenario_comptages_manuels`, `trppu_scenario_variations_prev`,
`trppu_neutralisations`, `trppu_pic_coefficients` (défaut v1 + version scénario), `trppu_pic_version`.
**Altéré** : aucun.

## `POST /trppu-api/scenarios` (DSR-634)
**Entrée** `ScenarioCreate` :
```json
{
  "co_regate": "400300", "lb_scenario": "Scénario 2026 T1", "co_roc": "750001",
  "lb_regate": "PARIS PLATEFORME", "type_site": "PPDC", "nb_jours_semaine": 6,
  "id_pic_version": 1, "periode_debut": "2025-06-10", "periode_fin": "2027-06-10",
  "dt_mise_en_oeuvre": "2026-06-10", "id_rh": "ABC1234",
  "tmh": [ { "co_produit": "OO", "volume_realise": 3500, "volume_previsionnel": 2435,
             "moyenne_journaliere": 120.50, "moyenne_hebdo": 723.00, "exclusion": false, "manuel": false } ]
}
```
**Sortie** : `ScenarioOut` (le scénario créé, voir §GET/{id}).
**Lu** : `trppu_site` (existence), résolution `id_pic_version` (`trppu_pic_version` si non fourni).
**Altéré** :
- `trppu_scenario` **INSERT** : co_regate, lb_scenario, co_roc, statut(='EN COURS'), dt_creation(NOW), dt_mise_en_oeuvre, dt_pivot(NOW), periode_debut, periode_fin, periode_realise_debut/fin🧮, periode_prev_debut/fin🧮, nb_jours_semaine, nb_jours_ouvres🧮, nb_jours_ouvrables🧮, nb_jours_scenario🧮, id_pic_version, version_scenario(=1), est_fige(=0), id_rh_creation🔒, id_rh_maj🔒
- `trppu_site` **INSERT** (si absent) : co_regate, lb_regate, type_site, co_roc
- `trppu_tmh` **INSERT** (par produit) : id_scenario, co_produit, volume_realise, volume_previsionnel, moyenne_journaliere, moyenne_hebdo, bl_exclu, bl_manuel, id_rh🔒, dt_calcul(NOW)

## `PUT /trppu-api/scenarios/{id_scenario}` (DSR-656)
**Entrée** `ScenarioMajRequest` :
```json
{
  "periode_debut": "2025-06-10", "periode_fin": "2027-06-10", "nb_jours_semaine": 6,
  "dt_mise_en_oeuvre": "2026-06-10", "id_rh": "ABC1234",
  "tmh": [ { "co_produit": "OO", "volume_realise": 3600, "volume_previsionnel": 2500,
             "moyenne_journaliere": 121.00, "moyenne_hebdo": 726.00, "exclusion": false, "manuel": false } ]
}
```
**Sortie** : `ScenarioOut` (mis à jour). Refusé (409) si statut ≠ EN COURS.
**Lu** : `trppu_scenario` (état + statut). **Altéré** :
- `trppu_scenario` **UPDATE** : periode_debut, periode_fin, periode_realise_*🧮, periode_prev_*🧮, dt_pivot(NOW), [dt_mise_en_oeuvre], nb_jours_*🧮, dt_maj(NOW), id_rh_maj🔒, puis version_scenario+1
- `trppu_tmh` **UPSERT** (mêmes colonnes que création)

## `PATCH /trppu-api/scenarios/{id_scenario}/periodes`
**Entrée** `PeriodeUpdate` : `{ "periode_debut": "2025-07-01", "periode_fin": "2027-07-01" }` (au moins un champ).
**Sortie** : `ScenarioOut`. **Lu** : `trppu_scenario`.
**Altéré** : `trppu_scenario` **UPDATE** periode_*, periode_realise_*🧮, periode_prev_*🧮 + version+1.

## `PATCH /trppu-api/scenarios/{id_scenario}/nb-jours-semaine`
**Entrée** : `{ "nb_jours_semaine": 5 }`. **Sortie** : `ScenarioOut`.
**Altéré** : `trppu_scenario` **UPDATE** nb_jours_semaine + version+1.

## `PATCH /trppu-api/scenarios/{id_scenario}/statut`
**Entrée** : `{ "statut": "VALIDE" }`. **Sortie** : `ScenarioOut`.
**Altéré** : `trppu_scenario` **UPDATE** statut (+ effets transition, ex. dt_validation via COALESCE) + version+1.

## `POST /trppu-api/scenarios/{id_scenario}/mise-en-prod`
**Entrée** : aucune. **Sortie** : `ScenarioOut` (EN PRODUCTION).
**Altéré** : `trppu_scenario` **UPDATE** statut='EN PRODUCTION', est_fige=1, dt_mise_en_prod(NOW) + version+1.

## `PATCH /trppu-api/scenarios/{id_scenario}/est-fige`
**Entrée** : `{ "est_fige": true }`. **Sortie** : `ScenarioOut`.
**Altéré** : `trppu_scenario` **UPDATE** est_fige + version+1.

## `PATCH /trppu-api/scenarios/{id_scenario}/figement` (DSR-669)
**Entrée** `FigementParStatutRequest` : `{ "statut": "validé" }` (libellé IHM ; 422 si non mappable).
**Sortie** : `ScenarioOut`. **Altéré** : `trppu_scenario` **UPDATE** est_fige🧮 (mappé : validé/simulation→1, en cours→0) + version+1. **Le `statut` DB n'est pas modifié.**

## `PATCH /trppu-api/scenarios/{id_scenario}/lb-scenario`
**Entrée** : `{ "lb_scenario": "Nouveau libellé" }`. **Sortie** : `ScenarioOut`.
**Altéré** : `trppu_scenario` **UPDATE** lb_scenario + version+1.

## `POST /trppu-api/scenarios/{id_scenario}/archive`
**Entrée** : aucune. **Sortie** : `ScenarioOut` (ARCHIVE).
**Altéré** : `trppu_scenario` **UPDATE** statut='ARCHIVE' + version+1.

## `POST /trppu-api/scenarios/{id_scenario}/duplicate`
**Entrée** `DuplicateRequest` (requis) : `{ "id_rh": "U123456", "lb_scenario": "Copie scénario T1" }` (`lb_scenario` optionnel).
**Sortie** : `ScenarioOut` (nouveau scénario). **Lu** : scénario source + sa version PIC niveau SCENARIO.
**Altéré** (copie profonde, transaction unique) : `trppu_scenario` **INSERT** (entête complète copiée dont dt_pivot, nb_jours_*, flags trafic ; statut='EN COURS', version=1, est_fige=0, id_rh_creation=id_rh chiffré) ; **INSERT...SELECT** dans `trppu_tmh`, `trppu_neutralisations`, `trppu_scenario_comptages_manuels`, `trppu_scenario_exclusions`, `trppu_scenario_variations_prev`, `trppu_scenario_pic_coeffs`, `trppu_trafic_agrebal`, `trppu_trafic_pdi` ; si version PIC SCENARIO source : **INSERT** `trppu_pic_version` + copie `trppu_pic_coefficients` + **UPDATE** id_pic_version du clone. Logs non copiés.

## `DELETE /trppu-api/scenarios/{id_scenario}`
**Entrée** : aucune. **Sortie** : `{ "deleted": true, "id_scenario": 42 }` (statut 200).
**Altéré** : **DELETE** en cascade sur `trppu_neutralisations`, `trppu_tmh`,
`trppu_scenario_comptages_manuels`, `trppu_scenario_exclusions`, `trppu_scenario_pic_coeffs`,
`trppu_scenario_variations_prev`, `trppu_trafic_agrebal`, `trppu_trafic_pdi`, `trppu_pic_version`,
puis `trppu_scenario`.

---

# 2. TMH (`trppu_tmh`)

## `GET /trppu-api/scenarios/{id_scenario}/tmh` (DSR-650)
**Entrée** : path `id_scenario`, query `id_session_ihm?`. **Sortie** : `list[TmhOut]` :
```json
[ { "co_produit": "OO", "volume_realise": 3500, "volume_previsionnel": 2435,
    "moyenne_journaliere": 120.50, "moyenne_hebdo": 723.00, "bl_exclu": false, "bl_manuel": false } ]
```
**Lu** : `trppu_tmh` (7 colonnes). **Altéré** : aucun.

## `PUT /trppu-api/scenarios/{id_scenario}/tmh` (DSR-648/659)
**Entrée** `TmhBatchUpdate` :
```json
{
  "tmh": [ { "co_produit": "OO", "volume_realise": 3500, "volume_previsionnel": 2435,
             "moyenne_journaliere": 120.50, "moyenne_hebdo": 723.00, "exclusion": false, "manuel": false } ],
  "id_rh": "ABC1234"
}
```
**Sortie** `TmhBatchResult` : `{ "id_scenario": 42, "nb_inserted": 1, "nb_updated": 5 }`.
**Lu** : `trppu_tmh` (existence par co_produit). **Altéré** : `trppu_tmh` **UPSERT** : volume_realise,
volume_previsionnel, moyenne_journaliere, moyenne_hebdo, bl_exclu, bl_manuel, id_rh🔒, dt_calcul(NOW).

## `PATCH /trppu-api/scenarios/{id_scenario}/tmh/{co_produit}` (DSR-649)
**Entrée** `TmhVolumeUpdate` :
```json
{ "volume_realise": 3600, "moyenne_journaliere": 121.00, "moyenne_hebdo": 726.00 }
```
**Sortie** : `TmhOut` (ligne mise à jour). **Lu** : `trppu_tmh` (existence).
**Altéré** : `trppu_tmh` **UPDATE** volume_realise, moyenne_journaliere, moyenne_hebdo, **bl_manuel=1**🧮,
dt_calcul(NOW). Ne touche pas volume_previsionnel ni bl_exclu.

---

# 3. COMPTAGES MANUELS (`trppu_scenario_comptages_manuels`)

## `GET /trppu-api/scenarios/{id_scenario}/comptages` (DSR-653)
**Entrée** : path `id_scenario`, query `id_session_ihm?`. **Sortie** : `list[ComptageOut]` :
```json
[ { "co_produit": "OO", "dt_comptage": "2026-05-12", "nb_produit": 1200 } ]
```
**Lu** : `trppu_scenario_comptages_manuels` (co_produit, dt_comptage, nb_produit). **Altéré** : aucun.

## `POST /trppu-api/scenarios/{id_scenario}/comptages` (DSR-644)
**Entrée** `ComptageCreate` :
```json
{ "co_produit": "OO", "dt_comptage": "2026-05-12", "nb_produit": 1200, "id_rh": "ABC1234" }
```
**Sortie** : `ComptageOut` (sans id_rh). 409 si comptage déjà présent pour le produit.
**Lu** : existence (id_scenario, co_produit). **Altéré** : `trppu_scenario_comptages_manuels` **INSERT** :
id_scenario, dt_comptage, co_produit, nb_produit. ⚠️ **`id_rh` reçu mais NON écrit.**

## `PUT /trppu-api/scenarios/{id_scenario}/comptages/{co_produit}` (DSR-644)
**Entrée** `ComptageUpdate` : `{ "dt_comptage": "2026-05-13", "nb_produit": 1300, "id_rh": "ABC1234" }`.
**Sortie** : `ComptageOut`. **Altéré** : **UPDATE** dt_comptage, nb_produit. ⚠️ **`id_rh` NON écrit.**

## `DELETE /trppu-api/scenarios/{id_scenario}/comptages/{co_produit}` (DSR-644)
**Entrée** : path. **Sortie** : `{ "deleted": true }`.
**Altéré** : **DELETE** par (id_scenario, co_produit).

---

# 4. VARIATIONS PRÉVISIONNELLES (`trppu_scenario_variations_prev`)

## `GET /trppu-api/scenarios/{id_scenario}/variations` (DSR-651)
**Entrée** : path, query `id_session_ihm?`. **Sortie** : `list[VariationOut]` (une ligne
par `co_produit` distinct du TMH non exclu, variation stockée ou 0 par défaut) :
```json
[ { "co_produit": "OO", "variation_pct": 5.00 }, { "co_produit": "PR", "variation_pct": 0.00 } ]
```
**Lu** : `trppu_tmh` (co_produit distincts, `bl_exclu = 0`) `LEFT JOIN`
`trppu_scenario_variations_prev` (`COALESCE(variation_pct, 0)`). **Altéré** : aucun.

## `PUT /trppu-api/scenarios/{id_scenario}/variations/{co_produit}` (DSR-646)
**Entrée** `VariationUpsert` : `{ "variation_pct": 5.00, "id_rh": "ABC1234" }` (0 ⇒ suppression).
**Sortie** `VariationUpsertResult` : `{ "co_produit": "OO", "variation_pct": 5.00, "action": "updated" }`
(action ∈ created/updated/deleted/noop). **Lu** : existence (id_scenario, co_produit).
**Altéré** : `trppu_scenario_variations_prev` **UPSERT** variation_pct, id_rh🔒, dt_creation(NOW) ;
ou **DELETE** si variation_pct = 0.

## `DELETE /trppu-api/scenarios/{id_scenario}/variations/{co_produit}` (DSR-646)
**Entrée** : path. **Sortie** : `{ "deleted": true }`. **Altéré** : **DELETE** par (id_scenario, co_produit).

---

# 5. NEUTRALISATIONS (`trppu_neutralisations`)

## `GET /trppu-api/scenarios/{id_scenario}/neutralisations` (DSR-652)
**Entrée** : path, query `id_session_ihm?`. **Sortie** : `list[NeutralisationItem]` :
```json
[ { "id": 7, "dt_debut": "2026-08-01", "dt_fin": "2026-08-15", "nb_jour": 12, "motif": "Congés" } ]
```
**Lu** : `trppu_neutralisations` (id_neutralisation AS id, dt_debut, dt_fin, nb_jour, motif). **Altéré** : aucun.

## `POST /trppu-api/scenarios/{id_scenario}/neutralisations` (DSR-645)
**Entrée** `NeutralisationCreate` :
```json
{ "dt_debut": "2026-08-01", "dt_fin": "2026-08-15", "motif": "Congés", "id_rh": "ABC1234" }
```
**Sortie** `NeutralisationOut` :
```json
{ "id": 7, "dt_debut": "2026-08-01", "dt_fin": "2026-08-15", "nb_jour": 12, "motif": "Congés", "action": "created" }
```
409 si période déjà présente ; 422 si nb_jour < 1. **Lu** : `trppu_scenario` (nb_jours_semaine pour
calcul), unicité période. **Altéré** : `trppu_neutralisations` **INSERT** : id_scenario, dt_debut,
dt_fin, **nb_jour🧮** (1 si jour unique, sinon ouvrés/ouvrables hors fériés), motif, id_rh🔒, dt_creation(NOW).

## `DELETE /trppu-api/scenarios/{id_scenario}/neutralisations` (DSR-645)
**Entrée** : query `dt_debut`, `dt_fin` (obligatoires). **Sortie** : `{ "deleted": true }`.
**Altéré** : **DELETE** par (id_scenario, dt_debut, dt_fin).

---

# 6. COEFFICIENTS PIC SCÉNARIO (`trppu_pic_version` + `trppu_pic_coefficients`)

## `GET /trppu-api/scenarios/{id_scenario}/pic-coefficients` (DSR-660)
**Entrée** : path, query `id_session_ihm?`. **Sortie** `PicScenarioOut` :
```json
{
  "id_pic_version_defaut": 1, "id_pic_version_scenario": 5, "niveau_scenario": "SCENARIO",
  "coefficients": [
    { "id_pic_version": 1, "co_produit": "OO", "jour_semaine": "LUNDI", "densite": 0, "coef": 1.0000, "modifie": false },
    { "id_pic_version": 5, "co_produit": "OO", "jour_semaine": "MARDI", "densite": 1, "coef": 1.2500, "modifie": true }
  ]
}
```
**Lu** : `trppu_pic_coefficients` (id_pic_version=1 défaut + version scénario), `trppu_pic_version`
(version SCENARIO active). Fusion sur (co_produit, jour_semaine, densite). **Altéré** : aucun.

## `PUT /trppu-api/scenarios/{id_scenario}/pic-coefficients` (DSR-661)
**Entrée** `PicCoefUpsert` :
```json
{ "co_produit": "OO", "jour_semaine": "LUNDI", "densite": 0, "coef": 1.2500, "id_rh": "ABC1234" }
```
**Sortie** `PicCoefUpsertResult` :
`{ "action": "insert_version_and_coef", "id_pic_version": 5 }` (action ∈ update / insert_coef / insert_version_and_coef).
**Lu** : `trppu_pic_version` (version SCENARIO existante ?), `trppu_pic_coefficients` (coef existant ?).
**Altéré** :
- si pas de version : `trppu_pic_version` **INSERT** lb_pic_version, niveau='SCENARIO', co_regate,
  id_scenario, dt_activation(NOW), id_rh_creation🔒, id_rh_maj🔒
- `trppu_pic_coefficients` **INSERT** id_pic_version, co_produit, jour_semaine, dt_effet(NOW), coef, densite, id_rh🔒
- ou **UPDATE** coef, dt_maj(NOW), id_rh🔒 si le coef existe déjà.

---

# 7. AUDIT id_rh (`trppu_audit`)

## `POST /trppu-api/audit/actions-id-rh`
**Entrée** `AuditRequest` : `{ "id_rh": "<token Fernet>", "cle": "<ID_RH_CRYPTO_KEY>" }`.
**Sortie** `AuditOut` :
```json
{
  "id_rh": "ABC1234", "nb_actions": 2,
  "actions": [
    { "ressource": "trppu_scenario", "action": "création", "id": 42, "id_scenario": 42,
      "date": "2026-06-10T09:15:00", "details": { "lb_scenario": "Scénario 2026 T1", "statut": "EN COURS" } },
    { "ressource": "trppu_tmh", "action": "écriture", "id": 88, "id_scenario": 42,
      "date": "2026-06-10T09:20:00", "details": { "co_produit": "OO" } }
  ]
}
```
**Lu** : `trppu_scenario` (id_rh_creation/maj), `trppu_pic_version` (id_rh_creation/maj),
`trppu_pic_coefficients` (id_rh), `trppu_neutralisations` (id_rh), `trppu_tmh` (id_rh) ; déchiffrement
et comparaison en clair. **Altéré** : aucun.

---

# 8. TRAFICS DATABRICKS (`trafics`) — aucune écriture MySQL

## `GET /trppu-api/trafics/get_trafics` (DSR-613)
**Entrée** : query `co_regate`, `date_debut`, `date_fin`, `limit?` (AAAAMMJJ). **Sortie** :
```json
{
  "execution_time_s": 1.2, "co_regate": "400300", "date_debut": "2025-03-01", "date_fin": "2026-03-31",
  "count": 1240, "data": [ { "...lignes Databricks brutes..." } ],
  "nb_jours": { "nbJoursOuvres": 272, "nbJoursOuvrables": 327 }
}
```
**Lu** : Databricks `g_trppu_trafics_jour|semaine|mois`. **Altéré** : aucun (MySQL).

## `GET /trppu-api/trafics/get_trafics_pivot` (DSR-666)
**Entrée** : query `co_regate`, `date_debut`, `date_fin`, `date_pivot`. **Sortie** :
```json
{
  "execution_time_s": 1.2, "co_regate": "400300", "date_debut": "2025-03-01", "date_fin": "2026-03-31",
  "date_pivot": "2025-10-01", "count": 6,
  "trafics": [ { "co_produit": "OO", "trafic_brut": 3500, "trafic_previsionnel": 2435 } ],
  "nb_jours": { "nbJoursOuvres": 272, "nbJoursOuvrables": 327 }
}
```
**Lu** : Databricks (ventilation réel/prév selon date_pivot). **Altéré** : aucun.

---

# 9. CALCUL JOURS (`calcl_nbr_jours`)

## `GET /trppu-api/calcl_nbr_jours/get_nb_jours` (DSR-613)
**Entrée** : query `date_debut`, `date_fin`. **Sortie** :
```json
{
  "date_debut": "2025-01-01", "date_fin": "2025-12-31",
  "nb_jours_total": 365, "nb_jours_ouvres_bruts": 261, "nb_jours_ouvrables_bruts": 313,
  "nb_feries_hors_weekend": 9, "nb_feries_samedi": 1,
  "nbJoursOuvres": 252, "nbJoursOuvrables": 312, "execution_time_ms": 45.0
}
```
**Lu** : API jours-fermes (pas de SQL). **Altéré** : aucun.

---

# 10. RÉFÉRENTIEL SITES (`trppu_site`)

## `GET /trppu-api/sites` · `GET /trppu-api/sites/{co_regate}`
**Entrée** : query filtres / path. **Sortie** : `SiteOut` :
```json
{ "co_regate": "400300", "lb_regate": "PARIS PLATEFORME", "type_site": "PPDC", "co_roc": "750001",
  "dt_maj": "2026-06-10T09:15:00" }
```
**Lu** : `trppu_site` (5 colonnes). **Altéré** : aucun.

## `POST /trppu-api/sites`
**Entrée** `SiteCreate` :
```json
{ "co_regate": "400300", "lb_regate": "PARIS PLATEFORME", "type_site": "PPDC", "co_roc": "750001" }
```
**Sortie** : `SiteOut`. **Altéré** : `trppu_site` **INSERT** co_regate, lb_regate, type_site, co_roc.

## `PUT /trppu-api/sites/{co_regate}`
**Entrée** `SiteUpdate` (partiel) : `{ "lb_regate": "PARIS PFC", "type_site": "PIC" }`.
**Sortie** : `SiteOut`. **Altéré** : `trppu_site` **UPDATE** sous-ensemble {lb_regate, type_site, co_roc}.

## `POST /trppu-api/sites/upload-excel`
**Entrée** : fichier `.xlsx` (col. co_regate, lb_regate?, type_site, co_roc). **Sortie** : `BulkUploadResult` :
```json
{ "nb_rows_read": 50, "nb_inserted": 30, "nb_updated": 18, "nb_unchanged": 0, "nb_errors": 2,
  "errors": [ { "row": 12, "error": "co_regate invalide", "raw": {} } ], "execution_time_s": 0.8 }
```
**Altéré** : `trppu_site` **UPSERT** co_regate, lb_regate, type_site, co_roc.

---

# 11. RÉFÉRENTIEL PRODUITS (`trppu_produit`)

## `GET /trppu-api/produits` · `GET /trppu-api/produits/{co_produit}`
**Sortie** : `ProduitOut` :
```json
{ "co_produit": "OO", "lb_produit": "OBJETS ORDINAIRES", "dt_desactivation": null,
  "motif_desactivation": null, "dt_creation": "2026-01-15T10:00:00" }
```
**Lu** : `trppu_produit` (5 colonnes). **Altéré** : aucun.

## `POST /trppu-api/produits`
**Entrée** `ProduitCreate` : `{ "co_produit": "OO", "lb_produit": "OBJETS ORDINAIRES", "dt_desactivation": null, "motif_desactivation": null }`.
**Sortie** : `ProduitOut`. **Altéré** : `trppu_produit` **INSERT** co_produit, lb_produit, dt_desactivation, motif_desactivation.

## `PUT /trppu-api/produits/{co_produit}`
**Entrée** `ProduitUpdate` (partiel) : `{ "lb_produit": "OBJETS ORDINAIRES (OO)" }`.
**Sortie** : `ProduitOut`. **Altéré** : **UPDATE** {lb_produit, dt_desactivation, motif_desactivation}.

## `DELETE /trppu-api/produits/{co_produit}`
**Entrée** : query `motif?` (défaut "Désactivé via API"). **Sortie** `SoftDeleteResult` :
```json
{ "co_produit": "OO", "dt_desactivation": "2026-06-10", "motif_desactivation": "Désactivé via API", "rows_affected": 1 }
```
**Altéré** : `trppu_produit` **UPDATE** dt_desactivation=today, motif_desactivation (soft delete).

## `POST /trppu-api/produits/upload-excel`
**Entrée** : fichier `.xlsx`. **Sortie** : `BulkUploadResult`.
**Altéré** : `trppu_produit` **UPSERT** co_produit, lb_produit, dt_desactivation, motif_desactivation.

---

# 12. RÉFÉRENTIEL VERSIONS PIC (`trppu_pic_version`)

## `GET /trppu-api/pic-versions` · `/{id}` · `/enums`
**Sortie** : `PicVersionOut` :
```json
{ "id_pic_version": 1, "lb_pic_version": "PIC NATIONAL", "niveau": "NATIONAL", "co_regate": "000000",
  "dt_activation": "2026-01-01T00:00:00", "dt_desactivation": null, "motif_desactivation": null,
  "commentaire": null, "est_par_defaut": true, "dt_creation": "2026-01-01T00:00:00",
  "dt_maj": "2026-01-01T00:00:00", "id_rh_creation": null, "id_rh_maj": null }
```
`/enums` → `{ "niveau": ["NATIONAL","DEX","SITE"] }`. **Lu** : `trppu_pic_version`. **Altéré** : aucun.

## `POST /trppu-api/pic-versions`
**Entrée** `PicVersionCreate` :
```json
{ "lb_pic_version": "PIC NATIONAL", "niveau": "NATIONAL", "co_regate": "000000",
  "dt_activation": "2026-01-01T00:00:00", "dt_desactivation": null, "motif_desactivation": null,
  "commentaire": null, "est_par_defaut": true }
```
**Sortie** : `PicVersionOut`. **Altéré** : `trppu_pic_version` **INSERT** lb_pic_version, niveau,
co_regate, dt_activation, dt_desactivation, motif_desactivation, commentaire, est_par_defaut.
⚠️ **`id_rh_creation`/`id_rh_maj` NON écrits** (restent NULL) ; `id_scenario` non fourni.

## `PUT /trppu-api/pic-versions/{id}`
**Entrée** `PicVersionUpdate` (partiel). **Sortie** : `PicVersionOut`. **Altéré** : **UPDATE** sous-ensemble.

## `DELETE /trppu-api/pic-versions/{id}`
**Entrée** : query `motif?`. **Sortie** : `SoftDeleteResult`.
**Altéré** : `trppu_pic_version` **UPDATE** dt_desactivation(NOW), motif_desactivation (soft delete).

## `POST /trppu-api/pic-versions/upload-excel`
**Entrée** : `.xlsx`. **Sortie** : `BulkUploadResult` (INSERT-only, nb_updated=0).
**Altéré** : `trppu_pic_version` **INSERT**.

---

# 13. RÉFÉRENTIEL COEFFICIENTS PIC NATIONAUX (`trppu_pic_coefficients`) ⚠️ MODULE CASSÉ

> Le module écrit/lit les colonnes `coef_dense, coef_faible1, coef_faible2, dt_fin_effet, id_rh_creation`
> + enum `LUN…SAM`, **inexistantes** sur la table réelle (`coef, densite, dt_fin, id_rh`, enum
> `LUNDI…SAMEDI`). **Toutes les requêtes échouent à l'exécution** (« Unknown column »). Détail :
> `audit_concordance_db_10_09_2026.md` §5.1. JSON ci-dessous = contrat théorique (non fonctionnel).

## `POST /trppu-api/pic-coefficients` (théorique)
**Entrée** `PicCoefCreate` :
```json
{ "id_pic_version": 1, "co_produit": "OO", "jour_semaine": "LUN", "dt_effet": "2026-01-01",
  "dt_fin_effet": null, "coef_dense": 1.2500, "coef_faible1": 0.8000, "coef_faible2": 0.5000 }
```
**Sortie** (théorique) `PicCoefOut` : mêmes champs + id_pic_coef, dt_creation, dt_maj, id_rh_creation.
**Altéré** (échoue) : `trppu_pic_coefficients` INSERT colonnes inexistantes.
Autres endpoints (`GET` liste/détail/enums, `PUT`, `DELETE` soft, `upload-excel`) : même défaut.

---

# 14. MODULES TECHNIQUES (lecture seule / hors métier)

| Route | Entrée | Sortie | Lu | Altéré |
| --- | --- | --- | --- | --- |
| `GET /`, `/health`, `/health/resources` | — | état/diagnostic | config + ping MySQL/Databricks | — |
| `GET /databricks/test` | — | résultat test | Databricks | — |
| `GET /mysql/test\|tables\|columns\|indexes\|sample\|schema\|dump\|export` | query selon cas | introspection | information_schema / tables | — |
| `POST /mysql/import` | `{table, rows[], columns?, truncate}` | bilan | schéma table | **INSERT dynamique** (+ TRUNCATE) — outil hors métier |
| `GET /logs/latest` | — | fichier log | — | — |
| `DELETE /logs` | query `keep_today` | bilan | — | **fichiers** (pas la base) |

---

# 15. Récapitulatif des écarts « entrée / sortie / base »

| Route | Écart |
| --- | --- |
| `POST`/`PUT` comptages | `id_rh` **dans l'entrée**, **absent de l'écriture** → invisible à l'audit |
| `POST`/`PUT` pic-versions | `id_rh_creation`/`id_rh_maj` jamais écrits (NULL) ; `id_scenario` non fourni |
| Tous endpoints `trppu_pic_coefficients` | entrée/sortie définies mais **écritures/lectures impossibles** (colonnes inexistantes) |
| `ScenarioOut.trafic_pdi_calcule` / `trafic_agrebal_calcule` | **dans la sortie** mais **jamais mis à 1** en base (toujours `false`) |
| `PATCH /figement` (669) | modifie `est_fige` mais **pas** `statut` (par design) |

Documents liés : `audit_concordance_db_10_09_2026.md` (défauts schéma/migrations),
`cartographie_donnees_persistees.md` (tables/colonnes jamais écrites),
`rapport_complet_fonctionnalites.md` (vue synthétique).
