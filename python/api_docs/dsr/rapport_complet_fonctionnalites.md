# Rapport complet des fonctionnalités TRPPU — Ce que ça fait · Récupéré · Enregistré

Pour **chaque endpoint** de l'API : ce qu'il **fait**, ce qu'il **récupère** (lit en base / renvoie au
client) et ce qu'il **enregistre** (écrit en base). 76 endpoints, ~17 modules (`app/main.py`).

**Conventions** : 🔒 = `id_rh` crypté Fernet avant stockage · 🧮 = valeur dérivée serveur (non saisie) ·
⚙️ = auto-DB (PK, `dt_creation`, `dt_maj`) · 🔎 = lecture seule (rien écrit).
Schéma de référence : `db_migrations/db_10_09_2026.sql`. Voir aussi `audit_concordance_db_10_09_2026.md`
et `cartographie_donnees_persistees.md`.

---

## 1. Scénarios — module `trppu_scenario`

Table principale `trppu_scenario` (+ `trppu_site`, `trppu_tmh`). Le `SELECT_SCENARIO_SQL` renvoie 25
colonnes : entête (`id_scenario, co_regate, lb_scenario, co_roc, statut`), dates (`dt_creation,
dt_validation, dt_mise_en_oeuvre, dt_mise_en_prod, dt_pivot AS dt_real_prev`), périodes
(`periode_debut/fin, periode_realise_debut/fin, periode_prev_debut/fin`), jours (`nb_jours_semaine,
nb_jours_ouvres, nb_jours_ouvrables, nb_jours_scenario`), `id_pic_version, version_scenario, est_fige,
trafic_pdi_calcule, trafic_agrebal_calcule`.

| Endpoint | Ce que ça fait | Récupéré | Enregistré |
| --- | --- | --- | --- |
| `GET /scenarios` | Liste paginée + filtres (co_regate, co_roc, statut, est_fige) | `ScenarioOut` (25 champs) | 🔎 |
| `GET /scenarios/{id}` | Détail d'un scénario | `ScenarioOut` | 🔎 |
| `GET /scenarios/enums` | Valeurs d'enum statut | `{statut:[EN COURS,VALIDE,EN PRODUCTION,ARCHIVE]}` | 🔎 |
| `GET /scenarios/{id}/periodes` (655) | Périodes + nb jours (slider IHM) | `ScenarioPeriodesOut` (périodes + nb_jours_*) | 🔎 |
| `GET /scenarios/{id}/edition` (654) | **Agrégateur** : tout le scénario en 1 appel | scénario + periodes + tmh[] + comptages[] + variations[] + neutralisations[] + pic{défaut+surcharge fusionnés} | 🔎 |
| `POST /scenarios` (634) | Crée scénario (+ site si absent, + lignes TMH), transaction | le scénario créé | `trppu_scenario` (toutes colonnes : statut='EN COURS', dt_pivot🧮, periode_realise/prev_*🧮, nb_jours_*🧮, version=1, est_fige=0, id_rh_creation/maj🔒) · `trppu_site` · `trppu_tmh` |
| `PUT /scenarios/{id}` (656) | MAJ scénario EN COURS (périodes, jours, TMH), recalcul bornes | scénario MAJ | `trppu_scenario`: periode_*, periode_realise/prev_*🧮, dt_pivot🧮, nb_jours_*🧮, dt_maj⚙️, id_rh_maj🔒, version+1 · upsert `trppu_tmh` |
| `PATCH /{id}/periodes` | MAJ bornes seules + recalcul réel/prév | scénario | periode_*, periode_realise/prev_*🧮, version+1 |
| `PATCH /{id}/nb-jours-semaine` | Bascule 5/6 jours | scénario | nb_jours_semaine, version+1 |
| `PATCH /{id}/statut` | Transition machine à états | scénario | statut + effets de transition, version+1 |
| `POST /{id}/mise-en-prod` | Passe VALIDE → EN PRODUCTION | scénario | statut='EN PRODUCTION', est_fige=1, dt_mise_en_prod🧮, version+1 |
| `PATCH /{id}/est-fige` | Fige/défige (booléen direct) | scénario | est_fige, version+1 |
| `PATCH /{id}/figement` (669) | Fige selon statut IHM ("validé"/"simulation"/"en cours") | scénario | est_fige🧮(mappé), version+1 — **statut DB inchangé** |
| `PATCH /{id}/lb-scenario` | Renomme | scénario | lb_scenario, version+1 |
| `POST /{id}/archive` | Archive (soft) | scénario | statut='ARCHIVE', version+1 |
| `POST /{id}/duplicate` | Copie l'entête (sans sous-ressources) | nouveau scénario | nouveau `trppu_scenario` (périodes, nb_jours_semaine, id_pic_version ; version=1, est_fige=0) |
| `DELETE /scenarios/{id}` | Suppression dure + cascade enfants | — | DELETE 9 tables enfants + `trppu_scenario` |

---

## 2. Trafics moyens hebdo (TMH) — module `trppu_tmh`

Table `trppu_tmh`. Lecture = 7 colonnes (`co_produit, volume_realise, volume_previsionnel,
moyenne_journaliere, moyenne_hebdo, bl_exclu, bl_manuel`).

| Endpoint | Ce que ça fait | Récupéré | Enregistré |
| --- | --- | --- | --- |
| `GET /scenarios/{id}/tmh` (650) | Liste TMH (1 ligne/produit) | `list[TmhOut]` (7 champs) | 🔎 |
| `PUT /scenarios/{id}/tmh` (648/659) | Upsert batch des trafics recalculés | lignes affectées | upsert `trppu_tmh`: volumes, moyennes, bl_exclu, bl_manuel, id_rh🔒, dt_calcul🧮 |
| `PATCH /…/tmh/{co_produit}` (649) | MAJ ciblée d'un produit (saisie manuelle) | ligne MAJ | volume_realise, moyennes, **bl_manuel=1**🧮, dt_calcul🧮 (ne touche pas volume_previsionnel/bl_exclu) |

---

## 3. Comptages manuels — module `trppu_comptages`

Table `trppu_scenario_comptages_manuels`. Lecture = `co_produit, dt_comptage, nb_produit`.

| Endpoint | Ce que ça fait | Récupéré | Enregistré |
| --- | --- | --- | --- |
| `GET /scenarios/{id}/comptages` (653) | Liste des comptages | `list[ComptageOut]` (3 champs) | 🔎 |
| `POST /scenarios/{id}/comptages` (644) | Ajoute un comptage (409 si existe) | comptage créé | id_scenario, dt_comptage, co_produit, nb_produit — ⚠️ **`id_rh` reçu mais non écrit** |
| `PUT /…/comptages/{co_produit}` (644) | Modifie un comptage | comptage MAJ | dt_comptage, nb_produit — ⚠️ **`id_rh` non écrit** |
| `DELETE /…/comptages/{co_produit}` (644) | Suppression dure | — | DELETE par (id_scenario, co_produit) |

---

## 4. Variations prévisionnelles — module `trppu_variations`

Table `trppu_scenario_variations_prev`. Lecture = `co_produit, variation_pct`.

| Endpoint | Ce que ça fait | Récupéré | Enregistré |
| --- | --- | --- | --- |
| `GET /scenarios/{id}/variations` (651) | Liste pilotée par le TMH (produits non exclus, variation ou 0 par défaut) | `list[VariationOut]` (2 champs) | 🔎 |
| `PUT /…/variations/{co_produit}` (646) | Upsert ; **0% ⇒ suppression** | variation MAJ/supprimée | variation_pct, id_rh🔒, dt_creation🧮 (réécrit à chaque modif) |
| `DELETE /…/variations/{co_produit}` (646) | Suppression dure explicite | — | DELETE par (id_scenario, co_produit) |

---

## 5. Neutralisations — module `trppu_neutralisations`

Table `trppu_neutralisations`. Lecture à plat = `id (=id_neutralisation), dt_debut, dt_fin, nb_jour, motif`.

| Endpoint | Ce que ça fait | Récupéré | Enregistré |
| --- | --- | --- | --- |
| `GET /scenarios/{id}/neutralisations` (652) | Liste à plat | `list[NeutralisationItem]` (5 champs) | 🔎 |
| `POST /scenarios/{id}/neutralisations` (645) | Ajoute (jour ou période) ; 409 si doublon ; 422 si nb_jour<1 | neutralisation créée | id_scenario, dt_debut, dt_fin, **nb_jour🧮**, motif, id_rh🔒, dt_creation⚙️ |
| `DELETE /…/neutralisations` (645) | Suppression par période (dt_debut+dt_fin en query) | — | DELETE par période |

---

## 6. Coefficients PIC d'un scénario — module `trppu_scenario_pic`

Tables `trppu_pic_version` + `trppu_pic_coefficients`. **Module conforme au schéma.**

| Endpoint | Ce que ça fait | Récupéré | Enregistré |
| --- | --- | --- | --- |
| `GET /scenarios/{id}/pic-coefficients` (660) | Fusionne coefficients **défaut national (v1)** + **surcharge scénario** | `PicScenarioOut` : id_pic_version_defaut, id_pic_version_scenario, niveau_scenario, coefficients[]{id_pic_version, co_produit, jour_semaine, densite, coef, **modifie**} | 🔎 |
| `PUT /scenarios/{id}/pic-coefficients` (661) | Enregistre une surcharge ; crée la version scénario au besoin (transaction) | coef enregistré | si version absente → `trppu_pic_version`(niveau='SCENARIO', co_regate, id_scenario, dt_activation🧮, id_rh_creation/maj🔒) ; `trppu_pic_coefficients`(id_pic_version, co_produit, jour_semaine, dt_effet🧮, coef, densite, id_rh🔒) ; sinon UPDATE coef, dt_maj⚙️, id_rh🔒 |

---

## 7. Audit traçabilité — module `trppu_audit`

| Endpoint | Ce que ça fait | Récupéré | Enregistré |
| --- | --- | --- | --- |
| `POST /audit/actions-id-rh` | Déchiffre un `id_rh` et retrouve **toutes ses actions** sur 5 tables | parcourt `trppu_scenario` (id_rh_creation/maj), `trppu_pic_version` (id_rh_creation/maj), `trppu_pic_coefficients` (id_rh), `trppu_neutralisations` (id_rh), `trppu_tmh` (id_rh) → `AuditOut`{id_rh en clair, nb_actions, actions[]{ressource, action, id, id_scenario, date, details}} | 🔎 |

---

## 8. Trafics Databricks — module `trafics` (aucune écriture MySQL)

| Endpoint | Ce que ça fait | Récupéré | Enregistré |
| --- | --- | --- | --- |
| `GET /trafics/get_trafics` (613) | Trafics bruts d'un site sur une période (découpe auto mois/sem/jours) | Databricks `g_trppu_trafics_*` → `{co_regate, date_debut, date_fin, count, data[] (lignes brutes), nb_jours}` | 🔎 (lecture Databricks) |
| `GET /trafics/get_trafics_pivot` (666) | Ventile réel/prévisionnel selon `date_pivot`, somme par objet | `{…, date_pivot, trafics[]{co_produit, trafic_brut, trafic_previsionnel}, nb_jours}` | 🔎 (lecture Databricks) |

---

## 9. Calcul nombre de jours — module `calcl_nbr_jours`

| Endpoint | Ce que ça fait | Récupéré | Enregistré |
| --- | --- | --- | --- |
| `GET /calcl_nbr_jours/get_nb_jours` (613) | Calcule jours ouvrés/ouvrables (fériés déduits via API jours-fermes) | `{date_debut, date_fin, nb_jours_total, nb_jours_ouvres_bruts, nb_jours_ouvrables_bruts, nb_feries_hors_weekend, nb_feries_samedi, nbJoursOuvres, nbJoursOuvrables, execution_time_ms}` | 🔎 (API externe) |

---

## 10. Référentiel Sites — module `trppu_site`

Table `trppu_site`. Lecture/écriture = `co_regate, lb_regate, type_site, co_roc` (+ `dt_maj`⚙️).

| Endpoint | Ce que ça fait | Récupéré | Enregistré |
| --- | --- | --- | --- |
| `GET /sites` | Liste paginée + filtres (type_site, co_roc) | `list[SiteOut]` (5 champs) | 🔎 |
| `GET /sites/{co_regate}` | Détail | `SiteOut` | 🔎 |
| `POST /sites` | Crée un site | site créé | INSERT co_regate, lb_regate, type_site, co_roc |
| `PUT /sites/{co_regate}` | MAJ partielle | site MAJ | UPDATE sous-ensemble {lb_regate, type_site, co_roc} |
| `POST /sites/upload-excel` | Upsert en masse (Excel) | bilan import | upsert co_regate, lb_regate, type_site, co_roc |

---

## 11. Référentiel Produits — module `trppu_produit`

Table `trppu_produit`. Lecture = `co_produit, lb_produit, dt_creation, dt_desactivation, motif_desactivation`.

| Endpoint | Ce que ça fait | Récupéré | Enregistré |
| --- | --- | --- | --- |
| `GET /produits` | Liste (filtre actif_only) | `list[ProduitOut]` (5 champs) | 🔎 |
| `GET /produits/{co_produit}` | Détail | `ProduitOut` | 🔎 |
| `POST /produits` | Crée un produit | produit créé | INSERT co_produit, lb_produit, dt_desactivation, motif_desactivation |
| `PUT /produits/{co_produit}` | MAJ partielle | produit MAJ | UPDATE {lb_produit, dt_desactivation, motif_desactivation} |
| `DELETE /produits/{co_produit}` | Désactivation (soft) | produit désactivé | UPDATE dt_desactivation=today, motif_desactivation |
| `POST /produits/upload-excel` | Upsert en masse | bilan import | upsert co_produit, lb_produit, dt_desactivation, motif_desactivation |

---

## 12. Référentiel Versions PIC — module `trppu_pic_version`

Table `trppu_pic_version`. Lecture = 13 champs (`id_pic_version, lb_pic_version, niveau, co_regate,
dt_activation, dt_desactivation, motif_desactivation, commentaire, est_par_defaut, dt_creation, dt_maj,
id_rh_creation, id_rh_maj`).

| Endpoint | Ce que ça fait | Récupéré | Enregistré |
| --- | --- | --- | --- |
| `GET /pic-versions` | Liste + filtres (co_regate, niveau, actif_only, est_par_defaut) | `list[PicVersionOut]` (13 champs) | 🔎 |
| `GET /pic-versions/{id}` | Détail | `PicVersionOut` | 🔎 |
| `GET /pic-versions/enums` | Enum niveau | `{niveau:[NATIONAL,DEX,SITE]}` | 🔎 |
| `POST /pic-versions` | Crée une version | version créée | INSERT lb_pic_version, niveau, co_regate, dt_activation, dt_desactivation, motif_desactivation, commentaire, est_par_defaut — ⚠️ **`id_rh_creation/maj` non écrits** |
| `PUT /pic-versions/{id}` | MAJ partielle | version MAJ | UPDATE sous-ensemble des mêmes colonnes |
| `DELETE /pic-versions/{id}` | Désactivation (soft) | version désactivée | UPDATE dt_desactivation, motif_desactivation |
| `POST /pic-versions/upload-excel` | Insert en masse (pas d'upsert) | bilan import | INSERT mêmes colonnes |

---

## 13. Référentiel Coefficients PIC nationaux — module `trppu_pic_coefficients` ⚠️

Table `trppu_pic_coefficients`. **⚠️ Module désaligné du schéma** : il lit/écrit `coef_dense,
coef_faible1, coef_faible2, dt_fin_effet, id_rh_creation` + enum `LUN…SAM`, colonnes **inexistantes**
sur la table réelle (`coef, densite, dt_fin, id_rh`, enum `LUNDI…SAMEDI`). **Toutes ces opérations
échouent à l'exécution.** Détail : `audit_concordance_db_10_09_2026.md` §5.1.

| Endpoint | Ce que ça fait (théorique) | Récupéré / Enregistré |
| --- | --- | --- |
| `GET /pic-coefficients` (+`/{id}`, `/enums`) | Liste / détail / enum | lit des colonnes inexistantes → **erreur SQL** |
| `POST /pic-coefficients` | Crée un coef | écrit des colonnes inexistantes → **erreur SQL** |
| `PUT /pic-coefficients/{id}` | MAJ | idem |
| `DELETE /pic-coefficients/{id}` | Clôture (soft, `dt_fin_effet`) | idem |
| `POST /pic-coefficients/upload-excel` | Upsert en masse | idem |

---

## 14. Modules techniques (lecture seule / hors métier)

| Module | Endpoints | Récupéré | Enregistré |
| --- | --- | --- | --- |
| Health | `GET /`, `/health`, `/health/resources` | état config + connectivité MySQL/Databricks | 🔎 |
| Databricks | `GET /databricks/test` | test de requête | 🔎 |
| MySQL debug | `GET /mysql/test|tables|columns|indexes|sample|schema|dump|export` | introspection schéma/données | 🔎 |
| MySQL debug | `POST /mysql/import` | import générique (outil) | INSERT dynamique table arbitraire (hors métier) |
| Logs | `GET /logs/latest`, `DELETE /logs` | fichiers de log | écrit/truncate **fichiers** (pas la base) |

---

## 15. Synthèse — points d'attention sur « récupéré vs enregistré »

1. **`id_rh` comptages** : reçu en entrée (POST/PUT comptages) mais **jamais enregistré** → la lecture
   audit (`/audit/actions-id-rh`) ne retrouvera **aucune action de comptage**.
2. **`id_rh_creation/maj` des versions PIC nationales** : non écrits par `POST/PUT /pic-versions` →
   l'audit ne retrouve que les versions créées via le **flux scénario-PIC** (DSR-661).
3. **`trafic_pdi_calcule` / `trafic_agrebal_calcule`** : **récupérés** en lecture (`ScenarioOut`,
   `/edition`) mais **jamais mis à 1** (service de calcul absent) → toujours `false` côté IHM.
4. **Module `trppu_pic_coefficients` national** : ni récupération ni enregistrement ne fonctionnent
   (colonnes inexistantes). Le `/edition` et `/pic-coefficients` scénario utilisent, eux, le **bon**
   accès (`trppu_scenario_pic`, colonnes `coef/densite`).
5. **Lectures Databricks** (`trafics`) et **calcul jours** : ne touchent pas MySQL — pures récupérations.
6. **Tables jamais récupérées ni enregistrées par l'API** : `trppu_trafic_pdi/agrebal`,
   `trppu_agrebal_pdi`, `trppu_cles_repartition`, `trppu_scenario_exclusions`,
   `trppu_scenario_pic_coeffs` (legacy), `trppu_pic_coefficients_ko`, `trppu_api_log`,
   `trppu_recalcul_log`, `demande_dsr` (cf. `cartographie_donnees_persistees.md` §2).
