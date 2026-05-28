# TRPPU / DSR — Améliorations & nouveaux tickets proposés

Recommandations issues de l'analyse croisée des 16 tickets : briques transverses
manquantes, harmonisations, et **nouveaux tickets** à créer (format prêt-à-créer).
Les améliorations « techniques » servent de socle commun à plusieurs US.

---

## A. Nouveaux tickets proposés (socle transverse)

### NEW-1 — 🔴 Migration de schéma TRPPU (colonnes + enum)
**But** : aligner la base sur les exigences des US d'écriture.
**Contenu** :
```sql
-- DSR-644
ALTER TABLE `trppu_scenario_comptages_manuels` ADD COLUMN `id_rh` VARCHAR(40) NULL;
-- DSR-645
ALTER TABLE `trppu_neutralisations`
  MODIFY COLUMN `type` ENUM('FERIE','PEAK','SAISON') NOT NULL,
  ADD COLUMN `dt_creation` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN `id_rh` VARCHAR(40) NULL;
-- DSR-646
ALTER TABLE `trppu_scenario_variations_prev`
  ADD COLUMN `dt_creation` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN `id_rh` VARCHAR(40) NULL;
```
**Pré-requis** : décision sur `LOCAL→SAISON` (existe-t-il des données ?).
**Bloque** : DSR-644, 645, 646.
**DoD** : schéma migré en recette ; `db_analyse/schema_trppu.sql` régénéré ;
`scripts/gen_schema_sql.py` rejoué.

### NEW-2 — 🔴 Module de cryptage `id_rh` (`app/security/crypto.py`)
**But** : un seul utilitaire de (dé)chiffrement réutilisé partout.
**Contenu** : `encrypt_id_rh(clear) -> str` / `decrypt_id_rh(token) -> str`,
algorithme et clé issus de la config (`app/config.py`), sortie garantie ≤ 40.
**Réutilisé par** : DSR-634, 644, 645, 646, 656, 661.
**DoD** : tests unitaires (round-trip, longueur, valeurs limites) ; clé hors code.

### NEW-3 — 🔴 Brique « jours fériés » + service de calcul des jours
**But** : source unique de fériés + calcul ouvrés/ouvrables/neutralisés.
**Contenu** :
- table `trppu_jours_feries(dt DATE PK, libelle, national TINYINT)` **ou** dépendance lib ;
- `app/services/jours_service.py` : `compute_nb_jours(debut, fin)` (DSR-613) et
  `compute_nb_jour_neutralise(debut, fin, nb_jours_semaine)` (DSR-645).
**Réutilisé par** : DSR-613, 645 ; indirectement 634, 655, 656 (`nb_jours_*`).
**DoD** : test de non-régression sur l'exemple DSR-613 (262 / 328).

### NEW-4 — 🟠 Module TMH unifié (`app/routes/trppu_tmh/`)
**But** : regrouper les opérations TMH dispersées sur 4 US.
**Contenu** : `GET /scenarios/{id}/tmh` (650), `PUT /scenarios/{id}/tmh` batch
(659), `PATCH /scenarios/{id}/tmh/{co_produit}` (649), + insertion initiale (634).
**Remarque** : voir IMP-1 (chevauchement 649/659).
**DoD** : un seul `schemas.TmhOut` / `TmhUpsert` partagé.

---

## B. Améliorations des tickets existants

### IMP-1 — 🟠 Fusionner / clarifier DSR-649 et DSR-659
- **Constat** : DSR-649 = UPDATE partiel (volume réalisé + moyennes) ; DSR-659 =
  UPDATE complet (+ prévisionnel + `bl_exclu`). Même table, même clé.
- **Proposition** : un seul service d'upsert TMH paramétrable (champs optionnels),
  DSR-649 devenant un cas d'usage de DSR-659. Évite deux implémentations divergentes.

### IMP-2 — 🟠 Harmoniser l'alimentation des `nb_jours_*` / dates scénario
- **Constat** : `nb_jours_ouvres/ouvrables/scenario`, `dt_mise_en_oeuvre`,
  `dt_real_prev`, `id_rh_creation/maj` existent en base mais ne sont **ni insérés
  (DSR-634) ni exposés (DSR-655)** par le code actuel.
- **Proposition** : étendre `INSERT` de création (DSR-634), `SELECT_SCENARIO_SQL`
  et `ScenarioOut` (DSR-655), et l'`UPDATE` (DSR-656) de façon cohérente. Sinon
  DSR-655 renverra des `NULL`.

### IMP-3 — 🟠 Statut & verrou d'écriture homogènes
- **Constat** : DSR-656 limite la MAJ au statut `EN COURS` ; les autres US
  d'écriture (644/645/646/649/659/661) ne précisent pas le contrôle de statut.
- **Proposition** : réutiliser systématiquement `assert_not_fige()` /
  vérification `EN COURS` dans tous les services d'écriture de paramètres.

### IMP-4 — 🟠 Endpoint agrégateur d'édition (DSR-654 option B)
- **Proposition** : `GET /scenarios/{id}/edition` renvoyant l'objet composite
  (périodes + TMH + comptages + variations + neutralisations + PIC) en un appel,
  avec propagation de l'id session IHM aux sous-traitements. Réduit la latence et
  garantit un instantané cohérent.

### IMP-5 — 🟡 Traçabilité applicative (`trppu_api_log`, `trppu_recalcul_log`)
- **Constat** : tables de log présentes au schéma mais non alimentées par l'API.
- **Proposition** : journaliser les appels de services (et recalculs) pour l'audit
  exigé implicitement par les critères « vérifier dans les logs Kibana ».

### IMP-6 — 🟡 Cohérence de nommage des champs de sortie
- **Constat** : mélange potentiel `nbJoursOuvres` (DSR-613, camelCase) vs
  `nb_jours_*` (colonnes/schemas, snake_case).
- **Proposition** : fixer une convention de sérialisation (alias Pydantic) pour
  l'interface IHM.

---

## C. Sujets à cadrer (renvois)

Les arbitrages fonctionnels nécessaires (cryptage, fériés, SAISON/LOCAL, contrats
d'API, bornes today, défauts 651/652, PIC version/legacy, id session IHM,
unicité/upsert) sont détaillés dans **`README_incomprehensions.md`**. Ils
conditionnent NEW-1 à NEW-3 et plusieurs IMP ci-dessus.

---

## D. Ordre de réalisation conseillé

1. **NEW-1** (migration) + **NEW-2** (crypto) + **NEW-3** (fériés/jours) — socle.
2. **IMP-2** (harmonisation scénario) puis compléter **DSR-634**.
3. Écritures : **DSR-644, 645, 646** ; TMH unifié **NEW-4 / IMP-1** (634/649/659).
4. Lectures : **DSR-650, 651, 652, 653, 655**.
5. **DSR-660 / 661** (PIC).
6. **DSR-656** (MAJ) puis **DSR-654 / IMP-4** (orchestration/édition).
7. **DSR-613** (peut être fait dès NEW-3 prêt).
8. Transverses : **IMP-3** (verrou statut), **IMP-5** (traçabilité), **IMP-6** (nommage).
