# Rapport d'impact — Migration base `db_09_08_2026.sql`

**Périmètre :** table `trppu_neutralisations` (jours / périodes neutralisés d'un scénario)
**Destinataire :** Métier TRPPU
**Objet :** vérifier toutes les modifications de la dernière migration susceptibles d'impacter l'application, suite au retrait de la colonne `type`.
**Statut :** constat & points d'arbitrage — *aucune modification de code n'a été réalisée.*

---

## 1. Résumé exécutif

- Le retrait de la colonne `type` de `trppu_neutralisations` **n'est pas une erreur** : il est **conforme à la dernière version du ticket `DSR-645` (jira/v2)**, qui ne prévoit plus de colonne `type` et confie la catégorisation des jours neutralisés à la colonne **`motif`** (texte libre saisi par l'utilisateur).
- **Une seule table** est réellement impactée : `trppu_neutralisations`. Les autres écarts de la migration (sur `trppu_scenario`) sont **bénins**. **16 tables sur 19 sont identiques.**
- En revanche, **l'application actuelle est en décalage** : son code a été développé sur l'**ancienne** version du ticket (avec une colonne `type` à valeurs imposées `FERIE` / `PEAK` / `SAISON`). Une adaptation du service sera nécessaire — elle fera l'objet d'un chantier distinct, **hors de ce rapport**.

---

## 2. Détail des modifications de la migration

Comparaison entre l'ancien schéma de référence (`db_analyse/v2/dbV2.sql`) et la nouvelle migration (`db_migrations/db_09_08_2026.sql`).

### 2.1 `trppu_neutralisations` — **modifications impactantes**

| Élément | Avant | Après | Nature |
|---|---|---|---|
| Colonne `type` `enum('FERIE','PEAK','SAISON')` | présente | **supprimée** | 🔴 Rupture |
| Clé unique `uq_neutre` | `(id_scenario, dt_debut, type)` | `(id_scenario, dt_debut, dt_fin)` | 🔴 Sémantique d'unicité modifiée |
| Index `idx_neutre_periode (id_scenario, dt_debut, dt_fin)` | présent | supprimé (absorbé par la nouvelle clé unique) | 🟠 Mineur |
| Colonne `motif varchar(255)` | présente (jusqu'ici inutilisée par le code) | présente | ✅ **Devient le porteur de la catégorie** |

**Structure finale de la table** (migration `db_09_08_2026.sql`) :

| Colonne | Type | Rôle |
|---|---|---|
| `id_neutralisation` | bigint (PK, auto) | identifiant |
| `id_scenario` | bigint | scénario rattaché |
| `dt_debut` | date | début de période |
| `dt_fin` | date | fin de période (= `dt_debut` si un seul jour) |
| `nb_jour` | int | nb de jours déduits de la période du scénario |
| `motif` | varchar(255) | **type / raison du jour neutralisé, saisi par l'utilisateur** |
| `dt_creation` | datetime | date d'ajout |
| `id_rh` | varchar(255) | ID RH crypté de l'auteur |

### 2.2 `trppu_scenario` — modifications **bénignes** (déjà alignées au code)

| Colonne | Avant | Après | Impact |
|---|---|---|---|
| `nb_jours_semaine` / `nb_jours_ouvres` / `nb_jours_ouvrables` / `nb_jours_scenario` | `tinyint` | `smallint` | Aucun (élargissement déjà attendu par le code) |
| `est_fige` | `tinyint(1)` | `smallint` | Aucun |
| `id_rh_creation` / `id_rh_maj` | `varchar(40)` | `varchar(255)` | Aucun (tokens RH cryptés) |

Ces changements **vont dans le sens de l'application** — aucun risque.

### 2.3 Les 16 autres tables — **inchangées**

`demande_dsr`, `trppu_agrebal_pdi`, `trppu_api_log`, `trppu_cles_repartition`, `trppu_pic_coefficients`, `trppu_pic_coefficients_ko`, `trppu_pic_version`, `trppu_produit`, `trppu_recalcul_log`, `trppu_scenario_comptages_manuels`, `trppu_scenario_exclusions`, `trppu_scenario_pic_coeffs`, `trppu_scenario_variations_prev`, `trppu_site`, `trppu_tmh`, `trppu_trafic_agrebal`, `trppu_trafic_pdi` : aucune modification de colonne.

> ℹ️ *L'absence de la colonne `id_rh` dans `trppu_scenario_comptages_manuels` et `trppu_scenario_variations_prev` est un écart **préexistant** (déjà présent dans l'ancien schéma) : il n'est **pas** introduit par cette migration.*

---

## 3. Cohérence avec le ticket `DSR-645` (jira/v2)

La nouvelle structure de la table correspond **exactement** au texte de la dernière version du ticket :

| Migration `db_09_08_2026.sql` | Ticket `DSR-645` v2 |
|---|---|
| Pas de colonne `type` ; catégorie portée par `motif` | « **motif** > type de jour(s) à neutraliser, **sera renseigné par l'utilisateur** » |
| Liste de colonnes : `id_scenario`, `dt_debut`, `dt_fin`, `nb_jour`, `motif`, `dt_creation`, `id_rh` | colonnes listées à l'identique dans le ticket |
| Clé unique `(id_scenario, dt_debut, dt_fin)` | « le service supprimera … la ligne pour **l'id du scénario, la date début et la date de fin** concernés » |

**Conclusion :** la migration applique fidèlement le ticket v2. Le concept de « type » à valeurs imposées (`FERIE`/`PEAK`/`SAISON`) est remplacé par un **motif libre**.

---

## 4. Impact sur l'application actuelle (constat)

Le code en place a été développé sur l'**ancienne** version du ticket et s'appuie partout sur la colonne `type`. Les zones suivantes devront être revues (le **comment** relève du chantier d'adaptation, pas de ce rapport) :

- **Lecture / regroupement** : le service regroupe aujourd'hui les neutralisations en 3 blocs imposés *feries / peak / saison* à partir de `type`.
- **Création / mise à jour / suppression** : les opérations filtrent et insèrent sur `type` ; la suppression se fait « par type », alors que le ticket v2 demande une suppression **par période** (`dt_debut` + `dt_fin`).
- **Contrat d'API** : le champ `type` (valeurs imposées) est exposé en entrée/sortie et en paramètre de suppression.
- **Audit RH** : la traçabilité expose actuellement la valeur `type`.

*Sans adaptation, ces opérations échoueront en base (colonne `type` inexistante).*

---

## 5. Points d'arbitrage à confirmer par le métier

1. **Remplacement `type` → `motif`** : confirmer que la catégorie d'un jour neutralisé est désormais un **texte libre** saisi par l'utilisateur, **sans liste imposée** (`FERIE` / `PEAK` / `SAISON` ne sont plus contraintes par la base).

2. **Suppression par période** : confirmer que la suppression se fait bien **par `dt_debut` + `dt_fin`** (et non plus « par type »), conformément à DSR-645 v2 — et préciser le comportement attendu côté IHM.

3. **Unicité `(id_scenario, dt_debut, dt_fin)`** : pour un jour férié (`dt_debut = dt_fin`), il ne peut désormais exister **qu'une seule ligne** pour ce jour. Est-ce acceptable, ou faut-il pouvoir saisir plusieurs motifs sur une même date ?

4. **Regroupement / affichage IHM** : l'écran actuel regroupe les neutralisations en *feries / peak / saison*. Sans valeurs imposées, sur **quelle base regrouper** l'affichage ? (liste de motifs de référence ? motifs normalisés ? plus de regroupement ?)

5. **Cohérence des numéros de ticket** : la version v2 de `DSR-652` porte désormais sur la table `trppu_tmh` (flag `bl_manuel`), alors que l'application rattache ce numéro à la *lecture regroupée des neutralisations*. À clarifier pour éviter toute confusion de périmètre.

---

## 6. Annexe — sources

- Nouvelle migration : `db_migrations/db_09_08_2026.sql`
- Ancien schéma de référence : `db_analyse/v2/dbV2.sql`
- Ticket (dernière version) : `jira/v2/DSR-645.md`, `jira/v2/DSR-652.md`
- Code applicatif concerné : `app/routes/trppu_neutralisations/`, `app/routes/trppu_audit/`
