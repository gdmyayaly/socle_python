# Analyse de la base de données — Module Scénarios TRPPU

> Périmètre analysé : `db.sql` (16 tables, préfixe `trppu_`)
> Date d'analyse : 2026-04-30

---

## 1. Vue d'ensemble du modèle

Le schéma modélise un **module de scénarisation du trafic postal** structuré autour de cinq familles fonctionnelles :

| Famille | Tables | Rôle |
|---|---|---|
| **Scénario (cœur)** | `trppu_scenario`, `trppu_scenario_variations_prev`, `trppu_scenario_comptages_manuels`, `trppu_scenario_exclusions`, `trppu_scenario_pic_coeffs`, `trppu_neutralisations`, `trppu_tmh` | Définition d'un scénario, paramètres figés, exclusions, neutralisations, agrégats hebdomadaires |
| **PIC (Plan Industriel & Commercial)** | `trppu_pic_version`, `trppu_pic_coefficients` | Référentiel des coefficients de répartition par produit / jour |
| **Référentiel** | `trppu_produit`, `trppu_amas_pdi`, `trppu_cles_repartition` | PDI, regroupements (amas), produits, clés de répartition |
| **Trafic calculé** | `trppu_trafic_pdi`, `trppu_trafic_amas` | Volumes ventilés par PDI puis agrégés par amas |
| **Journalisation** | `trppu_recalcul_log`, `trppu_api_log` | Audit des recalculs et appels API |

Le flux logique est cohérent : un **scénario** fige une **version PIC** + des **variations prévisionnelles**, applique des **neutralisations / exclusions**, déclenche un **calcul** matérialisé dans `trafic_pdi` puis agrégé dans `trafic_amas`.

---

## 2. Points forts

- **Séparation claire** entre données de référence (PIC, produits, clés) et données de scénario (figées).
- **Mécanisme de gel** explicite via `est_fige` + `trppu_scenario_pic_coeffs` (snapshot des coefficients PIC pour la reproductibilité du scénario).
- **Statut machine-à-états** sur `trppu_scenario.statut` (EN COURS → SIMULATION → VALIDE → VERROUILLE → ARCHIVE).
- **Index métier pertinents** sur les requêtes attendues :
  - `idx_scen_amas_prod_jour` (trafic_pdi) — calcul d'agrégation
  - `idx_scenario_site_statut` (scenario) — lecture par site
  - `idx_api_when` (api_log) — audit chronologique
- **Journalisation à deux niveaux** : technique (`api_log`) et fonctionnelle (`recalcul_log`).

---

## 3. Problèmes critiques

### 3.1 Intégrité référentielle incomplète

Plusieurs colonnes désignent des entités sans contrainte `FOREIGN KEY` :

| Table | Colonne | Devrait référencer |
|---|---|---|
| `trppu_scenario` | `id_pic_version` | `trppu_pic_version` ❌ aucune FK |
| `trppu_scenario_variations_prev` | `co_produit` | `trppu_produit` ❌ |
| `trppu_scenario_comptages_manuels` | `co_produit` | `trppu_produit` ❌ |
| `trppu_pic_coefficients` | `co_produit` | `trppu_produit` ❌ |
| `trppu_scenario_pic_coeffs` | `co_produit` | `trppu_produit` ❌ |
| `trppu_scenario_exclusions` | `co_produit` | `trppu_produit` ❌ |
| `trppu_tmh` | `co_produit` | `trppu_produit` ❌ |
| `trppu_trafic_pdi` | `co_produit`, `id_pdi`, `id_amas` | `trppu_produit`, `trppu_amas_pdi` ❌ |
| `trppu_trafic_amas` | `co_produit`, `id_amas` | idem ❌ |
| `trppu_amas_pdi` | `id_pdi` | `trppu_cles_repartition` ❌ |

> **Risque** : insertion de codes produits inexistants, scénarios pointant sur des PIC supprimés, orphelins après purge.

### 3.2 Incohérences de nommage

- **Préfixe PK incohérent** : `id_scenario`, `id_pic_version`, `id_tmh`, `id_log` (préfixés) **mais** `id` seul dans `trppu_scenario_pic_coeffs`, `trppu_neutralisations`, `trppu_scenario_exclusions`, `trppu_trafic_pdi`, `trppu_trafic_amas`.
- **`regate` vs `co_regate`** : `trppu_api_log.regate` rompt la convention (toutes les autres tables utilisent `co_regate`).
- **Mot-clé réservé** : `type` (`trppu_neutralisations`) est un mot réservé MySQL — à renommer en `co_type` ou `type_neutralisation`.

### 3.3 Tables de référence manquantes

- ❌ **Aucune table `trppu_site`** (ou équivalent) malgré `co_regate CHAR(6)` présent dans 7 tables → pas de garantie d'existence des sites.
- ❌ **Aucune table `trppu_pdi`** : `id_pdi` est PK dans `trppu_cles_repartition` mais ce n'est pas son rôle naturel (clés ≠ référentiel PDI).
- ❌ **Aucune table jours fériés** : les neutralisations FERIE sont saisies à la main, sans calendrier officiel.
- ❌ **Aucune table utilisateur / audit** : pas de `created_by`, `validated_by` sur `trppu_scenario` alors qu'un workflow de validation existe.

### 3.4 Contraintes métier absentes

Aucune contrainte `CHECK` n'est définie. Idéalement :

```sql
-- Cohérence des fenêtres
CHECK (periode_fin >= periode_debut)
CHECK (DATEDIFF(periode_fin, periode_debut) <= 730)  -- ≤ 24 mois
CHECK (dt_fin >= dt_debut)                            -- neutralisations

-- Cohérence des coefficients (somme = 1)
CHECK (coef_dense + coef_faible1 + coef_faible2 BETWEEN 0.99 AND 1.01)

-- Bornes
CHECK (variation_pct BETWEEN -100 AND 100)
CHECK (nb_produit >= 0)
CHECK (volume >= 0)
```

---

## 4. Problèmes de modélisation

### 4.1 `trppu_pic_coefficients` — sémantique ambiguë

```sql
jour_semaine ENUM('LUN',...,'SAM') NOT NULL,
dt_effet DATE NOT NULL,  -- ajouté : date pour le jour/produit – ??
UNIQUE KEY uq_tpc (id_pic_version, co_produit, jour_semaine, dt_effet)
```

Le commentaire `??` de l'auteur signale le doute. **Mélanger `jour_semaine` (catégoriel) et `dt_effet` (date)** est sémantiquement bancal :

- Soit la table porte des coefficients **hebdomadaires-types** (clé : `jour_semaine` seule)
- Soit elle porte des coefficients **datés** (clé : `dt_effet` seule, `jour_semaine` calculable)

→ **À clarifier** avec le métier. La duplication potentielle (deux dates pour le même `LUN`) est source d'ambiguïté lors du calcul.

### 4.2 `est_fige` redondant avec `statut`

Le booléen `est_fige` doublonne l'information portée par `statut IN ('VERROUILLE','ARCHIVE')`. Risque d'incohérence (ex. `est_fige=TRUE` mais `statut='EN COURS'`). **Recommandation** : supprimer `est_fige` ou imposer un trigger de cohérence.

### 4.3 `trppu_neutralisations.nb_jour` calculable

`nb_jour` peut être dérivé de `dt_fin - dt_debut + 1` (modulo neutralisations partielles). À transformer en colonne générée :
```sql
nb_jour INT GENERATED ALWAYS AS (DATEDIFF(dt_fin, dt_debut) + 1) STORED
```

### 4.4 `trppu_cles_repartition` — schéma non normalisé

15 colonnes `pct_*` empilées (pct_nature, pct_oo, pct_os_suivi, …). Si la liste de catégories doit évoluer, il faudra un `ALTER TABLE`. **Forme normalisée** :

```sql
CREATE TABLE trppu_cle_repartition (
  id_pdi BIGINT,
  co_categorie VARCHAR(20),
  pct DECIMAL(10,8),
  PRIMARY KEY (id_pdi, co_categorie)
);
```

De plus, **aucune contrainte ne garantit que la somme des `pct_*` = 1.0**.

### 4.5 `trppu_tmh` — nullabilité incohérente

```sql
volume_realise INT NULL,
volume_previsionnel INT NULL,
moyenne_journaliere DECIMAL(12,4) NOT NULL,  -- ⚠ NOT NULL alors que les volumes sont NULL
```

Si les deux volumes sont NULL, la moyenne et la semaine moyenne ne peuvent pas être calculées. Soit les rendre NULL, soit imposer la présence d'au moins un volume.

---

## 5. Risques de performance

### 5.1 `trppu_trafic_pdi` — table à très haut volume

Cardinalité estimée : `nb_scénarios × nb_pdi × nb_produits × 6 jours × 3 couleurs`.
Pour 100 scénarios, 50 000 PDI, 12 produits : **≈ 1,1 milliard de lignes**.

**Recommandations** :
- **Partitionnement** par `id_scenario` (RANGE ou KEY).
- Archivage des scénarios ARCHIVE dans une table séparée.
- Vérifier la sélectivité de l'index `idx_pdi_amas` (id_pdi, id_amas) — possiblement redondant avec un index sur `id_amas` seul vu les requêtes attendues.

### 5.2 Index manquants probables

| Table | Requête fréquente attendue | Index actuel | Manque |
|---|---|---|---|
| `trppu_scenario` | filtrage par `dt_creation`, `dt_validation` | aucun | `idx_scenario_dt` |
| `trppu_recalcul_log` | par `dt_recalcul` global | composé sur scenario+date | OK |
| `trppu_pic_coefficients` | recherche par `dt_effet` | aucun | `idx_pic_coef_date` |
| `trppu_amas_pdi` | par `co_regate` + `id_amas` | séparés | composite plus efficace |

### 5.3 Type `JSON` non indexé

`trppu_api_log.params JSON` n'est pas indexé. Pour des recherches sur `params->>'$.foo'`, prévoir un **index virtuel** ou conserver les paramètres critiques en colonnes typées.

---

## 6. Auditabilité

| Besoin | Présent ? |
|---|---|
| Qui a créé le scénario ? | ❌ |
| Qui a validé / verrouillé ? | ❌ |
| Qui a déclenché un recalcul manuel ? | ⚠️ `commentaire VARCHAR(255)` insuffisant |
| Quand un scénario a-t-il été modifié ? | ❌ pas de `dt_modification` |
| Historique des changements de statut ? | ❌ |

→ **Recommandation** : ajouter colonnes `created_by`, `updated_by`, `updated_at` sur `trppu_scenario`, et créer une table `trppu_scenario_historique_statut` pour la traçabilité du workflow.

---

## 7. Portabilité & cohérence technique

- **`ENUM` MySQL** non portable (PostgreSQL, Oracle). Préférer des tables de référence (`trppu_ref_statut`, `trppu_ref_jour_semaine`, `trppu_ref_couleur_pic`).
- **`AUTO_INCREMENT`** : à remplacer par `IDENTITY` ou séquences si migration prévue.
- **`BIGINT` partout** sur les PK : choix défensif raisonnable, mais surdimensionné pour `id_amas INT` qui reste en INT (incohérence de capacité avec les FK).
- **Pas de moteur déclaré** (`ENGINE=InnoDB` implicite) ni de charset (`utf8mb4`). À expliciter.
- **Pas de `ON DELETE` / `ON UPDATE`** sur les FK : comportement par défaut `RESTRICT` → suppression d'un scénario impossible sans cascade manuelle.

---

## 8. Synthèse des recommandations prioritaires

| Priorité | Action | Effort |
|---|---|---|
| 🔴 P1 | Ajouter les FK manquantes (cf. §3.1) | Faible |
| 🔴 P1 | Créer une table de référence `trppu_site` (co_regate) | Moyen |
| 🔴 P1 | Lever l'ambiguïté `dt_effet` / `jour_semaine` dans `trppu_pic_coefficients` | À spécifier métier |
| 🟠 P2 | Ajouter `created_by`, `updated_by`, `updated_at` + historique de statut | Moyen |
| 🟠 P2 | Renommer `regate` → `co_regate` dans `trppu_api_log`, uniformiser les PK `id` → `id_<entité>` | Faible |
| 🟠 P2 | Renommer `type` (`trppu_neutralisations`) — mot réservé | Faible |
| 🟠 P2 | Ajouter contraintes `CHECK` sur cohérence des dates et des coefficients | Faible |
| 🟡 P3 | Partitionner `trppu_trafic_pdi` par `id_scenario` | Moyen |
| 🟡 P3 | Normaliser `trppu_cles_repartition` (15 colonnes → table EAV) | Élevé (impact applicatif) |
| 🟡 P3 | Supprimer `est_fige` ou ajouter trigger de cohérence avec `statut` | Faible |
| 🟢 P4 | Calendrier des jours fériés | Moyen |
| 🟢 P4 | Migration `ENUM` → tables de référence (si portabilité requise) | Élevé |

---

## 9. Questions ouvertes pour le métier

1. **`co_roc`** (CHAR(6)) — quelle est la signification fonctionnelle ? Doit-il être FK vers une table de référence ?
2. **`version_scenario INT`** — versioning par incrément ? Lien avec `id_pic_version` ? Y a-t-il un mécanisme de scénario "parent" ?
3. **PIC `dt_effet`** — la même ligne `id_pic_version + co_produit + LUN` peut-elle exister pour plusieurs `dt_effet` ? Si oui, quelle est la règle de sélection au moment du gel ?
4. **Différence VALIDE vs VERROUILLE** — quel événement métier fait passer de l'un à l'autre ?
5. **Clés de répartition** — les 15 catégories sont-elles fixes ou amenées à évoluer ?
6. **Scope d'unicité** — peut-on avoir deux scénarios `EN COURS` simultanés sur le même `co_regate` ?
