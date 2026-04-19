# Projet ETL — Agrégation des Trafics Postaux par Site

> **Objectif :** transformer des données de trafic granulaires (multi-lignes par site et par période) en tables pivotées « une ligne = une période × un site » avec les catégories métier en colonnes, pour simplifier radicalement la consommation par les dashboards, applications et rapports.

---

## Sommaire

1. [Contexte et problématique](#1-contexte-et-problématique)
2. [Architecture cible](#2-architecture-cible)
3. [Logique métier](#3-logique-métier)
4. [Modèle de données](#4-modèle-de-données)
5. [Modes d'exécution](#5-modes-dexécution)
6. [Structure des scripts](#6-structure-des-scripts)
7. [Livrables](#7-livrables)
8. [Procédure de déploiement](#8-procédure-de-déploiement)
9. [Planification en production](#9-planification-en-production)
10. [Maintenance et évolutions](#10-maintenance-et-évolutions)
11. [Requête de consommation](#11-requête-de-consommation)
12. [FAQ](#12-faq)

---

## 1. Contexte et problématique

### 1.1 Point de départ

Les tables sources (couche **gold** existante) exposent le trafic postal sous forme **granulaire** : une même journée × un même site peut générer plusieurs dizaines de lignes, réparties selon le type de comptage (`co_comptage`), le type d'objet (`co_type_objet`), le processus (`co_process`), etc.

Résultat : toute requête métier doit embarquer une logique complexe de `CASE WHEN` imbriqués, répliquée dans chaque rapport, chaque dashboard, chaque application.

### 1.2 Problèmes identifiés

| Problème | Impact |
|---|---|
| Logique métier dupliquée | Incohérences entre rapports (un oubli sur un code = écart entre deux dashboards) |
| Requêtes longues et complexes | Temps de réponse élevé, coût de calcul répété |
| Maintenance coûteuse | Un nouveau code métier oblige à modifier N rapports |
| Courbe d'apprentissage | Nouveaux utilisateurs perdus devant la complexité |

### 1.3 Solution retenue

Créer des **tables agrégées cibles** (pré-calculées, pivotées par catégorie métier) alimentées par un ETL industrialisé et planifié. Les consommateurs n'écrivent plus qu'un simple `SELECT * FROM table WHERE co_regate = ? AND periode = ?`.

---

## 2. Architecture cible

### 2.1 Vue d'ensemble

```
┌─────────────────────────────────────────────┐
│  COUCHE SOURCE (gold granulaire existante)  │
│  ─────────────────────────────────────────  │
│  g_mdp_trafics_jour_actualise               │
│  g_mdp_trafics_semaine_actualise            │
│  g_mdp_trafics_mois_actualise               │
└───────────────────┬─────────────────────────┘
                    │
                    │  ETL PySpark (Databricks Job)
                    │  • Filtrage whitelist
                    │  • Pivot par catégorie métier
                    │  • Écriture Delta (partitionnée)
                    ▼
┌─────────────────────────────────────────────┐
│      COUCHE CIBLE (gold agrégée)            │
│  ─────────────────────────────────────────  │
│  g_trafic_site_jour                         │
│  g_trafic_site_semaine                      │
│  g_trafic_site_mois                         │
└───────────────────┬─────────────────────────┘
                    │
                    │  SELECT simple (1 ligne)
                    ▼
         ┌──────────────────────┐
         │  Dashboards, API,    │
         │  rapports, Power BI  │
         └──────────────────────┘
```

### 2.2 Choix techniques

| Choix | Justification |
|---|---|
| **Delta Lake** | Transactions ACID, Time Travel, MERGE natif, Z-ORDER |
| **Partitionnement temporel** (date ou année) | Filtres par période quasi gratuits (partition pruning) |
| **Z-ORDER sur `co_regate`** | Filtres par site accélérés via data skipping |
| **PySpark (notebooks Databricks)** | Intégration native avec les Jobs et les widgets, debuggable interactivement |
| **Version SQL parallèle** | Alternative pour les équipes orientées SQL pur, même logique garantie |

---

## 3. Logique métier

### 3.1 Typologie des sites postaux

Les sites sont classés selon leur rôle dans la chaîne postale :

| Type | Rôle | Code `lb_type_entite_regate_court` |
|---|---|---|
| **Sites distributeurs** (aval) | Livrent au client final | `PDC1`, `PDC2`, `PPDC` |
| **Centres de tri** (amont) | Trient le courrier avant distribution | `PIC`, `CTC` |

Cette distinction est **critique** : certains flux ne sont comptés que sur un type de site, et les règles de classification diffèrent.

### 3.2 Catégories métier produites

| Catégorie | Description | Codes source principaux |
|---|---|---|
| **Courrier OO** | Courrier ordinaire (lettres, plis ménages/CEDEX) | `TI_COL_MENAGE`, `TI_COL_CEDEX`, `OR4PM`/`OR4PX` type `R` |
| **Presse mécanisée** | Presse triée mécaniquement | `OR4PM`/`OR4PX` type `P` + process `VT`/`DT` sur PIC/CTC |
| **Presse locale déclarative** | Presse comptée en déclaratif | `PPLP0` |
| **Presse Viapost hors méca** | Presse Viapost non mécanisée | `1POP0`, `VPIP0` |
| **Objets suivis (OS)** | Recommandés, suivis | `TRSP1` |
| **Imprimés publicitaires (IP)** | Prospectus, publicités | `IMPJ` |
| **Colis** | Colis standards | `TLOP1` |
| **PPI / e-Paq** | Petits paquets internationaux, e-commerce | `VQQP0`, `2QNP1`, `2QQP1` |

### 3.3 Le pattern de pivot

Chaque catégorie est calculée via un `SUM(CASE WHEN ...)` conditionnel :

```sql
SUM(CASE
    WHEN <conditions catégorie X> THEN <mesure>
    ELSE 0
END) AS categorie_x
```

En PySpark, encapsulé dans une fonction `somme_si(condition)` réutilisable :

```python
def somme_si(condition):
    return F.sum(F.when(condition, F.col(measure_col)).otherwise(0))
```

### 3.4 La whitelist de pré-filtrage

Avant même d'appliquer les `CASE WHEN`, on filtre en amont via une **liste blanche** : on ne garde que les lignes dont le `co_process` est `VT`/`DT` **ou** le `co_comptage` figure dans la liste autorisée.

**Double bénéfice :**
- **Performance** : on transporte 10× moins de lignes jusqu'à l'agrégation
- **Sécurité** : même si un nouveau code parasite apparaît en source (test, ajustement comptable), il ne pollue pas la cible

---

## 4. Modèle de données

### 4.1 Tables cibles

#### `g_trafic_site_jour`

| Colonne | Type | Description |
|---|---|---|
| `da_comptage` | `DATE` | Date du comptage (clé + partition) |
| `co_regate` | `STRING` | Code Regate du site (clé) |
| `type_site` | `STRING` | Type d'entité (PDC1/PDC2/PPDC/PIC/CTC) |
| `trafic_oo` | `BIGINT` | Volume courrier ordinaire |
| `presse_mecanisee` | `BIGINT` | Volume presse mécanisée |
| `presse_locale_declarative` | `BIGINT` | Volume presse locale |
| `presse_viapost_hors_meca` | `BIGINT` | Volume presse Viapost |
| `trafic_os` | `BIGINT` | Volume objets suivis |
| `trafic_ip` | `BIGINT` | Volume imprimés publicitaires |
| `trafic_colis` | `BIGINT` | Volume colis |
| `trafic_ppi` | `BIGINT` | Volume PPI / e-Paq |
| `dh_maj` | `TIMESTAMP` | Date/heure de dernière mise à jour |

#### `g_trafic_site_semaine`

Mêmes colonnes métier, avec la clé temporelle suivante :

| Colonne | Type |
|---|---|
| `co_annee_comptage` | `INT` (partition) |
| `co_semaine_comptage` | `INT` |
| `da_lundi_semaine_comptage` | `DATE` (date du lundi de la semaine) |

#### `g_trafic_site_mois`

| Colonne | Type |
|---|---|
| `co_annee_comptage` | `INT` (partition) |
| `co_mois_comptage` | `INT` |

### 4.2 Différences clés entre grains

| Grain | Mesure source | Granularité temporelle | Partition |
|---|---|---|---|
| **Jour** | `trafic_reel` | 1 ligne / jour / site | `da_comptage` |
| **Semaine** | `nb_objet_retenu` | 1 ligne / semaine / site | `co_annee_comptage` |
| **Mois** | `nb_objet_retenu` | 1 ligne / mois / site | `co_annee_comptage` |

> ⚠️ **Point d'attention :** la colonne de mesure diffère entre le grain jour (`trafic_reel`) et les grains semaine/mois (`nb_objet_retenu`). Cette différence vient des tables sources et doit être préservée pour garantir la cohérence numérique avec les rapports existants.

---

## 5. Modes d'exécution

L'ETL propose 3 modes, sélectionnés via un widget Databricks :

### 5.1 Mode `initial`

**Quand :** première fois que l'ETL est lancé, ou reset complet d'une table cible.

**Comportement :** `INSERT OVERWRITE` de toute la période demandée, sans préservation de l'existant.

**Exemple d'usage :** premier déploiement en production, chargement historique d'une nouvelle table.

### 5.2 Mode `backfill`

**Quand :** correction d'une période historique (ex. la source a été corrigée pour mars 2025 et il faut rejouer).

**Comportement :** `INSERT OVERWRITE` avec `replaceWhere` ciblé → seule la fenêtre demandée est remplacée, le reste de la table n'est pas touché.

**Exemple d'usage :** rejouer l'année 2024 suite à une correction silver, sans écraser 2025.

### 5.3 Mode `incremental`

**Quand :** exécution quotidienne/hebdomadaire/mensuelle en production.

**Comportement :** `MERGE` (upsert) sur les N dernières périodes — gère proprement les arrivées tardives de données source sans dupliquer.

**Exemple d'usage :** Job planifié qui rafraîchit automatiquement les 7 derniers jours à 4h du matin.

### 5.4 Tableau récapitulatif

| Mode | Opération Delta | Paramètres clés | Cas d'usage |
|---|---|---|---|
| `initial` | `INSERT OVERWRITE` | Plage totale | Premier chargement |
| `backfill` | `INSERT OVERWRITE` + `replaceWhere` | Fenêtre ciblée | Correction historique |
| `incremental` | `MERGE` | Nb périodes à rafraîchir | Production quotidienne |

---

## 6. Structure des scripts

Tous les scripts (PySpark et SQL) suivent la **même architecture en 9 étapes** pour garantir cohérence et maintenance simplifiée.

### 6.1 Les 9 étapes

1. **Widgets / paramètres** — ce qui change entre environnements
2. **Config métier** — codes, types de sites, process (LE point de configuration)
3. **Création de la table cible** — idempotent (`CREATE IF NOT EXISTS`)
4. **Détermination de la fenêtre** — calculée selon le mode
5. **Lecture source + pré-filtre whitelist** — performance
6. **Agrégation / pivot** — la logique métier
7. **Écriture cible** — 3 stratégies selon le mode
8. **Contrôle qualité** — vérification post-écriture
9. **Optimisation** — `OPTIMIZE` + `VACUUM` (job séparé recommandé)

### 6.2 La section « CONFIG MÉTIER »

C'est le cœur de la paramétrisation. Tout ajout futur se fait ici et **uniquement ici** :

```python
SITES_DISTRIB    = ["PDC1", "PDC2", "PPDC"]
SITES_TRI        = ["PIC", "CTC"]
PROCESS_COURRIER = ["VT", "DT"]

CODES = {
    "oo_menage_cedex": ["TI_COL_MENAGE", "TI_COL_CEDEX"],
    "oo_or4":          ["OR4PM", "OR4PX"],
    "presse_locale":   ["PPLP0"],
    "presse_viapost":  ["1POP0", "VPIP0"],
    "os":              ["TRSP1"],
    "ip":              ["IMPJ"],
    "colis":           ["TLOP1"],
    "ppi":             ["VQQP0", "2QNP1", "2QQP1"],
}
```

### 6.3 La fonction d'agrégation (PySpark)

Fonction **pure** (sans side-effect), testable en isolation, prend un DataFrame en entrée et retourne un DataFrame agrégé :

```python
def agreger_trafic_jour(df: DataFrame, measure_col: str) -> DataFrame:
    ...
```

---

## 7. Livrables

Six scripts sont livrés, deux par grain temporel :

| Grain | Version PySpark (notebook Databricks) | Version SQL pure |
|---|---|---|
| Jour | `etl_trafic_jour_spark.py` | `etl_trafic_jour.sql` |
| Semaine | `etl_trafic_semaine_spark.py` | `etl_trafic_semaine.sql` |
| Mois | `etl_trafic_mois_spark.py` | `etl_trafic_mois.sql` |

### 7.1 PySpark vs SQL — lequel choisir ?

| Aspect | PySpark | SQL |
|---|---|---|
| Testabilité unitaire | ✅ Fonctions pures | ⚠️ Plus difficile |
| Paramétrisation | ✅ Widgets natifs | ✅ `IDENTIFIER(:param)` |
| Orchestration | ✅ Jobs Databricks | ✅ Jobs Databricks SQL |
| Courbe d'apprentissage équipe data | ⚠️ Python requis | ✅ SQL universel |
| Factorisation de la config | ✅ Dict + helpers | ⚠️ Listes répétées |

**Recommandation :** utiliser **PySpark** pour la version maintenue en production (meilleure factorisation), et conserver la **version SQL** pour les tests rapides en Databricks SQL ou les consommateurs orientés SQL pur.

---

## 8. Procédure de déploiement

### 8.1 Prérequis

- Accès en écriture sur le schéma cible `ppd_dd_kairos_int.03_gold`
- Cluster Databricks avec Delta Lake (DBR 11+ recommandé)
- Droits sur les tables sources `g_mdp_trafics_*_actualise`

### 8.2 Étapes de déploiement

1. **Import du notebook** dans le workspace Databricks
2. **Premier run en mode `initial`** avec les dates souhaitées (valide la création de la table cible)
3. **Vérification QC** : exécuter la cellule de contrôle qualité, confronter avec la requête source d'origine
4. **Création du Job** en mode `incremental` (planifié)
5. **Documentation interne** : partager ce README avec les consommateurs

### 8.3 Validation croisée

Pour valider que la nouvelle table donne bien les mêmes chiffres que la requête historique :

```sql
-- Requête historique (source directe)
SELECT SUM(CASE WHEN ... END) AS trafic_oo_ref
FROM g_mdp_trafics_jour_actualise
WHERE co_regate = '245840' AND da_comptage = '2025-06-15';

-- Nouvelle table agrégée
SELECT trafic_oo AS trafic_oo_new
FROM g_trafic_site_jour
WHERE co_regate = '245840' AND da_comptage = '2025-06-15';

-- Les deux valeurs doivent être strictement égales
```

---

## 9. Planification en production

### 9.1 Jobs recommandés

| Job | Mode | Fréquence | Cron | Paramètres |
|---|---|---|---|---|
| **Refresh quotidien** | `incremental` | Chaque jour 4h | `0 0 4 * * ?` | `jours_refresh = 7` |
| **Refresh hebdo** | `incremental` | Chaque lundi 5h | `0 0 5 ? * MON` | `semaines_refresh = 4` |
| **Refresh mensuel** | `incremental` | 1er du mois 6h | `0 0 6 1 * ?` | `mois_refresh = 3` |
| **Maintenance** | `OPTIMIZE` + `VACUUM` | Chaque dimanche | `0 0 2 ? * SUN` | — |
| **Backfill** | `backfill` | À la demande | Manuel | Dates en widgets |

### 9.2 Pourquoi un chevauchement de fenêtres ?

En mode `incremental`, on rafraîchit plus que la dernière période (J-7 pour le jour, 4 semaines, 3 mois). Cela garantit que les **données tardives** (corrections appliquées en source quelques jours après la date de comptage) soient bien intégrées dans la cible.

### 9.3 Monitoring

Metrics à surveiller :

- Durée d'exécution du Job (alerte si dépasse X minutes)
- Nombre de lignes insérées/mises à jour (anomalie si 0 ou chute brutale)
- Delta entre `trafic_total_global` du jour et du jour précédent (détection de drift)

---

## 10. Maintenance et évolutions

### 10.1 Ajouter un nouveau code de comptage

**Un seul endroit à modifier : la section `CONFIG MÉTIER`.**

Exemple : le métier ajoute un nouveau flux « Presse X » avec le code `PXNEW`.

```python
# Avant
CODES = {
    ...
    "presse_locale": ["PPLP0"],
}

# Après
CODES = {
    ...
    "presse_locale": ["PPLP0", "PXNEW"],  # ← ajout du nouveau code
}
```

Aucune autre modification nécessaire — le pré-filtre whitelist et l'agrégation utilisent automatiquement la liste mise à jour.

### 10.2 Ajouter une nouvelle catégorie métier

Trois modifications :

1. **Ajouter la clé** dans le dictionnaire `CODES` :
   ```python
   CODES["nouvelle_categorie"] = ["CODE1", "CODE2"]
   ```
2. **Ajouter une ligne** dans la fonction `agreger_trafic_*` :
   ```python
   somme_si(est_distrib & F.col("co_comptage").isin(CODES["nouvelle_categorie"]))
       .alias("nouvelle_categorie"),
   ```
3. **Ajouter la colonne** dans le `CREATE TABLE` :
   ```sql
   nouvelle_categorie BIGINT,
   ```

### 10.3 Ajouter un nouveau type de site

Modifier les listes `SITES_DISTRIB` ou `SITES_TRI` dans la config métier. Si un **nouveau rôle** de site apparaît (ni distributeur ni tri), il faut créer une nouvelle constante et adapter les conditions d'agrégation.

### 10.4 Optimisation régulière

Planifier une fois par semaine :

```sql
OPTIMIZE g_trafic_site_jour ZORDER BY (co_regate);
VACUUM   g_trafic_site_jour RETAIN 168 HOURS;  -- 7 jours
```

Impact :
- **`OPTIMIZE` + `ZORDER`** : compaction des petits fichiers + réorganisation pour data skipping sur `co_regate`
- **`VACUUM`** : suppression des anciennes versions Delta (réduit le coût de stockage)

---

## 11. Requête de consommation

C'est la finalité du projet : après ETL, les consommateurs n'écrivent plus qu'une requête triviale.

### 11.1 Grain jour

```sql
SELECT *
FROM g_trafic_site_jour
WHERE co_regate    = '245840'
  AND da_comptage >= '2025-01-01'
  AND da_comptage <  '2026-01-01'
ORDER BY da_comptage;
```

### 11.2 Grain semaine

```sql
SELECT *
FROM g_trafic_site_semaine
WHERE co_regate         = '245840'
  AND co_annee_comptage = 2025
ORDER BY co_semaine_comptage;
```

### 11.3 Grain mois

```sql
SELECT *
FROM g_trafic_site_mois
WHERE co_regate         = '245840'
  AND co_annee_comptage = 2025
ORDER BY co_mois_comptage;
```

### 11.4 Comparaison avant/après

| Aspect | Avant (requête source) | Après (table agrégée) |
|---|---|---|
| Lignes de SQL | ~110 | 5 |
| Temps de réponse (site × 1 an) | 8–15 s | < 1 s |
| Lisibilité | ⚠️ Complexe | ✅ Trivial |
| Risque d'erreur métier | ⚠️ Élevé | ✅ Nul (logique centralisée) |

---

## 12. FAQ

**Q : Que se passe-t-il si la source est vide sur une journée ?**
R : La cible n'aura pas de ligne pour cette journée. Les consommateurs peuvent détecter les trous via un `LEFT JOIN` avec une table calendrier si besoin.

**Q : Peut-on revenir en arrière après un run raté ?**
R : Oui, via Delta Time Travel :
```sql
DESCRIBE HISTORY g_trafic_site_jour;
RESTORE TABLE g_trafic_site_jour TO VERSION AS OF <n>;
```

**Q : Comment vérifier la cohérence avec les anciens rapports ?**
R : Voir §8.3 — exécuter en parallèle la requête historique et la requête agrégée sur un échantillon (site + période), les chiffres doivent être strictement identiques.

**Q : Les deux versions (PySpark et SQL) donnent-elles exactement les mêmes résultats ?**
R : Oui, par construction. Les deux implémentent la même logique. La version SQL est fournie comme alternative, pas comme doublon fonctionnel.

**Q : Faut-il lancer les trois grains (jour/semaine/mois) indépendamment ?**
R : Oui, chaque grain lit sa propre source et alimente sa propre cible. Ils sont totalement indépendants et peuvent être orchestrés séparément.

**Q : Que faire si un code métier change de signification ?**
R : Mettre à jour la `CONFIG MÉTIER`, puis relancer un `backfill` sur la période impactée pour recalculer les valeurs historiques.

---

## Contact et support

- **Propriétaire du projet :** _à compléter_
- **Équipe data :** _à compléter_
- **Documentation source :** `/notebooks/etl_trafic/`
- **Repo Git :** _à compléter_

---

*Document rédigé dans le cadre du projet d'industrialisation des trafics postaux — couche gold agrégée.*
