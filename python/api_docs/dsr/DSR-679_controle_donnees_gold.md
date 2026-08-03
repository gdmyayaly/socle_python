# DSR-679 — Contrôle des données gold TRPPU

**Destinataires** : équipe métier / équipe data (lecture SQL requise, pas de connaissance de
l'API nécessaire).
**Objet** : deux écarts constatés entre les tables gold `g_trppu_trafics_jour_3`,
`g_trppu_trafics_semaine_3` et `g_trppu_trafics_mois_3`, et les requêtes pour les rejouer.
**Périmètre du contrôle** : site **400300**, période **01-03-2025 → 31-03-2026**.

Toutes les requêtes ci-dessous sont **directement exécutables dans Databricks** (copier-coller,
aucun paramètre à substituer hors du bloc « à adapter » en tête de chaque requête).

---

## 1. En deux phrases

L'API TRPPU interroge trois tables qui contiennent le même trafic à trois niveaux d'agrégation
(jour, semaine, mois) et choisit la plus adaptée à la période demandée. Le contrôle vérifie que
les trois racontent la même chose.

**Résultat** : la semaine est un agrégat fidèle du jour. **Le mois ne l'est pas pour le trafic
prévisionnel** — et une petite quantité de trafic PPI constaté n'est présente que dans le jour.

---

## 2. Synthèse

| Comparaison | Trafic constaté | Trafic prévisionnel |
| ----------- | --------------- | ------------------- |
| jour vs semaine | identique, **sauf PPI : −48** | **identique sur les 6 objets** |
| jour vs mois | identique, **sauf PPI : −48** | **différent sur les 6 objets** |

Les objets restitués par les tables trafics sont `OO`, `OS`, `PR`, `PPI`, `CO`, `IP` — deux
d'entre eux (`PR`, `PPI`) ne figurent pas dans la table de correspondance
`g_trppu_obj_mapping`, qui déclare de son côté `PQ` et `EQ` (voir §5).

Trois points sont à trancher avec l'équipe data : le **prévisionnel mensuel** (§3), les
**48 objets PPI** (§4) et le **référentiel des codes objets** (§5). Le récapitulatif est en §7.

---

## 3. Anomalie 1 — le prévisionnel du mois n'est pas la somme des prévisionnels du jour

### Ce qui est constaté

Sur **octobre 2025 → mars 2026** (six mois entiers, donc aucun effet de bord possible) :

| Objet | Somme des jours | Table mois | Écart | % |
| ----- | --------------: | ---------: | ----: | -: |
| **OO** | 45 477 | 73 530 | **+28 053** | **+61,7 %** |
| PR | 88 251 | 83 937 | −4 314 | −4,9 % |
| IP | 136 968 | 135 008 | −1 960 | −1,4 % |
| CO | 67 937 | 67 278 | −659 | −1,0 % |
| OS | 24 350 | 24 219 | −131 | −0,5 % |
| PPI | 9 212 | 9 191 | −21 | −0,2 % |
| **Total** | **372 195** | **393 163** | **+20 968** | **+5,6 %** |

### Pourquoi ce n'est pas un problème de bornes de dates

Deux éléments l'excluent :

1. La période retenue commence un **1er du mois** et finit un **dernier jour du mois** : la
   table mois couvre donc exactement les mêmes journées que la table jour, ni plus ni moins.
2. Le même contrôle mené sur la **table semaine**, sur une période alignée du lundi au
   dimanche, donne **zéro écart** sur le prévisionnel. Si le problème venait de la méthode de
   comparaison, la semaine serait fausse elle aussi.

Les écarts vont par ailleurs **dans les deux sens** selon l'objet (+61 % sur OO, −4,9 % sur PR),
ce qui exclut un simple décalage de périmètre.

### Hypothèse à confirmer

Le prévisionnel mensuel serait **calculé directement au niveau mois** (le ticket mentionne
`nb_objet_prevu_recadre_bu` — un recadrage BU) plutôt qu'obtenu en additionnant les prévisions
journalières. Ce ne serait alors pas une erreur de chargement mais **deux définitions
différentes du prévisionnel**.

### Conséquence côté API

L'API choisit automatiquement la table la mieux adaptée : pour une période longue, elle utilise
la table **mois**. Une demande de prévisionnel sur plusieurs mois renvoie donc le chiffre de la
table mois — soit, sur OO, 61 % de plus que la somme des journées.

### ❓ Question à trancher

> **Pour le trafic prévisionnel, quelle maille fait foi : le mois (recadré) ou la somme des
> jours ?**

- Si c'est le **mois** : rien à changer dans l'API, mais il faut documenter que le prévisionnel
  dépend de la maille interrogée.
- Si c'est le **jour** : l'API devra interroger la table jour pour toute la partie
  prévisionnelle, quelle que soit la durée demandée.

### Requête A — reproduire l'écart global (les 6 objets)

```sql
-- ============ à adapter ============
-- site : 400300   |   période prévisionnelle : 2025-10 -> 2026-03
-- ===================================
SELECT COALESCE(j.co_type_objet, m.co_type_objet)      AS objet,
       j.prevu_somme_des_jours,
       m.prevu_table_mois,
       m.prevu_table_mois - j.prevu_somme_des_jours    AS ecart,
       ROUND(100 * (m.prevu_table_mois - j.prevu_somme_des_jours)
             / NULLIF(j.prevu_somme_des_jours, 0), 2)  AS ecart_pct
FROM (
    SELECT co_type_objet, SUM(trafic_prevu) AS prevu_somme_des_jours
    FROM ppd_dd_kairos_int.03_gold.g_trppu_trafics_jour_3
    WHERE co_regate = '400300'
      AND co_niveau_regroupement_operationnel = 'SITE'
      AND da_comptage BETWEEN '2025-10-01' AND '2026-03-31'
      AND co_annee_comptage IN (2025, 2026)
    GROUP BY co_type_objet
) j
FULL OUTER JOIN (
    SELECT co_type_objet, SUM(trafic_prevu) AS prevu_table_mois
    FROM ppd_dd_kairos_int.03_gold.g_trppu_trafics_mois_3
    WHERE co_regate = '400300'
      AND co_niveau_regroupement_operationnel = 'SITE'
      AND co_mois_comptage BETWEEN '2025-10' AND '2026-03'
      AND co_annee_comptage IN (2025, 2026)
    GROUP BY co_type_objet
) m ON j.co_type_objet = m.co_type_objet
ORDER BY ABS(COALESCE(m.prevu_table_mois, 0) - COALESCE(j.prevu_somme_des_jours, 0)) DESC
```

### Requête B — localiser l'écart, mois par mois

Permet de distinguer un **incident de chargement** (écart concentré sur un ou deux mois) d'une
**différence de méthode de calcul** (écart réparti sur tous les mois).

```sql
-- ============ à adapter ============
-- site : 400300   |   objet : 'OO'   |   période : 2025-10 -> 2026-03
-- ===================================
SELECT COALESCE(j.mois, m.mois)        AS mois,
       j.prevu_somme_des_jours,
       m.prevu_table_mois,
       m.prevu_table_mois - j.prevu_somme_des_jours AS ecart
FROM (
    SELECT co_mois_comptage AS mois, SUM(trafic_prevu) AS prevu_somme_des_jours
    FROM ppd_dd_kairos_int.03_gold.g_trppu_trafics_jour_3
    WHERE co_regate = '400300'
      AND co_niveau_regroupement_operationnel = 'SITE'
      AND co_type_objet = 'OO'
      AND co_mois_comptage BETWEEN '2025-10' AND '2026-03'
      AND co_annee_comptage IN (2025, 2026)
    GROUP BY co_mois_comptage
) j
FULL OUTER JOIN (
    SELECT co_mois_comptage AS mois, SUM(trafic_prevu) AS prevu_table_mois
    FROM ppd_dd_kairos_int.03_gold.g_trppu_trafics_mois_3
    WHERE co_regate = '400300'
      AND co_niveau_regroupement_operationnel = 'SITE'
      AND co_type_objet = 'OO'
      AND co_mois_comptage BETWEEN '2025-10' AND '2026-03'
      AND co_annee_comptage IN (2025, 2026)
    GROUP BY co_mois_comptage
) m ON j.mois = m.mois
ORDER BY 1
```

### Requête C — témoin : la semaine, elle, tombe juste

À exécuter pour vérifier que la méthode de comparaison est saine. **Tous les écarts doivent
être à 0.** La période va du lundi 29-09-2025 au dimanche 29-03-2026, donc des semaines ISO
entières.

```sql
-- ============ à adapter ============
-- site : 400300   |   période : lundi 2025-09-29 -> dimanche 2026-03-29
--                  (semaines ISO 2025-40 -> 2026-13)
-- ===================================
SELECT COALESCE(j.co_type_objet, s.co_type_objet)       AS objet,
       j.prevu_somme_des_jours,
       s.prevu_table_semaine,
       s.prevu_table_semaine - j.prevu_somme_des_jours  AS ecart
FROM (
    SELECT co_type_objet, SUM(trafic_prevu) AS prevu_somme_des_jours
    FROM ppd_dd_kairos_int.03_gold.g_trppu_trafics_jour_3
    WHERE co_regate = '400300'
      AND co_niveau_regroupement_operationnel = 'SITE'
      AND da_comptage BETWEEN '2025-09-29' AND '2026-03-29'
      AND co_annee_comptage IN (2025, 2026)
    GROUP BY co_type_objet
) j
FULL OUTER JOIN (
    SELECT co_type_objet, SUM(trafic_prevu) AS prevu_table_semaine
    FROM ppd_dd_kairos_int.03_gold.g_trppu_trafics_semaine_3
    WHERE co_regate = '400300'
      AND co_niveau_regroupement_operationnel = 'SITE'
      AND co_semaine_comptage BETWEEN '2025-40' AND '2026-13'
      AND co_annee_comptage IN (2025, 2026)
    GROUP BY co_type_objet
) s ON j.co_type_objet = s.co_type_objet
ORDER BY 1
```

---

## 4. Anomalie 2 — PPI : 48 objets constatés présents seulement dans la table jour

### Ce qui est constaté

Sur **mars → septembre 2025**, trafic **constaté** de l'objet `PPI` :

| Source | Valeur |
| ------ | -----: |
| Somme des jours | 9 201 |
| Table mois | 9 153 |
| Table semaine | même écart de **−48** |

Le **même écart de 48** se retrouve dans les deux agrégats, alors que tous les autres objets
sont strictement identiques. La table jour porte donc 48 objets PPI que ni la semaine ni le mois
ne reprennent. Le volume est faible, mais l'écart est systématique — ce n'est pas un arrondi.

### Requête D — localiser le mois concerné

```sql
-- ============ à adapter ============
-- site : 400300   |   objet : 'PPI'   |   période : 2025-03 -> 2025-09
-- ===================================
SELECT COALESCE(j.mois, m.mois)         AS mois,
       j.constate_somme_des_jours,
       m.constate_table_mois,
       m.constate_table_mois - j.constate_somme_des_jours AS ecart
FROM (
    SELECT co_mois_comptage AS mois, SUM(trafic_constate) AS constate_somme_des_jours
    FROM ppd_dd_kairos_int.03_gold.g_trppu_trafics_jour_3
    WHERE co_regate = '400300'
      AND co_niveau_regroupement_operationnel = 'SITE'
      AND co_type_objet = 'PPI'
      AND co_mois_comptage BETWEEN '2025-03' AND '2025-09'
      AND co_annee_comptage IN (2025)
    GROUP BY co_mois_comptage
) j
FULL OUTER JOIN (
    SELECT co_mois_comptage AS mois, SUM(trafic_constate) AS constate_table_mois
    FROM ppd_dd_kairos_int.03_gold.g_trppu_trafics_mois_3
    WHERE co_regate = '400300'
      AND co_niveau_regroupement_operationnel = 'SITE'
      AND co_type_objet = 'PPI'
      AND co_mois_comptage BETWEEN '2025-03' AND '2025-09'
      AND co_annee_comptage IN (2025)
    GROUP BY co_mois_comptage
) m ON j.mois = m.mois
ORDER BY 1
```

### Requête E — descendre au jour, une fois le mois identifié

```sql
-- ============ à adapter ============
-- site : 400300   |   objet : 'PPI'   |   mois identifié par la requête D
-- ===================================
SELECT da_comptage, trafic_constate, trafic_prevu
FROM ppd_dd_kairos_int.03_gold.g_trppu_trafics_jour_3
WHERE co_regate = '400300'
  AND co_niveau_regroupement_operationnel = 'SITE'
  AND co_type_objet = 'PPI'
  AND co_mois_comptage = '2025-06'          -- <= remplacer par le mois en écart
  AND co_annee_comptage IN (2025)
  AND trafic_constate > 0
ORDER BY da_comptage
```

---

## 5. Anomalie 3 — deux référentiels d'objets qui ne concordent pas

### Ce qui est constaté

Le ticket prévoit que les codes objets soient résolus via la table de correspondance
`g_trppu_obj_mapping` (mapping entre codes comptage CTR et codes TRPPU). **Cette jointure est
impossible** : les tables `g_trppu_trafics_*_3` ne portent aucun code comptage CTR — le mapping
a déjà été appliqué en amont, elles sont directement agrégées à l'objet TRPPU
(`co_type_objet`). Schéma complet de `g_trppu_trafics_jour_3` :

```
co_niveau_regroupement_operationnel, co_regate, co_type_regate, co_type_objet,
da_comptage, co_annee_comptage, co_mois_comptage, co_semaine_comptage,
trafic_constate, trafic_prevu
```

Il reste toutefois un désaccord entre les deux tables sur **2 codes objets sur 6** :

| Code | Présent dans les tables trafics `_3` | Présent dans `g_trppu_obj_mapping` |
| ---- | ------------------------------------ | ---------------------------------- |
| OO | oui | oui — OBJETS ORDINAIRES |
| OS | oui | oui — OBJETS SUIVIS |
| CO | oui | oui — COLIS |
| IP | oui | oui — IMPRIMÉS PUBLICITAIRES |
| **PR** | **oui** | **non** |
| **PPI** | **oui** | **non** |
| **PQ** | **non** | **oui — PRESSE QUOTIDIENNE** |
| **EQ** | **non** | **oui — E-PAQ / PPI** |

`PR` (trafics) et `PQ` (mapping) désignent vraisemblablement tous deux la presse ; `PPI`
(trafics) et `EQ` (mapping, libellé « E-PAQ / PPI ») semblent également se recouvrir. Mais rien
ne le garantit, et **aucune correspondance n'est déclarée nulle part**.

### Conséquence

L'API restitue les codes tels qu'ils sont présents dans les tables trafics : `OO`, `OS`, `PR`,
`PPI`, `CO`, `IP`. Un consommateur qui s'appuierait sur `g_trppu_obj_mapping` pour libeller ces
codes n'obtiendrait de libellé que pour 4 objets sur 6.

### ❓ Question à trancher

> **Quel est le référentiel qui fait foi pour les codes objets : celui des tables trafics
> (`PR`, `PPI`) ou celui de `g_trppu_obj_mapping` (`PQ`, `EQ`) ? Et si les deux sont valides,
> quelle est la correspondance officielle entre eux ?**

### Requête F — comparer les deux référentiels

```sql
-- ============ à adapter ============
-- site : 400300
-- ===================================
SELECT t.co_type_objet AS objet_dans_trafics,
       m.co_type_objet AS objet_dans_mapping,
       m.lb_type_objet AS libelle_du_mapping
FROM (
    SELECT DISTINCT co_type_objet
    FROM ppd_dd_kairos_int.03_gold.g_trppu_trafics_jour_3
    WHERE co_regate = '400300'
      AND co_niveau_regroupement_operationnel = 'SITE'
      AND co_annee_comptage IN (2025, 2026)
) t
FULL OUTER JOIN (
    SELECT DISTINCT co_type_objet, lb_type_objet
    FROM ppd_dd_kairos_int.03_gold.g_trppu_obj_mapping
) m ON t.co_type_objet = m.co_type_objet
ORDER BY 1 NULLS LAST, 2
```

Les lignes où une des deux colonnes est `NULL` sont les codes orphelins.

---

## 6. Point de vigilance sur toute requête de ces tables

Les tables gold contiennent le trafic **à plusieurs niveaux de regroupement** :
`SITE`, `ETABLISSEMENT`, `DEXC`, `DEPARTEMENT`, `PIC`, `PFC`, `NATIONAL`, `SIEGE`.

**Sans la clause `AND co_niveau_regroupement_operationnel = 'SITE'`, les niveaux s'additionnent
et les totaux sont gonflés** — silencieusement, sans erreur ni doublon visible. Toutes les
requêtes de ce document intègrent ce filtre ; il est indispensable dans toute analyse ad hoc.

Pour vérifier à quels niveaux un site est présent :

```sql
-- ============ à adapter ============
-- site : 400300
-- ===================================
SELECT co_niveau_regroupement_operationnel AS niveau,
       co_type_objet                       AS objet,
       COUNT(*)                            AS nb_lignes,
       SUM(trafic_constate)                AS constate,
       SUM(trafic_prevu)                   AS prevu
FROM ppd_dd_kairos_int.03_gold.g_trppu_trafics_jour_3
WHERE co_regate = '400300'
  AND da_comptage BETWEEN '2025-03-01' AND '2026-03-31'
  AND co_annee_comptage IN (2025, 2026)
GROUP BY 1, 2
ORDER BY 1, 2
```

Sur 400300, cette requête ne renvoie que `SITE`, avec **396 lignes par objet pour 396 jours** —
soit exactement une ligne par jour et par objet, sans doublon. Le contrôle reste à mener sur une
régate qui est aussi tête de PIC ou d'établissement.

---

## 7. Récapitulatif des questions posées à l'équipe data

| # | Question | Bloquant |
| - | -------- | -------- |
| 1 | Pour le **trafic prévisionnel**, quelle maille fait foi : le mois recadré, ou la somme des jours ? | **Oui** — conditionne le comportement de l'API |
| 2 | Quel **référentiel d'objets** fait foi — `PR`/`PPI` des tables trafics ou `PQ`/`EQ` de `g_trppu_obj_mapping` — et quelle est la correspondance officielle ? | **Oui** — conditionne les codes restitués |
| 3 | D'où viennent les **48 objets PPI constatés** présents dans la table jour et absents des agrégats semaine et mois ? | Non — faible volume, mais à corriger |
| 4 | Le trafic constaté et le trafic prévisionnel sont-ils bien tous deux exploitables **quelle que soit la date** ? (le contrôle montre du constaté après la date de mise en œuvre et du prévisionnel avant) | Non — comportement conforme à l'attendu, confirmation de principe |

---

## Annexe — repères de lecture des tables

| Table | Colonne de date | Format | Partition |
| ----- | --------------- | ------ | --------- |
| `g_trppu_trafics_jour_3` | `da_comptage` | `AAAA-MM-JJ` | `co_annee_comptage`, `co_mois_comptage` |
| `g_trppu_trafics_semaine_3` | `co_semaine_comptage` | `AAAA-NS` (ex. `2026-13`, semaine ISO) | `co_annee_comptage` |
| `g_trppu_trafics_mois_3` | `co_mois_comptage` | `AAAA-MM` | `co_annee_comptage` |

Colonnes communes : `co_regate` (code régate du site),
`co_niveau_regroupement_operationnel`, `co_type_objet` (objet/produit TRPPU),
`trafic_constate`, `trafic_prevu`.

**Renseigner les colonnes de partition dans le `WHERE`** (`co_annee_comptage`, et
`co_mois_comptage` pour la table jour) accélère fortement les requêtes : Databricks n'ouvre
alors que les partitions concernées. Ce n'est pas obligatoire pour la justesse du résultat.

Attention également au format des périodes : sur la table semaine, une borne s'écrit
`'2026-13'` et non `'2026-03-29'` ; sur la table mois, `'2026-03'`.
