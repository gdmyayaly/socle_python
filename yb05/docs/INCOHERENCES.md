# Incohérences relevées — chaîne « clés de répartition » (DSR-696 à DSR-703)

Constats issus de l'implémentation de **DSR-696** et **DSR-698** dans `yb05`, obtenus en
confrontant les tickets au schéma réellement déployé (`python/db/db_new.sql`), aux volumes
constatés (`python/db/count.json`) et au code des deux projets.

Référence de volumétrie : `trppu_cles_repartition` porte **22 395 341 lignes**
(`AUTO_INCREMENT` à 24 217 441) ; `trppu_site_trafic`, `trppu_version_cle`,
`trppu_cles_repartition_calcule` et `trppu_referentiel` sont **vides**.

## Synthèse

| # | Incohérence | Gravité | Statut |
| - | ----------- | ------- | ------ |
| 1 | `id_site` n'existe pas dans `trppu_site_trafic` | **Bloquant** | Corrigé dans le script |
| 2 | CA2 (unicité site + référentiel) sans contrainte en base | **Bloquant** pour la recette | Corrigé par la migration |
| 3 | Aucun index ne sert l'agrégation sur 22,4 M lignes — contredit le CA6 | Fort | Corrigé par la migration |
| 4 | `trppu_version_cle` sans index sur `(co_regate, actif)` | Moyen | Corrigé par la migration |
| 5 | Débordement décimal possible : `decimal(25,19)` sommé dans `decimal(24,18)` | **Fort** | **Ouvert** — contrôle à jouer |
| 6 | `trppu_referentiel` ne permet pas d'exprimer « référentiel actif » | Fort | **Ouvert** — impacte DSR-701 |
| 7 | `docs/DSR-697.md` est vide (0 octet) | Fort | **Ouvert** — spécification manquante |
| 8 | YS04 laisse `id_referentiel` / `id_version_cle` à `0` sur les scénarios | Fort | **Ouvert** — hors périmètre 696/698 |
| 9 | `trppu_cles_repartition_calcule` sans unicité ni index | Moyen | **Ouvert** — à traiter en DSR-697 |
| 10 | Nom de table : `TRPPU_CLE_REPARTITION_CALCULE` (docs) vs `trppu_cles_repartition_calcule` (base) | Faible | **Ouvert** — cosmétique |
| 11 | Trois noms pour l'identifiant d'agrébal | Faible | **Ouvert** |
| 12 | `db_new.sql` n'est pas déployable sur MariaDB | Moyen | **Ouvert** — selon la cible |
| 13 | Documents d'analyse périmés décrivant des colonnes inexistantes | Faible | **Ouvert** |

---

## Bloquants, corrigés dans le livrable

### 1. `id_site` n'existe pas

`DSR-696.md` déclare la structure cible `TRPPU_SITE_TRAFIC (id_site, id_referentiel, …)`
(ligne 46) et écrit `INSERT INTO trppu_site_trafic (id_site, …)` (ligne 148). **Cette colonne
n'existe pas** : la table porte `co_regate_site varchar(10)`.

Le ticket se contredit lui-même — son propre `SELECT` (ligne 158) liste bien `co_regate_site`.
Recopié tel quel, l'`INSERT` échoue en `ERROR 1054 (Unknown column)`.

> **Traité** : `db/DSR-696_site_trafic.sql` utilise les noms réels. Un test de non-régression
> (`tests/test_scripts_dsr.py`) vérifie qu'aucun script ne réintroduit ce nom, et confronte
> toute la liste d'insertion au schéma.

À noter au passage : la source `trppu_cles_repartition.co_regate_site` est un `char(6)`, la
cible un `varchar(10)`. Sans incidence fonctionnelle, mais l'élargissement n'est motivé nulle
part.

### 2. Le CA2 ne repose sur rien

> CA2 — « Une seule ligne existe par site + id_referentiel. »

`trppu_site_trafic` ne porte que `PRIMARY KEY (id_site_trafic)`, auto-incrémentée : **aucune
clé unique, aucun index**. L'invariant n'est donc garanti que par la séquence `DELETE` puis
`INSERT` du traitement. Deux exécutions concurrentes, ou un `INSERT` rejoué sans son `DELETE`,
le violeraient silencieusement — et rien en base ne le signalerait.

> **Traité** : la migration ajoute `UNIQUE KEY uq_site_trafic (id_referentiel, co_regate_site)`.
> Le CA2 devient vrai par construction et un double lancement échoue proprement.

---

## Incohérences de performance, corrigées par la migration

### 3. L'agrégation balaie 22,4 M lignes

`trppu_cles_repartition` ne porte que `PRIMARY KEY (id)` et `UNIQUE KEY uk_pdi_ref (id_pdi,
id_referentiel)`. **Aucun index ne commence par `id_referentiel`**, aucun ne porte
`date_fin_validite`. La requête du ticket

```sql
WHERE id_referentiel = 1 AND date_fin_validite IS NULL GROUP BY co_regate_site
```

impose donc un balayage complet de la table à chaque exécution, suivi d'un tri pour le
`GROUP BY`. C'est directement contraire au bénéfice annoncé par le ticket :

> CA6 / Bénéfice — « Les sommes ne sont calculées qu'une seule fois par référentiel, ce qui
> devrait réduire les temps de calcul. »

> **Traité** : `KEY idx_cr_ref_actif (id_referentiel, date_fin_validite, co_regate_site)`.
> L'ordre des colonnes suit la requête : égalité, puis test `IS NULL`, puis la colonne qui
> porte à la fois le filtre site et le `GROUP BY` — résolu par l'index, sans tri.

### 4. `trppu_version_cle` n'est indexée que sur le référentiel

Son unique index est `idx_ref (id_referentiel)`, alors que les deux accès réels portent sur
`(co_regate, actif)` : le `UPDATE` de désactivation de DSR-698, et la lecture d'éligibilité de
**DSR-701 règle 9** (`WHERE co_regate = :coRegate AND actif = 'O'`).

> **Traité** : `KEY idx_vc_site_actif (co_regate, actif)`, posé tant que la table est vide.

---

## Points ouverts, à trancher

### 5. Risque de débordement décimal — à vérifier avant la première exécution

| | Source (`trppu_cles_repartition`) | Cible (`trppu_site_trafic`) |
| --- | --- | --- |
| trafic colis / oo / 3s | `decimal(25,19)` | `decimal(24,18)` |
| potentiel IP | `smallint` (nullable) | `bigint` (NOT NULL) |

`decimal(24,18)` n'autorise que **six chiffres avant la virgule**, soit un maximum de
`999999.999999999999999999`. Or on y écrit la **somme** de valeurs qui peuvent elles-mêmes
atteindre ce plafond. Un site dont le total dépasse le seuil fera échouer l'insertion
(`ERROR 1264, Out of range value`) en mode strict, ou sera tronqué silencieusement sinon.

Contrôle à jouer **avant** la première exécution, sur un référentiel réel :

```sql
SELECT MAX(t) FROM (
  SELECT SUM(trafic_colis) t FROM trppu_cles_repartition
   WHERE id_referentiel = 1 AND date_fin_validite IS NULL GROUP BY co_regate_site) x;
```

Si le maximum approche `999999`, élargir les trois colonnes (`decimal(34,18)` laisse seize
chiffres entiers) avant de charger quoi que ce soit. L'élargissement n'a **pas** été inclus par
défaut : il modifie le schéma sans qu'on sache encore s'il est nécessaire.

Question subsidiaire pour l'équipe data : dix-neuf décimales pour un trafic laisse penser que
ces colonnes portent déjà des ratios plutôt que des volumes — la sémantique mérite d'être
confirmée, elle conditionne l'interprétation des totaux.

### 6. « Référentiel actif » n'est pas exprimable en base

**DSR-701 règle 10** exige de trouver le référentiel actif d'un site :

```sql
SELECT id_referentiel FROM trppu_referentiel
 WHERE co_regate = :coRegate ORDER BY id_referentiel DESC LIMIT 1
```

Or `trppu_referentiel` ne comporte que quatre colonnes — `id_referentiel`, `co_regate`,
`date_reference`, `commentaire` — avec trois conséquences :

- **aucune colonne `actif`** : « actif » se réduit à « le plus grand id du site », ce qui
  interdit de désactiver un référentiel sans en créer un autre ;
- **`co_regate` est nullable et sans index** : des référentiels nationaux (`co_regate IS NULL`)
  ne seraient jamais retournés par la requête ci-dessus ;
- aucune date de fin de validité, alors que les tables filles en portent une.

À arbitrer avant DSR-697/701 : soit on aligne la table (colonne `actif`, index sur
`co_regate`), soit on documente que « actif = dernier id du site » est la définition officielle.

### 7. `DSR-697.md` est vide

Le fichier existe mais fait **0 octet**. C'est le maillon central de la chaîne — le calcul de
`trppu_cles_repartition_calcule`, soit `trafic du PDI / total du site` — celui qui consomme
précisément ce que produisent DSR-696 et DSR-698. Aucun autre exemplaire n'existe dans le
dépôt ; le ticket est à ré-exporter depuis Jira.

### 8. Les scénarios YS04 ne référencent aucun référentiel ni aucune version

`python/app/` ne renseigne jamais `trppu_scenario.id_referentiel` ni `id_version_cle` : les
scénarios sont créés avec `0`, valeur qui ne correspond à aucune ligne, et l'absence de clé
étrangère fait que rien ne le signale. La duplication de scénario perd également ces deux
colonnes.

Conséquence directe : les versions produites par DSR-698 **ne seront rattachées à aucun
scénario** tant que YS04 n'aura pas été modifié — c'est l'objet de DSR-700, à cadrer avec le
module YS04.

### 9. `trppu_cles_repartition_calcule` n'a aucune contrainte

La table cible de DSR-697 ne porte que sa PK auto-incrémentée : ni unicité sur
`(id_version_cle, id_pdi)`, ni index sur `id_version_cle` alors que c'est la clé de lecture du
batch `CALCUL_TRAFIC_PDI` (DSR-702). À traiter en même temps que DSR-697, sur le modèle de ce
qui a été fait ici pour `trppu_site_trafic`.

### 10. Nom de table incohérent entre docs et base

Toutes les spécifications écrivent `TRPPU_CLE_REPARTITION_CALCULE` (singulier), y compris le
schéma de flux de DSR-696. La table réelle s'appelle **`trppu_cles_repartition_calcule`**
(pluriel). Sans conséquence tant qu'on écrit le SQL depuis le schéma, mais toute requête
copiée depuis un ticket échouera.

### 11. Trois noms pour l'identifiant d'agrébal

Le même concept métier apparaît sous trois formes : `trppu_agrebal_pdi.agrebal_id` (`int`),
`trppu_trafic_agrebal.id_agrebal` (`int`) et `trppu_trafic_pdi.id_agrebal` (`bigint`) — sans
compter `agrebal_id_pdi`, qui est la clé technique de la première table et non un identifiant
de PDI, malgré son nom. Source d'erreur garantie lors de l'écriture des jointures de DSR-702.

### 12. `db_new.sql` n'est pas déployable sur MariaDB

Deux éléments réservés à MySQL 8 ont été réintroduits dans le schéma alors que la version
précédente (`db.sql`) les avait délibérément retirés pour compatibilité :

- la colonne générée `trppu_agrebal_pdi.agrebal_pdi_ids` et son **index multi-valué**
  `idx_pdi_ids` (MySQL 8.0.17+, inconnu de MariaDB) ;
- la collation `utf8mb4_0900_ai_ci` (l'ancien schéma utilisait `utf8mb4_general_ci`).

Sans incidence sur les trois scripts livrés, qui restent du SQL standard — mais à trancher si
la cible de déploiement n'est pas MySQL 8.

### 13. Documents d'analyse périmés

- `python/analyse_db_scenario.md` décrit `trppu_cles_repartition` avec « 15 colonnes `pct_*` »
  — aucune n'existe, ni dans le schéma actuel ni dans le précédent.
- `python/api_docs/dsr/cartographie_complete_systeme.md` annonce
  `trppu_agrebal_pdi (PK (id_agrebal, id_pdi))` et `trppu_cles_repartition (PK id_pdi)` avec
  « 9 colonnes `trafic_*` decimal(10,8) » : les clés primaires et les types sont faux.
- `python/api_docs/dsr/cartographie_donnees_persistees.md` qualifie `trppu_cles_repartition` de
  « jamais peuplée », alors qu'elle porte 22,4 M lignes.

Ces documents précèdent la réécriture `db.sql` → `db_new.sql`. À ne pas utiliser comme
référence : seul `python/db/db_new.sql` fait foi.

---

## Note technique — la migration échappe à la détection de DDL

Sans rapport avec les tickets, mais à connaître pour l'exploitation. Pour être rejouable, la
migration transporte ses `ALTER` dans une chaîne exécutée par `PREPARE`/`EXECUTE` — MySQL ne
connaissant pas `ADD INDEX IF NOT EXISTS`. Pour l'analyseur du socle, le fichier ne contient
donc que des `SET`, `PREPARE`, `EXECUTE` et `DEALLOCATE` : **`is_ddl` renvoie `False` partout**,
l'avertissement « DDL en mode transactionnel » ne se déclenche pas, alors que le commit
implicite a bien lieu. Jouer ce fichier avec `transactional=False`. Comportement verrouillé par
`tests/test_scripts_dsr.py`.
