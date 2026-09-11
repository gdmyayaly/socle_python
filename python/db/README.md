# Scripts SQL — base `dsr_mercure_aa`

Le schéma est découpé en trois scripts, à jouer **dans l'ordre**.

| Ordre | Fichier | Contenu | Rejouable |
|-------|---------|---------|-----------|
| 1 | `01_structure.sql` | `CREATE TABLE IF NOT EXISTS` : colonnes + clés primaires | oui (`IF NOT EXISTS`) |
| 2 | `02_donnees.sql` | Jeu de données de développement, identifiants explicites | oui (`INSERT IGNORE`) |
| 3 | `03_contraintes.sql` | Clés uniques, index, clés étrangères, contraintes `CHECK` | oui (procédures de test) |

```bash
mysql -u root -p < db/01_structure.sql
mysql -u root -p < db/02_donnees.sql
mysql -u root -p < db/03_contraintes.sql
```

`db_new.sql` reste le **dump de référence** du schéma (24 tables, sans données) :
c'est la source dont les fichiers 01 et 03 sont dérivés. Il n'est pas destiné à
être joué tel quel — il commence par `DROP TABLE IF EXISTS` sur chaque table.

## Pourquoi ce découpage

**Les clés primaires sont dans le fichier 1, pas le 3.** MySQL exige qu'une
colonne `AUTO_INCREMENT` soit indexée dès le `CREATE TABLE` (erreur 1075 :
*« there can be only one auto column and it must be defined as a key »*). La clé
primaire est donc indissociable de la création de la table. Tout le reste — clés
uniques, index secondaires, clés étrangères, `CHECK` — est différé au fichier 3.

**L'ordre des sections du fichier 2 suit les dépendances de clés étrangères.**
Il compte : le fichier 3 valide les FK et les `CHECK` sur les lignes déjà
chargées. Une incohérence introduite dans le fichier 2 ne se voit donc qu'à
l'exécution du fichier 3.

**Le fichier 3 est idempotent.** MySQL ne connaît pas `ADD INDEX IF NOT EXISTS`.
Deux procédures utilitaires (`_trppu_add_index_if_missing`,
`_trppu_add_constraint_if_missing`) interrogent `information_schema` avant chaque
ajout et ignorent ce qui existe déjà. Elles sont supprimées en fin de script.
Ses trois sections sont ordonnées : index d'abord (une FK a besoin d'un index sur
ses colonnes), puis clés étrangères, puis `CHECK`.

## Le jeu de données

> `02_donnees.sql` n'est **pas** un export de production. `db_new.sql` ne contient
> aucune ligne ; les données sont écrites à la main. Ne jamais le jouer sur une
> base réelle.

129 lignes réparties sur 23 des 24 tables (`trafic_staging` reste vide : c'est une
table de chargement, sans clé, donc non idempotente avec `INSERT IGNORE`).

Les scénarios sont construits pour couvrir la visibilité OPTIPACC et la machine à
états :

| id | site | statut | figé | pdi | agrébal | usage |
|----|------|--------|------|-----|---------|-------|
| 125 | 123456 | `VALIDE` | 1 | 1 | 1 | visible OPTIPACC, éligible mise en production |
| 126 | 123456 | `EN COURS` | 0 | 0 | 0 | éditable, invisible |
| 127 | 123456 | `SIMULATION` | 1 | 1 | 0 | Agrébal non calculé |
| 128 | 654321 | `EN PRODUCTION` | 1 | 1 | 1 | déjà en production |
| 129 | 123456 | `ARCHIVE` | 1 | 1 | 1 | terminal |

Le seul scénario `EN PRODUCTION` est sur le site **654321**, jamais sur 123456 :
sinon le contrôle C5 de DSR-707 (un seul scénario en production par site)
refuserait toute mise en production du scénario 125.

Quelques cas limites volontairement présents dans le jeu :

- `trppu_tmh` reproduit la formule du volume brut de DSR-689 — deux lignes `IP`
  (calculée + ajout manuel) qui s'additionnent, une ligne `CO` dont le
  prévisionnel recalculé est `NULL` et retombe sur le prévisionnel, une ligne
  `PQ` en `bl_exclu = 1` jamais restituée.
- L'amas **7004** existe dans `trppu_trafic_agrebal` mais pas dans
  `trppu_agrebal_pdi` : il couvre le `LEFT JOIN` de DSR-705, ses trafics sont
  restitués avec `"nom_amas": null` au lieu de disparaître.
- L'amas **7002** n'a qu'une densité sur trois : les deux autres sortent à `0`.
- Les lignes de `trppu_trafic_pdi` somment exactement aux agrégats de
  `trppu_trafic_agrebal` (amas 7001, lundi, OO : 7 + 5 = 12 dense).

Volumes bruts attendus pour le scénario 125 via
`POST /trppu-api/optipacc/scenario-trafic-brut` :

| Produit | Volume brut |
|---------|-------------|
| `OO` | 1 250 000 |
| `IP` | 5 900 000 |
| `CO` | 791 000 |
| `EP` | 0 |
| `PQ` | *(exclu)* |

## Points à connaître

- **`Calcul_trafic_en_cours`** est la seule colonne capitalisée du schéma. MySQL
  est insensible à la casse sur les noms de colonnes, mais le driver renvoie la
  clé telle qu'elle est écrite dans le `SELECT` : les requêtes de l'application
  l'aliasent en `calcul_trafic_en_cours`.
- **`trppu_agrebal_pdi.agrebal_pdi_ids`** est une colonne `GENERATED ALWAYS …
  STORED` : elle ne doit jamais figurer dans un `INSERT`, MySQL la dérive de
  `agrebal_pdiList`.
- **`trppu_scenario_pic_coeffs.jour_semaine`** utilise un enum abrégé
  (`LUN`, `MAR`, …), contrairement à `trppu_pic_coefficients` et
  `trppu_trafic_agrebal` qui utilisent `LUNDI`, `MARDI`, …
- **`trppu_pic_coefficients_ko`** a disparu du schéma lors du re-dump du
  11/09/2026 : `db_new.sql` est passé de 25 à 24 tables. Elle reste listée dans
  `count.json` (42 lignes) et dans les documents d'audit, mais elle est absente
  des trois scripts. Table legacy jamais lue par le code (cf.
  `../api_docs/dsr/cartographie_donnees_persistees.md`), sa suppression n'a aucun
  impact applicatif.

Voir aussi `RAPPORT-ECARTS-db_new-2026-08-17.md` et `RUNBOOK-purge-tables.md`.
