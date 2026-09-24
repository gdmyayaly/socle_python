# Scripts SQL (`db/`)

Ce répertoire accueille les scripts `.sql` du module : jeux de données de référence,
migrations, traitements écrits en SQL pur.

Il porte aujourd'hui les cinq scripts de la **chaîne d'initialisation des clés de répartition
des PDI** (DSR-696 à DSR-699), orchestrés par la commande `init` — voir « La chaîne
d'initialisation » plus bas.

Les scripts sont exécutés par le runner du socle (`app/db/mysql.py`), documenté dans le
`README.md` à la racine — options, contenu de `ScriptResult`, et surtout les **limites à
connaître**.

## Jouer un script

```python
from app.db.mysql import db_write

res = await db_write.execute_sql_file("db/schema.sql")
```

Ou en enchaînant plusieurs fichiers dans l'ordre, sur une seule transaction :

```python
res = await db_write.execute_sql_files(["db/schema.sql", "db/data.sql"])
```

Avant de jouer un script pour de bon, `dry_run=True` le lit et le découpe **sans ouvrir
aucune connexion** : c'est la façon la moins coûteuse de vérifier que le découpage donne
bien les instructions attendues.

Un script qui fait lui-même `CREATE DATABASE` puis `USE` doit être lancé avec
`database=None`, sinon la connexion est déjà rattachée à un schéma.

## Conventions

- **Écrire des scripts rejouables.** MySQL valide implicitement le DDL (`CREATE`, `DROP`,
  `ALTER`, `TRUNCATE`…) : un `ROLLBACK` ne le défait pas. Un script de schéma qui échoue à
  mi-parcours laisse la base dans un état intermédiaire, la seule protection est de pouvoir
  le rejouer — `DROP TABLE IF EXISTS` puis `CREATE TABLE`, `INSERT … ON DUPLICATE KEY
  UPDATE` plutôt qu'un `INSERT` nu.
- **Un fichier = une étape**, nommé par son ordre d'exécution s'il y a une chaîne.
- **Paramétrer par variables de session** (`SET @ma_variable = …` en tête de fichier)
  plutôt qu'en codant les valeurs en dur dans les requêtes.
- **Pas de données personnelles en commentaire** : l'aperçu journalisé est tronqué à 120
  caractères, mais le fichier, lui, est versionné.
- Réserver l'exécution à `db_write` : `db_read` porte des identifiants en lecture seule.

## Documenter une chaîne

Dès qu'un enchaînement de scripts dépasse deux fichiers, décrire ici : l'ordre d'exécution,
les paramètres attendus de chaque étape, ce qu'on lit en sortie pour vérifier qu'une étape
s'est bien passée, et la marche à suivre pour rejouer.

---

# La chaîne d'initialisation des clés de répartition

```
fichier CSV (S3)
      │  chargement — commande charger-cles-repartition, lots commités
      ▼
trppu_cles_repartition           24 M lignes : trafic de chaque PDI
      │  migration + correctif — le schéma que la suite exige
      │  agregats  (DSR-696)
      ▼
trppu_trafic_site                le DÉNOMINATEUR des clés
      │  versions  (DSR-698), un site à la fois
      ▼
trppu_version_cle                le CONTENEUR auquel les clés se rattachent
      │  cles      (DSR-699)
      ▼
trppu_cles_repartition_calcule   les CLÉS
```

**L'ordre n'est pas négociable, et une erreur d'enchaînement ne se voit pas.** DSR-699 joint
les trois tables : un site auquel il manque un agrégat ou une version est écarté sans erreur
et sans trace. Le référentiel est alors incomplet — et, par le CA4 du même ticket, figé.

## Ordre, paramètres, mode d'exécution

| # | Étape `init` | Fichier | Paramètres injectés | `transactional` |
|---|---|---|---|---|
| 1 | `chargement` | *(aucun — CSV S3, cf. `charger-cles-repartition`)* | — | lots commités |
| 2 | `migration` | `DSR-696-699_migration.sql` | aucun | **`False`** |
| 3 | `correctif` | `fix_error.sql` | `@id_referentiel` | **`False`** |
| 4 | `agregats` | `DSR-696_site_trafic.sql` | `@id_referentiel`, `@co_regate` | `True` |
| 5 | `versions` | `DSR-698_version_cle.sql` | `@id_referentiel`, `@co_regate`, `@commentaire`, `@libelle` | `True` |
| 6 | `cles` | `DSR-699_cles_calculees.sql` | `@id_referentiel`, `@co_regate` | `True` |

Les paramètres ne se modifient **pas** dans les fichiers : `app/db/sql_parametres.py` remplace
le bloc `SET @…` de tête au vol, à partir des arguments de la commande. Les `SET SESSION
sql_mode` et les variables de travail des scripts (`@sql`, `@deja`) ne sont jamais touchés.

### Pourquoi `transactional=False` sur la migration et le correctif

Leurs `ALTER` voyagent dans une chaîne exécutée par `PREPARE`/`EXECUTE` — ce qui les rend
rejouables, MySQL ne connaissant pas `ADD INDEX IF NOT EXISTS`, mais **invisibles** à la
détection de DDL du socle (`is_ddl`). L'avertissement « DDL en mode transactionnel » ne se
déclencherait donc pas, alors que le COMMIT implicite, lui, a bien lieu : on ouvrirait une
transaction qui ne protège rien. Verrouillé par `tests/test_scripts_dsr.py`.

## Ce que ce répertoire n'a pas le droit de contenir

Deux tests le vérifient, parce qu'une note ne suffirait pas :

- **aucun DDL de schéma** (`CREATE TABLE`, `DROP TABLE`, `CREATE DATABASE`, `TRUNCATE TABLE`).
  Le déploiement de structure de la base TRPPU relève du ticket DSR-721 / module MD01. C'est
  aussi ce qui empêche `database.sql` — 22 `DROP TABLE` — d'arriver ici par copie machinale et
  d'effacer 24 M de lignes au premier `init` ;
- **aucun `LOAD DATA`**. Le chargement passe par S3 et des lots commités. `LOAD DATA INFILE`
  est lu par le serveur MySQL, son chemin est un littéral non paramétrable, et le socle
  n'active pas la variante `LOCAL` : `DSR-697_chargement_cles_repartition.sql` n'a rien à faire
  dans ce module.

`suivi.sql` n'est pas repris non plus : il se joue **depuis une seconde session**, en boucle,
pendant qu'une étape travaille. Il reste disponible dans `yb05/db/`.

## Rejouer

| Script | Relancé, il… |
|---|---|
| `DSR-696-699_migration.sql` | ne recrée rien — chaque `ALTER` est précédé d'un test de présence |
| `fix_error.sql` | ne reconstruit rien — le garde-fou teste la largeur courante des colonnes |
| `DSR-696_site_trafic.sql` | **recalcule** — `DELETE` ciblé puis `INSERT` |
| `DSR-698_version_cle.sql` | **ne fait rien** si le site a déjà une version active sur ce référentiel |
| `DSR-699_cles_calculees.sql` | **ne fait rien** si une version du périmètre porte déjà des clés (CA4) |

**Le CA4 de DSR-699 est un cliquet.** Une seule version calculée fige le référentiel : les
clés d'une version existante ne sont jamais recalculées. C'est pourquoi `init` refuse de jouer
l'étape `cles` si une version du périmètre porte déjà des clés, et pourquoi il ne propose
**aucune option pour ne traiter qu'un site** — elle calculerait un site et rendrait tous les
autres définitivement incalculables sur ces versions. La seule sortie après un calcul partiel
est de créer une nouvelle série de versions, qui désactive les précédentes.

## Duplication avec `yb05/db/`

Ces cinq fichiers sont des copies de `yb05/db/`, comme le socle lui-même est dupliqué entre
modules. Une correction sur l'un doit être reportée à la main sur l'autre. Le seul garde-fou
contre une divergence silencieuse est le nombre d'instructions figé dans les deux
`tests/test_scripts_dsr.py` : le faire évoluer des deux côtés est délibéré.
