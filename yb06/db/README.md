# Scripts SQL (`db/`)

Ce répertoire accueille les scripts `.sql` du module : création de schéma, jeux de données
de référence, migrations, traitements écrits en SQL pur. Il est vide au démarrage du projet.

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
