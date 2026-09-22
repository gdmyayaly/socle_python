# yb07

Socle technique du module **YB07** : une **application console** (pas de serveur HTTP, pas
de port). Le projet est volontairement **vide de métier** — il apporte la connexion MySQL,
la journalisation JSON, les vérifications de base et l'exécution de scripts `.sql`, sur
lesquels le module vient se greffer.

## Sommaire

- [Démarrage rapide](#démarrage-rapide) · [Prérequis](#prérequis) · [Arborescence](#arborescence)
- [Configuration](#configuration) — [MySQL](#mysql) · [Logging](#application--logging)
- [Commandes](#commandes) — [`db-info`](#db-info--état-de-la-connexion-mysql) · [`db-check`](#db-check--disponibilité-des-instances) · [Ajouter une commande](#ajouter-une-commande-métier)
- [Docker](#docker)
- [Classe utilitaire Database](#classe-utilitaire-database) · [Exécution de scripts SQL](#exécution-de-scripts-sql)
- [Logging](#logging) · [Tests](#tests) · [Utilisation comme bibliothèque](#utilisation-comme-bibliothèque)

## Démarrage rapide

```bash
cd yb07
pip install -r requirements.txt
cp .env.example .env            # renseigner les SGBD_*
python -m app.main db-check
```

## Prérequis

- Python >= 3.12
- MySQL (une instance en lecture, une en écriture — ou la même pour les deux)

## Arborescence

```
yb07/
├── app/
│   ├── config.py           toutes les variables d'environnement, source de vérité
│   ├── health.py           diagnostics MySQL (fonctions, pas des routes)
│   ├── json_formatter.py   format de log JSON + setup_logging
│   ├── log_utils.py        ctx() et l'identifiant de corrélation
│   ├── main.py             point d'entrée console, une sous-commande par action
│   └── db/                 Database : pools, transactions, runner de scripts .sql
├── db/                     scripts .sql du module (cf. db/README.md)
├── docs/CONVENTION-LOGS.md la convention de log, verrouillée par les tests
└── tests/
```

Le code métier s'ajoute dans ses propres paquets — par exemple `app/traitements/` pour les
traitements et `app/services/` pour les accès externes. Rien de tout cela n'est imposé par
le socle, mais c'est la structure retenue par les modules voisins.

## Configuration

Toutes les variables sont lues depuis `yb07/.env` par `app/config.py`, qui reste la source
de vérité en cas de doute.

### MySQL

| Variable | Par défaut | Description |
|---|---|---|
| `SGBD_SERVER_WRITE` | `localhost` | Hôte du serveur d'écriture |
| `SGBD_SERVER_READ` | valeur de `SGBD_SERVER_WRITE` | Hôte du serveur de lecture |
| `SGBD_PORT` | `3306` | Port MySQL (commun aux deux) |
| `SGBD_APP_USER` | `root` | Utilisateur par défaut, si les variantes READ/WRITE sont absentes |
| `SGBD_APP_USER_WRITE` / `SGBD_APP_USER_READ` | `SGBD_APP_USER` | Utilisateurs dédiés |
| `SGBD_APP_PWD` | `""` | Mot de passe par défaut |
| `SGBD_APP_PWD_WRITE` / `SGBD_APP_PWD_READ` | `SGBD_APP_PWD` | Mots de passe dédiés |
| `SGBD_DB_NAME` | `yb07` | Nom de la base |
| `SGBD_MAX_RETRIES` | `3` | Nombre de tentatives de connexion |
| `SGBD_RETRY_DELAY` | `1.0` | Délai de base entre tentatives (backoff linéaire : `délai × tentative`) |
| `MYSQL_POOL_SIZE` | `10` | Taille maximale de chaque pool. Toute valeur inexploitable est ramenée au défaut : un batch d'exploitation ne refuse pas de démarrer pour une variable mal saisie. |
| `SQL_SCRIPT_WARN_SIZE` | `10485760` | Taille (octets) au-delà de laquelle un script `.sql` déclenche un avertissement |

### Application / Logging

| Variable | Par défaut | Description |
|---|---|---|
| `APP` | `dsr` | Contexte applicatif, champ `app_ccx` des logs |
| `APP_ENV` | `sdev` | `local`, `sdev`, `sacc`, `sass`, `prod` |
| `MODULE` | `yb07` | Code module, champ `app_tm` des logs |
| `APP_VERSION` | `1.0.0` | Champ `app_version` des logs |
| `LOGS_DIR` | `""` | Dossier de logs ; vide = `./logs` |

> Ne pas mettre de commentaire en fin de ligne dans `.env` : `python-dotenv` ne le retire
> pas d'une valeur non quotée, il finirait **dans** la valeur.

## Commandes

Toutes les commandes s'invoquent de la même façon, depuis le répertoire `yb07/` :

```bash
python -m app.main <commande> [options]
python -m app.main --help              # liste des commandes
python -m app.main <commande> --help   # options d'une commande
```

| Commande | Ce qu'elle fait | Arguments |
|---|---|---|
| `db-info` | Identité applicative, configuration, paramètres de connexion et informations du serveur MySQL | — |
| `db-check` | Disponibilité réelle des instances MySQL lecture et écriture | — |

Options communes à toutes les commandes :

| Option | Effet |
|---|---|
| `--json` | Sort le résultat en JSON au lieu du texte (pour chaînage / supervision). |
| `-v`, `--verbose` | Détaille les logs applicatifs en DEBUG. Le niveau par défaut est INFO — les logs partent sur la **sortie d'erreur**, ils ne polluent donc pas le rapport. |

**Code de retour** : `0` si la commande aboutit, `1` sinon — directement exploitable par un
ordonnanceur ou une probe. Les deux flux sont séparés : le rapport part sur la sortie
standard, les logs JSON sur la sortie d'erreur (`2>/dev/null` rend un rapport nu).

### `db-info` — état de la connexion MySQL

Affiche l'identité applicative, l'état de la configuration, les paramètres de connexion
**lecture** et **écriture** (mot de passe jamais affiché), puis interroge le serveur :
version MySQL, schéma courant, utilisateur, hôte, date serveur et nombre de tables.

```bash
$ python -m app.main db-info
Application  : dsr/yb07 v1.0.0 (env sdev)
Configuration: ok
Connexion MySQL (mot de passe masqué)
  lecture  : root@localhost:3306/yb07
             retries=3, delai=1.0s
  écriture : root@localhost:3306/yb07
             retries=3, delai=1.0s
Serveur MySQL (via l'instance de lecture)
  version        : 8.0.36
  schéma courant : yb07
  utilisateur    : root@localhost
  hôte serveur   : mysql-01
  date serveur   : 2026-09-21 10:14:22
  nb tables      : 12

$ python -m app.main db-info --json
{
  "application": { "app": "dsr", "env": "sdev", "module": "yb07", "version": "1.0.0" },
  "config": { "status": "ok", "mysql_config": true },
  "connexion": { "lecture": { "host": "localhost", "port": 3306, ... }, "ecriture": { ... } },
  "serveur": { "status": "ok", "version": "8.0.36", "schema_courant": "yb07", "nb_tables": 12 }
}
```

### `db-check` — disponibilité des instances

Teste la connectivité réelle des deux instances. Chaque ressource est reportée à
`connected`, `error` ou `disconnected`, l'échec étant journalisé en WARNING.

```bash
$ python -m app.main db-check
Disponibilité MySQL : ok
  lecture  : connected
  écriture : connected
```

Les fonctions sous-jacentes vivent dans `app/health.py` (`check_config`,
`describe_connection`, `fetch_server_info`, `check_resources`) : elles retournent de simples
dictionnaires et sont réutilisables depuis un module métier, sans passer par la CLI.

### Ajouter une commande métier

Une sous-commande se résume à une coroutine et à son enregistrement :

```python
async def cmd_mon_traitement(args: argparse.Namespace) -> int:
    ...
    return EXIT_OK

# dans build_parser()
sous_commandes.add_parser(
    "mon-traitement",
    parents=[commun],
    help="…",
).set_defaults(handler=cmd_mon_traitement)
```

Le parent `commun` apporte `-v` et `--json` ; `_run` ferme les pools dans un `finally`. Le
code métier va dans un package dédié, jamais dans `main.py`.

Pour un traitement qui rend un compte rendu d'exploitation plutôt qu'un simple code retour,
le motif éprouvé est un objet `Rapport` (contrôles `[OK]` / `[KO]`, verdict, motifs) que le
traitement **retourne sans jamais lever** : la CLI décide alors de l'afficher en texte ou en
JSON et en déduit le code de retour. Les modules `yb05` et `yb06` en portent une
implémentation directement reprenable.

## Docker

```bash
docker compose build
docker compose run --rm yb07 db-info
docker compose run --rm yb07 db-check
```

Le conteneur exécute une commande puis s'arrête : aucun port n'est exposé. La sous-commande
est passée à l'exécution (`ENTRYPOINT` = `python -m app.main`, `CMD` = `db-check`). La
configuration vient du `.env` via `env_file`, et `./logs` est monté sur `/app/logs`.

## Classe utilitaire Database

Deux instances globales sont exposées par `app/db/mysql.py` : `db_read` et `db_write`,
configurées avec des hôtes et des identifiants distincts.

### Connexion avec pool et retry

Le pool est créé **paresseusement**, au premier appel, avec `SGBD_MAX_RETRIES` tentatives
et un backoff linéaire. Le programme démarre donc même si MySQL est injoignable.

Il revient à l'appelant de fermer les pools en fin de traitement — c'est ce que fait la CLI
dans un `finally` (`app/main.py`) :

```python
await db_read.disconnect()
await db_write.disconnect()
```

### Requêtes simples

```python
from app.db.mysql import db_read, db_write

ligne = await db_read.fetch_one("SELECT * FROM t WHERE id = %s", (42,))
lignes = await db_read.fetch_all("SELECT * FROM t")
nb = await db_write.execute("UPDATE t SET a = %s WHERE id = %s", (1, 42))
```

### Transactions

```python
async with db_write.transaction() as tx:
    await tx.execute("INSERT INTO t (a) VALUES (%s)", (1,))
    await tx.execute_many("INSERT INTO u (b) VALUES (%s)", [(1,), (2,)])
# commit automatique à la sortie, rollback en cas d'exception
```

`execute_many` ne découpe pas de lui-même : sur un gros volume, c'est à l'appelant de
boucler par lots.

## Exécution de scripts SQL

Trois méthodes de `Database` exécutent un script instruction par instruction. Le découpage
s'appuie sur `sqlparse` (`app/db/sql_script.py`) et gère guillemets, backticks, commentaires
`--` / `#` / `/* */`, ainsi que la directive `DELIMITER` des procédures et triggers.

```python
from app.db.mysql import db_write

# Un fichier
res = await db_write.execute_sql_file("db/schema.sql")

# Plusieurs fichiers, dans l'ordre, sur une seule transaction
res = await db_write.execute_sql_files(["db/schema.sql", "db/data.sql"])

# Un script fourni en chaîne
res = await db_write.execute_sql_script("INSERT INTO t (a) VALUES (1);", label="seed")
```

### Options

| Option | Défaut | Effet |
|---|---|---|
| `transactional` | `True` | `BEGIN` / `COMMIT` unique couvrant **tous** les fichiers, `ROLLBACK` en cas d'échec. `False` = autocommit instruction par instruction. |
| `continue_on_error` | `False` | `False` : la première erreur interrompt le script et lève `SqlScriptError`. `True` : l'erreur est journalisée, enregistrée, et l'exécution continue. |
| `dry_run` | `False` | Lit et découpe le script **sans ouvrir aucune connexion** ; le résultat liste les instructions avec `skipped=True`. |
| `encoding` | `utf-8-sig` | Décode aussi l'UTF-8 nu et absorbe le BOM des dumps. |
| `database` | schéma de l'instance | `None` pour se connecter **sans schéma** — nécessaire si le script fait lui-même `CREATE DATABASE` puis `USE`. |
| `disable_foreign_keys` | `False` | Exécute `SET FOREIGN_KEY_CHECKS = 0` avant la première instruction. |

### Résultat

`ScriptResult` détaille chaque instruction (`source`, `index` 1-based par fichier,
`preview`, `is_ddl`, `rowcount`, `duration_ms`, `error`, `skipped`) et agrège
`total_count`, `executed_count`, `error_count`, `ddl_count`, `errors`, `ok`, `committed`.

En cas d'échec sans `continue_on_error`, `SqlScriptError` porte le fichier fautif
(`source`), l'index de l'instruction, son SQL complet (`statement`), l'exception d'origine
(`original`) et le `ScriptResult` **partiel** au moment de l'échec (`result`).

```python
from app.db.sql_script import SqlScriptError

try:
    res = await db_write.execute_sql_files(["db/schema.sql", "db/data.sql"])
except SqlScriptError as e:
    log.error("%s instruction %d : %s", e.source, e.index, e.original)
```

### Limites à connaître

- **COMMIT implicite sur le DDL.** MySQL valide implicitement `CREATE`, `DROP`, `ALTER`,
  `TRUNCATE`, `RENAME`… : `transactional=True` ne garantit l'atomicité que pour le DML.
  Un script de schéma qui échoue à mi-parcours laisse la base dans un état intermédiaire.
  Écrire des scripts **rejouables** (`DROP TABLE IF EXISTS` / `CREATE TABLE`). Un
  avertissement est journalisé dès qu'une instruction DDL est détectée en mode
  transactionnel, et `StatementResult.is_ddl` indique lesquelles ont survécu au rollback.
- **Connexion dédiée, hors pool.** Un script modifie l'état de session (`USE`,
  `SET FOREIGN_KEY_CHECKS`, variables de session) : il tourne sur une connexion créée pour
  lui et fermée à la fin, afin de ne pas contaminer les requêtes applicatives.
- **Pas de retry par instruction.** Seul l'établissement de la connexion est retenté ;
  rejouer un `INSERT` ou un `ALTER` partiellement appliqué serait destructeur.
- **`DELIMITER`** : dans un bloc à délimiteur personnalisé, le découpage est textuel. Un
  délimiteur apparaissant dans une chaîne littérale du corps couperait à tort — cas que
  `mysqldump` ne produit pas.
- **`LOAD DATA LOCAL INFILE` n'est pas disponible** : `local_infile` n'est activé ni à la
  connexion ni au pool. Un chargement de masse depuis Python se fait par `INSERT` batchés.
- Réserver ces méthodes à `db_write` : `db_read` porte des identifiants en lecture seule.

## Logging

> La convention (grammaire des messages, niveaux, champ de corrélation) est décrite
> dans **`docs/CONVENTION-LOGS.md`**. À lire avant d'ajouter un log.

### Fonctionnement

`setup_logging()` doit être appelé **explicitement** au démarrage du programme (la CLI le
fait dans `main()`). Un handler console est toujours installé ; le handler fichier n'est
activé que si `APP_ENV=local` (ou si un dossier est passé explicitement à `setup_logging`).

Le fichier est nommé `AAAA-MM-JJ.log` dans `LOGS_DIR` (ou `./logs`). Le nom est calculé
**une seule fois au démarrage** : ce n'est pas une rotation quotidienne, un processus qui
tourne plusieurs jours continue d'écrire dans le fichier du jour de son démarrage.

**Niveau par défaut : INFO.** Le batch tourne sous ordonnanceur, sans `-v`, et ses logs
sont la seule trace de ce qu'il a fait. Cela ne gêne pas l'exploitant : le rapport part sur
la sortie standard, les logs JSON sur la sortie d'erreur. `-v` ajoute le niveau DEBUG.

### Format des logs (JSON)

```json
{"app_datetime": "2026-07-31T07:49:35.874Z", "app_ccx": "dsr", "app_env": "sdev", "app_ptf": "build", "app_tm": "yb07", "app_version": "1.0.0", "severity_label": "INFO", "app_message": "Fin commande (commande=db-check, exit_code=0, duration_ms=84.2)", "id_traitement": null, "name": "yb07", "filename": "main.py", "lineno": 158}
```

`id_traitement` est posé une fois par unité de travail (`set_id_traitement`) et repris sur
**toutes** les lignes qui en découlent, y compris celles de `app.db.mysql`. Quand plusieurs
unités sont traitées en parallèle et que les lignes s'entrelacent, c'est ce champ qui permet
de reconstituer une trace. Il vaut `null` hors traitement.

Le jeu de clés est fixe : `logger.info(..., extra={...})` est **sans effet**. Le contexte
métier vit donc dans `app_message`, sous la grammaire
`Début|Avancement|Fin|Rejet|Erreur <action> (cle=valeur, …)`.

### Ce qui est loggé par le socle

- Début et fin de chaque commande, avec `exit_code` et `duration_ms`.
- Les exceptions, avec la stack trace.
- Les tentatives de connexion MySQL et les vérifications en échec.
- L'exécution des scripts SQL (début, fin, avertissements DDL, échecs) — l'aperçu des
  instructions est tronqué à 120 caractères et **jamais** le SQL complet, les scripts de
  données pouvant contenir des informations personnelles.

Un traitement long journalise sa progression avec le verbe `Avancement` : voir la section
« Traitements longs » de `docs/CONVENTION-LOGS.md`.

## Tests

```bash
python -m pytest tests/           # tous les tests
python -m pytest tests/ -k split  # un sous-ensemble
python -m pytest tests/ -q        # sortie compacte
```

Les tests ne nécessitent ni base MySQL ni réseau : `aiomysql.connect` est remplacé par des
doublures et le code async est lancé via `asyncio.run` (pas de dépendance à pytest-asyncio).

- `tests/test_sql_script.py` — découpage et exécution des scripts SQL.
- `tests/test_log_convention.py` — rendu de `ctx()`, rendu paresseux, et le champ
  `id_traitement` (toujours présent, `null` hors contexte, isolé entre tâches asyncio).
  Ces tests **verrouillent la convention de log** : un nouveau code qui s'en écarte les
  fait échouer.

`tests/conftest.py` porte `FausseBase`, un substitut de `Database` qui rend des réponses
indexées par fragment de requête et journalise les écritures dans l'ordre — c'est ce qui
permet de tester un verrou ou une purge. Deux partis pris utiles à connaître : une requête
sans réponse déclarée **lève**, pour qu'un test n'interroge jamais une table à laquelle il
n'a pas pensé, et `FausseBase(lecture_seule=True)` lève sur toute écriture.

## Utilisation comme bibliothèque

Le socle s'importe directement depuis un module métier, sans passer par la CLI :

```python
import asyncio

from app.db.mysql import db_read, db_write
from app.health import check_resources
from app.json_formatter import setup_logging


async def traitement():
    if (await check_resources())["status"] != "ok":
        raise RuntimeError("Base indisponible")
    await db_write.execute_sql_file("db/schema.sql")
    return await db_read.fetch_all("SELECT * FROM ma_table")


async def main():
    try:
        lignes = await traitement()
        print(len(lignes))
    finally:
        await db_read.disconnect()
        await db_write.disconnect()


if __name__ == "__main__":
    setup_logging()
    asyncio.run(main())
```

---

Socle repris de `yb05/`, dont il partage la structure et les choix techniques. Le module
`yb06/` en montre une mise en œuvre complète : accès S3, traitement métier et rapport
d'exploitation.
