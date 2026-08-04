# yb05

Socle technique du module **YB05** : une **application console** 

## Demarrage rapide

```bash
cd yb05
pip install -r requirements.txt
cp .env.example .env            # renseigner les SGBD_*
python -m app.main db-check
```

## Prerequis

- Python >= 3.12
- MySQL (une instance en lecture, une en écriture — ou la même pour les deux)

## Configuration

Toutes les variables sont lues depuis `yb05/.env` par `app/config.py`, qui reste la source
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
| `SGBD_DB_NAME` | `yb05` | Nom de la base |
| `SGBD_MAX_RETRIES` | `3` | Nombre de tentatives de connexion |
| `SGBD_RETRY_DELAY` | `1.0` | Délai de base entre tentatives (backoff linéaire : `délai × tentative`) |
| `SQL_SCRIPT_WARN_SIZE` | `10485760` | Taille (octets) au-delà de laquelle un script `.sql` déclenche un avertissement |

### Application / Logging

| Variable | Par défaut | Description |
|---|---|---|
| `APP` | `dsr` | Contexte applicatif, champ `app_ccx` des logs |
| `APP_ENV` | `sdev` | `local`, `sdev`, `sacc`, `sass`, `prod` |
| `MODULE` | `yb05` | Code module, champ `app_tm` des logs |
| `APP_VERSION` | `1.0.0` | Champ `app_version` des logs |
| `LOGS_DIR` | `""` | Dossier de logs ; vide = `./logs` |
| `DEBUG_SHOW_QUERY` | `false` | Trace les requêtes |

## Commandes console

```bash
python -m app.main db-info      # infos de connexion configurées + infos du serveur MySQL
python -m app.main db-check     # disponibilité des instances lecture et écriture
```

| Commande | Description |
|---|---|
| `db-info` | Affiche l'identité applicative, l'état de la configuration, les paramètres de connexion **lecture** et **écriture** (mot de passe jamais affiché), puis interroge le serveur : version MySQL, schéma courant, utilisateur, hôte, date serveur et nombre de tables du schéma. |
| `db-check` | Teste la connectivité réelle des deux instances. Chaque ressource est reportée à `connected`, `error` ou `disconnected`, l'échec étant journalisé en WARNING. |

Options communes :

| Option | Effet |
|---|---|
| `--json` | Sort le résultat en JSON au lieu du texte (pour chaînage / supervision). |
| `-v`, `--verbose` | Passe les logs applicatifs en INFO ; par défaut seuls les WARNING sont affichés pour ne pas polluer la sortie. |

**Code de retour** : `0` si la vérification est concluante, `1` sinon — directement
exploitable par un ordonnanceur ou une probe.

```bash
$ python -m app.main db-check
Disponibilité MySQL : ok
  lecture  : connected
  écriture : connected

$ python -m app.main db-info --json
{
  "application": { "app": "dsr", "env": "sdev", "module": "yb05", "version": "1.0.0" },
  "config": { "status": "ok", "mysql_config": true },
  "connexion": { "lecture": { "host": "localhost", "port": 3306, ... }, "ecriture": { ... } },
  "serveur": { "status": "ok", "version": "8.0.36", "schema_courant": "yb05", "nb_tables": 12 }
}
```

Les fonctions sous-jacentes vivent dans `app/health.py` (`check_config`,
`describe_connection`, `fetch_server_info`, `check_resources`) : elles retournent de simples
dictionnaires et sont réutilisables depuis un module métier, sans passer par la CLI.

### Docker

```bash
docker compose build
docker compose run --rm yb05 db-info
docker compose run --rm yb05 db-check
```

Le conteneur exécute une commande puis s'arrête : aucun port n'est exposé.

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
- Réserver ces méthodes à `db_write` : `db_read` porte des identifiants en lecture seule.

## Logging

### Fonctionnement

`setup_logging()` doit être appelé **explicitement** au démarrage du programme (la CLI le
fait dans `main()`). Un handler console est toujours installé ; le handler fichier n'est
activé que si `APP_ENV=local` (ou si un dossier est passé explicitement à `setup_logging`).

Le fichier est nommé `AAAA-MM-JJ.log` dans `LOGS_DIR` (ou `./logs`). Le nom est calculé
**une seule fois au démarrage** : ce n'est pas une rotation quotidienne, un processus qui
tourne plusieurs jours continue d'écrire dans le fichier du jour de son démarrage.

### Format des logs (JSON)

```json
{"app_datetime": "2026-07-31T07:49:35.874Z", "app_ccx": "dsr", "app_env": "sdev", "app_ptf": "build", "app_tm": "yb05", "app_version": "1.0.0", "severity_label": "INFO", "app_message": "Commande db-check", "name": "yb05", "filename": "main.py", "lineno": 143}
```

### Ce qui est loggé

- La commande exécutée (niveau INFO, visible avec `-v`)
- Les exceptions non gérées, avec la stack trace
- Les tentatives de connexion MySQL et les vérifications en échec
- L'exécution des scripts SQL (début, fin, avertissements DDL, échecs) — l'aperçu des
  instructions est tronqué à 120 caractères et **jamais** le SQL complet, les scripts de
  données pouvant contenir des informations personnelles

## Tests

```bash
python -m pytest tests/           # tous les tests
python -m pytest tests/ -k split  # un sous-ensemble
```

Les tests ne nécessitent ni base MySQL ni réseau : `aiomysql.connect` est remplacé par des
doublures et le code async est lancé via `asyncio.run` (pas de dépendance à pytest-asyncio).

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
