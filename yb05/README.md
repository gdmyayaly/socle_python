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

### Calcul des trafics

| Variable | Par défaut | Description |
|---|---|---|
| `CLES_PAR_PRODUIT` | `CO:colis,OO:oo,IP:potentielip,OS:3s,PR:3s,PPI:3s` | Correspondance code produit → famille de clé de répartition (`colis`, `oo`, `3s`, `potentielip`). Une entrée malformée fait échouer le démarrage ; un produit absent fait échouer le calcul qui le rencontre. |
| `NB_WORKER` | `1` | Nombre de scénarios traités simultanément par le mode `all`. `1` = séquentiel. Toute valeur inexploitable est ramenée à `1` : un batch d'exploitation ne refuse pas de démarrer pour une variable mal saisie. Les pools MySQL sont dimensionnés sur cette valeur (`max(10, NB_WORKER)`). |

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

### Calcul des trafics d'un scénario

Trois traitements, à jouer dans cet ordre pour un scénario donné (DSR-701, DSR-702, DSR-703) :

```bash
python -m app.main eligibilite 12345            # contrôle, sans aucune écriture
python -m app.main calcul-trafic-pdi 12345      # TMH × coefficient × clé, par PDI
python -m app.main calcul-trafic-agrebal 12345  # agrégation des trafics PDI
```

| Commande | Description |
|---|---|
| `eligibilite` | Les douze règles d'éligibilité du scénario. **Aucune écriture** : ni insertion, ni mise à jour, ni changement de statut. Toutes les règles sont évaluées, pour rendre d'un coup la liste complète des motifs bloquants. |
| `calcul-trafic-pdi` | Vérifie l'éligibilité, verrouille le scénario, y mémorise le référentiel et la version de clés utilisés (**DSR-700**), purge les trafics existants puis calcule et écrit `trppu_trafic_pdi`. Laisse volontairement `CALCUL_TRAFIC_EN_COURS = 1`. |
| `calcul-trafic-agrebal` | Agrège les trafics PDI par Agrébal dans `trppu_trafic_agrebal`, puis clôt le calcul complet : `TRAFIC_AGREBAL_CALCULE = 1` et `CALCUL_TRAFIC_EN_COURS = 0`. Aucune clé ni coefficient n'est relu à cette étape. |
| `all` | **Mode nominal d'exploitation.** Enchaîne les trois traitements ci-dessus, pour un scénario ou pour tous les scénarios éligibles, sur `NB_WORKER` workers. Ne porte aucune règle métier : il orchestre. |

Les orthographes des tickets sont acceptées telles quelles, ainsi que la forme longue :

```bash
python -m app.main ELIGIBILITE 12345
python -m app.main --traitement=CALCUL_TRAFIC_PDI --scenario=12345
```

**Code de retour** : `0` si le scénario est éligible ou le calcul réussi, `1` sinon. `--json` et
`-v` fonctionnent sur les trois commandes, avant ou après la sous-commande.

**Deux flux distincts** : le rapport part sur la sortie standard — c'est lui qui est destiné à
l'exploitant et au `RESULTAT` que lit l'ordonnanceur. Les logs JSON, y compris la stack trace
d'un incident, partent sur la sortie d'erreur. `2>/dev/null` donne donc un rapport nu, et une
supervision peut consommer les deux séparément.

```bash
$ python -m app.main eligibilite 12345
--------------------------------------------------
Contrôle d'éligibilité YB05
Scénario : 12345
--------------------------------------------------

[OK] Scénario trouvé
[OK] Statut VALIDE
[OK] Scénario figé
[OK] Aucun calcul en cours
[OK] Trafic PDI non calculé
[OK] Trafic Agrébal non calculé
[OK] Version PIC trouvée
[OK] Coefficients de rétention disponibles (30)
[OK] Référentiel actif disponible (2)
[OK] Version de clés active disponible (4)
[OK] Agrébals trouvés (12)
[OK] PDI trouvés (348)

RESULTAT : ELIGIBLE AU CALCUL COMPLET DES TRAFICS (PDI + Agrébals)
```

Les traitements vivent dans `app/traitements/` et s'importent directement, sans la CLI :
`controle_eligibilite`, `calcul_trafic_pdi`, `calcul_trafic_agrebal`. Chacun retourne un
`Rapport` — il n'écrit rien sur la sortie standard et ne lève pas. Les instances de base sont
injectables (`db_lecture=`, `db_ecriture=`), ce qui rend les traitements testables sans MySQL et
documente au passage lequel écrit : `controle_eligibilite` ne reçoit tout simplement pas
d'instance d'écriture.

### Mode `ALL` — exploitation courante

```bash
NB_WORKER=4 python -m app.main all     # tous les scénarios éligibles, 4 en parallèle
python -m app.main all 12345           # un seul scénario
```

Le batch cherche lui-même les scénarios `VALIDE`, figés, sans calcul en cours et dont aucun
trafic n'est calculé, les place dans une file, et fait tourner `NB_WORKER` workers dessus.
La réservation d'un scénario n'est pas un mécanisme à part : c'est le verrou que pose déjà
`calcul-trafic-pdi` (`UPDATE … WHERE calcul_trafic_en_cours = 0`), atomique, donc un seul worker
peut l'obtenir. Une erreur sur un scénario n'arrête jamais les autres.

Sortie : une ligne par scénario, puis un bilan.

```
[OK] 12345
[OK] 12346
[KO] 12347
     Version de clés introuvable
[--] 12348
     Le scénario n'est pas figé

--------------------------------------------------
BILAN
--------------------------------------------------

NB_WORKER            : 4
Scénarios trouvés    : 4
Scénarios éligibles  : 3
Succès               : 2
Échecs               : 1
Non éligibles        : 1
Durée totale         : 00:03:12
Durée moyenne        : 00:00:48

RESULTAT : ECHEC
```

Trois marques, et non deux : `[--]` signale un scénario **non éligible**, qui n'est pas une
panne — il n'était pas prêt. Le code de retour vaut `1` dès qu'un scénario est en `[KO]`, jamais
pour un `[--]`.

Un scénario dont le calcul PDI a abouti mais pas l'Agrébal sort des critères de recherche : il
ne sera jamais repris automatiquement. Le bilan le liste sous « À REPRENDRE À LA MAIN », avec la
commande à jouer.

**Correspondance produit / clé de répartition.** `trppu_cles_repartition_calcule` porte quatre
clés (colis, oo, 3s, potentielip) alors que les produits sont des codes alimentés dynamiquement
depuis Databricks. Rien en base ne dit à quelle famille appartient un code : la correspondance
est portée par `CLES_PAR_PRODUIT` (cf. `.env.example`). Un produit absent de cette liste **fait
échouer le calcul**, avec le message qui le nomme — plutôt que de produire un trafic faux.

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

## Scripts métier (`db/`)

Traitements de la chaîne « clés de répartition des PDI », écrits en SQL pur : ils n'ont pas
d'appelant dans le code du socle et se jouent soit au client `mysql`, soit via
`db_write.execute_sql_file()`. Les paramètres se règlent **en tête de fichier**, dans des
variables de session — le fichier entier tournant sur une connexion unique, elles restent
visibles par toutes ses instructions.

| Ordre | Fichier | Rôle |
|---|---|---|
| 1 | `db/DSR-696-699_migration.sql` | Clés uniques, index d'agrégation et colonne `date_creation` absents du schéma livré. À jouer **une fois**, avant les trois autres. |
| 2 | `db/DSR-696_site_trafic.sql` | Alimente `trppu_trafic_site` : somme des trafics des PDI actifs par site, pour un référentiel. |
| 3 | `db/DSR-698_version_cle.sql` | Crée la version de clés d'un site dans `trppu_version_cle` et désactive la précédente. |
| 4 | `db/DSR-699_cles_calculees.sql` | Calcule les clés — trafic du PDI / total de son site — et alimente `trppu_cles_repartition_calcule`. |

**Le mode d'emploi complet est dans [`db/README.md`](db/README.md)** : ordre d'exécution et
dépendances, paramètres de chaque script, exemple d'initialisation d'un site, lecture des
contrôles, rejouabilité, erreurs typiques et contrôles à jouer avant le premier chargement
réel.

Deux points à connaître avant de les jouer :

- **La migration échappe à la détection de DDL.** Ses `ALTER` sont transportés dans une
  chaîne exécutée par `PREPARE`/`EXECUTE` — ce qui la rend rejouable, MySQL ne connaissant
  pas `ADD INDEX IF NOT EXISTS`, mais invisible à `is_ddl`. L'avertissement « DDL en mode
  transactionnel » ne se déclenchera donc pas alors que le commit implicite a bien lieu :
  passer explicitement `transactional=False` pour ce fichier.
- **DSR-699 échoue volontairement** sur un site dont un total de trafic est à zéro, plutôt
  que de charger une clé fausse. Il durcit pour cela son propre `sql_mode` de session, et
  désigne les sites concernés avant d'écrire quoi que ce soit.

Les tickets sources sont dans `docs/`, et **aucun des trois ne décrit exactement la base** :
`docs/DIAGNOSTIC-DSR-696-699.md` reprend écart par écart la formulation fautive, la lecture
retenue et la correction appliquée. Les trois pièges principaux :

- `docs/DSR-696.md` nomme la colonne du site `id_site`, puis `id_site_trafic` après
  amendement. Ni l'une ni l'autre : la colonne est `co_regate_site`, et `id_site_trafic` est
  la PK auto-incrémentée. Le `SELECT` du ticket, lui, la nomme correctement.
- `docs/DSR-698.md` attend une colonne `date_creation` que le schéma ré-extrait a supprimée.
  Ici c'est le ticket qui a raison : la migration la rétablit, en `DEFAULT CURRENT_TIMESTAMP`.
- `docs/DSR-699.md` est le mieux écrit des trois, mais sa clé potentiel IP divise deux
  entiers : sans `CAST`, MySQL n'en rendrait que quatre décimales.

`tests/test_scripts_dsr.py` verrouille ces points, ainsi que le renommage
`trppu_site_trafic` → `trppu_trafic_site`.

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

- `tests/test_sql_script.py` — découpage et exécution des scripts (socle).
- `tests/test_scripts_dsr.py` — scripts métier de `db/` : découpage, ordre des instructions,
  et surtout confrontation des colonnes insérées à un extrait du schéma réel, recopié dans le
  test pour ne pas dépendre de l'arborescence du projet voisin.
- `tests/test_traitements_eligibilite.py` — les douze règles de DSR-701, une par une, et la
  preuve qu'aucune écriture n'a lieu.
- `tests/test_traitements_trafic_pdi.py` — la formule sur un jeu calculable de tête, l'ordre des
  écritures (verrou, traçabilité, purge, insertion) et chaque cause d'échec.
- `tests/test_traitements_trafic_agrebal.py` — contrôles préalables, les trois agrégations, la
  libération du verrou.
- `tests/test_traitements_orchestrateur.py` — mode `ALL` : sélection des scénarios, parallélisme
  (vérifié par un compteur de concurrence observée, jamais par des durées), isolation des
  échecs, filet de sécurité sur le verrou, bilan.
- `tests/test_traitements_rapport.py` — le format de sortie des tickets et le branchement CLI.

`tests/conftest.py` porte `FausseBase`, un substitut de `Database` qui rend des réponses
indexées par fragment de requête et journalise les écritures dans l'ordre — c'est ce qui permet
de tester un verrou ou une purge. Deux partis pris utiles à connaître : une requête sans réponse
déclarée **lève**, pour qu'un test n'interroge jamais une table à laquelle il n'a pas pensé, et
`FausseBase(lecture_seule=True)` lève sur toute écriture.

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
