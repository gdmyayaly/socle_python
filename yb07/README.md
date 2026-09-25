# yb07

Module **YB07** : une **application console** (pas de serveur HTTP, pas de port) qui
**initialise les clés de répartition des PDI** d'un référentiel TRPPU — du fichier CSV livré
par le métier sur S3 jusqu'aux clés calculées que consomment les traitements de trafic (YB05).

```
CSV (S3) ──chargement──▶ trppu_cles_repartition          trafic de chaque PDI (≈ 22-24 M lignes)
                                │ migration + correctif   le schéma que la suite exige
                                │ agregats   (DSR-696)
                                ▼
                         trppu_trafic_site                le DÉNOMINATEUR : trafic total par site
                                │ versions   (DSR-698)    un site à la fois
                                ▼
                         trppu_version_cle                le CONTENEUR des clés, une version active par site
                                │ cles       (DSR-699)
                                ▼
                         trppu_cles_repartition_calcule   les CLÉS : part de chaque PDI dans son site
```

| Commande | Rôle |
|---|---|
| `db-info` / `db-check` | Diagnostic de la base MySQL |
| `s3-check` | Diagnostic du stockage S3 et exploration du bucket |
| `charger-cles-repartition` | Première étape seule : charge `trppu_cles_repartition` depuis le CSV (purge puis lots commités) |
| `charger-cles-repartition-local` | Idem, depuis un CSV du disque local au lieu de S3 |
| `init` | Chaîne complète en six étapes, ou reprise à partir d'une étape |

Le module repose sur le socle technique commun aux batchs TRPPU (connexion MySQL,
journalisation JSON, vérifications de ressources, exécution de scripts `.sql`), auquel il
ajoute l'accès S3, l'injection de paramètres dans les scripts et les traitements métier.

**Point d'attention majeur : l'étape `cles` est irréversible.** Le CA4 de DSR-699 interdit de
recalculer les clés d'une version existante ; un calcul partiel fige donc les sites restants.
Toute la conception de `init` (prérequis avant écriture, arrêt net à la première étape en
échec, refus de rejouer `cles`) découle de cette contrainte — voir
[`init`](#init--initialiser-les-clés-de-répartition) et [`db/README.md`](db/README.md).

> **Historique.** Ce projet s'est appelé `yb06/`, puis `yb04/` (le périmètre YB06 défini par
> DSR-715 / DSR-717 étant autre chose, cf. `DSR-715-717_analyse_yb06.txt` à la racine du
> dépôt). Il a été renommé `yb07/` le 24/09/2026, en remplacement de l'ancien socle vierge
> `yb07/`, qui ne portait aucun code métier et a été supprimé.

## Sommaire

- [Démarrage rapide](#démarrage-rapide) · [Aide-mémoire des commandes](#aide-mémoire-des-commandes) · [Prérequis](#prérequis) · [Arborescence](#arborescence)
- [Configuration](#configuration) — [MySQL](#mysql) · [Logging](#application--logging) · [S3](#s3) · [CSV et chargements](#fichiers-csv-et-chargements)
- [Commandes](#commandes) — [`db-info`](#db-info--état-de-la-connexion-mysql) · [`db-check`](#db-check--disponibilité-des-instances) · [`s3-check`](#s3-check--explorer-le-bucket-s3) · [`charger-cles-repartition`](#charger-cles-repartition--charger-le-référentiel-des-pdi) · [`charger-cles-repartition-local`](#charger-cles-repartition-local--charger-depuis-un-fichier-local) · [`init`](#init--initialiser-les-clés-de-répartition) · [Ajouter une commande](#ajouter-une-commande-métier)
- [Procédure d'initialisation d'un référentiel](#procédure-dinitialisation-dun-référentiel)
- [Docker](#docker)
- [Classe utilitaire Database](#classe-utilitaire-database) · [Exécution de scripts SQL](#exécution-de-scripts-sql)
- [Logging](#logging) · [Tests](#tests) · [Utilisation comme bibliothèque](#utilisation-comme-bibliothèque)
- Scripts SQL de la chaîne : [`db/README.md`](db/README.md) · Convention de log : [`docs/CONVENTION-LOGS.md`](docs/CONVENTION-LOGS.md)

## Démarrage rapide

```bash
cd yb07
pip install -r requirements.txt
cp .env.example .env              # renseigner les SGBD_* puis les S3_*
python -m app.main db-check       # la base répond ?
python -m app.main s3-check       # le bucket est accessible ? que contient-il ?
python -m app.main init 1 --dry-run   # découpe les scripts de la chaîne, n'écrit rien
```

Sous Windows (PowerShell), remplacer `cp` par `Copy-Item .env.example .env`. Un
environnement virtuel est recommandé :

```bash
python -m venv .venv
source .venv/bin/activate         # Windows : .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Aide-mémoire des commandes

Toutes les commandes se lancent depuis `yb07/`. Code de retour : `0` si la commande aboutit,
`1` sinon.

```bash
# Aide
python -m app.main --help
python -m app.main init --help                         # liste aussi les noms d'étapes

# Diagnostic MySQL
python -m app.main db-info
python -m app.main db-info --json
python -m app.main db-check

# Diagnostic S3
python -m app.main s3-check                            # niveau de S3_PREFIXE
python -m app.main s3-check --prefixe ""               # racine du bucket
python -m app.main s3-check --prefixe referentiels/    # un dossier précis
python -m app.main s3-check --recursif --limite 1000   # toute l'arborescence

# Chargement seul (étape 1 de la chaîne)
python -m app.main charger-cles-repartition 1
python -m app.main charger-cles-repartition 1 --fichier autre.csv --json
python -m app.main charger-cles-repartition-local 1 data/cles.csv

# Chaîne d'initialisation
# étapes : chargement, migration, correctif, agregats, versions, cles
python -m app.main init 1 --dry-run                    # marche à blanc, n'écrit rien
python -m app.main init 1 --etape migration            # une seule étape
python -m app.main init 1 --depuis versions            # reprend ici et enchaîne
python -m app.main init 1 --fichier autre.csv          # autre CSV que CSV_CLES_REPARTITION
python -m app.main init 1 --depuis versions --commentaire "Livraison 2026-09" --libelle "Réf. 1"
python -m app.main init 1 --etape cles --sans-controles-longs   # saute le CA3 (sommes de clés)
python -m app.main init 1                              # la chaîne entière d'un bloc

# Options communes
python -m app.main -v init 1 --etape agregats          # logs DEBUG
python -m app.main init 1 --json                       # rapport JSON
python -m app.main init 1 2>/dev/null                  # rapport seul, sans les logs

# Docker
docker compose build
docker compose run --rm yb07 db-check
docker compose run --rm yb07 init 1 --etape agregats

# Tests
python -m pytest tests/
python -m pytest tests/test_initialisation.py
python -m pytest tests/ -k rg3
```

## Prérequis

- Python >= 3.12
- MySQL (une instance en lecture, une en écriture — ou la même pour les deux). La base doit
  déjà porter le schéma TRPPU : ce module ne crée aucune table (le déploiement de structure
  relève de DSR-721 / MD01), il n'ajoute que des index et une colonne (étape `migration`) et
  élargit trois colonnes (étape `correctif`).
- Un stockage objet S3 (AWS ou compatible : MinIO, Ceph…) pour l'étape de chargement
- Le référentiel à initialiser doit être déclaré dans `trppu_referentiel`

## Arborescence

```
yb07/
├── app/
│   ├── config.py               toutes les variables d'environnement, source de vérité
│   ├── erreurs.py              TraitementImpossible — l'erreur commune
│   ├── health.py               diagnostics MySQL (fonctions, pas des routes)
│   ├── json_formatter.py       format de log JSON + setup_logging
│   ├── log_utils.py            ctx() et l'identifiant de corrélation
│   ├── main.py                 point d'entrée console, une sous-commande par action
│   ├── db/
│   │   ├── mysql.py            Database : pools, transactions, runner de scripts .sql
│   │   ├── sql_script.py       découpage des scripts (sqlparse, DELIMITER)
│   │   └── sql_parametres.py   substitue les SET @… d'un script sans le réécrire
│   ├── services/s3.py          client S3, listing, lecture en streaming
│   ├── services/fichier_local.py  même lecture en streaming, depuis le disque local
│   └── traitements/            traitements métier — rendent un Rapport, ne lèvent pas
│       ├── rapport.py          Rapport, Controle : sortie texte / JSON
│       ├── cles_repartition.py chargement du CSV (commande charger-cles-repartition)
│       ├── initialisation.py   orchestration des six étapes (commande init)
│       └── controles_init.py   prérequis et critères d'acceptation, rejoués en Python
├── db/                         scripts .sql de la chaîne DSR-696→699 (cf. db/README.md)
│   ├── DSR-696-699_migration.sql   étape migration
│   ├── fix_error.sql               étape correctif
│   ├── DSR-696_site_trafic.sql     étape agregats
│   ├── DSR-698_version_cle.sql     étape versions
│   └── DSR-699_cles_calculees.sql  étape cles
├── docs/CONVENTION-LOGS.md     la convention de log, verrouillée par les tests
└── tests/
```

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

### S3

Aucune région n'est configurée : boto3 retombe sur `us-east-1`, valeur que les S3 internes
acceptent pour la signature (et, sur AWS, S3 redirige vers la bonne région).

| Variable | Par défaut | Description |
|---|---|---|
| `S3_ENDPOINT_URL` | `""` | Endpoint du stockage. Renseigné, il désigne un S3 interne (MinIO, Ceph…) et l'adressage passe en *path-style*. Vide = AWS. |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | `""` | Identifiants (noms standard AWS). **Vides, boto3 résout seul** (rôle de la machine, profil `~/.aws`). Renseignés, ils priment. Il faut les deux : une clé sans secret est ignorée. |
| `S3_BUCKET` | `""` | Bucket source |
| `S3_PREFIXE` | `""` | Dossier dans le bucket, sans slash de début ni de fin ; vide = racine |
| `S3_TIMEOUT` | `60` | Délai d'attente réseau, en secondes |

### Fichiers CSV et chargements

| Variable | Par défaut | Description |
|---|---|---|
| `CSV_CLES_REPARTITION` | `""` | Nom du fichier des clés de répartition dans le bucket. Il change à chaque livraison du métier, d'où sa présence ici. `--fichier` le surcharge ponctuellement. |
| `CSV_DELIMITEUR` | `;` | Séparateur de champs |
| `CSV_ENCODAGE` | `utf-8-sig` | Décode aussi l'UTF-8 nu et absorbe le BOM |
| `CHARGEMENT_TAILLE_LOT` | `5000` | Nombre de lignes par lot inséré — chaque lot est commité séparément |
| `CHARGEMENT_LOG_TOUTES_LES` | `100000` | Fréquence des lignes de log d'avancement, en lignes chargées |
| `INIT_LOG_TOUS_LES_SITES` | `50` | `init`, étape `versions` : fréquence des lignes d'avancement, en sites traités |
| `INIT_MAX_ANOMALIES_LOGUEES` | `50` | `init`, étape `cles` : plafond des sommes de clés hors tolérance journalisées une à une. Le compte total est toujours rendu. |

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
| `s3-check` | Configuration S3, test d'accès, et contenu du bucket | `--prefixe`, `--recursif`, `--limite` |
| `charger-cles-repartition` | Charge `trppu_cles_repartition` depuis un CSV déposé sur S3 | `id_referentiel` (obligatoire), `--fichier` |
| `charger-cles-repartition-local` | Même chargement, depuis un CSV du disque local | `id_referentiel`, `chemin` (obligatoires) |
| `init` | Enchaîne toute la chaîne d'initialisation des clés de répartition (DSR-696 à DSR-699) | `id_referentiel` (obligatoire), `--depuis`/`--etape`, `--fichier`, `--commentaire`, `--libelle`, `--dry-run`, `--sans-controles-longs` |

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

Les fonctions sous-jacentes de ces deux commandes vivent dans `app/health.py`
(`check_config`, `describe_connection`, `fetch_server_info`, `check_resources`) : elles
retournent de simples dictionnaires et sont réutilisables depuis un module métier, sans
passer par la CLI.

### `s3-check` — explorer le bucket S3

Sert à régler `S3_PREFIXE` et `CSV_CLES_REPARTITION` **avant** de lancer un chargement : on
voit ce que le bucket contient réellement au lieu de deviner un chemin et d'attendre
l'échec.

```bash
python -m app.main s3-check                          # niveau de S3_PREFIXE
python -m app.main s3-check --prefixe ""             # racine du bucket
python -m app.main s3-check --prefixe referentiels/  # un dossier précis
python -m app.main s3-check --recursif --limite 1000 # toute l'arborescence
```

```
Configuration S3
  endpoint     : https://s3.interne.example
  bucket       : trppu
  prefixe      : referentiels/
  adressage    : path
  identifiants : explicites (AWS_ACCESS_KEY_ID=AK**********90)
Accès : ok
  buckets visibles : trppu, archives

Contenu de s3://trppu/referentiels/
  [dossier]  2026/
  [dossier]  archives/
     1.2 Go  cles_repartitions_final_joined_ref1.csv  2026-07-21 08:12
2 objet(s), 2 dossier(s)
```

| Option | Effet |
|---|---|
| `--prefixe` | Dossier à lister, à défaut de `S3_PREFIXE`. `--prefixe ""` remonte à la racine. |
| `--recursif` | Déroule toute l'arborescence. Sans lui, le listing s'arrête au niveau courant et les sous-dossiers apparaissent comme `[dossier]`, de sorte qu'on navigue de niveau en niveau. |
| `--limite` | Nombre maximum d'objets listés (200 par défaut). Un listing tronqué le dit. |

Le secret n'est **jamais** affiché et la clé d'accès est masquée : le diagnostic permet de
dire « ce n'est pas la bonne clé » sans imprimer un secret dans une console ou un log. Si
une seule des deux valeurs est renseignée, la commande le signale — `construire_client` les
ignore toutes les deux, et c'est la chaîne boto3 par défaut qui sert.

Code de retour `0` si l'accès et le listing aboutissent, `1` sinon.

### `charger-cles-repartition` — charger le référentiel des PDI

```bash
python -m app.main charger-cles-repartition 1
python -m app.main charger-cles-repartition 1 --fichier livraison_2026-07.csv --json
```

Récupère le CSV déposé sur S3 et charge `trppu_cles_repartition` pour le référentiel
demandé.

| Argument | Effet |
|---|---|
| `id_referentiel` | **Obligatoire.** Référentiel à charger : il cible la purge, et toute ligne du fichier portant un autre référentiel fait échouer le chargement. |
| `--fichier` | Nom du fichier dans le bucket, à défaut de `CSV_CLES_REPARTITION`. Pour un rechargement ponctuel sans toucher au `.env`. |

**Format attendu** — 18 colonnes séparées par `;`, en-tête compris :

```
id;pdi_rattache;trafic_colis;trafic_oo;trafic_3s;nature;regate_site;type;libelle_site;
regate_etab;libelle_etab;regate_dex;libelle_dex;nb_pre;potentielip;id_referentiel;
date_debut_validite;date_fin_validite
```

La colonne `id` du fichier est le **PDI** : elle alimente `id_pdi`, la colonne `id` de la
table étant auto-incrémentée. Un en-tête différent fait échouer la commande avant toute
écriture — pas à la millionième ligne.

**Règles appliquées**, reprises du chargement historique en SQL pur du module voisin
(`yb05/db/DSR-697_chargement_cles_repartition.sql`) :

- le référentiel cible est **purgé** avant chargement (`DELETE`, jamais `TRUNCATE` qui
  viderait tous les référentiels et interdirait le retour arrière) — c'est ce qui rend la
  commande rejouable ;
- les quatre champs facultatifs vides deviennent `NULL` et non `0` ni `''` :
  `regate_etab`, `libelle_etab`, `nb_pre`, `potentielip`. « 0 » et « inconnu » ne se
  confondent pas pour un potentiel IP ;
- toute colonne obligatoire vide **fait échouer** le chargement en citant le numéro de
  ligne du fichier. C'est la transposition du `sql_mode` strict : mieux vaut une erreur au
  chargement qu'un trafic silencieusement ramené à zéro ;
- le fichier fait foi pour `id_referentiel`, `date_debut_validite` et `date_fin_validite`,
  mais une ligne portant un autre référentiel que celui demandé est refusée — c'est le seul
  garde-fou contre un fichier chargé sous un référentiel qui n'est pas le sien ;
- le référentiel doit exister dans `trppu_referentiel`. Aucune clé étrangère ne l'impose :
  sans ce contrôle, on charge 22 M de lignes sous un identifiant inexistant sans que rien
  ne le signale.

**Déroulé** — garde-fous (référentiel déclaré, fichier présent sur S3), purge, puis lecture
en streaming et insertion par lots de `CHARGEMENT_TAILLE_LOT` lignes. Le fichier n'est ni
téléchargé sur disque ni chargé en mémoire.

**Atomicité** : chaque lot est commité séparément. Sur 22 M de lignes, une transaction
unique ferait du journal d'annulation, de la durée de connexion et du coût du `ROLLBACK` le
vrai risque. En contrepartie, un échec laisse un chargement **partiel** : le rapport indique
combien de lignes étaient passées, et il suffit de relancer — la purge rend l'opération
idempotente.

**Erreur la plus fréquente** : `Duplicate entry` sur `uk_pdi_ref`, retraduit en « le fichier
porte deux fois le même couple (PDI, référentiel) ». Le fichier doit être dédoublonné en
amont ; un `DISTINCT` ne suffit pas, deux lignes d'un même PDI aux trafics différents y
survivent.

**Attention** : recharger un référentiel périme tout ce qui en découle (agrégats, clés
calculées d'autres traitements). Ils sont à rejouer ensuite.

**Sortie** :

```
--------------------------------------------------
CHARGEMENT DES CLES DE REPARTITION
Référentiel : 1
--------------------------------------------------

[OK] Référentiel 1 déclaré
[OK] Fichier 'referentiels/cles.csv' présent sur S3 (1288490188 octets)
[OK] Purge du référentiel : 22395341 ligne(s) supprimée(s)
[OK] 22395341 ligne(s) insérée(s) en 4480 lot(s)
[OK] Volumétrie en base : 22395341 ligne(s)
[OK] PDI distincts : 22395341
[OK] Lignes actives (date_fin_validite NULL) : 22395341

LIGNES_CHARGEES = 22395341
LIGNES_PRECEDENTES = 22395341
DATE_DEBUT_VALIDITE_MIN = 2026-07-21

RESULTAT : SUCCES
```

En cas d'échec, `RESULTAT : ECHEC`, les lignes fautives passent en `[KO]` et la section
`Motifs :` les reprend. `--json` rend la même chose sous forme structurée
(`reussi`, `controles`, `etats`, `motifs`, `erreur`).

### `charger-cles-repartition-local` — charger depuis un fichier local

```bash
python -m app.main charger-cles-repartition-local 1 data/cles.csv
python -m app.main charger-cles-repartition-local 1 "C:/depot/cles_repartitions.csv.gz" --json
```

Même traitement que `charger-cles-repartition` — mêmes garde-fous, même purge, mêmes
règles, même rapport — mais le CSV est lu sur le disque. **S3 n'est pas sollicité** : les
variables `S3_*` et `AWS_*` peuvent rester vides. Utile en développement, en recette, ou
pour un fichier transmis hors bucket.

| Argument | Effet |
|---|---|
| `id_referentiel` | **Obligatoire.** Comme pour le chargement S3. |
| `chemin` | **Obligatoire.** Chemin du fichier, absolu ou relatif au dossier courant. Un `.gz` est décompressé à la volée. |

Le fichier est localisé (existence, lisibilité) **avant** la purge. `CSV_CLES_REPARTITION`
n'est pas utilisé ; `CSV_DELIMITEUR` et `CSV_ENCODAGE` s'appliquent. Dans le rapport, la
ligne de localisation devient `Fichier '…' présent en local (… octets)`.

En Docker, le fichier doit être visible dans le conteneur : monter son dossier en volume
(`-v /chemin/hote:/data`) et passer `/data/cles.csv`.

### `init` — initialiser les clés de répartition

Enchaîne les six étapes qui produisent les clés de répartition des PDI à partir du CSV
métier : chargement S3, migration de schéma, correctif de colonnes, agrégats par site,
versions de clés, calcul des clés. L'ordre et les paramètres de chaque script sont décrits
dans [`db/README.md`](db/README.md).

```bash
python -m app.main init 1                       # la chaîne entière
python -m app.main init 1 --dry-run             # découpe les scripts, n'écrit rien
python -m app.main init 1 --etape agregats      # une seule étape
python -m app.main init 1 --depuis versions     # reprend ici et enchaîne
```

| Option | Effet |
|---|---|
| `id_referentiel` | Référentiel à initialiser. Le fichier CSV doit porter le même. |
| `--fichier` | Nom du fichier dans le bucket, à défaut de `CSV_CLES_REPARTITION`. |
| `--depuis <etape>` | Reprend à cette étape et enchaîne les suivantes. |
| `--etape <etape>` | Ne joue que cette étape. S'exclut avec `--depuis`. |
| `--commentaire`, `--libelle` | Portés par les versions de clés créées (DSR-698). |
| `--dry-run` | Lit et découpe les scripts sans rien écrire. L'étape `chargement` est sautée : elle n'a pas de mode à blanc. |
| `--sans-controles-longs` | Saute les contrôles qui balaient les 24 M de lignes. Une clé fausse ne serait alors pas détectée. |

Étapes, dans l'ordre : `chargement`, `migration`, `correctif`, `agregats`, `versions`, `cles`.

**Ne pas lancer `init` d'un bloc au premier passage sur un référentiel réel.** Deux étapes se
comptent en heures (`migration` construit un index sur 24 M de lignes, `cles` en écrit autant)
et n'émettent aucun avancement : ce sont des `INSERT … SELECT` monolithiques, le socle ne rend
la main qu'à la fin. Dérouler `--etape` par `--etape`, en relevant le `duration_ms` de chaque
ligne `Fin étape`, dit si la chaîne entière tiendra dans la fenêtre d'exploitation. Pendant ce
temps, `yb05/db/suivi.sql` joué depuis un second terminal donne la seule visibilité disponible.

En cas d'échec, le rapport nomme l'étape à reprendre :

```
[OK] Étape 4/6 agregats — 3412 site(s) agrégé(s)
[KO] Étape 5/6 versions — 2 site(s) en échec sur 3412 (premiers : 372920, 833280). Le calcul
     des clés n'est pas joué — le CA4 de DSR-699 figerait définitivement les sites restants.

SITES_TRAITES = 3412
VERSIONS_CREEES = 3410
SITES_VERSIONS_KO = 2
REPRENDRE_A = versions
```

Relancer `--depuis versions` est sans dommage : DSR-698 ne fait rien pour un site qui a déjà
sa version active. En revanche, **une fois l'étape `cles` passée, le référentiel est figé** —
le CA4 de DSR-699 interdit de recalculer les clés d'une version existante, et `init` refuse de
rejouer l'étape plutôt que de rendre un `[OK]` sur un script qui n'aurait rien écrit.

### Ajouter une commande métier

Une commande de diagnostic se résume à une coroutine et à son enregistrement :

```python
async def cmd_mon_diagnostic(args: argparse.Namespace) -> int:
    ...
    return EXIT_OK

# dans build_parser()
sous_commandes.add_parser(
    "mon-diagnostic",
    parents=[commun],
    help="…",
).set_defaults(handler=cmd_mon_diagnostic)
```

Un **traitement métier** suit un second motif : il vit dans `app/traitements/`, rend un
`Rapport` et ne lève jamais — c'est la CLI qui décide de l'afficher en texte ou en JSON et
qui en déduit le code de retour. Son branchement passe par `_executer_traitement`, qui pose
l'identifiant de corrélation, rattrape toute exception résiduelle et libère le contexte :

```python
async def cmd_mon_traitement(args: argparse.Namespace) -> int:
    return await _executer_traitement(lambda a: mon_traitement(a.id_traitement), args)
```

Dans les deux cas le parent `commun` apporte `-v` et `--json`, et `_run` ferme les pools
dans un `finally`. Le code métier ne va jamais dans `main.py`.

## Procédure d'initialisation d'un référentiel

Déroulé recommandé pour un **premier passage sur un référentiel réel** : étape par étape, en
ne passant à la suivante que si la précédente rend `0` et `RESULTAT : SUCCES`. Chaque
commande vérifie elle-même ses prérequis avant d'écrire.

| # | Commande | Durée indicative | Rejouable ? | À contrôler dans le rapport |
|---|---|---|---|---|
| 0 | `db-check`, puis `s3-check --prefixe …` | secondes | oui | les deux instances `connected` ; le CSV visible dans le listing |
| 1 | `init 1 --dry-run` | secondes | oui | les cinq scripts découpés, aucune écriture |
| 2 | `init 1 --etape chargement` | long (streaming 22 M lignes) | **oui** — purge du référentiel | `LIGNES_CHARGEES`, `LIGNES_ACTIVES` |
| 3 | `init 1 --etape migration` | **heures** (index sur 24 M lignes) | oui — chaque `ALTER` testé | 4 index et `date_creation` en place |
| 4 | `init 1 --etape correctif` | variable | oui | totaux en `decimal(35+,19)` |
| 5 | `init 1 --etape agregats` | long (agrégation 24 M lignes) | **oui** — `DELETE` ciblé puis `INSERT` | nombre de sites, CA2, CA5 (autres référentiels intacts) |
| 6 | `init 1 --etape versions` | ≈ un script par site | **oui** — un site déjà versionné est sauté | `SITES_TRAITES`, `VERSIONS_CREEES`, `SITES_VERSIONS_KO = 0` |
| 7 | `init 1 --etape cles` | **heures** (écrit 24 M lignes) | **NON — irréversible** | CA1 (une clé par PDI actif), CA2, CA3 (sommes à 1 ± 10⁻⁴) |

Points de vigilance :

- **Avant l'étape 7**, relire le rapport de l'étape 6 : aucun site en échec, et autant de
  versions actives que de sites agrégés. `init` le vérifie et refuse de démarrer sinon, mais
  c'est la dernière occasion de corriger sans créer une nouvelle série de versions.
- Les étapes `migration` et `cles` **n'émettent aucun avancement** (des `INSERT … SELECT` /
  `ALTER` monolithiques) : relever le `duration_ms` de chaque ligne `Fin étape` donne la
  mesure pour les passages suivants. `yb05/db/suivi.sql`, joué depuis une seconde session,
  est la seule visibilité en cours d'exécution.
- En cas d'échec, le rapport indique `REPRENDRE_A = <étape>` : corriger la cause, puis
  `init 1 --depuis <étape>`.
- Une fois les durées connues et la fenêtre d'exploitation validée, les passages suivants
  peuvent se faire d'un bloc (`init <id>`), sur un **nouveau** référentiel.

## Docker

```bash
docker compose build
docker compose run --rm yb07 --help
docker compose run --rm yb07 db-info
docker compose run --rm yb07 db-check
docker compose run --rm yb07 s3-check --prefixe ""
docker compose run --rm yb07 charger-cles-repartition 1
docker compose run --rm yb07 init 1 --dry-run
docker compose run --rm yb07 init 1 --etape agregats
docker compose run --rm -e INIT_LOG_TOUS_LES_SITES=200 yb07 init 1 --depuis versions
```

Le conteneur exécute une commande puis s'arrête : aucun port n'est exposé. La sous-commande
est passée à l'exécution (`ENTRYPOINT` = `python -m app.main`, `CMD` = `db-check`) : lancé
sans argument, il fait donc un `db-check`. La configuration vient du `.env` via `env_file`
(une variable passée par `-e` la surcharge), et `./logs` est monté sur `/app/logs`. Les
scripts de `db/` sont copiés dans l'image (`.dockerignore` n'exclut que `*.md`, `docs/`,
`tests/`) : `init` y trouve donc ses fichiers.

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
{"app_datetime": "2026-07-31T07:49:35.874Z", "app_ccx": "dsr", "app_env": "sdev", "app_ptf": "build", "app_tm": "yb07", "app_version": "1.0.0", "severity_label": "INFO", "app_message": "Fin chargement (lignes=1240, duration_ms=8421.0)", "id_traitement": 12345, "name": "app.main", "filename": "main.py", "lineno": 158}
```

`id_traitement` est posé une fois par unité de travail (`set_id_traitement`) et repris sur
**toutes** les lignes qui en découlent, y compris celles de `app.db.mysql`. Quand plusieurs
unités sont traitées en parallèle et que les lignes s'entrelacent, c'est ce champ qui permet
de reconstituer une trace. Il vaut `null` hors traitement.

Le jeu de clés est fixe : `logger.info(..., extra={...})` est **sans effet**. Le contexte
métier vit donc dans `app_message`, sous la grammaire
`Début|Avancement|Fin|Rejet|Erreur <action> (cle=valeur, …)`.

### Ce qui est loggé

- Début et fin de chaque commande, avec `exit_code` et `duration_ms`.
- Début, **avancement** et fin de chaque traitement, avec sa volumétrie.
- Les rejets métier, avec leur `motif` — un traitement qui rend un rapport d'échec ne lève
  pas : sans une ligne dédiée, l'échec ne laisserait aucune trace.
- Les exceptions, avec la stack trace.
- Les tentatives de connexion MySQL et les vérifications en échec.
- L'accès S3 : fichier localisé, listing, refus d'accès. **Jamais** le secret, et la clé
  d'accès uniquement masquée.
- L'exécution des scripts SQL (début, fin, avertissements DDL, échecs) — l'aperçu des
  instructions est tronqué à 120 caractères et **jamais** le SQL complet, les scripts de
  données pouvant contenir des informations personnelles.
- `init` : début et fin de chaque étape (`Début|Fin étape initialisation`, avec `etape`,
  `rang`, `verdict`, `duration_ms`), les prérequis refusés (`Rejet prérequis
  initialisation`), chaque site en échec à l'étape `versions` (`Rejet création de version`),
  et chaque site dont la somme des clés sort de la tolérance (`Rejet contrôle somme des clés`,
  plafonné à `INIT_MAX_ANOMALIES_LOGUEES` lignes, le compte total restant au rapport).

### Suivre un chargement en cours

Un chargement de 22 M de lignes dure. Il journalise sa progression toutes les
`CHARGEMENT_LOG_TOUTES_LES` lignes (100 000 par défaut), ce qui permet de distinguer un
traitement lent d'un traitement bloqué :

```
Avancement chargement clés de répartition (id_referentiel=1, lignes=400000, lots=80, debit_lignes_s=12500.0, duration_ms=32000.0)
```

Le total n'est pas annoncé : le fichier est lu en streaming, il n'est jamais compté
d'avance. On journalise un volume et un débit, pas un pourcentage inventé.

L'étape `versions` de `init`, elle, connaît son total (la liste des sites est lue d'avance) :
elle journalise un pourcentage toutes les `INIT_LOG_TOUS_LES_SITES` itérations.

```
Avancement création des versions (id_referentiel=1, sites=1500, sites_total=3412, pct=44.0, versions_creees=1500, sites_ko=0, debit_sites_s=8.3, duration_ms=180000.0)
```

## Tests

```bash
python -m pytest tests/                                # tous les tests
python -m pytest tests/test_initialisation.py          # un fichier
python -m pytest tests/ -k rg3                         # un sous-ensemble
python -m pytest tests/ -q                             # sortie compacte
```

Les tests ne nécessitent ni base MySQL ni réseau : `aiomysql.connect` et `boto3.client` sont
remplacés par des doublures, et le code async est lancé via `asyncio.run` (pas de dépendance
à pytest-asyncio).

- `tests/test_sql_script.py` — découpage et exécution des scripts SQL.
- `tests/test_log_convention.py` — rendu de `ctx()`, rendu paresseux, et le champ
  `id_traitement` (toujours présent, `null` hors contexte, isolé entre tâches asyncio).
  Ces tests **verrouillent la convention de log** : un nouveau code qui s'en écarte les
  fait échouer.
- `tests/test_chargement_cles_repartition.py` — règles de gestion du chargement : purge
  avant insertion, conversions vide → `NULL`, refus d'un référentiel discordant ou non
  déclaré, découpage en lots, numéro de ligne exact dans les messages d'erreur.
- `tests/test_s3.py` — résolution de la configuration S3 (identifiants du `.env` ou chaîne
  boto3, adressage path-style), masquage de la clé, listing dossiers/objets, décompression
  `.gz`, fermeture du flux, traduction des erreurs.
- `tests/test_sql_parametres.py` — injection des paramètres `SET @…` : rendu des littéraux
  SQL (échappement, `NULL`, dates), refus d'un paramètre absent du script, `SET SESSION` et
  variables de travail intacts — joué aussi sur le texte réel des scripts de `db/`.
- `tests/test_scripts_dsr.py` — les scripts de `db/` sans base : nombre d'instructions figé
  (garde-fou contre une divergence avec `yb05/db/`), ordre des instructions qui porte la
  rejouabilité, et **périmètre** : aucun DDL de schéma, aucun `LOAD DATA`.
- `tests/test_initialisation.py` — orchestration de `init` : ordre des étapes, mode
  d'exécution de chaque script, `--depuis` / `--etape` / `--dry-run`, refus de démarrer sur un
  état incohérent, et refus de franchir l'étape `cles` quand ce qui précède est incomplet.

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

Socle repris de `yb05/`, dont il partage la structure et les choix techniques ; il est
dupliqué (et non partagé) avec `yb05/` et `yb06/` : un correctif de `app/db/mysql.py` ou de
`app/json_formatter.py` est à reporter à la main dans les autres modules. Il en va de même
pour les cinq scripts de `db/`, copies de `yb05/db/`.
