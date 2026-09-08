# API MySQL Debug — exploration du schéma et exports de données

> Module : `app/routes/mysql_debug.py`
> Préfixe HTTP : `/mysql`
> Tag Swagger : **MySQL Debug**

⚠️ **Routes de debug/admin, sans authentification.** À ne pas exposer en production —
`POST /mysql/import` écrit sur une table arbitraire avec `FOREIGN_KEY_CHECKS = 0`.

---

## 1. Vue d'ensemble

| Méthode | Chemin | Description |
|---------|--------|-------------|
| `GET` | `/mysql/test` | `SELECT 1` de contrôle |
| `GET` | `/mysql/tables` | Liste des tables (+ nombre de lignes approximatif) |
| `GET` | `/mysql/columns?table=` | Colonnes d'une table |
| `GET` | `/mysql/indexes?table=` | Index d'une table |
| `GET` | `/mysql/sample?table=&limit=` | Échantillon (≤ 100 lignes) |
| `GET` | `/mysql/schema` | Schéma complet (tables + colonnes) |
| `GET` | `/mysql/dump` | **Toute la base** : structure ± données |
| `GET` | `/mysql/export?table=` | **Une table** : structure ± données |
| `POST` | `/mysql/import` | Recharge une table depuis un export JSON |

`dump` et `export` sont symétriques : ce que l'on peut faire sur la base entière
(`data=true/false`) se fait aussi table par table.

---

## 2. Volumétrie — ce qu'il faut savoir avant d'exporter

Ces routes sont dimensionnées pour des tables atteignant **plusieurs dizaines de millions
de lignes**. Trois mécanismes le permettent, et trois conséquences en découlent.

**Ce qui est en place**

1. **Curseur serveur** (`db_read.iter_rows`, `SSDictCursor`) : MySQL streame le résultat au
   lieu que le client le bufferise. `fetch_all` sur 30 M de lignes ferait tomber le process ;
   ici la mémoire de l'API reste constante.
2. **Réponse streamée** (`StreamingResponse`) : les octets partent au fil de la lecture,
   rien n'est assemblé avant l'envoi. Effet de bord utile : le client voit des données
   circuler immédiatement, ce qui évite les coupures de proxy sur *idle timeout*.
3. **`INSERT` multi-lignes** (`rows_per_insert`, 200 par défaut) : un rechargement d'un
   ordre de grandeur plus rapide que des `INSERT` unitaires. Chaque ordre reste très en
   deçà de `max_allowed_packet` (4 Mo par défaut sous MySQL 8) ; monter ce paramètre sur
   des tables larges peut faire dépasser cette limite au rechargement.

**Ce qu'il faut accepter en échange**

1. **Une erreur en cours d'export ne peut plus produire un 500** : le statut HTTP est parti
   avant la première ligne. Elle est journalisée puis écrite dans le flux. **Contrôler la
   fin du fichier**, c'est le seul contrôle d'intégrité fiable :
   - `fmt=sql` → dernière ligne `-- FIN DE L'EXPORT — <n> ligne(s), <durée>s`
   - `fmt=json` → clé `"complete": true`
   Un export coupé porte `-- !! EXPORT INCOMPLET` / `"complete": false` + `"error"`.
2. **Une connexion du pool est mobilisée pendant tout l'export** (pool de 10). Ne pas
   lancer plusieurs gros exports en parallèle sur une instance qui sert l'IHM.
3. **Le nombre de lignes n'est connu qu'à la fin** : le compter d'avance imposerait un scan
   complet supplémentaire. En JSON, `count` est donc placé après `rows`.

---

## 3. `GET /mysql/export` — une table

### 3.1 Portée : structure et/ou données

| `schema` | `data` | Résultat |
|----------|--------|----------|
| `false` (défaut) | `true` (défaut) | Données seules — comportement historique de la route |
| `true` | `true` | `CREATE TABLE` puis `INSERT` : table rechargeable telle quelle |
| `true` | `false` | Structure seule |
| `false` | `false` | `400` — rien à exporter |

Paramètres associés : `drop=true` ajoute un `DROP TABLE IF EXISTS` avant le `CREATE` ;
`truncate=true` (défaut) ajoute un `TRUNCATE TABLE` avant les `INSERT` — il est
automatiquement omis si `schema=true&drop=true`, le `DROP`+`CREATE` ayant déjà rendu la
table vide.

### 3.2 Formats

- **`fmt=sql`** — script `text/plain` rejouable dans un client MySQL. **C'est le format à
  utiliser sur les grosses tables.**
- **`fmt=json`** — payload réinjectable tel quel via `POST /mysql/import`. Attention :
  cet import lit le corps entier en mémoire, il ne convient qu'aux tables modestes.

`download=true` renvoie la réponse en pièce jointe (`Content-Disposition`), pour
enregistrer directement dans un fichier depuis un navigateur.

### 3.3 Découpage en lots — `limit` + `after`

`limit` fixe librement la taille d'un lot (10 000, 100 000, 1 000 000 : **aucun plafond
imposé**). Pour enchaîner les lots, utiliser **`after`, pas `offset`** :

| | `offset=N` | `after=<pk>` |
|---|---|---|
| Coût d'une page | MySQL lit puis **jette** N lignes → le découpage complet devient quadratique | attaque directement l'index → **coût constant** |
| Sur 30 M de lignes | la dernière page relit 30 M de lignes | la dernière page coûte comme la première |

Chaque réponse indique où reprendre :

- `fmt=sql` → commentaire final `-- Lot suivant : &after=<valeur>`
- `fmt=json` → clé `"next_after": "<valeur>"`

L'indication n'est présente que si le lot a été **rempli jusqu'à `limit`** : son absence
signifie que la fin de la table est atteinte.

`after` exige une **clé primaire mono-colonne** (`400` sinon : pas de PK, ou PK composite —
se rabattre alors sur `limit`+`offset`).

> **Pourquoi le tri apparaît quand on découpe.** Dès qu'un `limit`, `offset` ou `after` est
> demandé, la requête est triée sur la clé primaire. Sans `ORDER BY`, MySQL ne garantit
> aucun ordre entre deux exécutions : deux lots successifs pourraient se recouvrir ou
> sauter des lignes. À l'inverse, un export **intégral** n'est pas trié — sur 30 M de
> lignes ce tri coûterait cher pour rien, l'index clusterisé InnoDB donnant déjà l'ordre.

### 3.4 Exemples

```bash
# Structure + données d'une table, prêt à rejouer
curl "http://localhost:8080/mysql/export?table=trppu_tmh&fmt=sql&schema=true&drop=true" \
  -o trppu_tmh.sql

# Structure seule
curl "http://localhost:8080/mysql/export?table=trppu_tmh&fmt=sql&schema=true&data=false"

# Un lot de 100 000 lignes, puis le suivant
curl "http://localhost:8080/mysql/export?table=trppu_trafic_agrebal&fmt=sql&limit=100000" \
  -o lot_01.sql
tail -2 lot_01.sql        # -> -- Lot suivant : &after=100000
curl "http://localhost:8080/mysql/export?table=trppu_trafic_agrebal&fmt=sql&limit=100000&after=100000" \
  -o lot_02.sql

# Export complet d'une très grosse table, directement dans un fichier
curl "http://localhost:8080/mysql/export?table=trppu_trafic_agrebal&fmt=sql" \
  -o agrebal.sql
tail -1 agrebal.sql       # DOIT afficher "-- FIN DE L'EXPORT"
```

Rechargement d'un export `fmt=sql` — passer par le client MySQL, pas par l'API :

```bash
mysql -h <hote> -u <user> -p <base> < agrebal.sql
```

---

## 4. `GET /mysql/dump` — toute la base

Mêmes principes, à l'échelle de la base : `data=false` (défaut) donne le DDL seul,
`data=true` ajoute les `INSERT` de chaque table, streamés **table par table** (rien n'est
accumulé entre deux tables).

- `drop=true` (défaut) : `DROP TABLE/VIEW IF EXISTS` avant chaque `CREATE`.
- `limit_per_table=N` : au plus N lignes par table — de quoi fabriquer un **dump de dev
  allégé** à partir d'une base de production.
- Les vues sont dumpées après les tables (elles en dépendent) et n'ont jamais de données.
- Un objet dont le DDL échoue est annoté (`-- !! ERREUR`) sans interrompre le dump : on
  récupère ainsi toutes les tables même si une vue casse.

En `fmt=json` avec `data=true`, la clé `sql` **n'est pas produite** : elle dupliquerait
tout le contenu en mémoire, ce qui annulerait le bénéfice du streaming. Les lignes sont
dans `objects[].rows` ; pour obtenir le script rechargeable, utiliser `fmt=sql`.

```bash
# Schéma complet de la base
curl "http://localhost:8080/mysql/dump?fmt=sql" -o schema.sql

# Base complète avec données
curl "http://localhost:8080/mysql/dump?fmt=sql&data=true" -o base_complete.sql

# Dump de dev : structure complète, 10 000 lignes par table
curl "http://localhost:8080/mysql/dump?fmt=sql&data=true&limit_per_table=10000" -o base_dev.sql
```

---

## 5. `POST /mysql/import`

Recharge une table à partir d'un `GET /mysql/export?fmt=json`. Corps :

```json
{ "table": "trppu_tmh", "rows": [ ... ], "columns": ["..."], "truncate": true }
```

- **Non streamé** : le corps est désérialisé entièrement avant traitement. Au-delà de
  quelques centaines de milliers de lignes, rapatrier un `fmt=sql` et le rejouer avec un
  client MySQL, ou découper l'export avec `limit`+`after`.
- `truncate=true` vide la table par `DELETE FROM` et non `TRUNCATE` : ce dernier est du
  DDL, donc auto-commité — un échec d'insertion laissait la table définitivement vide
  malgré le rollback. Contrepartie : l'`AUTO_INCREMENT` n'est pas remis à zéro, sans
  incidence pour un rechargement d'export où les identifiants sont fournis.
- `FOREIGN_KEY_CHECKS` est remis à 1 sur **tous** les chemins : la connexion retourne au
  pool sans reset de session, la laisser à 0 contaminerait les requêtes suivantes.
- Seules les colonnes réellement présentes dans la table sont insérées.

---

## 6. Fidélité des types

Un export doit se recharger à l'identique. Les conversions sont centralisées dans
`_serialize_value` (JSON) et `_sql_literal` (SQL) :

| Type MySQL | JSON | SQL |
|------------|------|-----|
| `DATETIME` / `DATE` | chaîne au format MySQL | `'2026-09-07 14:30:05'` |
| `TIME` (rendu `timedelta`) | `'25:00:00'` | `'25:00:00'` |
| `DECIMAL` | chaîne (préserve la précision) | littéral non quoté |
| `BLOB` / `BINARY` | `{"__b64__": "..."}` | `0x<hex>` |

Deux pièges traités explicitement :

- **`TIME` au-delà de 24 h ou négatif** : `str(timedelta)` produirait `1 day, 1:00:00` ou
  `-1 day, 23:00:00`, que MySQL refuse. La notation horaire cumulée est reconstruite
  (`_time_mysql`), le type acceptant `-838:59:59` à `838:59:59`.
- **`NUL` et `Ctrl-Z` dans une chaîne** : échappés au même titre que le quote et le
  backslash. Bruts dans un script ils tronquent silencieusement la valeur (`Ctrl-Z` vaut
  EOF sous Windows) — le genre d'erreur qui ne se voit que des milliers de lignes plus loin.

---

## 7. Tests

`tests/test_mysql_debug_export.py` — fidélité des littéraux SQL, règles de découpage
(tri présent seulement quand on tronque, `after` sur index, curseur de reprise), contrat
de streaming (JSON analysable et réinjectable, marqueur de fin, sortie marquée incomplète
si le flux casse) et comportement du curseur serveur `Database.iter_rows`.
