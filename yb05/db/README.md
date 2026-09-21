# Scripts SQL — chaîne « clés de répartition des PDI »

Cinq scripts, à jouer **dans l'ordre**, qui initialisent les clés de répartition des PDI à
partir du fichier CSV métier — plus un correctif de schéma, `fix_error.sql`. Écrits en SQL pur : aucun code applicatif du socle ne les
appelle, ils se jouent au client `mysql` ou via le runner de scripts de `Database`.

Les tickets sources sont dans `../docs/` ; aucun ne décrit exactement la base —
`../docs/DIAGNOSTIC-DSR-696-699.md` recense écart par écart la formulation fautive, la lecture
retenue et la correction appliquée.

## La chaîne

```
fichier CSV métier              la photographie du référentiel, fournie par le métier
         │
         │  DSR-697 : chargement, un référentiel par fichier
         ▼
trppu_cles_repartition          22,4 M lignes — trafic de chaque PDI, historisé
         │
         │  DSR-696 : somme des trafics des PDI actifs, par site
         ▼
trppu_trafic_site               le DÉNOMINATEUR des clés
         │
         │  DSR-698 : un conteneur de clés par site, rattaché à un référentiel
         ▼
trppu_version_cle               la VERSION à laquelle les clés se rattachent
         │
         │  DSR-699 : trafic du PDI / total de son site
         ▼
trppu_cles_repartition_calcule  les CLÉS, consommées ensuite par DSR-702
```

Ces scripts **initialisent** les clés. Le calcul des trafics d'un scénario, qui les
consomme, est porté par les commandes du batch (DSR-701 à DSR-703) — cf. la section
« Calcul des trafics d'un scénario » du [README du module](../README.md) :

```bash
python -m app.main eligibilite 12345
python -m app.main calcul-trafic-pdi 12345
python -m app.main calcul-trafic-agrebal 12345
```

## Ordre d'exécution

| # | Fichier | Ce qu'il exige en amont | Ce qu'il laisse |
|---|---------|-------------------------|-----------------|
| 1 | `DSR-697_chargement_cles_repartition.sql` | le fichier CSV dédoublonné, déposé sur le serveur | `trppu_cles_repartition` alimentée pour le référentiel |
| 2 | `DSR-696-699_migration.sql` | rien — **à jouer une seule fois par base** | 3 index, 1 colonne |
| 2 bis | `fix_error.sql` | rien — **une seule fois par base**, avant DSR-696 | les trois totaux de site élargis |
| 3 | `DSR-696_site_trafic.sql` | les étapes 1 et 2 | `trppu_trafic_site` alimentée pour le référentiel |
| 4 | `DSR-698_version_cle.sql` | la migration | une version active par site traité |
| 5 | `DSR-699_cles_calculees.sql` | les étapes 3 et 4, **sur le même référentiel** | les clés du référentiel |

L'ordre 3 → 4 → 5 n'est pas négociable : DSR-699 joint les trois tables et écarte
silencieusement les sites auxquels il manque un agrégat ou une version. Ses deux premiers
garde-fous comptent précisément ces sites-là, avant d'écrire quoi que ce soit.

**Les deux premières lignes, elles, commutent** — DSR-697 ne lit aucun des objets posés par
la migration, et la migration ne lit aucune donnée. L'ordre du tableau est celui qui coûte le
moins cher : sur une base neuve, charger **avant** de migrer laisse MySQL construire
`idx_cr_ref_actif` une fois sur la table pleine, au lieu de le maintenir ligne à ligne
pendant tout le chargement. Sur une base déjà migrée, la question ne se pose plus : on
recharge, c'est tout.

`fix_error.sql` corrige un défaut du schéma livré, découvert au premier chargement réel : les
trois colonnes de totaux de `trppu_trafic_site` étaient en `decimal(24,18)`, soit **six
chiffres avant la virgule**, alors qu'elles reçoivent la somme des trafics d'un site. DSR-696
échouait en `ERROR 1264 (Out of range value for column 'trafic_oo_total')`. Le correctif les
porte à `decimal(35,19)` : seize chiffres entiers, et l'échelle de la source. Il se joue une
fois, comme la migration, et se relance sans dommage.

Ces scripts ne se rejouent pas au même rythme :

| Rythme | Scripts |
|--------|---------|
| une fois par base | la migration, `fix_error.sql` |
| une fois par référentiel | DSR-697, puis DSR-696 |
| une fois par site et par référentiel | DSR-698, puis DSR-699 |

Un nouveau référentiel se traite donc en reprenant à l'étape 1 et en sautant les étapes 2 et
2 bis.

## Avant l'étape 1 — préparer le fichier CSV

Trois choses à faire **hors base**, dans cet ordre. Le script ne peut en porter aucune :
`LOAD DATA` lit un fichier déjà prêt, il ne le corrige pas.

**1. Dédoublonner (RG4).** Le ticket décrit le doublon comme une ligne strictement identique
et propose DuckDB :

```sql
COPY (SELECT DISTINCT * FROM read_csv_auto('cles_repartitions_final_joined.csv'))
  TO 'cles_repartitions_final_joined_ref1.csv' (FORMAT CSV, HEADER, DELIMITER ';');
```

**2. Contrôler l'unicité des PDI (RG5).** La déduplication ci-dessus ne suffit pas : elle
porte sur la ligne entière, alors que la base exige un PDI unique par référentiel
(`uk_pdi_ref`). Deux lignes d'un même PDI aux trafics différents survivent au `DISTINCT` et
feront échouer le chargement en `ERROR 1062`, après avoir tout inséré et tout annulé. Le
contrôle coûte quelques secondes, l'échec coûte un chargement complet :

```sql
SELECT id_pdi, COUNT(*) FROM read_csv_auto('cles_repartitions_final_joined_ref1.csv')
 GROUP BY id_pdi HAVING COUNT(*) > 1;   -- doit renvoyer 0 ligne
```

**3. Déposer le fichier sur le serveur MySQL**, dans le répertoire autorisé — c'est le
serveur qui lit le fichier, pas le client. Le garde-fou du script affiche la valeur de
`@@secure_file_priv` ; le fichier doit être dans ce répertoire (typiquement
`/var/lib/mysql-files/`) et lisible par le compte système `mysql`.

Si le fichier ne peut pas être déposé sur le serveur, la variante `LOAD DATA LOCAL INFILE`
lit depuis la machine cliente — elle exige `local_infile = 1` côté serveur **et** côté
client (`mysql --local-infile=1`), et elle transfère le fichier par le réseau. À réserver aux
environnements de test : sur 22 M de lignes, la différence de durée n'est pas anecdotique.

## Paramètres

Chaque script se règle **en tête de fichier**, dans des variables de session — le fichier
entier tournant sur une connexion unique, elles restent visibles par toutes ses instructions.

| Script | Paramètres |
|--------|-----------|
| DSR-697 | `@id_referentiel` obligatoire, **plus le chemin du fichier, à éditer dans le `LOAD DATA`** |
| migration | aucun |
| `fix_error.sql` | `@id_referentiel`, pour ses deux constats seulement — l'`ALTER`, lui, ne dépend d'aucun paramètre |
| DSR-696 | `@id_referentiel` obligatoire, `@co_regate` (NULL = tout le référentiel) |
| DSR-698 | `@id_referentiel`, `@co_regate`, `@commentaire`, `@libelle` — les deux premiers obligatoires |
| DSR-699 | `@id_referentiel` obligatoire, `@co_regate` (NULL = tout le référentiel) |

DSR-697 est le seul à ne pas être entièrement paramétrable en tête de fichier : `LOAD DATA`
n'accepte qu'un **littéral** comme chemin, et le contournement par `PREPARE`/`EXECUTE`
qu'emploie la migration ne lui est pas ouvert — cette instruction ne figure pas parmi les
instructions préparables. Le chemin est donc à éditer à la main, dans le corps du script, en
même temps que `@id_referentiel`. Les deux vont ensemble : un fichier `…_ref1.csv` chargé
sous `@id_referentiel := 2` ne produit aucune erreur, seulement un référentiel faux.

### Exemple — initialiser un site

```sql
-- 1. chargement du référentiel (fichier déjà dédoublonné et déposé sur le serveur)
SET @id_referentiel := 7;
--    éditer aussi le chemin dans le LOAD DATA du fichier
--    mysql … < db/DSR-697_chargement_cles_repartition.sql

-- 2. une seule fois par base
--    mysql … < db/DSR-696-699_migration.sql
--    mysql … < db/fix_error.sql

-- 3. agrégats du site
SET @id_referentiel := 7;
SET @co_regate      := '123456';
--    mysql … < db/DSR-696_site_trafic.sql

-- 4. version de clés du site
SET @id_referentiel := 7;
SET @co_regate      := '123456';
SET @commentaire    := 'Réorganisation DEX';
SET @libelle        := NULL;
--    mysql … < db/DSR-698_version_cle.sql

-- 5. calcul des clés
SET @id_referentiel := 7;
SET @co_regate      := '123456';
--    mysql … < db/DSR-699_cles_calculees.sql
```

## Exécution

### Au client `mysql`

Le nom de base des exemples est à adapter : le dump de schéma livré ici
(`database.sql`) porte `dsr_trppu_aa`, les exemples des scripts `dsr_mercure_aa`. Les deux
noms circulent dans le projet — vérifier celui de l'environnement visé avant de jouer quoi
que ce soit.

```bash
mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-697_chargement_cles_repartition.sql
mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-696-699_migration.sql
mysql -h <hote> -u <user> -p dsr_mercure_aa < db/fix_error.sql
mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-696_site_trafic.sql
mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-698_version_cle.sql
mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-699_cles_calculees.sql
```

### Par le socle

```python
from app.db.mysql import db_write

# Le chargement, puis la migration : transactional=False OBLIGATOIRE sur elle, voir plus bas.
await db_write.execute_sql_file("db/DSR-697_chargement_cles_repartition.sql")
await db_write.execute_sql_file("db/DSR-696-699_migration.sql", transactional=False)
await db_write.execute_sql_file("db/fix_error.sql", transactional=False)

# Les trois autres scripts de données sont, eux aussi, entièrement annulables.
await db_write.execute_sql_file("db/DSR-696_site_trafic.sql")
```

Deux réserves sur DSR-697 joué par le socle. Le fichier du `LOAD DATA INFILE` est lu **par le
serveur** : son chemin n'a rien à voir avec la machine qui exécute le Python, et la variante
`LOCAL` demanderait un client configuré pour l'autoriser — ce que le socle ne fait pas. Et
le script entier tient dans une transaction, purge comprise : la connexion reste ouverte le
temps du chargement, et un timeout réseau annule tout. Un chargement de masse se joue au
client `mysql` ; le socle convient au rechargement d'un référentiel de taille modeste.

**Pourquoi `transactional=False` sur la migration.** Ses `ALTER` voyagent dans une chaîne
exécutée par `PREPARE`/`EXECUTE` — ce qui la rend rejouable, MySQL ne connaissant pas
`ADD INDEX IF NOT EXISTS`, mais invisible à la détection de DDL du socle (`is_ddl`,
`app/db/sql_script.py`). L'avertissement « DDL en mode transactionnel » ne se déclenchera donc
pas, alors que le commit implicite, lui, a bien lieu. Le comportement est verrouillé par
`tests/test_scripts_dsr.py`.

Les paramètres se règlent dans le fichier avant de le jouer. Le runner exécute une instruction
à la fois sur une connexion dédiée hors pool : les variables de session survivent d'une
instruction à l'autre, mais ne contaminent pas les requêtes applicatives.

## Suivre un traitement long

`suivi.sql` se joue dans un **second terminal**, pendant qu'un script travaille. Il ne modifie
rien — que des `SELECT` sur `information_schema` et `performance_schema` — et peut donc tourner
en boucle sans risque :

```bash
while true; do clear; mysql -h <hote> -u <user> -p<mdp> dsr_mercure_aa < db/suivi.sql; sleep 5; done
```

**Le compteur à regarder dépend du traitement.** Un `INSERT … SELECT`, un `LOAD DATA` ou un
`DELETE` n'est visible par les autres sessions qu'une fois **validé** : compter les lignes de
la table cible renvoie l'état d'avant jusqu'à la toute fin. Ce n'est pas un blocage.

| Traitement | Ce qui avance en direct |
|------------|-------------------------|
| `INSERT INTO trppu_trafic_site` (DSR-696) | `lignes_lues` du bloc 1 — l'instruction balaie 24 M de lignes pour n'en écrire que quelques milliers, tout est dans le balayage |
| `LOAD DATA` (DSR-697) | `lignes_modifiees` du bloc 2 (`trx_rows_modified`), ligne à ligne |
| `DELETE` de purge (DSR-697, DSR-696) | `lignes_modifiees` du bloc 2, également |
| `ALTER` (`fix_error.sql`, migration) | `etat` du bloc 1 (« copy to tmp table ») ; le bloc 6 chiffre la progression si l'instrumentation est activée |

Renseigner `@lignes_attendues` en tête de fichier donne en plus un pourcentage et une
estimation du temps restant. La valeur n'a pas besoin d'être exacte : le `nb_lignes` du
contrôle 1 de DSR-697, ou le `lignes_estimees` du bloc 4, suffisent.

Le script demande le privilège `PROCESS` et le droit de lire `performance_schema`. Sans
`PROCESS`, il ne montre que vos propres sessions — donc rien d'utile depuis un second
terminal.

## Lire les sorties

Chaque script se termine par les contrôles de ses critères d'acceptation. Il n'y a rien à
écrire de plus : ce sont eux qui font foi en recette.

| Script | Contrôle | Attendu |
|--------|----------|---------|
| DSR-697 | garde-fou *(avant chargement)* | `repertoire_autorise` renseigné, `referentiel_declare` = 1 |
| | volumétrie (CA1+CA3) | `nb_lignes` = lignes du fichier dédoublonné, en-tête déduit, et `nb_pdi_distincts` identique |
| | doublons de PDI (CA3, RG5) | 0 ligne |
| | lignes non actives (CA5+CA6) | 0 ligne |
| | champs vides (CA4) | `nb_chaines_vides` = 0 |
| | débordement décimal | `verdict = OK` — **à lire avant DSR-696** |
| | historisation (CA7) | un jeu par référentiel, les autres intacts |
| migration | liste des index posés | 4 index, dont `idx_regate_actif` déjà fourni par la base |
| `fix_error.sql` | définition des colonnes *(avant)* | `chiffres_avant_virgule` = 6 — la cause de l'`ERROR 1264` |
| | dix plus gros sites | `chiffres_*` ≤ 16 ; au-delà, question métier avant d'aller plus loin |
| | définition des colonnes *(après)* | `verdict = OK` : 16 chiffres entiers, 19 décimales |
| DSR-696 | écarts site par site (CA1+CA3) | toutes les colonnes `ecart_*` à 0 |
| | doublons site + référentiel (CA2) | 0 ligne |
| | sites chargés sans PDI actif (CA4) | 0 ligne |
| | récapitulatif par référentiel (CA5) | un jeu par référentiel, les autres intacts |
| DSR-698 | état de la version (CA1+CA2) | la version demandée, `actif = 'O'` |
| | verdict (CA3) | `OK` — une seule version active, portant le référentiel demandé |
| DSR-699 | état du périmètre *(avant calcul)* | `nb_sites_sans_agregat` et `nb_sites_sans_version` à 0 |
| | dénominateurs nuls *(avant calcul)* | 0 ligne |
| | PDI actifs sans clé (CA1) | 0 ligne |
| | clés sans version active (CA2) | 0 ligne |
| | sommes par site (CA3) | `verdict = OK` partout — les lignes en `ANOMALIE` sont triées en tête |
| | doublons version + PDI (CA4) | 0 ligne |

DSR-699 demande « une alerte dans les logs » en cas de somme hors tolérance. Le SQL ne sait pas
journaliser : c'est l'appelant — socle ou exploitant — qui remonte les lignes en `ANOMALIE`,
avec le site et la famille de clés concernés.

## Rejouabilité

Tous se relancent sans dommage, mais pas de la même façon :

| Script | Relancé, il… |
|--------|--------------|
| DSR-697 | **recharge** — `DELETE` ciblé puis `LOAD DATA`, le référentiel revient au même état |
| migration | ne recrée rien : chaque objet est précédé d'un test de présence |
| `fix_error.sql` | ne reconstruit rien : le garde-fou teste la largeur courante des colonnes |
| DSR-696 | **recalcule** — `DELETE` ciblé puis `INSERT`, la table revient au même état |
| DSR-698 | **ne fait rien** si le site a déjà une version active sur ce référentiel |
| DSR-699 | **ne fait rien** si une version du périmètre porte déjà des clés (CA4) |

Conséquence pour DSR-697 : recharger un référentiel **périme tout ce qui en découle**. Les
agrégats DSR-696 du même référentiel restent en base avec leurs anciennes valeurs — son
garde-fou les compte, sous `agregats_dsr696_a_recalculer` — et les clés DSR-699 déjà
calculées, elles, ne se recalculent pas (CA4). Rechargement ⇒ rejouer DSR-696, puis créer une
nouvelle version DSR-698 et calculer ses clés.

Autre conséquence, purement technique : la purge de DSR-697 est un `DELETE` de 22 M de lignes,
dont le journal d'annulation est à l'avenant. Quand il devient problématique — espace de
`undo`, durée, verrous — les deux issues sont de supprimer par tranches (`DELETE … LIMIT`
répété, hors transaction unique) ou, si et seulement si la table ne porte **qu'un seul**
référentiel, de la vider par `TRUNCATE`. Ce dernier est du DDL : commit implicite, aucun
retour arrière, `AUTO_INCREMENT` remis à zéro — à jouer à la main, en connaissance de cause,
jamais depuis le script.

Conséquence pour DSR-699 : une seule version déjà calculée neutralise tout le référentiel, y
compris les sites qui n'ont pas encore leurs clés — pour ne pas laisser un référentiel à
moitié calculé sans que rien ne le signale. Pour traiter les sites restants, relancer site par
site avec `@co_regate`.

Recalculer les clés d'un site n'est **pas** prévu : le CA4 interdit de toucher aux clés d'une
version existante. La démarche est de créer une nouvelle version (DSR-698 avec un nouveau
référentiel), qui désactive la précédente, puis de calculer ses clés.

## Erreurs typiques

| Erreur | Cause | Quoi faire |
|--------|-------|-----------|
| `1290 --secure-file-priv` | le fichier n'est pas dans le répertoire autorisé | le déplacer là où pointe `@@secure_file_priv`, que le garde-fou affiche |
| `13 Can't get stat / Errcode 2` | chemin faux, ou fichier illisible par le compte système `mysql` | vérifier le chemin **sur le serveur**, et les droits du fichier |
| `1148 command is not allowed` | variante `LOCAL` sans `local_infile` | l'activer des deux côtés, ou repasser par un dépôt sur le serveur |
| `1062 Duplicate entry` sur `uk_pdi_ref` | le fichier porte deux fois le même PDI | le `DISTINCT` ne suffit pas : jouer le contrôle d'unicité PDI **avant** de charger |
| `1265 Data truncated` sur `potentielip` | fichier en fins de ligne Windows | `LINES TERMINATED BY '\r\n'` dans le `LOAD DATA` |
| `1054 Unknown column 'COUNT'` | contrôle recopié du ticket DSR-697 (`SELECT COUNT FROM …`) | `COUNT(*)`, comme dans le script |
| `1054 Unknown column 'id_site'` | requête recopiée depuis DSR-696 | la colonne du site est `co_regate_site` ; `id_site_trafic` est la PK |
| `1146 Table 'trppu_site_trafic' doesn't exist` | ancien nom de table | `trppu_trafic_site` depuis le 17/08/2026 |
| `1062 Duplicate entry` sur `uq_crc_version_pdi` | clés d'une version rechargées | c'est le CA4 qui se défend : créer une nouvelle version |
| `1062 Duplicate entry` sur `uq_site_trafic` | deux exécutions concurrentes de DSR-696 | rejouer seul, la séquence `DELETE`/`INSERT` n'est pas concurrente |
| `1264 Out of range value` sur `trafic_*_total` | colonnes `decimal(24,18)` : six chiffres entiers | jouer `db/fix_error.sql`, puis relancer DSR-696 |
| `1365 Division by 0` | un site dont un total de trafic est à zéro | le 2ᵉ garde-fou de DSR-699 le désignait ; question métier avant d'aller plus loin |
| `1093 You can't specify target table` | `INSERT … WHERE NOT EXISTS (SELECT … FROM cible)` | déporter le test sur une variable de session, comme `@deja` |

## À vérifier avant le premier chargement réel

**Débordement décimal (DSR-696) — corrigé, mais à surveiller.** Les trafics sources sont en
`decimal(25,19)` et les totaux cibles étaient en `decimal(24,18)`, soit six chiffres avant la
virgule. Or on y écrit la somme de valeurs qui peuvent elles-mêmes atteindre ce plafond :
l'`INSERT` de DSR-696 a échoué en `ERROR 1264` au premier chargement réel. `fix_error.sql`
porte les trois colonnes à `decimal(35,19)`, ce qui règle la question du stockage.

Reste à surveiller la question de fond, que le correctif ne tranche pas. Le contrôle est
intégré à DSR-697 — son avant-dernier résultat, celui qui rend un `verdict` :

```sql
SELECT MAX(t) FROM (
  SELECT SUM(trafic_colis) t FROM trppu_cles_repartition
   WHERE id_referentiel = 1 AND date_fin_validite IS NULL GROUP BY co_regate_site) x;
```

Des totaux à douze ou quinze chiffres entiers tiendront désormais en base, mais poseront une
question à l'équipe data : des colonnes à dix-neuf décimales portent-elles des volumes, ou
déjà des ratios chargés comme des volumes ? Le deuxième constat de `fix_error.sql` affiche,
pour les dix plus gros sites, le nombre de chiffres entiers réellement atteint.

**Précision de la clé potentiel IP (DSR-699).** Après le premier calcul, vérifier que le
`CAST` a bien tenu — sans lui, la division entière ne rendrait que quatre décimales :

```sql
SELECT COUNT(*) FROM trppu_cles_repartition_calcule
 WHERE cle_potentielip <> ROUND(cle_potentielip, 4);
```

Le résultat doit être supérieur à 0 sur un jeu de données non trivial.

## Tests

Les scripts sont couverts sans base ni réseau par `tests/test_scripts_dsr.py` : découpage,
ordre des instructions, et surtout confrontation des colonnes insérées à un extrait du schéma
réel recopié dans le test. Pour DSR-697, le test lit la liste de colonnes du `LOAD DATA` et
son bloc `SET` : il vérifie que la purge précède le chargement, que les quatre conversions en
NULL (RG3) sont là, et qu'aucun `IGNORE` ni `REPLACE` ne vient masquer un doublon de PDI.

```bash
python -m pytest tests/test_scripts_dsr.py -q
```

À resynchroniser (`SCHEMA_REFERENCE` dans le test) à chaque évolution du schéma.
