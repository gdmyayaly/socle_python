# Scripts SQL — chaîne « clés de répartition des PDI »

Quatre scripts, à jouer **dans l'ordre**, qui initialisent les clés de répartition des PDI à
partir du référentiel source. Écrits en SQL pur : aucun code applicatif du socle ne les
appelle, ils se jouent au client `mysql` ou via le runner de scripts de `Database`.

Les tickets sources sont dans `../docs/` ; aucun ne décrit exactement la base —
`../docs/DIAGNOSTIC-DSR-696-699.md` recense écart par écart la formulation fautive, la lecture
retenue et la correction appliquée.

## La chaîne

```
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

## Ordre d'exécution

| # | Fichier | Ce qu'il exige en amont | Ce qu'il laisse |
|---|---------|-------------------------|-----------------|
| 1 | `DSR-696-699_migration.sql` | rien — **à jouer une seule fois par base** | 3 index, 1 colonne |
| 2 | `DSR-696_site_trafic.sql` | la migration | `trppu_trafic_site` alimentée pour le référentiel |
| 3 | `DSR-698_version_cle.sql` | la migration | une version active par site traité |
| 4 | `DSR-699_cles_calculees.sql` | les étapes 2 et 3, **sur le même référentiel** | les clés du référentiel |

L'ordre 2 → 3 → 4 n'est pas négociable : DSR-699 joint les trois tables et écarte
silencieusement les sites auxquels il manque un agrégat ou une version. Ses deux premiers
garde-fous comptent précisément ces sites-là, avant d'écrire quoi que ce soit.

## Paramètres

Chaque script se règle **en tête de fichier**, dans des variables de session — le fichier
entier tournant sur une connexion unique, elles restent visibles par toutes ses instructions.

| Script | Paramètres |
|--------|-----------|
| migration | aucun |
| DSR-696 | `@id_referentiel` obligatoire, `@co_regate` (NULL = tout le référentiel) |
| DSR-698 | `@id_referentiel`, `@co_regate`, `@commentaire`, `@libelle` — les deux premiers obligatoires |
| DSR-699 | `@id_referentiel` obligatoire, `@co_regate` (NULL = tout le référentiel) |

### Exemple — initialiser un site

```sql
-- 1. une seule fois par base
--    mysql … < db/DSR-696-699_migration.sql

-- 2. agrégats du site
SET @id_referentiel := 7;
SET @co_regate      := '123456';
--    mysql … < db/DSR-696_site_trafic.sql

-- 3. version de clés du site
SET @id_referentiel := 7;
SET @co_regate      := '123456';
SET @commentaire    := 'Réorganisation DEX';
SET @libelle        := NULL;
--    mysql … < db/DSR-698_version_cle.sql

-- 4. calcul des clés
SET @id_referentiel := 7;
SET @co_regate      := '123456';
--    mysql … < db/DSR-699_cles_calculees.sql
```

## Exécution

### Au client `mysql`

```bash
mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-696-699_migration.sql
mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-696_site_trafic.sql
mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-698_version_cle.sql
mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-699_cles_calculees.sql
```

### Par le socle

```python
from app.db.mysql import db_write

# La migration : transactional=False OBLIGATOIRE, voir plus bas.
await db_write.execute_sql_file("db/DSR-696-699_migration.sql", transactional=False)

# Les trois scripts de données sont, eux, entièrement annulables.
await db_write.execute_sql_file("db/DSR-696_site_trafic.sql")
```

**Pourquoi `transactional=False` sur la migration.** Ses `ALTER` voyagent dans une chaîne
exécutée par `PREPARE`/`EXECUTE` — ce qui la rend rejouable, MySQL ne connaissant pas
`ADD INDEX IF NOT EXISTS`, mais invisible à la détection de DDL du socle (`is_ddl`,
`app/db/sql_script.py`). L'avertissement « DDL en mode transactionnel » ne se déclenchera donc
pas, alors que le commit implicite, lui, a bien lieu. Le comportement est verrouillé par
`tests/test_scripts_dsr.py`.

Les paramètres se règlent dans le fichier avant de le jouer. Le runner exécute une instruction
à la fois sur une connexion dédiée hors pool : les variables de session survivent d'une
instruction à l'autre, mais ne contaminent pas les requêtes applicatives.

## Lire les sorties

Chaque script se termine par les contrôles de ses critères d'acceptation. Il n'y a rien à
écrire de plus : ce sont eux qui font foi en recette.

| Script | Contrôle | Attendu |
|--------|----------|---------|
| migration | liste des index posés | 4 index, dont `idx_regate_actif` déjà fourni par la base |
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

Les quatre scripts se relancent sans dommage, mais pas de la même façon :

| Script | Relancé, il… |
|--------|--------------|
| migration | ne recrée rien : chaque objet est précédé d'un test de présence |
| DSR-696 | **recalcule** — `DELETE` ciblé puis `INSERT`, la table revient au même état |
| DSR-698 | **ne fait rien** si le site a déjà une version active sur ce référentiel |
| DSR-699 | **ne fait rien** si une version du périmètre porte déjà des clés (CA4) |

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
| `1054 Unknown column 'id_site'` | requête recopiée depuis DSR-696 | la colonne du site est `co_regate_site` ; `id_site_trafic` est la PK |
| `1146 Table 'trppu_site_trafic' doesn't exist` | ancien nom de table | `trppu_trafic_site` depuis le 17/08/2026 |
| `1062 Duplicate entry` sur `uq_crc_version_pdi` | clés d'une version rechargées | c'est le CA4 qui se défend : créer une nouvelle version |
| `1062 Duplicate entry` sur `uq_site_trafic` | deux exécutions concurrentes de DSR-696 | rejouer seul, la séquence `DELETE`/`INSERT` n'est pas concurrente |
| `1264 Out of range value` sur `trafic_*_total` | débordement décimal (voir ci-dessous) | élargir les colonnes avant de charger |
| `1365 Division by 0` | un site dont un total de trafic est à zéro | le 2ᵉ garde-fou de DSR-699 le désignait ; question métier avant d'aller plus loin |
| `1093 You can't specify target table` | `INSERT … WHERE NOT EXISTS (SELECT … FROM cible)` | déporter le test sur une variable de session, comme `@deja` |

## À vérifier avant le premier chargement réel

**Débordement décimal (DSR-696).** Les trafics sources sont en `decimal(25,19)` et les totaux
cibles en `decimal(24,18)`, soit six chiffres avant la virgule. Or on y écrit la somme de
valeurs qui peuvent elles-mêmes atteindre ce plafond :

```sql
SELECT MAX(t) FROM (
  SELECT SUM(trafic_colis) t FROM trppu_cles_repartition
   WHERE id_referentiel = 1 AND date_fin_validite IS NULL GROUP BY co_regate_site) x;
```

Si le maximum approche `999999`, élargir les trois colonnes `trafic_*_total` **avant** de
charger quoi que ce soit.

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
réel recopié dans le test.

```bash
python -m pytest tests/test_scripts_dsr.py -q
```

À resynchroniser (`SCHEMA_REFERENCE` dans le test) à chaque évolution du schéma.
