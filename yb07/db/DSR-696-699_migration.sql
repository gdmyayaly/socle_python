-- =====================================================================================
-- DSR-696 / DSR-698 / DSR-699 — contraintes, index et colonne manquants
-- =====================================================================================
-- À jouer UNE FOIS, avant les trois scripts de données. Mode d'emploi complet de la chaîne :
-- `db/README.md`.
--
-- Ces quatre objets n'existent pas dans le schéma livré (`python/db/db_new.sql`) alors que
-- les critères d'acceptation et le contenu des tickets les exigent. Chacun est motivé
-- ci-dessous.
--
-- Un cinquième objet a figuré ici jusqu'à la ré-extraction du schéma du 17/08/2026 : l'index
-- `(co_regate, actif)` de `trppu_version_cle`, que la base porte désormais sous le nom
-- `idx_regate_actif`. Le bloc a été retiré parce que son garde-fou testait le NOM de
-- l'index et non ses colonnes : il ne voyait donc pas `idx_regate_actif` et aurait posé un
-- second index redondant sur le même couple.
--
-- ATTENTION — `ALTER TABLE` est du DDL : MySQL effectue un COMMIT IMPLICITE, aucun ROLLBACK
-- n'est possible. C'est la raison pour laquelle ce fichier est séparé des deux scripts de
-- données, qui restent eux entièrement annulables.
--
-- Le fichier est REJOUABLE : chaque ALTER est précédé d'un test de présence, MySQL ne
-- connaissant pas `ADD INDEX IF NOT EXISTS`.
--
-- PIÈGE À CONNAÎTRE — cette rejouabilité a un coût. L'ALTER voyage dans une chaîne exécutée
-- par PREPARE/EXECUTE : pour l'analyseur, les instructions de ce fichier sont des `SET`,
-- `PREPARE`, `EXECUTE` et `DEALLOCATE`, jamais des `ALTER`. La détection de DDL du socle
-- (`is_ddl`, app/db/sql_script.py) ne peut donc PAS les voir, et l'avertissement
-- « DDL en mode transactionnel » de `Database.execute_sql_files` ne se déclenchera pas —
-- alors que le commit implicite, lui, a bien lieu. Si ce fichier est joué par le socle,
-- passer explicitement `transactional=False`. Ce comportement est verrouillé par
-- `tests/test_scripts_dsr.py`.
--
-- USAGE
--   mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-696-699_migration.sql
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- 1. trppu_trafic_site — unicité (référentiel, site)
-- -------------------------------------------------------------------------------------
-- DSR-696 CA2 : « une seule ligne existe par site + id_referentiel ». La table ne porte que
-- sa PK auto-incrémentée : rien n'empêche aujourd'hui les doublons. Sans cette contrainte,
-- l'invariant ne tient que par la séquence DELETE puis INSERT du script — deux exécutions
-- concurrentes le violeraient silencieusement. Avec elle, la seconde échoue proprement.
--
-- Le nom `uq_site_trafic` est conservé alors que la table s'appelle désormais
-- `trppu_trafic_site` : si la migration a déjà été jouée avant le renommage, l'index a suivi
-- sa table sous ce nom, et c'est lui que le garde-fou ci-dessous doit reconnaître.
SET @sql := IF(
    (SELECT COUNT(*) FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'trppu_trafic_site'
        AND INDEX_NAME = 'uq_site_trafic') > 0,
    'SELECT ''uq_site_trafic : déjà présent'' AS resultat',
    'ALTER TABLE `trppu_trafic_site`
       ADD UNIQUE KEY `uq_site_trafic` (`id_referentiel`, `co_regate_site`)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


-- -------------------------------------------------------------------------------------
-- 2. trppu_cles_repartition — index de l'agrégation DSR-696
-- -------------------------------------------------------------------------------------
-- La table porte 22,4 M lignes et seulement `PRIMARY KEY (id)` + `UNIQUE (id_pdi,
-- id_referentiel)` : aucun index ne commence par `id_referentiel`, et aucun ne porte
-- `date_fin_validite`. La requête d'agrégation balaierait donc l'intégralité de la table à
-- chaque exécution — à l'opposé du bénéfice annoncé par le ticket (CA6 : « les sommes ne
-- sont calculées qu'une seule fois par référentiel […] réduire les temps de calcul »).
--
-- L'ordre des colonnes suit la requête : égalité sur `id_referentiel`, puis test
-- `date_fin_validite IS NULL` (RG1), puis `co_regate_site` qui porte à la fois le filtre
-- site optionnel et le GROUP BY — ce dernier est ainsi résolu par l'index, sans tri.
SET @sql := IF(
    (SELECT COUNT(*) FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'trppu_cles_repartition'
        AND INDEX_NAME = 'idx_cr_ref_actif') > 0,
    'SELECT ''idx_cr_ref_actif : déjà présent'' AS resultat',
    'ALTER TABLE `trppu_cles_repartition`
       ADD KEY `idx_cr_ref_actif` (`id_referentiel`, `date_fin_validite`, `co_regate_site`)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


-- -------------------------------------------------------------------------------------
-- 3. trppu_version_cle — colonne date_creation
-- -------------------------------------------------------------------------------------
-- DSR-698 « Résultat attendu » liste `date_creation` parmi les colonnes de la ligne créée,
-- avec l'exemple « 01/09/2026 ». La ré-extraction du schéma du 17/08/2026 l'a fait
-- disparaître au profit du couple `date_debut_validite` / `date_fin_validite`, qui exprime
-- la période de validité de la version — pas la date à laquelle elle a été créée. Les deux
-- informations ne se confondent pas : une version peut être créée aujourd'hui avec une
-- validité qui court depuis plus tôt, et sa fin de validité ne dit rien de sa création.
--
-- La colonne est donc rétablie, et alimentée SANS INTERVENTION du script : `DEFAULT
-- CURRENT_TIMESTAMP` la renseigne à l'insertion. Aucune reprise n'est nécessaire, la table
-- est vide. À vérifier avant de jouer ce bloc sur un environnement où elle ne le serait
-- pas : les lignes existantes prendraient l'horodatage de l'ALTER, pas leur vraie date de
-- création.
SET @sql := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'trppu_version_cle'
        AND COLUMN_NAME = 'date_creation') > 0,
    'SELECT ''trppu_version_cle.date_creation : déjà présente'' AS resultat',
    'ALTER TABLE `trppu_version_cle`
       ADD COLUMN `date_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
       AFTER `actif`'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


-- -------------------------------------------------------------------------------------
-- 4. trppu_cles_repartition_calcule — unicité (version, PDI)
-- -------------------------------------------------------------------------------------
-- DSR-699 CA4 : « aucune clé d'une version existante n'est modifiée ». La table cible du
-- calcul ne porte, elle aussi, que sa PK auto-incrémentée : rien n'empêche de charger deux
-- fois les clés d'une même version. Le garde-fou `@deja` de `DSR-699_cles_calculees.sql` y
-- pourvoit côté script, mais il ne protège pas d'un INSERT joué à la main ni de deux
-- exécutions concurrentes ; la clé unique, si.
--
-- Elle sert en même temps d'index de lecture pour DSR-702 (`WHERE id_version_cle = ?`) :
-- `id_version_cle` en est le premier membre, aucun index supplémentaire n'est nécessaire.
SET @sql := IF(
    (SELECT COUNT(*) FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'trppu_cles_repartition_calcule'
        AND INDEX_NAME = 'uq_crc_version_pdi') > 0,
    'SELECT ''uq_crc_version_pdi : déjà présent'' AS resultat',
    'ALTER TABLE `trppu_cles_repartition_calcule`
       ADD UNIQUE KEY `uq_crc_version_pdi` (`id_version_cle`, `id_pdi`)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


-- -------------------------------------------------------------------------------------
-- Contrôle final — les trois index posés ici, plus celui déjà fourni par la base
-- -------------------------------------------------------------------------------------
-- `idx_regate_actif` n'est pas créé par ce fichier : il est listé pour vérifier d'un coup
-- d'œil que DSR-698 dispose bien de son index d'accès (co_regate, actif).
--
-- La colonne `trppu_version_cle.date_creation` ajoutée par le bloc 3 n'apparaît pas ici,
-- puisqu'elle n'est pas un index : son bloc affiche son propre résultat.
SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME
  FROM information_schema.STATISTICS
 WHERE TABLE_SCHEMA = DATABASE()
   AND INDEX_NAME IN ('uq_site_trafic', 'idx_cr_ref_actif', 'idx_regate_actif',
                      'uq_crc_version_pdi')
 ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX;
