-- =====================================================================================
-- DSR-696 / DSR-698 — contraintes et index manquants
-- =====================================================================================
-- À jouer UNE FOIS, avant `DSR-696_site_trafic.sql` et `DSR-698_version_cle.sql`.
--
-- Ces trois objets n'existent pas dans le schéma livré (`python/db/db_new.sql`) alors que
-- les critères d'acceptation et les volumes en jeu les exigent. Chacun est motivé
-- ci-dessous.
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
--   mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-696-698_migration.sql
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- 1. trppu_site_trafic — unicité (référentiel, site)
-- -------------------------------------------------------------------------------------
-- DSR-696 CA2 : « une seule ligne existe par site + id_referentiel ». La table ne porte que
-- sa PK auto-incrémentée : rien n'empêche aujourd'hui les doublons. Sans cette contrainte,
-- l'invariant ne tient que par la séquence DELETE puis INSERT du script — deux exécutions
-- concurrentes le violeraient silencieusement. Avec elle, la seconde échoue proprement.
SET @sql := IF(
    (SELECT COUNT(*) FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'trppu_site_trafic'
        AND INDEX_NAME = 'uq_site_trafic') > 0,
    'SELECT ''uq_site_trafic : déjà présent'' AS resultat',
    'ALTER TABLE `trppu_site_trafic`
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
-- 3. trppu_version_cle — index (site, actif)
-- -------------------------------------------------------------------------------------
-- La table n'a qu'un index sur `id_referentiel`, alors que les deux accès réels portent sur
-- le couple (co_regate, actif) : le UPDATE de désactivation de `DSR-698_version_cle.sql`, et
-- la lecture d'éligibilité de DSR-701 règle 9
-- (`SELECT id_version_cle … WHERE co_regate = :coRegate AND actif = 'O'`).
-- La table est vide aujourd'hui : l'index est posé maintenant, tant que c'est gratuit.
SET @sql := IF(
    (SELECT COUNT(*) FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'trppu_version_cle'
        AND INDEX_NAME = 'idx_vc_site_actif') > 0,
    'SELECT ''idx_vc_site_actif : déjà présent'' AS resultat',
    'ALTER TABLE `trppu_version_cle`
       ADD KEY `idx_vc_site_actif` (`co_regate`, `actif`)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


-- -------------------------------------------------------------------------------------
-- Contrôle final — les trois index doivent être listés
-- -------------------------------------------------------------------------------------
SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME
  FROM information_schema.STATISTICS
 WHERE TABLE_SCHEMA = DATABASE()
   AND INDEX_NAME IN ('uq_site_trafic', 'idx_cr_ref_actif', 'idx_vc_site_actif')
 ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX;
