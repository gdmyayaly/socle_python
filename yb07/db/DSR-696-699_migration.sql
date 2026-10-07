-- =====================================================================================
-- DSR-696 / DSR-698 / DSR-699 — contraintes, index et colonne manquants
-- =====================================================================================
-- À jouer UNE FOIS, avant les scripts de données (cf. `db/README.md`).
--
-- ATTENTION — DDL : COMMIT IMPLICITE, aucun ROLLBACK. Les ALTER voyagent dans
-- PREPARE/EXECUTE (rejouabilité, MySQL n'a pas `ADD INDEX IF NOT EXISTS`) : `is_ddl` ne les
-- voit pas, donc jouer ce fichier avec `transactional=False`.
--
-- REJOUABLE : chaque ALTER est précédé d'un test de présence.
-- USAGE : mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-696-699_migration.sql
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- 1. trppu_trafic_site — unicité (référentiel, site)
-- -------------------------------------------------------------------------------------
-- DSR-696 CA2 : une ligne par site + référentiel, même en cas d'exécutions concurrentes.
-- Nom `uq_site_trafic` conservé : l'index a suivi la table lors de son renommage.
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
-- Colonnes dans l'ordre de la requête : égalité référentiel, `date_fin_validite IS NULL`
-- (RG1), puis `co_regate_site` (filtre site et GROUP BY).
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
-- Exigée par DSR-698 ; distincte de la période de validité. Alimentée par DEFAULT
-- CURRENT_TIMESTAMP : sur une table non vide, les lignes existantes prendraient la date de
-- l'ALTER.
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
-- DSR-699 CA4 : protège aussi des INSERT manuels et concurrents. Sert d'index au
-- `NOT EXISTS` de DSR-699 et à la lecture de DSR-702 (`WHERE id_version_cle = ?`).
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
-- Contrôle final — les trois index posés ici, plus `idx_regate_actif` fourni par la base
-- -------------------------------------------------------------------------------------
SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME
  FROM information_schema.STATISTICS
 WHERE TABLE_SCHEMA = DATABASE()
   AND INDEX_NAME IN ('uq_site_trafic', 'idx_cr_ref_actif', 'idx_regate_actif',
                      'uq_crc_version_pdi')
 ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX;
