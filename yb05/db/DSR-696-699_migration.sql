-- =====================================================================================
-- DSR-696 / DSR-698 / DSR-699 — contraintes, index et colonne manquants
-- =====================================================================================
-- À jouer UNE FOIS, avant les scripts de données (mode d'emploi : `db/README.md`).
-- ATTENTION — `ALTER TABLE` = COMMIT IMPLICITE, pas de ROLLBACK : d'où ce fichier séparé.
-- REJOUABLE : chaque ALTER est précédé d'un test de présence (pas d'`ADD INDEX IF NOT EXISTS`).
-- PIÈGE — l'ALTER passe par PREPARE/EXECUTE : `is_ddl` (app/db/sql_script.py) ne le voit
-- pas et n'avertit pas. Joué par le socle, passer `transactional=False` (verrouillé par
-- `tests/test_scripts_dsr.py`).
-- Usage : mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-696-699_migration.sql
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- 1. trppu_trafic_site — unicité (référentiel, site)
-- -------------------------------------------------------------------------------------
-- DSR-696 CA2 : une ligne par site + référentiel, garantie même en cas d'exécutions
-- concurrentes. Nom `uq_site_trafic` conservé : l'index a suivi le renommage de la table.
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
-- Sans lui, l'agrégation balaie les 22,4 M lignes (DSR-696 CA6). Ordre des colonnes :
-- égalité référentiel, `date_fin_validite IS NULL` (RG1), puis site (filtre + GROUP BY sans tri).
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
-- DSR-698 « Résultat attendu » : date d'écriture de la ligne, distincte de la période de
-- validité. Alimentée par DEFAULT CURRENT_TIMESTAMP ; sur une table non vide, les lignes
-- existantes prendraient l'horodatage de l'ALTER.
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
-- DSR-699 CA4 : complète `@deja` de `DSR-699_cles_calculees.sql` (INSERT manuel, exécutions
-- concurrentes). Sert aussi d'index de lecture DSR-702 (`WHERE id_version_cle = ?`).
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
