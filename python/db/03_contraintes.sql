-- =============================================================================
--  03_contraintes.sql  --  Cles uniques, index, cles etrangeres, contraintes
-- =============================================================================
--  Base : dsr_mercure_aa            Genere depuis db/db_new.sql
--
--  Contenu : tout ce qui n'est pas dans 01_structure.sql, c'est-a-dire les
--  cles uniques, les index secondaires, les cles etrangeres et les contraintes
--  CHECK. Les cles primaires sont dans 01_structure.sql (contrainte MySQL sur
--  AUTO_INCREMENT, voir l'en-tete de ce fichier).
--
--  Rejouable : MySQL ne connait pas ADD INDEX IF NOT EXISTS. Deux procedures
--  utilitaires interrogent information_schema avant chaque ajout et ignorent ce
--  qui existe deja. Elles sont supprimees en fin de script.
--
--  Ordre des sections, impose par les dependances :
--    1. index et cles uniques   -- une FK a besoin d'un index sur ses colonnes
--    2. cles etrangeres         -- MySQL en cree un d'office s'il manque
--    3. contraintes CHECK       -- validees sur les donnees deja presentes
--
--  Les sections 2 et 3 sont validees contre les donnees existantes : si
--  02_donnees.sql a charge des lignes incoherentes, l'erreur sort ici.
--
--  Ordre d'execution : 01_structure -> 02_donnees -> 03_contraintes
-- =============================================================================

USE `dsr_mercure_aa`;

DELIMITER $$

DROP PROCEDURE IF EXISTS `_trppu_add_index_if_missing`$$
-- Couvre KEY et UNIQUE KEY : les deux apparaissent dans information_schema.STATISTICS.
CREATE PROCEDURE `_trppu_add_index_if_missing`(
  IN p_table VARCHAR(64), IN p_index VARCHAR(64), IN p_ddl TEXT)
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = p_table
      AND INDEX_NAME = p_index
  ) THEN
    SET @sql = CONCAT('ALTER TABLE `', p_table, '` ', p_ddl);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
  END IF;
END$$

DROP PROCEDURE IF EXISTS `_trppu_add_constraint_if_missing`$$
-- Couvre FOREIGN KEY et CHECK : TABLE_CONSTRAINTS liste les deux
-- (CHECK depuis MySQL 8.0.16).
CREATE PROCEDURE `_trppu_add_constraint_if_missing`(
  IN p_table VARCHAR(64), IN p_name VARCHAR(64), IN p_ddl TEXT)
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.TABLE_CONSTRAINTS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = p_table
      AND CONSTRAINT_NAME = p_name
  ) THEN
    SET @sql = CONCAT('ALTER TABLE `', p_table, '` ', p_ddl);
    PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
  END IF;
END$$

DELIMITER ;


-- --- 1. Cles uniques et index secondaires ---

-- trppu_agrebal_pdi
CALL `_trppu_add_index_if_missing`('trppu_agrebal_pdi', 'uq_agrpdi_courant',
  'ADD UNIQUE KEY `uq_agrpdi_courant` (`agrebal_id`,`agrebal_code_regate`)');
CALL `_trppu_add_index_if_missing`('trppu_agrebal_pdi', 'idx_agrpdi_site',
  'ADD KEY `idx_agrpdi_site` (`agrebal_code_regate`)');
CALL `_trppu_add_index_if_missing`('trppu_agrebal_pdi', 'idx_updated_at',
  'ADD KEY `idx_updated_at` (`agrebal_updatedAt`)');
CALL `_trppu_add_index_if_missing`('trppu_agrebal_pdi', 'idx_pdi_ids',
  'ADD KEY `idx_pdi_ids` ((cast(`agrebal_pdi_ids` as unsigned array)))');

-- trppu_api_log
CALL `_trppu_add_index_if_missing`('trppu_api_log', 'id_scenario',
  'ADD KEY `id_scenario` (`id_scenario`)');
CALL `_trppu_add_index_if_missing`('trppu_api_log', 'idx_api_when',
  'ADD KEY `idx_api_when` (`api_name`,`dt_appel`)');

-- trppu_cles_repartition
CALL `_trppu_add_index_if_missing`('trppu_cles_repartition', 'uk_pdi_ref',
  'ADD UNIQUE KEY `uk_pdi_ref` (`id_pdi`,`id_referentiel`)');
CALL `_trppu_add_index_if_missing`('trppu_cles_repartition', 'idx_cr_ref_actif',
  'ADD KEY `idx_cr_ref_actif` (`id_referentiel`,`date_fin_validite`,`co_regate_site`)');

-- trppu_cles_repartition_calcule
CALL `_trppu_add_index_if_missing`('trppu_cles_repartition_calcule', 'uq_crc_version_pdi',
  'ADD UNIQUE KEY `uq_crc_version_pdi` (`id_version_cle`,`id_pdi`)');

-- trppu_neutralisations
CALL `_trppu_add_index_if_missing`('trppu_neutralisations', 'uq_neutre',
  'ADD UNIQUE KEY `uq_neutre` (`id_scenario`,`dt_debut`,`dt_fin`)');

-- trppu_pic_coefficients
CALL `_trppu_add_index_if_missing`('trppu_pic_coefficients', 'uq_picc',
  'ADD UNIQUE KEY `uq_picc` (`id_pic_version`,`co_produit`,`jour_semaine`,`densite`)');
CALL `_trppu_add_index_if_missing`('trppu_pic_coefficients', 'idx_picc_produit',
  'ADD KEY `idx_picc_produit` (`co_produit`)');

-- trppu_pic_version
CALL `_trppu_add_index_if_missing`('trppu_pic_version', 'idx_picv_site',
  'ADD KEY `idx_picv_site` (`co_regate`)');
CALL `_trppu_add_index_if_missing`('trppu_pic_version', 'idx_picv_defaut',
  'ADD KEY `idx_picv_defaut` (`co_regate`,`est_par_defaut`)');

-- trppu_recalcul_log
CALL `_trppu_add_index_if_missing`('trppu_recalcul_log', 'idx_log_scenario',
  'ADD KEY `idx_log_scenario` (`id_scenario`,`dt_recalcul`)');

-- trppu_scenario
CALL `_trppu_add_index_if_missing`('trppu_scenario', 'idx_scenario_site_statut',
  'ADD KEY `idx_scenario_site_statut` (`co_regate`,`statut`)');

-- trppu_scenario_comptages_manuels
CALL `_trppu_add_index_if_missing`('trppu_scenario_comptages_manuels', 'idx_scm',
  'ADD KEY `idx_scm` (`id_scenario`,`dt_comptage`,`co_produit`)');

-- trppu_scenario_exclusions
CALL `_trppu_add_index_if_missing`('trppu_scenario_exclusions', 'uq_exclusion',
  'ADD UNIQUE KEY `uq_exclusion` (`id_scenario`,`co_produit`)');

-- trppu_scenario_pic_coeffs
CALL `_trppu_add_index_if_missing`('trppu_scenario_pic_coeffs', 'uq_tspc',
  'ADD UNIQUE KEY `uq_tspc` (`id_scenario`,`co_produit`,`jour_semaine`)');

-- trppu_scenario_variations_prev
CALL `_trppu_add_index_if_missing`('trppu_scenario_variations_prev', 'uq_var_scen_prod',
  'ADD UNIQUE KEY `uq_var_scen_prod` (`id_scenario`,`co_produit`)');
CALL `_trppu_add_index_if_missing`('trppu_scenario_variations_prev', 'idx_var_produit',
  'ADD KEY `idx_var_produit` (`co_produit`)');

-- trppu_site
CALL `_trppu_add_index_if_missing`('trppu_site', 'idx_site_roc',
  'ADD KEY `idx_site_roc` (`co_roc`)');

-- trppu_suivi_batch
CALL `_trppu_add_index_if_missing`('trppu_suivi_batch', 'idx_batch',
  'ADD KEY `idx_batch` (`lb_batch`,`dt_debut`)');

-- trppu_tmh
CALL `_trppu_add_index_if_missing`('trppu_tmh', 'uq_tmh',
  'ADD UNIQUE KEY `uq_tmh` (`id_tmh`,`id_scenario`,`co_produit`)');
CALL `_trppu_add_index_if_missing`('trppu_tmh', 'fk_tmh_scen',
  'ADD KEY `fk_tmh_scen` (`id_scenario`)');
CALL `_trppu_add_index_if_missing`('trppu_tmh', 'fk_tmh_produit',
  'ADD KEY `fk_tmh_produit` (`co_produit`)');

-- trppu_trafic_agrebal
CALL `_trppu_add_index_if_missing`('trppu_trafic_agrebal', 'idx_scen_site_agrebal',
  'ADD KEY `idx_scen_site_agrebal` (`id_scenario`,`co_regate`,`id_agrebal`)');
CALL `_trppu_add_index_if_missing`('trppu_trafic_agrebal', 'idx_agrebal_prod',
  'ADD KEY `idx_agrebal_prod` (`id_agrebal`,`co_produit`)');

-- trppu_trafic_pdi
CALL `_trppu_add_index_if_missing`('trppu_trafic_pdi', 'idx_tp_scen_agr_prod_jour',
  'ADD KEY `idx_tp_scen_agr_prod_jour` (`id_scenario`,`id_agrebal`,`co_produit`,`jour_semaine`)');
CALL `_trppu_add_index_if_missing`('trppu_trafic_pdi', 'idx_tp_pdi_agr',
  'ADD KEY `idx_tp_pdi_agr` (`id_pdi`,`id_agrebal`)');
CALL `_trppu_add_index_if_missing`('trppu_trafic_pdi', 'idx_tp_site',
  'ADD KEY `idx_tp_site` (`co_regate`)');

-- trppu_trafic_site
CALL `_trppu_add_index_if_missing`('trppu_trafic_site', 'uq_site_trafic',
  'ADD UNIQUE KEY `uq_site_trafic` (`id_referentiel`,`co_regate_site`)');

-- trppu_version_cle
CALL `_trppu_add_index_if_missing`('trppu_version_cle', 'idx_ref',
  'ADD KEY `idx_ref` (`id_referentiel`)');
CALL `_trppu_add_index_if_missing`('trppu_version_cle', 'idx_regate_actif',
  'ADD KEY `idx_regate_actif` (`co_regate`,`actif`)');


-- --- 2. Cles etrangeres ---

-- trppu_api_log
CALL `_trppu_add_constraint_if_missing`('trppu_api_log', 'trppu_api_log_ibfk_1',
  'ADD CONSTRAINT `trppu_api_log_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)');

-- trppu_neutralisations
CALL `_trppu_add_constraint_if_missing`('trppu_neutralisations', 'fk_neutre_scen',
  'ADD CONSTRAINT `fk_neutre_scen` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`) ON DELETE CASCADE');

-- trppu_pic_coefficients
CALL `_trppu_add_constraint_if_missing`('trppu_pic_coefficients', 'fk_picc_produit',
  'ADD CONSTRAINT `fk_picc_produit` FOREIGN KEY (`co_produit`) REFERENCES `trppu_produit` (`co_produit`) ON DELETE RESTRICT');
CALL `_trppu_add_constraint_if_missing`('trppu_pic_coefficients', 'fk_picc_version',
  'ADD CONSTRAINT `fk_picc_version` FOREIGN KEY (`id_pic_version`) REFERENCES `trppu_pic_version` (`id_pic_version`) ON DELETE CASCADE');

-- trppu_recalcul_log
CALL `_trppu_add_constraint_if_missing`('trppu_recalcul_log', 'trppu_recalcul_log_ibfk_1',
  'ADD CONSTRAINT `trppu_recalcul_log_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)');

-- trppu_scenario_comptages_manuels
CALL `_trppu_add_constraint_if_missing`('trppu_scenario_comptages_manuels', 'trppu_scenario_comptages_manuels_ibfk_1',
  'ADD CONSTRAINT `trppu_scenario_comptages_manuels_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)');

-- trppu_scenario_exclusions
CALL `_trppu_add_constraint_if_missing`('trppu_scenario_exclusions', 'trppu_scenario_exclusions_ibfk_1',
  'ADD CONSTRAINT `trppu_scenario_exclusions_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)');

-- trppu_scenario_pic_coeffs
CALL `_trppu_add_constraint_if_missing`('trppu_scenario_pic_coeffs', 'trppu_scenario_pic_coeffs_ibfk_1',
  'ADD CONSTRAINT `trppu_scenario_pic_coeffs_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)');

-- trppu_scenario_variations_prev
CALL `_trppu_add_constraint_if_missing`('trppu_scenario_variations_prev', 'fk_var_produit',
  'ADD CONSTRAINT `fk_var_produit` FOREIGN KEY (`co_produit`) REFERENCES `trppu_produit` (`co_produit`) ON DELETE RESTRICT');
CALL `_trppu_add_constraint_if_missing`('trppu_scenario_variations_prev', 'fk_var_scen',
  'ADD CONSTRAINT `fk_var_scen` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`) ON DELETE CASCADE');

-- trppu_tmh
CALL `_trppu_add_constraint_if_missing`('trppu_tmh', 'fk_tmh_produit',
  'ADD CONSTRAINT `fk_tmh_produit` FOREIGN KEY (`co_produit`) REFERENCES `trppu_produit` (`co_produit`) ON DELETE RESTRICT');
CALL `_trppu_add_constraint_if_missing`('trppu_tmh', 'fk_tmh_scen',
  'ADD CONSTRAINT `fk_tmh_scen` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`) ON DELETE CASCADE');

-- trppu_trafic_agrebal
CALL `_trppu_add_constraint_if_missing`('trppu_trafic_agrebal', 'trppu_trafic_agrebal_ibfk_1',
  'ADD CONSTRAINT `trppu_trafic_agrebal_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)');


-- --- 3. Contraintes CHECK ---

-- trppu_neutralisations
CALL `_trppu_add_constraint_if_missing`('trppu_neutralisations', 'chk_neutre_dates',
  'ADD CONSTRAINT `chk_neutre_dates` CHECK ((`dt_debut` <= `dt_fin`))');
CALL `_trppu_add_constraint_if_missing`('trppu_neutralisations', 'chk_neutre_jour',
  'ADD CONSTRAINT `chk_neutre_jour` CHECK ((`nb_jour` > 0))');

-- trppu_pic_coefficients
CALL `_trppu_add_constraint_if_missing`('trppu_pic_coefficients', 'chk_pic_coefs',
  'ADD CONSTRAINT `chk_pic_coefs` CHECK ((`coef` >= 0))');
CALL `_trppu_add_constraint_if_missing`('trppu_pic_coefficients', 'chk_pic_densite',
  'ADD CONSTRAINT `chk_pic_densite` CHECK ((`densite` in (0,1,2)))');

-- trppu_pic_version
CALL `_trppu_add_constraint_if_missing`('trppu_pic_version', 'chk_picv_dates',
  'ADD CONSTRAINT `chk_picv_dates` CHECK (((`dt_desactivation` is null) or (`dt_desactivation` > `dt_activation`)))');

-- trppu_scenario
CALL `_trppu_add_constraint_if_missing`('trppu_scenario', 'trppu_scenario_chk_1',
  'ADD CONSTRAINT `trppu_scenario_chk_1` CHECK ((`nb_jours_semaine` in (5,6)))');

-- trppu_scenario_variations_prev
CALL `_trppu_add_constraint_if_missing`('trppu_scenario_variations_prev', 'chk_var_borne',
  'ADD CONSTRAINT `chk_var_borne` CHECK ((`variation_pct` between -(100) and 100))');

-- trppu_tmh
CALL `_trppu_add_constraint_if_missing`('trppu_tmh', 'chk_tmh_volumes',
  'ADD CONSTRAINT `chk_tmh_volumes` CHECK ((((`volume_realise` is null) or (`volume_realise` >= 0)) and ((`volume_previsionnel` is null) or (`volume_previsionnel` >= 0))))');


DROP PROCEDURE IF EXISTS `_trppu_add_index_if_missing`;
DROP PROCEDURE IF EXISTS `_trppu_add_constraint_if_missing`;

-- FIN 03_contraintes.sql
