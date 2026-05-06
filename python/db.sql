
DROP TABLE IF EXISTS `trppu_agrebal_pdi`;
CREATE TABLE `trppu_agrebal_pdi` (
  `id_agrebal` int NOT NULL,
  `id_pdi` bigint NOT NULL,
  `co_regate` char(6) NOT NULL,
  PRIMARY KEY (`id_agrebal`,`id_pdi`),
  KEY `idx_regate` (`co_regate`),
  KEY `idx_pdi` (`id_pdi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `trppu_cles_repartition`
--

DROP TABLE IF EXISTS `trppu_cles_repartition`;
CREATE TABLE `trppu_cles_repartition` (
  `id_pdi` bigint NOT NULL,
  `pct_nature` decimal(10,8) NOT NULL,
  `pct_oo` decimal(10,8) NOT NULL,
  `pct_os_suivi` decimal(10,8) NOT NULL,
  `pct_os_rmp` decimal(10,8) NOT NULL,
  `pct_os_rmp2` decimal(10,8) NOT NULL,
  `pct_colis_suivi` decimal(10,8) NOT NULL,
  `pct_colis_signe` decimal(10,8) NOT NULL,
  `pct_colis` decimal(10,8) NOT NULL,
  `pct_pf` decimal(10,8) NOT NULL,
  `pct_gf` decimal(10,8) NOT NULL,
  `pct_suivi` decimal(10,8) NOT NULL,
  `pct_signe` decimal(10,8) NOT NULL,
  `pct_3s` decimal(10,8) NOT NULL,
  `pct_ppi` decimal(10,8) NOT NULL,
  `pct_ip` decimal(10,8) NOT NULL,
  PRIMARY KEY (`id_pdi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


--
-- Table structure for table `trppu_pic_version`
--

DROP TABLE IF EXISTS `trppu_pic_version`;
CREATE TABLE `trppu_pic_version` (
  `id_pic_version` int NOT NULL AUTO_INCREMENT,
  `lb_pic_version` varchar(50) DEFAULT NULL,
  `niveau` enum('NATIONAL','DEX','SITE') NOT NULL,
  `co_regate` char(6) NOT NULL,
  `dt_activation` datetime NOT NULL,
  `dt_desactivation` datetime DEFAULT NULL,
  PRIMARY KEY (`id_pic_version`),
  KEY `idx_pic_site` (`co_regate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


--
-- Table structure for table `trppu_pic_coefficients`
--

DROP TABLE IF EXISTS `trppu_pic_coefficients`;
CREATE TABLE `trppu_pic_coefficients` (
  `id_pic_coef` bigint NOT NULL AUTO_INCREMENT,
  `id_pic_version` int NOT NULL,
  `co_produit` char(2) NOT NULL,
  `jour_semaine` enum('LUN','MAR','MER','JEU','VEN','SAM') NOT NULL,
  `dt_effet` date NOT NULL,
  `coef_dense` decimal(8,5) NOT NULL,
  `coef_faible1` decimal(8,5) NOT NULL,
  `coef_faible2` decimal(8,5) NOT NULL,
  PRIMARY KEY (`id_pic_coef`),
  UNIQUE KEY `uq_tpc` (`id_pic_version`,`co_produit`,`jour_semaine`,`dt_effet`),
  CONSTRAINT `trppu_pic_coefficients_ibfk_1` FOREIGN KEY (`id_pic_version`) REFERENCES `trppu_pic_version` (`id_pic_version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


--
-- Table structure for table `trppu_produit`
--

DROP TABLE IF EXISTS `trppu_produit`;
CREATE TABLE `trppu_produit` (
  `co_produit` char(2) NOT NULL,
  `lb_produit` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`co_produit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


--
-- Table structure for table `trppu_scenario`
--

DROP TABLE IF EXISTS `trppu_scenario`;
CREATE TABLE `trppu_scenario` (
  `id_scenario` bigint NOT NULL AUTO_INCREMENT,
  `co_roc` char(6) NOT NULL,
  `co_regate` char(6) NOT NULL,
  `lb_scenario` varchar(20) NOT NULL,
  `statut` enum('BROUILLON','SIMULATION','VALIDE','PRODUCTION','ARCHIVE') NOT NULL,
  `dt_creation` datetime NOT NULL,
  `dt_validation` datetime DEFAULT NULL,
  `dt_mise_en_prod` datetime DEFAULT NULL,
  `periode_debut` date NOT NULL,
  `periode_fin` date NOT NULL,
  `periode_realise_debut` date DEFAULT NULL,
  `periode_realise_fin` date DEFAULT NULL,
  `periode_prev_debut` date DEFAULT NULL,
  `periode_prev_fin` date DEFAULT NULL,
  `nb_jours_semaine` tinyint NOT NULL,
  `id_pic_version` int NOT NULL,
  `version_scenario` int NOT NULL,
  `est_fige` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`id_scenario`),
  KEY `idx_scenario_site_statut` (`co_regate`,`statut`),
  CONSTRAINT `trppu_scenario_chk_1` CHECK ((`nb_jours_semaine` in (5,6)))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



--
-- Table structure for table `trppu_api_log`
--

DROP TABLE IF EXISTS `trppu_api_log`;
CREATE TABLE `trppu_api_log` (
  `id_log` bigint NOT NULL AUTO_INCREMENT,
  `id_rh` varchar(255) DEFAULT NULL,
  `profil` varchar(20) DEFAULT NULL,
  `api_name` varchar(50) DEFAULT NULL,
  `id_scenario` bigint DEFAULT NULL,
  `regate` char(6) DEFAULT NULL,
  `dt_appel` datetime NOT NULL,
  `caller` varchar(120) DEFAULT NULL,
  `params` json DEFAULT NULL,
  PRIMARY KEY (`id_log`),
  KEY `id_scenario` (`id_scenario`),
  KEY `idx_api_when` (`api_name`,`dt_appel`),
  CONSTRAINT `trppu_api_log_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


--
-- Table structure for table `trppu_neutralisations`
--

DROP TABLE IF EXISTS `trppu_neutralisations`;
CREATE TABLE `trppu_neutralisations` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `dt_debut` date NOT NULL,
  `dt_fin` date NOT NULL,
  `nb_jour` int NOT NULL,
  `type` enum('FERIE','PEAK','LOCAL') NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_neutre` (`id_scenario`,`dt_debut`,`type`),
  CONSTRAINT `trppu_neutralisations_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `trppu_recalcul_log`
--

DROP TABLE IF EXISTS `trppu_recalcul_log`;
CREATE TABLE `trppu_recalcul_log` (
  `id_log` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `id_agrebal` int DEFAULT NULL,
  `dt_recalcul` datetime NOT NULL,
  `raison` enum('TOPIC_agrebal','MANUEL','SYSTEME') NOT NULL,
  `commentaire` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id_log`),
  KEY `idx_log_scenario` (`id_scenario`,`dt_recalcul`),
  CONSTRAINT `trppu_recalcul_log_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `trppu_scenario_comptages_manuels`
--

DROP TABLE IF EXISTS `trppu_scenario_comptages_manuels`;
CREATE TABLE `trppu_scenario_comptages_manuels` (
  `id_comptage` int NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `dt_comptage` date NOT NULL,
  `co_produit` char(2) NOT NULL,
  `nb_produit` int NOT NULL,
  PRIMARY KEY (`id_comptage`),
  KEY `idx_scm` (`id_scenario`,`dt_comptage`,`co_produit`),
  CONSTRAINT `trppu_scenario_comptages_manuels_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `trppu_scenario_exclusions`
--

DROP TABLE IF EXISTS `trppu_scenario_exclusions`;
CREATE TABLE `trppu_scenario_exclusions` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_produit` char(2) NOT NULL,
  `motif` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_exclusion` (`id_scenario`,`co_produit`),
  CONSTRAINT `trppu_scenario_exclusions_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `trppu_scenario_pic_coeffs`
--

DROP TABLE IF EXISTS `trppu_scenario_pic_coeffs`;
CREATE TABLE `trppu_scenario_pic_coeffs` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_produit` char(2) NOT NULL,
  `jour_semaine` enum('LUN','MAR','MER','JEU','VEN','SAM') NOT NULL,
  `coef_dense` decimal(8,5) NOT NULL,
  `coef_faible1` decimal(8,5) NOT NULL,
  `coef_faible2` decimal(8,5) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_tspc` (`id_scenario`,`co_produit`,`jour_semaine`),
  CONSTRAINT `trppu_scenario_pic_coeffs_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `trppu_scenario_variations_prev`
--

DROP TABLE IF EXISTS `trppu_scenario_variations_prev`;
CREATE TABLE `trppu_scenario_variations_prev` (
  `id_variation` int NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_produit` char(2) NOT NULL,
  `variation_pct` decimal(5,2) NOT NULL,
  PRIMARY KEY (`id_variation`),
  UNIQUE KEY `uq_scen_prod` (`id_scenario`,`co_produit`),
  CONSTRAINT `trppu_scenario_variations_prev_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `trppu_tmh`
--

DROP TABLE IF EXISTS `trppu_tmh`;
CREATE TABLE `trppu_tmh` (
  `id_tmh` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_produit` char(2) NOT NULL,
  `volume_realise` int DEFAULT NULL,
  `volume_previsionnel` int DEFAULT NULL,
  `moyenne_journaliere` decimal(12,4) NOT NULL,
  `semaine_moyenne` decimal(12,4) NOT NULL,
  PRIMARY KEY (`id_tmh`),
  UNIQUE KEY `uq_tmh` (`id_scenario`,`co_produit`),
  CONSTRAINT `trppu_tmh_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `trppu_trafic_agrebal`
--

DROP TABLE IF EXISTS `trppu_trafic_agrebal`;
CREATE TABLE `trppu_trafic_agrebal` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_regate` char(6) NOT NULL,
  `id_agrebal` int NOT NULL,
  `co_produit` char(2) NOT NULL,
  `jour_semaine` enum('LUN','MAR','MER','JEU','VEN','SAM') NOT NULL,
  `couleur_pic` enum('DENSE','FAIBLE1','FAIBLE2') NOT NULL,
  `volume` decimal(12,4) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_scen_site_agrebal` (`id_scenario`,`co_regate`,`id_agrebal`),
  KEY `idx_agrebal_prod` (`id_agrebal`,`co_produit`),
  CONSTRAINT `trppu_trafic_agrebal_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `trppu_trafic_pdi`
--

DROP TABLE IF EXISTS `trppu_trafic_pdi`;
CREATE TABLE `trppu_trafic_pdi` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_regate` char(6) NOT NULL,
  `id_agrebal` int NOT NULL,
  `id_pdi` bigint NOT NULL,
  `co_produit` char(2) NOT NULL,
  `jour_semaine` enum('LUN','MAR','MER','JEU','VEN','SAM') NOT NULL,
  `couleur_pic` enum('DENSE','FAIBLE1','FAIBLE2') NOT NULL,
  `volume` decimal(12,4) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_pdi_agrebal` (`id_pdi`,`id_agrebal`),
  KEY `idx_scen_agrebal_prod_jour` (`id_scenario`,`id_agrebal`,`co_produit`,`jour_semaine`),
  KEY `idx_site` (`co_regate`),
  CONSTRAINT `trppu_trafic_pdi_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
