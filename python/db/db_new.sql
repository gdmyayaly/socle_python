-- Dump du schéma de la base `dsr_mercure_aa`
-- 24 objet(s) — schéma uniquement (sans données)

CREATE DATABASE IF NOT EXISTS `dsr_mercure_aa`;
USE `dsr_mercure_aa`;

SET FOREIGN_KEY_CHECKS = 0;

-- ----- TABLE `demande_dsr` -----
DROP TABLE IF EXISTS `demande_dsr`;
CREATE TABLE `demande_dsr` (
  `id` int NOT NULL AUTO_INCREMENT,
  `nomFichier` text NOT NULL,
  `statut` enum('EN_ATTENTE','EN_COURS','TERMINEE','ERREUR') NOT NULL,
  `idrh` char(7) NOT NULL,
  `codeRegate` char(6) DEFAULT NULL,
  `message` text,
  `bassins` json DEFAULT NULL,
  `simuOptiTheo` tinyint(1) NOT NULL,
  `simuScenarDex` tinyint(1) NOT NULL,
  `simuScenarRef` tinyint(1) NOT NULL,
  `simuExistProj` tinyint(1) NOT NULL,
  `forcerExec` tinyint(1) NOT NULL,
  `restitTousBassins` tinyint(1) NOT NULL,
  `optiTransport` tinyint(1) NOT NULL,
  `choixPI` tinyint(1) NOT NULL,
  `simulerACP` tinyint(1) NOT NULL,
  `createdAt` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updatedAt` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=47 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trafic_staging` -----
DROP TABLE IF EXISTS `trafic_staging`;
CREATE TABLE `trafic_staging` (
  `id` text,
  `pdi_rattache` text,
  `trafic_colis_signe` text,
  `trafic_colis_suivi` text,
  `trafic_colis` text,
  `trafic_pf` text,
  `trafic_gf` text,
  `trafic_oo` text,
  `trafic_suivi` text,
  `trafic_signe` text,
  `trafic_3s` text,
  `nature` text,
  `regate_site` text,
  `type` text,
  `libelle_site` text,
  `regate_etab` text,
  `libelle_etab` text,
  `regate_dex` text,
  `libelle_dex` text,
  `erreur_colonne` varchar(255) DEFAULT NULL,
  `erreur_detail` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_agrebal_pdi` -----
DROP TABLE IF EXISTS `trppu_agrebal_pdi`;
CREATE TABLE `trppu_agrebal_pdi` (
  `agrebal_id_pdi` int NOT NULL AUTO_INCREMENT,
  `agrebal_id` int NOT NULL,
  `agrebal_uuid` varchar(45) NOT NULL,
  `agrebal_nom` varchar(45) DEFAULT NULL,
  `agrebal_code_roc` char(6) NOT NULL,
  `agrebal_code_regate` char(6) NOT NULL,
  `agrebal_pdiQuantity` int NOT NULL,
  `agrebal_pdiList` json DEFAULT NULL,
  `agrebal_event` varchar(40) DEFAULT NULL,
  `agrebal_createdAt` datetime NOT NULL,
  `agrebal_updatedAt` datetime NOT NULL,
  `agrebal_deleteddAt` datetime DEFAULT NULL,
  `agrebal_pdi_ids` json GENERATED ALWAYS AS (ifnull(json_extract(`agrebal_pdiList`,_utf8mb4'$[*].pdi_id'),json_array())) STORED,
  PRIMARY KEY (`agrebal_id_pdi`),
  UNIQUE KEY `uq_agrpdi_courant` (`agrebal_id`,`agrebal_code_regate`),
  KEY `idx_agrpdi_site` (`agrebal_code_regate`),
  KEY `idx_updated_at` (`agrebal_updatedAt`),
  KEY `idx_pdi_ids` ((cast(`agrebal_pdi_ids` as unsigned array)))
) ENGINE=InnoDB AUTO_INCREMENT=9389 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_api_log` -----
DROP TABLE IF EXISTS `trppu_api_log`;
CREATE TABLE `trppu_api_log` (
  `id_log` bigint NOT NULL AUTO_INCREMENT,
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

-- ----- TABLE `trppu_cles_repartition` -----
DROP TABLE IF EXISTS `trppu_cles_repartition`;
CREATE TABLE `trppu_cles_repartition` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `id_pdi` bigint NOT NULL,
  `pdi_rattache` bigint NOT NULL,
  `trafic_colis` decimal(25,19) NOT NULL,
  `trafic_oo` decimal(25,19) NOT NULL,
  `trafic_3s` decimal(25,19) NOT NULL,
  `nature` char(3) NOT NULL,
  `co_regate_site` char(6) NOT NULL,
  `type_site` varchar(10) NOT NULL,
  `lb_regate` varchar(100) NOT NULL,
  `co_regate_etablissement` char(6) DEFAULT NULL,
  `lb_etablissement` varchar(100) DEFAULT NULL,
  `co_regate_dex` char(6) NOT NULL,
  `lb_dex` varchar(100) NOT NULL,
  `nb_pre` smallint DEFAULT NULL,
  `potentielip` smallint DEFAULT NULL,
  `id_referentiel` int NOT NULL,
  `date_debut_validite` date NOT NULL,
  `date_fin_validite` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_pdi_ref` (`id_pdi`,`id_referentiel`)
) ENGINE=InnoDB AUTO_INCREMENT=24247952 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_cles_repartition_calcule` -----
DROP TABLE IF EXISTS `trppu_cles_repartition_calcule`;
CREATE TABLE `trppu_cles_repartition_calcule` (
  `id_cle_repartition` bigint NOT NULL AUTO_INCREMENT,
  `id_version_cle` int NOT NULL,
  `id_referentiel` int NOT NULL,
  `id_pdi` bigint NOT NULL,
  `co_regate_site` varchar(10) NOT NULL,
  `cle_colis` decimal(24,18) NOT NULL,
  `cle_oo` decimal(24,18) NOT NULL,
  `cle_3s` decimal(24,18) NOT NULL,
  `cle_potentielip` decimal(24,18) NOT NULL,
  `date_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_cle_repartition`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_neutralisations` -----
DROP TABLE IF EXISTS `trppu_neutralisations`;
CREATE TABLE `trppu_neutralisations` (
  `id_neutralisation` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `dt_debut` date NOT NULL,
  `dt_fin` date NOT NULL,
  `nb_jour` int NOT NULL,
  `motif` varchar(255) DEFAULT NULL,
  `dt_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `id_rh` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id_neutralisation`),
  UNIQUE KEY `uq_neutre` (`id_scenario`,`dt_debut`,`dt_fin`),
  CONSTRAINT `fk_neutre_scen` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`) ON DELETE CASCADE,
  CONSTRAINT `chk_neutre_dates` CHECK ((`dt_debut` <= `dt_fin`)),
  CONSTRAINT `chk_neutre_jour` CHECK ((`nb_jour` > 0))
) ENGINE=InnoDB AUTO_INCREMENT=16 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_pic_coefficients` -----
DROP TABLE IF EXISTS `trppu_pic_coefficients`;
CREATE TABLE `trppu_pic_coefficients` (
  `id_pic_coef` bigint NOT NULL AUTO_INCREMENT,
  `id_pic_version` int NOT NULL,
  `co_produit` varchar(3) NOT NULL,
  `jour_semaine` enum('LUNDI','MARDI','MERCREDI','JEUDI','VENDREDI','SAMEDI') NOT NULL,
  `dt_effet` datetime NOT NULL,
  `dt_fin` datetime DEFAULT NULL,
  `coef` decimal(7,4) NOT NULL,
  `densite` tinyint NOT NULL,
  `dt_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `dt_maj` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `id_rh` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id_pic_coef`),
  UNIQUE KEY `uq_picc` (`id_pic_version`,`co_produit`,`jour_semaine`,`densite`),
  KEY `idx_picc_produit` (`co_produit`),
  CONSTRAINT `fk_picc_produit` FOREIGN KEY (`co_produit`) REFERENCES `trppu_produit` (`co_produit`) ON DELETE RESTRICT,
  CONSTRAINT `fk_picc_version` FOREIGN KEY (`id_pic_version`) REFERENCES `trppu_pic_version` (`id_pic_version`) ON DELETE CASCADE,
  CONSTRAINT `chk_pic_coefs` CHECK ((`coef` >= 0)),
  CONSTRAINT `chk_pic_densite` CHECK ((`densite` in (0,1,2)))
) ENGINE=InnoDB AUTO_INCREMENT=130 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_pic_coefficients_ko` -----
DROP TABLE IF EXISTS `trppu_pic_coefficients_ko`;
CREATE TABLE `trppu_pic_coefficients_ko` (
  `id_pic_coef` bigint NOT NULL AUTO_INCREMENT,
  `id_pic_version` int NOT NULL,
  `co_produit` char(2) NOT NULL,
  `jour_semaine` enum('LUNDI','MARDI','MERCREDI','JEUDI','VENDREDI','SAMEDI') NOT NULL,
  `dt_effet` date NOT NULL,
  `dt_fin` date DEFAULT NULL,
  `coef_dense` decimal(7,4) NOT NULL,
  `coef_faible1` decimal(7,4) NOT NULL,
  `coef_faible2` decimal(7,4) NOT NULL,
  `dt_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `dt_maj` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `id_rh` varchar(40) DEFAULT NULL,
  PRIMARY KEY (`id_pic_coef`),
  UNIQUE KEY `uq_picc` (`id_pic_version`,`co_produit`,`jour_semaine`),
  KEY `idx_picc_produit` (`co_produit`)
) ENGINE=InnoDB AUTO_INCREMENT=43 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_pic_version` -----
DROP TABLE IF EXISTS `trppu_pic_version`;
CREATE TABLE `trppu_pic_version` (
  `id_pic_version` int NOT NULL AUTO_INCREMENT,
  `lb_pic_version` varchar(80) DEFAULT NULL,
  `niveau` enum('NATIONAL','DEX','SITE','SCENARIO') NOT NULL,
  `co_regate` char(6) NOT NULL,
  `id_scenario` bigint NOT NULL,
  `dt_activation` datetime NOT NULL,
  `dt_desactivation` datetime DEFAULT NULL,
  `motif_desactivation` varchar(255) DEFAULT NULL,
  `commentaire` varchar(500) DEFAULT NULL,
  `est_par_defaut` tinyint(1) NOT NULL DEFAULT '0',
  `dt_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `dt_maj` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `id_rh_creation` varchar(255) DEFAULT NULL,
  `id_rh_maj` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id_pic_version`),
  KEY `idx_picv_site` (`co_regate`),
  KEY `idx_picv_defaut` (`co_regate`,`est_par_defaut`),
  CONSTRAINT `chk_picv_dates` CHECK (((`dt_desactivation` is null) or (`dt_desactivation` > `dt_activation`)))
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_produit` -----
DROP TABLE IF EXISTS `trppu_produit`;
CREATE TABLE `trppu_produit` (
  `co_produit` varchar(3) NOT NULL,
  `lb_produit` varchar(80) NOT NULL,
  `dt_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `dt_desactivation` date DEFAULT NULL,
  `motif_desactivation` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`co_produit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_recalcul_log` -----
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

-- ----- TABLE `trppu_referentiel` -----
DROP TABLE IF EXISTS `trppu_referentiel`;
CREATE TABLE `trppu_referentiel` (
  `id_referentiel` int NOT NULL AUTO_INCREMENT,
  `date_reference` date NOT NULL,
  `commentaire` varchar(255) NOT NULL,
  PRIMARY KEY (`id_referentiel`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_scenario` -----
DROP TABLE IF EXISTS `trppu_scenario`;
CREATE TABLE `trppu_scenario` (
  `id_scenario` bigint NOT NULL AUTO_INCREMENT,
  `co_roc` char(6) NOT NULL,
  `co_regate` char(6) NOT NULL,
  `lb_scenario` varchar(20) NOT NULL,
  `statut` enum('EN COURS','SIMULATION','VALIDE','EN PRODUCTION','ARCHIVE') NOT NULL,
  `dt_creation` datetime NOT NULL,
  `dt_validation` datetime DEFAULT NULL,
  `dt_mise_en_oeuvre` datetime DEFAULT NULL,
  `dt_mise_en_prod` datetime DEFAULT NULL,
  `dt_pivot` datetime DEFAULT NULL,
  `periode_debut` date NOT NULL,
  `periode_fin` date NOT NULL,
  `periode_realise_debut` date DEFAULT NULL,
  `periode_realise_fin` date DEFAULT NULL,
  `periode_prev_debut` date DEFAULT NULL,
  `periode_prev_fin` date DEFAULT NULL,
  `nb_jours_semaine` smallint DEFAULT NULL,
  `nb_jours_ouvres` smallint DEFAULT NULL,
  `nb_jours_ouvrables` smallint DEFAULT NULL,
  `nb_jours_scenario` smallint DEFAULT NULL,
  `id_pic_version` int NOT NULL,
  `version_scenario` int NOT NULL,
  `est_fige` smallint DEFAULT '0',
  `dt_maj` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `id_rh_creation` varchar(255) DEFAULT NULL,
  `id_rh_maj` varchar(255) DEFAULT NULL,
  `trafic_pdi_calcule` smallint DEFAULT '0',
  `trafic_agrebal_calcule` smallint DEFAULT '0',
  `Calcul_trafic_en_cours` smallint DEFAULT '0',
  `id_referentiel` int NOT NULL DEFAULT '0',
  `id_version_cle` int NOT NULL DEFAULT '0',
  PRIMARY KEY (`id_scenario`),
  KEY `idx_scenario_site_statut` (`co_regate`,`statut`),
  CONSTRAINT `trppu_scenario_chk_1` CHECK ((`nb_jours_semaine` in (5,6)))
) ENGINE=InnoDB AUTO_INCREMENT=98 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_scenario_comptages_manuels` -----
DROP TABLE IF EXISTS `trppu_scenario_comptages_manuels`;
CREATE TABLE `trppu_scenario_comptages_manuels` (
  `id_comptage` int NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `dt_comptage` date NOT NULL,
  `co_produit` varchar(3) NOT NULL,
  `nb_produit` int NOT NULL,
  PRIMARY KEY (`id_comptage`),
  KEY `idx_scm` (`id_scenario`,`dt_comptage`,`co_produit`),
  CONSTRAINT `trppu_scenario_comptages_manuels_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_scenario_exclusions` -----
DROP TABLE IF EXISTS `trppu_scenario_exclusions`;
CREATE TABLE `trppu_scenario_exclusions` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_produit` varchar(3) NOT NULL,
  `motif` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_exclusion` (`id_scenario`,`co_produit`),
  CONSTRAINT `trppu_scenario_exclusions_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_scenario_pic_coeffs` -----
DROP TABLE IF EXISTS `trppu_scenario_pic_coeffs`;
CREATE TABLE `trppu_scenario_pic_coeffs` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_produit` varchar(3) NOT NULL,
  `jour_semaine` enum('LUN','MAR','MER','JEU','VEN','SAM') NOT NULL,
  `coef_dense` decimal(8,5) NOT NULL,
  `coef_faible1` decimal(8,5) NOT NULL,
  `coef_faible2` decimal(8,5) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_tspc` (`id_scenario`,`co_produit`,`jour_semaine`),
  CONSTRAINT `trppu_scenario_pic_coeffs_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_scenario_variations_prev` -----
DROP TABLE IF EXISTS `trppu_scenario_variations_prev`;
CREATE TABLE `trppu_scenario_variations_prev` (
  `id_variation` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_produit` varchar(3) NOT NULL,
  `variation_pct` decimal(6,2) NOT NULL,
  `dt_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `id_rh` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id_variation`),
  UNIQUE KEY `uq_var_scen_prod` (`id_scenario`,`co_produit`),
  KEY `idx_var_produit` (`co_produit`),
  CONSTRAINT `fk_var_produit` FOREIGN KEY (`co_produit`) REFERENCES `trppu_produit` (`co_produit`) ON DELETE RESTRICT,
  CONSTRAINT `fk_var_scen` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`) ON DELETE CASCADE,
  CONSTRAINT `chk_var_borne` CHECK ((`variation_pct` between -(100) and 100))
) ENGINE=InnoDB AUTO_INCREMENT=57 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_site` -----
DROP TABLE IF EXISTS `trppu_site`;
CREATE TABLE `trppu_site` (
  `co_regate` char(6) NOT NULL,
  `lb_regate` varchar(40) NOT NULL,
  `type_site` char(5) NOT NULL,
  `co_roc` char(6) NOT NULL,
  `dt_maj` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`co_regate`),
  KEY `idx_site_roc` (`co_roc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_site_trafic` -----
DROP TABLE IF EXISTS `trppu_site_trafic`;
CREATE TABLE `trppu_site_trafic` (
  `id_site_trafic` bigint NOT NULL AUTO_INCREMENT,
  `id_referentiel` int NOT NULL,
  `co_regate_site` varchar(10) NOT NULL,
  `trafic_colis_total` decimal(24,18) NOT NULL,
  `trafic_oo_total` decimal(24,18) NOT NULL,
  `trafic_3s_total` decimal(24,18) NOT NULL,
  `potentielip_total` bigint NOT NULL,
  `date_debut_validite` date NOT NULL,
  `date_fin_validite` date DEFAULT NULL,
  `date_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_site_trafic`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_tmh` -----
DROP TABLE IF EXISTS `trppu_tmh`;
CREATE TABLE `trppu_tmh` (
  `id_tmh` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_produit` varchar(3) NOT NULL,
  `volume_realise` int DEFAULT NULL,
  `volume_previsionnel` int DEFAULT NULL,
  `moyenne_journaliere` decimal(12,2) DEFAULT NULL,
  `moyenne_hebdo` decimal(12,2) DEFAULT NULL,
  `dt_calcul` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `bl_exclu` tinyint(1) NOT NULL,
  `bl_manuel` tinyint(1) NOT NULL,
  `id_rh` varchar(255) DEFAULT NULL,
  `motif` varchar(255) DEFAULT NULL,
  `volume_previsionnel_recalcule` int DEFAULT NULL,
  PRIMARY KEY (`id_tmh`),
  UNIQUE KEY `uq_tmh` (`id_tmh`,`id_scenario`,`co_produit`),
  KEY `fk_tmh_scen` (`id_scenario`),
  KEY `fk_tmh_produit` (`co_produit`),
  CONSTRAINT `fk_tmh_produit` FOREIGN KEY (`co_produit`) REFERENCES `trppu_produit` (`co_produit`) ON DELETE RESTRICT,
  CONSTRAINT `fk_tmh_scen` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`) ON DELETE CASCADE,
  CONSTRAINT `chk_tmh_volumes` CHECK ((((`volume_realise` is null) or (`volume_realise` >= 0)) and ((`volume_previsionnel` is null) or (`volume_previsionnel` >= 0))))
) ENGINE=InnoDB AUTO_INCREMENT=454 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_trafic_agrebal` -----
DROP TABLE IF EXISTS `trppu_trafic_agrebal`;
CREATE TABLE `trppu_trafic_agrebal` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_regate` char(6) NOT NULL,
  `id_agrebal` int NOT NULL,
  `co_produit` varchar(3) NOT NULL,
  `jour_semaine` enum('LUN','MAR','MER','JEU','VEN','SAM') NOT NULL,
  `couleur_pic` enum('DENSE','FAIBLE1','FAIBLE2') NOT NULL,
  `volume` decimal(12,4) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_scen_site_agrebal` (`id_scenario`,`co_regate`,`id_agrebal`),
  KEY `idx_agrebal_prod` (`id_agrebal`,`co_produit`),
  CONSTRAINT `trppu_trafic_agrebal_ibfk_1` FOREIGN KEY (`id_scenario`) REFERENCES `trppu_scenario` (`id_scenario`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_trafic_pdi` -----
DROP TABLE IF EXISTS `trppu_trafic_pdi`;
CREATE TABLE `trppu_trafic_pdi` (
  `id_trafic_pdi` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_regate` char(6) NOT NULL,
  `id_agrebal` bigint NOT NULL,
  `id_pdi` bigint NOT NULL,
  `co_produit` varchar(3) NOT NULL,
  `jour_semaine` enum('LUNDI','MARDI','MERCREDI','JEUDI','VENDREDI','SAMEDI') NOT NULL,
  `dense` smallint unsigned DEFAULT NULL,
  `faible1` smallint unsigned DEFAULT NULL,
  `faible2` smallint unsigned DEFAULT NULL,
  `dt_calcul` datetime NOT NULL,
  `id_calcul_batch` bigint NOT NULL,
  PRIMARY KEY (`id_trafic_pdi`,`id_scenario`,`id_pdi`),
  KEY `idx_tp_scen_agr_prod_jour` (`id_scenario`,`id_agrebal`,`co_produit`,`jour_semaine`),
  KEY `idx_tp_pdi_agr` (`id_pdi`,`id_agrebal`),
  KEY `idx_tp_site` (`co_regate`),
  KEY `idx_tp_batch` (`id_calcul_batch`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_version_cle` -----
DROP TABLE IF EXISTS `trppu_version_cle`;
CREATE TABLE `trppu_version_cle` (
  `id_version_cle` int NOT NULL AUTO_INCREMENT,
  `id_referentiel` int NOT NULL,
  `libelle` varchar(100) DEFAULT NULL,
  `actif` char(1) NOT NULL DEFAULT 'O',
  `date_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `commentaire` varchar(500) DEFAULT NULL,
  PRIMARY KEY (`id_version_cle`),
  KEY `idx_ref` (`id_referentiel`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SET FOREIGN_KEY_CHECKS = 1;
