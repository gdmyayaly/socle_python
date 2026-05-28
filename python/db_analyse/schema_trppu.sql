-- Schéma reconstruit depuis les dumps JSON de db_analyse/
-- (introspection de la base dsr_mercure_aa via /mysql/columns).
-- Fidélité strict JSON : pas de clés étrangères ni d'index secondaires.
-- Tables : 19

CREATE DATABASE IF NOT EXISTS `trppu` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE `trppu`;

SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS `demande_dsr`;
CREATE TABLE `demande_dsr` (
  `id` int NOT NULL AUTO_INCREMENT,
  `nomFichier` text NOT NULL,
  `statut` enum('EN_ATTENTE','EN_COURS','TERMINEE','ERREUR') NOT NULL,
  `idrh` char(7) NOT NULL,
  `codeRegate` char(6) NULL,
  `message` text NULL,
  `bassins` json NULL,
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `trppu_agrebal_pdi`;
CREATE TABLE `trppu_agrebal_pdi` (
  `id_agrebal` int NOT NULL,
  `id_pdi` bigint NOT NULL,
  `co_regate` char(6) NOT NULL,
  PRIMARY KEY (`id_agrebal`, `id_pdi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `trppu_api_log`;
CREATE TABLE `trppu_api_log` (
  `id_log` bigint NOT NULL AUTO_INCREMENT,
  `api_name` varchar(50) NULL,
  `id_scenario` bigint NULL,
  `regate` char(6) NULL,
  `dt_appel` datetime NOT NULL,
  `caller` varchar(120) NULL,
  `params` json NULL,
  PRIMARY KEY (`id_log`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `trppu_cles_repartition`;
CREATE TABLE `trppu_cles_repartition` (
  `id_pdi` bigint NOT NULL AUTO_INCREMENT,
  `pdi_rattache` bigint NOT NULL,
  `trafic_colis_signe` decimal(10,8) NOT NULL,
  `trafic_colis_suivi` decimal(10,8) NOT NULL,
  `trafic_colis` decimal(10,8) NOT NULL,
  `trafic_pf` decimal(10,8) NOT NULL,
  `trafic_gf` decimal(10,8) NOT NULL,
  `trafic_oo` decimal(10,8) NOT NULL,
  `trafic_suivi` decimal(10,8) NOT NULL,
  `trafic_signe` decimal(10,8) NOT NULL,
  `trafic_3s` decimal(10,8) NOT NULL,
  `nature` char(3) NOT NULL,
  `co_regate_site` char(6) NOT NULL,
  `type_site` varchar(5) NOT NULL,
  `lb_regate` varchar(40) NOT NULL,
  `co_regate_etablissement` char(6) NOT NULL,
  `lb_etablissement` varchar(40) NOT NULL,
  `co_regate_dex` char(6) NOT NULL,
  `lb_dex` varchar(40) NOT NULL,
  PRIMARY KEY (`id_pdi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `trppu_neutralisations`;
CREATE TABLE `trppu_neutralisations` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `dt_debut` date NOT NULL,
  `dt_fin` date NOT NULL,
  `nb_jour` int NOT NULL,
  `type` enum('FERIE','PEAK','LOCAL') NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `trppu_pic_coefficients`;
CREATE TABLE `trppu_pic_coefficients` (
  `id_pic_coef` bigint NOT NULL AUTO_INCREMENT,
  `id_pic_version` int NOT NULL,
  `co_produit` char(2) NOT NULL,
  `jour_semaine` enum('LUNDI','MARDI','MERCREDI','JEUDI','VENDREDI','SAMEDI') NOT NULL,
  `dt_effet` datetime NOT NULL,
  `dt_fin` datetime NULL,
  `coef` decimal(7,4) NOT NULL,
  `densite` tinyint NOT NULL,
  `dt_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `dt_maj` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `id_rh` varchar(40) NULL,
  PRIMARY KEY (`id_pic_coef`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `trppu_pic_coefficients_ko`;
CREATE TABLE `trppu_pic_coefficients_ko` (
  `id_pic_coef` bigint NOT NULL AUTO_INCREMENT,
  `id_pic_version` int NOT NULL,
  `co_produit` char(2) NOT NULL,
  `jour_semaine` enum('LUNDI','MARDI','MERCREDI','JEUDI','VENDREDI','SAMEDI') NOT NULL,
  `dt_effet` date NOT NULL,
  `dt_fin` date NULL,
  `coef_dense` decimal(7,4) NOT NULL,
  `coef_faible1` decimal(7,4) NOT NULL,
  `coef_faible2` decimal(7,4) NOT NULL,
  `dt_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `dt_maj` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `id_rh` varchar(40) NULL,
  PRIMARY KEY (`id_pic_coef`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `trppu_pic_version`;
CREATE TABLE `trppu_pic_version` (
  `id_pic_version` int NOT NULL AUTO_INCREMENT,
  `lb_pic_version` varchar(80) NULL,
  `niveau` enum('NATIONAL','DEX','SITE','SCENARIO') NOT NULL,
  `co_regate` char(6) NOT NULL,
  `id_scenario` bigint NOT NULL,
  `dt_activation` datetime NOT NULL,
  `dt_desactivation` datetime NULL,
  `motif_desactivation` varchar(255) NULL,
  `commentaire` varchar(500) NULL,
  `est_par_defaut` tinyint(1) NOT NULL DEFAULT 0,
  `dt_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `dt_maj` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `id_rh_creation` varchar(40) NULL,
  `id_rh_maj` varchar(40) NULL,
  PRIMARY KEY (`id_pic_version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `trppu_produit`;
CREATE TABLE `trppu_produit` (
  `co_produit` char(2) NOT NULL,
  `lb_produit` varchar(80) NOT NULL,
  `dt_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `dt_desactivation` date NULL,
  `motif_desactivation` varchar(255) NULL,
  PRIMARY KEY (`co_produit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `trppu_recalcul_log`;
CREATE TABLE `trppu_recalcul_log` (
  `id_log` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `id_agrebal` int NULL,
  `dt_recalcul` datetime NOT NULL,
  `raison` enum('TOPIC_agrebal','MANUEL','SYSTEME') NOT NULL,
  `commentaire` varchar(255) NULL,
  PRIMARY KEY (`id_log`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `trppu_scenario`;
CREATE TABLE `trppu_scenario` (
  `id_scenario` bigint NOT NULL AUTO_INCREMENT,
  `co_roc` char(6) NOT NULL,
  `co_regate` char(6) NOT NULL,
  `lb_scenario` varchar(20) NOT NULL,
  `statut` enum('EN COURS','VALIDE','EN PRODUCTION','ARCHIVE') NOT NULL,
  `dt_creation` datetime NOT NULL,
  `dt_validation` datetime NULL,
  `dt_mise_en_oeuvre` datetime NULL,
  `dt_mise_en_prod` datetime NULL,
  `dt_real_prev` datetime NULL,
  `periode_debut` date NOT NULL,
  `periode_fin` date NOT NULL,
  `periode_realise_debut` date NULL,
  `periode_realise_fin` date NULL,
  `periode_prev_debut` date NULL,
  `periode_prev_fin` date NULL,
  `nb_jours_semaine` tinyint NULL,
  `nb_jours_ouvres` tinyint NULL,
  `nb_jours_ouvrables` tinyint NULL,
  `nb_jours_scenario` tinyint NULL,
  `id_pic_version` int NOT NULL,
  `version_scenario` int NOT NULL,
  `est_fige` tinyint(1) NULL DEFAULT 0,
  `dt_maj` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `id_rh_creation` varchar(40) NULL,
  `id_rh_maj` varchar(40) NULL,
  PRIMARY KEY (`id_scenario`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `trppu_scenario_comptages_manuels`;
CREATE TABLE `trppu_scenario_comptages_manuels` (
  `id_comptage` int NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `dt_comptage` date NOT NULL,
  `co_produit` char(2) NOT NULL,
  `nb_produit` int NOT NULL,
  PRIMARY KEY (`id_comptage`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `trppu_scenario_exclusions`;
CREATE TABLE `trppu_scenario_exclusions` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_produit` char(2) NOT NULL,
  `motif` varchar(255) NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `trppu_scenario_pic_coeffs`;
CREATE TABLE `trppu_scenario_pic_coeffs` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_produit` char(2) NOT NULL,
  `jour_semaine` enum('LUN','MAR','MER','JEU','VEN','SAM') NOT NULL,
  `coef_dense` decimal(8,5) NOT NULL,
  `coef_faible1` decimal(8,5) NOT NULL,
  `coef_faible2` decimal(8,5) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `trppu_scenario_variations_prev`;
CREATE TABLE `trppu_scenario_variations_prev` (
  `id_variation` int NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_produit` char(2) NOT NULL,
  `variation_pct` decimal(5,2) NOT NULL,
  PRIMARY KEY (`id_variation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `trppu_site`;
CREATE TABLE `trppu_site` (
  `co_regate` char(6) NOT NULL,
  `lb_regate` varchar(40) NOT NULL,
  `type_site` char(5) NOT NULL,
  `co_roc` char(6) NOT NULL,
  `dt_maj` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`co_regate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `trppu_tmh`;
CREATE TABLE `trppu_tmh` (
  `id_tmh` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_produit` char(2) NOT NULL,
  `volume_realise` int NULL,
  `volume_previsionnel` int NULL,
  `moyenne_journaliere` decimal(12,2) NOT NULL,
  `moyenne_hebdo` decimal(12,2) NOT NULL,
  `dt_calcul` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `bl_exclu` tinyint(1) NOT NULL,
  PRIMARY KEY (`id_tmh`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

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
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

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
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SET FOREIGN_KEY_CHECKS = 1;
