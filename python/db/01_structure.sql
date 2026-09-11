-- =============================================================================
--  01_structure.sql  --  Creation des tables (structure seule)
-- =============================================================================
--  Base : dsr_mercure_aa            Genere depuis db/db_new.sql
--
--  Contenu : CREATE TABLE IF NOT EXISTS, colonnes et cles primaires uniquement.
--
--  Pourquoi les cles primaires sont ici et non dans 03_contraintes.sql :
--  MySQL exige qu'une colonne AUTO_INCREMENT soit indexee des le CREATE TABLE
--  (erreur 1075 : "there can be only one auto column and it must be defined as
--  a key"). La cle primaire est donc le strict minimum indissociable de la
--  creation. Tout le reste -- cles uniques, index secondaires, cles etrangeres,
--  contraintes CHECK -- est dans 03_contraintes.sql.
--
--  Rejouable : IF NOT EXISTS, aucune table existante n'est modifiee ni supprimee.
--  ATTENTION : si une table existe deja avec une structure differente, elle est
--  laissee telle quelle, en silence. Ce script ne fait pas de migration.
--
--  Ordre d'execution : 01_structure -> 02_donnees -> 03_contraintes
-- =============================================================================

CREATE DATABASE IF NOT EXISTS `dsr_mercure_aa`
  DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE `dsr_mercure_aa`;

-- Les cles etrangeres sont posees par 03_contraintes.sql : l'ordre de creation
-- des tables n'a donc aucune importance ici.
SET FOREIGN_KEY_CHECKS = 0;

-- ----- TABLE `demande_dsr` -----
CREATE TABLE IF NOT EXISTS `demande_dsr` (
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
CREATE TABLE IF NOT EXISTS `trafic_staging` (
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
CREATE TABLE IF NOT EXISTS `trppu_agrebal_pdi` (
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
  PRIMARY KEY (`agrebal_id_pdi`)
) ENGINE=InnoDB AUTO_INCREMENT=9389 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_api_log` -----
CREATE TABLE IF NOT EXISTS `trppu_api_log` (
  `id_log` bigint NOT NULL AUTO_INCREMENT,
  `api_name` varchar(50) DEFAULT NULL,
  `id_scenario` bigint DEFAULT NULL,
  `regate` char(6) DEFAULT NULL,
  `dt_appel` datetime NOT NULL,
  `caller` varchar(120) DEFAULT NULL,
  `params` json DEFAULT NULL,
  PRIMARY KEY (`id_log`)
) ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_cles_repartition` -----
CREATE TABLE IF NOT EXISTS `trppu_cles_repartition` (
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
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=24217441 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_cles_repartition_calcule` -----
CREATE TABLE IF NOT EXISTS `trppu_cles_repartition_calcule` (
  `id_cle_repartition` bigint NOT NULL AUTO_INCREMENT,
  `id_version_cle` int NOT NULL,
  `id_referentiel` int NOT NULL,
  `id_pdi` bigint NOT NULL,
  `co_regate_site` char(6) NOT NULL,
  `cle_colis` decimal(24,18) NOT NULL,
  `cle_oo` decimal(24,18) NOT NULL,
  `cle_3s` decimal(24,18) NOT NULL,
  `cle_potentielip` decimal(24,18) NOT NULL,
  `date_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_cle_repartition`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_neutralisations` -----
CREATE TABLE IF NOT EXISTS `trppu_neutralisations` (
  `id_neutralisation` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `dt_debut` date NOT NULL,
  `dt_fin` date NOT NULL,
  `nb_jour` int NOT NULL,
  `motif` varchar(255) DEFAULT NULL,
  `dt_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `id_rh` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id_neutralisation`)
) ENGINE=InnoDB AUTO_INCREMENT=25 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_pic_coefficients` -----
CREATE TABLE IF NOT EXISTS `trppu_pic_coefficients` (
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
  PRIMARY KEY (`id_pic_coef`)
) ENGINE=InnoDB AUTO_INCREMENT=226 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_pic_version` -----
CREATE TABLE IF NOT EXISTS `trppu_pic_version` (
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
  PRIMARY KEY (`id_pic_version`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_produit` -----
CREATE TABLE IF NOT EXISTS `trppu_produit` (
  `co_produit` varchar(3) NOT NULL,
  `lb_produit` varchar(80) NOT NULL,
  `dt_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `dt_desactivation` date DEFAULT NULL,
  `motif_desactivation` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`co_produit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_recalcul_log` -----
CREATE TABLE IF NOT EXISTS `trppu_recalcul_log` (
  `id_log` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `dt_recalcul` datetime NOT NULL,
  `raison` enum('AGREBAL','CLE_REPARTITION','MANUEL','INITIAL') NOT NULL,
  `commentaire` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id_log`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_referentiel` -----
CREATE TABLE IF NOT EXISTS `trppu_referentiel` (
  `id_referentiel` int NOT NULL AUTO_INCREMENT,
  `co_regate` char(6) DEFAULT NULL,
  `date_reference` date NOT NULL,
  `commentaire` varchar(255) NOT NULL,
  PRIMARY KEY (`id_referentiel`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_scenario` -----
CREATE TABLE IF NOT EXISTS `trppu_scenario` (
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
  PRIMARY KEY (`id_scenario`)
) ENGINE=InnoDB AUTO_INCREMENT=163 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_scenario_comptages_manuels` -----
CREATE TABLE IF NOT EXISTS `trppu_scenario_comptages_manuels` (
  `id_comptage` int NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `dt_comptage` date NOT NULL,
  `co_produit` varchar(3) NOT NULL,
  `nb_produit` int NOT NULL,
  PRIMARY KEY (`id_comptage`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_scenario_exclusions` -----
CREATE TABLE IF NOT EXISTS `trppu_scenario_exclusions` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_produit` varchar(3) NOT NULL,
  `motif` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_scenario_pic_coeffs` -----
CREATE TABLE IF NOT EXISTS `trppu_scenario_pic_coeffs` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_produit` varchar(3) NOT NULL,
  `jour_semaine` enum('LUN','MAR','MER','JEU','VEN','SAM') NOT NULL,
  `coef_dense` decimal(8,5) NOT NULL,
  `coef_faible1` decimal(8,5) NOT NULL,
  `coef_faible2` decimal(8,5) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_scenario_variations_prev` -----
CREATE TABLE IF NOT EXISTS `trppu_scenario_variations_prev` (
  `id_variation` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_produit` varchar(3) NOT NULL,
  `variation_pct` decimal(6,2) NOT NULL,
  `dt_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `id_rh` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id_variation`)
) ENGINE=InnoDB AUTO_INCREMENT=81 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_site` -----
CREATE TABLE IF NOT EXISTS `trppu_site` (
  `co_regate` char(6) NOT NULL,
  `lb_regate` varchar(40) NOT NULL,
  `type_site` char(5) NOT NULL,
  `co_roc` char(6) NOT NULL,
  `dt_maj` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`co_regate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_suivi_batch` -----
CREATE TABLE IF NOT EXISTS `trppu_suivi_batch` (
  `id_calcul_batch` bigint NOT NULL AUTO_INCREMENT,
  `lb_batch` varchar(255) NOT NULL,
  `dt_debut` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `dt_fin` datetime DEFAULT NULL,
  `statut` enum('EN_COURS','SUCCES','ECHEC','PARTIEL') NOT NULL,
  `duree` time DEFAULT NULL,
  `commande` varchar(255) NOT NULL,
  `commentaire` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id_calcul_batch`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_tmh` -----
CREATE TABLE IF NOT EXISTS `trppu_tmh` (
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
  `volume_brut` int DEFAULT NULL,
  PRIMARY KEY (`id_tmh`)
) ENGINE=InnoDB AUTO_INCREMENT=677 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_trafic_agrebal` -----
CREATE TABLE IF NOT EXISTS `trppu_trafic_agrebal` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_regate` char(6) NOT NULL,
  `id_agrebal` int NOT NULL,
  `agrebal_uuid` varchar(45) NOT NULL,
  `co_produit` varchar(3) NOT NULL,
  `jour_semaine` enum('LUNDI','MARDI','MERCREDI','JEUDI','VENDREDI','SAMEDI') NOT NULL,
  `couleur_pic` enum('DENSE','FAIBLE1','FAIBLE2') NOT NULL,
  `volume` decimal(12,4) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_trafic_pdi` -----
CREATE TABLE IF NOT EXISTS `trppu_trafic_pdi` (
  `id_trafic_pdi` bigint NOT NULL AUTO_INCREMENT,
  `id_scenario` bigint NOT NULL,
  `co_regate` char(6) NOT NULL,
  `id_agrebal` bigint NOT NULL,
  `agrebal_uuid` varchar(45) NOT NULL,
  `id_pdi` bigint NOT NULL,
  `co_produit` varchar(3) NOT NULL,
  `jour_semaine` enum('LUNDI','MARDI','MERCREDI','JEUDI','VENDREDI','SAMEDI') NOT NULL,
  `dense` smallint unsigned DEFAULT NULL,
  `faible1` smallint unsigned DEFAULT NULL,
  `faible2` smallint unsigned DEFAULT NULL,
  `dt_calcul` datetime NOT NULL,
  PRIMARY KEY (`id_trafic_pdi`,`id_scenario`,`id_pdi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_trafic_site` -----
CREATE TABLE IF NOT EXISTS `trppu_trafic_site` (
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
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ----- TABLE `trppu_version_cle` -----
CREATE TABLE IF NOT EXISTS `trppu_version_cle` (
  `id_version_cle` int NOT NULL AUTO_INCREMENT,
  `id_referentiel` int NOT NULL,
  `libelle` varchar(100) DEFAULT NULL,
  `co_regate` char(6) NOT NULL,
  `actif` char(1) NOT NULL DEFAULT 'O',
  `date_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `date_debut_validite` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `date_fin_validite` datetime DEFAULT NULL,
  `commentaire` varchar(500) DEFAULT NULL,
  PRIMARY KEY (`id_version_cle`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SET FOREIGN_KEY_CHECKS = 1;

-- FIN 01_structure.sql
