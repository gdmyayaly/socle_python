
SET FOREIGN_KEY_CHECKS = 0;

-- =============================================================================
--  00  TABLES DE RÉFÉRENCE (nouvelles)
-- =============================================================================

-- Sites postaux (Régate)
DROP TABLE IF EXISTS `trppu_site`;
CREATE TABLE `trppu_site` (
  `co_regate`        CHAR(6)        NOT NULL,
  `lb_site`          VARCHAR(120)   NOT NULL,
  `type_site`        ENUM('PIC','PPDC','CDIS','AGENCE','AUTRE') NOT NULL,
  `est_actif`        TINYINT(1)     NOT NULL DEFAULT 1,
  `dt_maj`           DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                    ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`co_regate`),
  KEY `idx_site_type`     (`type_site`, `est_actif`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Référentiel des PDI (synchro depuis le SI maître)
DROP TABLE IF EXISTS `trppu_pdi`;
CREATE TABLE `trppu_pdi` (
  `id_pdi`           BIGINT         NOT NULL,
  `co_regate`        CHAR(6)        NOT NULL,
  `lb_pdi`           VARCHAR(150)   DEFAULT NULL,
  `est_actif`        TINYINT(1)     NOT NULL DEFAULT 1,
  `dt_maj`           DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                    ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_pdi`),
  KEY `idx_pdi_site` (`co_regate`),
  CONSTRAINT `fk_pdi_site` FOREIGN KEY (`co_regate`)
    REFERENCES `trppu_site`(`co_regate`) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Référentiel des agrégats balade (NB : id_agrebal passe en BIGINT)
DROP TABLE IF EXISTS `trppu_agrebal`;
CREATE TABLE `trppu_agrebal` (
  `id_agrebal`       BIGINT         NOT NULL,
  `co_regate`        CHAR(6)        NOT NULL,
  `lb_agrebal`       VARCHAR(120)   DEFAULT NULL,
  `est_actif`        TINYINT(1)     NOT NULL DEFAULT 1,
  `dt_maj`           DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                    ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_agrebal`),
  KEY `idx_agrebal_site` (`co_regate`),
  CONSTRAINT `fk_agrebal_site` FOREIGN KEY (`co_regate`)
    REFERENCES `trppu_site`(`co_regate`) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Référentiel ROC (codes de regroupement organisationnel)
DROP TABLE IF EXISTS `trppu_roc`;
CREATE TABLE `trppu_roc` (
  `co_roc`           CHAR(6)        NOT NULL,
  `lb_roc`           VARCHAR(120)   NOT NULL,
  `est_actif`        TINYINT(1)     NOT NULL DEFAULT 1,
  PRIMARY KEY (`co_roc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Utilisateurs applicatifs (référentiel local minimal pour les FK d'audit)
DROP TABLE IF EXISTS `trppu_utilisateur`;
CREATE TABLE `trppu_utilisateur` (
  `id_rh`            VARCHAR(40)    NOT NULL,
  `lb_utilisateur`   VARCHAR(120)   DEFAULT NULL,
  `profil`           VARCHAR(20)    DEFAULT NULL,
  `est_actif`        TINYINT(1)     NOT NULL DEFAULT 1,
  `dt_maj`           DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                    ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_rh`),
  KEY `idx_user_profil` (`profil`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
--  01  trppu_produit
-- =============================================================================
DROP TABLE IF EXISTS `trppu_produit`;
CREATE TABLE `trppu_produit` (
  `co_produit`            CHAR(2)        NOT NULL,
  `lb_produit`            VARCHAR(80)    NOT NULL,
  `dt_activation`         DATE           NOT NULL,
  `dt_desactivation`      DATE           DEFAULT NULL,
  `motif_desactivation`   VARCHAR(255)   DEFAULT NULL,
  `est_fige`              TINYINT(1)     NOT NULL DEFAULT 0,
  `created_at`            DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`            DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                         ON UPDATE CURRENT_TIMESTAMP,
  `created_by`            VARCHAR(40)    DEFAULT NULL,
  `updated_by`            VARCHAR(40)    DEFAULT NULL,
  PRIMARY KEY (`co_produit`),
  CONSTRAINT `chk_produit_dates`
    CHECK (`dt_desactivation` IS NULL OR `dt_desactivation` >= `dt_activation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
--  02  trppu_pic_version
-- =============================================================================
DROP TABLE IF EXISTS `trppu_pic_version`;
CREATE TABLE `trppu_pic_version` (
  `id_pic_version`        INT            NOT NULL AUTO_INCREMENT,
  `lb_pic_version`        VARCHAR(80)    DEFAULT NULL,
  `niveau`                ENUM('NATIONAL','DEX','SITE') NOT NULL,
  `co_regate`             CHAR(6)        NOT NULL,
  `dt_activation`         DATETIME       NOT NULL,
  `dt_desactivation`      DATETIME       DEFAULT NULL,
  `motif_desactivation`   VARCHAR(255)   DEFAULT NULL,
  `commentaire`           VARCHAR(500)   DEFAULT NULL,
  `est_par_defaut`        TINYINT(1)     NOT NULL DEFAULT 0,
  `created_at`            DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`            DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                         ON UPDATE CURRENT_TIMESTAMP,
  `created_by`            VARCHAR(40)    DEFAULT NULL,
  `updated_by`            VARCHAR(40)    DEFAULT NULL,
  PRIMARY KEY (`id_pic_version`),
  KEY `idx_picv_site`     (`co_regate`),
  KEY `idx_picv_defaut`   (`co_regate`, `est_par_defaut`),
  CONSTRAINT `fk_picv_site` FOREIGN KEY (`co_regate`)
    REFERENCES `trppu_site`(`co_regate`) ON DELETE RESTRICT,
  CONSTRAINT `chk_picv_dates`
    CHECK (`dt_desactivation` IS NULL OR `dt_desactivation` > `dt_activation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
--  03  trppu_pic_coefficients
-- =============================================================================
DROP TABLE IF EXISTS `trppu_pic_coefficients`;
CREATE TABLE `trppu_pic_coefficients` (
  `id_pic_coef`           BIGINT         NOT NULL AUTO_INCREMENT,
  `id_pic_version`        INT            NOT NULL,
  `co_produit`            CHAR(2)        NOT NULL,
  `jour_semaine`          ENUM('LUN','MAR','MER','JEU','VEN','SAM') NOT NULL,
  `dt_effet`              DATE           NOT NULL,
  `dt_fin_effet`          DATE           DEFAULT NULL,
  `coef_dense`            DECIMAL(7,4)   NOT NULL,
  `coef_faible1`          DECIMAL(7,4)   NOT NULL,
  `coef_faible2`          DECIMAL(7,4)   NOT NULL,
  `created_at`            DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`            DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                         ON UPDATE CURRENT_TIMESTAMP,
  `created_by`            VARCHAR(40)    DEFAULT NULL,
  PRIMARY KEY (`id_pic_coef`),
  UNIQUE KEY `uq_picc` (`id_pic_version`, `co_produit`, `jour_semaine`, `dt_effet`),
  KEY `idx_picc_produit` (`co_produit`),
  CONSTRAINT `fk_picc_version` FOREIGN KEY (`id_pic_version`)
    REFERENCES `trppu_pic_version`(`id_pic_version`) ON DELETE CASCADE,
  CONSTRAINT `fk_picc_produit` FOREIGN KEY (`co_produit`)
    REFERENCES `trppu_produit`(`co_produit`) ON DELETE RESTRICT,
  CONSTRAINT `chk_picc_dates`
    CHECK (`dt_fin_effet` IS NULL OR `dt_fin_effet` > `dt_effet`),
  CONSTRAINT `chk_picc_coefs`
    CHECK (`coef_dense` >= 0 AND `coef_faible1` >= 0 AND `coef_faible2` >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
--  04  trppu_agrebal_pdi
-- =============================================================================
DROP TABLE IF EXISTS `trppu_agrebal_pdi`;
CREATE TABLE `trppu_agrebal_pdi` (
  `id_agrebal_pdi`         BIGINT         NOT NULL AUTO_INCREMENT,
  `id_agrebal`             BIGINT         NOT NULL,
  `id_pdi`                 BIGINT         NOT NULL,
  `co_regate`              CHAR(6)        NOT NULL,
  `dt_debut_rattachement`  DATE           NOT NULL,
  `dt_fin_rattachement`    DATE           DEFAULT NULL,
  `created_at`             DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`             DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                          ON UPDATE CURRENT_TIMESTAMP,
  `created_by`             VARCHAR(40)    DEFAULT NULL,
  `updated_by`             VARCHAR(40)    DEFAULT NULL,
  PRIMARY KEY (`id_agrebal_pdi`),
  UNIQUE KEY `uq_agrpdi_courant` (`id_agrebal`, `id_pdi`, `dt_debut_rattachement`),
  KEY `idx_agrpdi_pdi`    (`id_pdi`),
  KEY `idx_agrpdi_site`   (`co_regate`),
  KEY `idx_agrpdi_actif`  (`dt_fin_rattachement`),
  CONSTRAINT `fk_agrpdi_agr`  FOREIGN KEY (`id_agrebal`)
    REFERENCES `trppu_agrebal`(`id_agrebal`) ON DELETE RESTRICT,
  CONSTRAINT `fk_agrpdi_pdi`  FOREIGN KEY (`id_pdi`)
    REFERENCES `trppu_pdi`(`id_pdi`) ON DELETE RESTRICT,
  CONSTRAINT `fk_agrpdi_site` FOREIGN KEY (`co_regate`)
    REFERENCES `trppu_site`(`co_regate`) ON DELETE RESTRICT,
  CONSTRAINT `chk_agrpdi_dates`
    CHECK (`dt_fin_rattachement` IS NULL
           OR `dt_fin_rattachement` >= `dt_debut_rattachement`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
--  05  trppu_cles_repartition  (HISTORISÉE par dt_effet)
-- =============================================================================
DROP TABLE IF EXISTS `trppu_cles_repartition`;
CREATE TABLE `trppu_cles_repartition` (
  `id_cle_repartition`  BIGINT         NOT NULL AUTO_INCREMENT,
  `id_pdi`              BIGINT         NOT NULL,
  `version`             INT            NOT NULL DEFAULT 1,
  `dt_effet`            DATE           NOT NULL,
  `dt_fin_effet`        DATE           DEFAULT NULL,
  `pct_nature`          DECIMAL(10,8)   NOT NULL,
  `pct_oo`              DECIMAL(10,8)   NOT NULL,
  `pct_os_suivi`        DECIMAL(10,8)   NOT NULL,
  `pct_os_rmp`          DECIMAL(10,8)   NOT NULL,
  `pct_os_rmp2`         DECIMAL(10,8)   NOT NULL,
  `pct_colis_suivi`     DECIMAL(10,8)   NOT NULL,
  `pct_colis_signe`     DECIMAL(10,8)   NOT NULL,
  `pct_colis`           DECIMAL(10,8)   NOT NULL,
  `pct_pf`              DECIMAL(10,8)   NOT NULL,
  `pct_gf`              DECIMAL(10,8)   NOT NULL,
  `pct_suivi`           DECIMAL(10,8)   NOT NULL,
  `pct_signe`           DECIMAL(10,8)   NOT NULL,
  `pct_3s`              DECIMAL(10,8)   NOT NULL,
  `pct_ppi`             DECIMAL(10,8)   NOT NULL,
  `pct_ip`              DECIMAL(10,8)   NOT NULL,
  `created_at`          DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`          DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                       ON UPDATE CURRENT_TIMESTAMP,
  `created_by`          VARCHAR(40)    DEFAULT NULL,
  `updated_by`          VARCHAR(40)    DEFAULT NULL,
  PRIMARY KEY (`id_cle_repartition`),
  UNIQUE KEY `uq_cles_pdi_eff` (`id_pdi`, `dt_effet`),
  KEY `idx_cles_pdi_actif` (`id_pdi`, `dt_fin_effet`),
  CONSTRAINT `fk_cles_pdi` FOREIGN KEY (`id_pdi`)
    REFERENCES `trppu_pdi`(`id_pdi`) ON DELETE RESTRICT,
  CONSTRAINT `chk_cles_dates`
    CHECK (`dt_fin_effet` IS NULL OR `dt_fin_effet` > `dt_effet`),
  CONSTRAINT `chk_cles_pcts` CHECK (
    `pct_nature`      BETWEEN 0 AND 1 AND
    `pct_oo`          BETWEEN 0 AND 1 AND
    `pct_os_suivi`    BETWEEN 0 AND 1 AND
    `pct_os_rmp`      BETWEEN 0 AND 1 AND
    `pct_os_rmp2`     BETWEEN 0 AND 1 AND
    `pct_colis_suivi` BETWEEN 0 AND 1 AND
    `pct_colis_signe` BETWEEN 0 AND 1 AND
    `pct_colis`       BETWEEN 0 AND 1 AND
    `pct_pf`          BETWEEN 0 AND 1 AND
    `pct_gf`          BETWEEN 0 AND 1 AND
    `pct_suivi`       BETWEEN 0 AND 1 AND
    `pct_signe`       BETWEEN 0 AND 1 AND
    `pct_3s`          BETWEEN 0 AND 1 AND
    `pct_ppi`         BETWEEN 0 AND 1 AND
    `pct_ip`          BETWEEN 0 AND 1
  )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
--  06  trppu_scenario
-- =============================================================================
DROP TABLE IF EXISTS `trppu_scenario`;
CREATE TABLE `trppu_scenario` (
  `id_scenario`             BIGINT         NOT NULL AUTO_INCREMENT,
  `co_roc`                  CHAR(6)        NOT NULL,
  `co_regate`               CHAR(6)        NOT NULL,
  `lb_scenario`             VARCHAR(50)    NOT NULL,
  `statut`                  ENUM('BROUILLON','SIMULATION','VALIDE',
                                 'PRODUCTION','ARCHIVE') NOT NULL,
  `dt_creation`             DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `dt_validation`           DATETIME       DEFAULT NULL,
  `dt_mise_en_prod`         DATETIME       DEFAULT NULL,
  `dt_archivage`            DATETIME       DEFAULT NULL,
  `motif_archivage`         VARCHAR(500)   DEFAULT NULL,
  `periode_debut`           DATE           NOT NULL,
  `periode_fin`             DATE           NOT NULL,
  `periode_realise_debut`   DATE           DEFAULT NULL,
  `periode_realise_fin`     DATE           DEFAULT NULL,
  `periode_prev_debut`      DATE           DEFAULT NULL,
  `periode_prev_fin`        DATE           DEFAULT NULL,
  `nb_jours_semaine`        TINYINT        NOT NULL,
  `id_pic_version`          INT            NOT NULL,
  `version_scenario`        INT            NOT NULL DEFAULT 1,
  `id_scenario_parent`      BIGINT         DEFAULT NULL,
  `est_fige`                TINYINT(1)     NOT NULL DEFAULT 0,
  `created_at`              DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`              DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                           ON UPDATE CURRENT_TIMESTAMP,
  `created_by`              VARCHAR(40)    DEFAULT NULL,
  `updated_by`              VARCHAR(40)    DEFAULT NULL,
  PRIMARY KEY (`id_scenario`),
  KEY `idx_scen_site_statut` (`co_regate`, `statut`),
  KEY `idx_scen_roc`         (`co_roc`),
  KEY `idx_scen_periode`     (`periode_debut`, `periode_fin`),
  KEY `idx_scen_parent`      (`id_scenario_parent`),
  CONSTRAINT `fk_scen_site`    FOREIGN KEY (`co_regate`)
    REFERENCES `trppu_site`(`co_regate`)        ON DELETE RESTRICT,
  CONSTRAINT `fk_scen_roc`     FOREIGN KEY (`co_roc`)
    REFERENCES `trppu_roc`(`co_roc`)            ON DELETE RESTRICT,
  CONSTRAINT `fk_scen_picv`    FOREIGN KEY (`id_pic_version`)
    REFERENCES `trppu_pic_version`(`id_pic_version`) ON DELETE RESTRICT,
  CONSTRAINT `fk_scen_parent`  FOREIGN KEY (`id_scenario_parent`)
    REFERENCES `trppu_scenario`(`id_scenario`)  ON DELETE SET NULL,
  CONSTRAINT `chk_scen_jours`        CHECK (`nb_jours_semaine` IN (5,6)),
  CONSTRAINT `chk_scen_periode`      CHECK (`periode_debut` <= `periode_fin`),
  CONSTRAINT `chk_scen_periode_real` CHECK (`periode_realise_debut` IS NULL
                                            OR `periode_realise_debut` <= `periode_realise_fin`),
  CONSTRAINT `chk_scen_periode_prev` CHECK (`periode_prev_debut` IS NULL
                                            OR `periode_prev_debut` <= `periode_prev_fin`),
  CONSTRAINT `chk_scen_workflow`     CHECK (`dt_validation` IS NULL
                                            OR `dt_validation` >= `dt_creation`),
  CONSTRAINT `chk_scen_prod`         CHECK (`dt_mise_en_prod` IS NULL
                                            OR `dt_mise_en_prod` >= `dt_validation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
--  07  trppu_scenario_exclusions
-- =============================================================================
DROP TABLE IF EXISTS `trppu_scenario_exclusions`;
CREATE TABLE `trppu_scenario_exclusions` (
  `id_exclusion`        BIGINT         NOT NULL AUTO_INCREMENT,
  `id_scenario`         BIGINT         NOT NULL,
  `co_produit`          CHAR(2)        NOT NULL,
  `motif`               VARCHAR(500)   NOT NULL,
  `created_at`          DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_by`          VARCHAR(40)    DEFAULT NULL,
  PRIMARY KEY (`id_exclusion`),
  UNIQUE KEY `uq_excl` (`id_scenario`, `co_produit`),
  KEY `idx_excl_produit` (`co_produit`),
  CONSTRAINT `fk_excl_scen`    FOREIGN KEY (`id_scenario`)
    REFERENCES `trppu_scenario`(`id_scenario`) ON DELETE CASCADE,
  CONSTRAINT `fk_excl_produit` FOREIGN KEY (`co_produit`)
    REFERENCES `trppu_produit`(`co_produit`)   ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
--  08  trppu_scenario_pic_coeffs
-- =============================================================================
DROP TABLE IF EXISTS `trppu_scenario_pic_coeffs`;
CREATE TABLE `trppu_scenario_pic_coeffs` (
  `id_scen_pic_coef`    BIGINT         NOT NULL AUTO_INCREMENT,
  `id_scenario`         BIGINT         NOT NULL,
  `co_produit`          CHAR(2)        NOT NULL,
  `jour_semaine`        ENUM('LUN','MAR','MER','JEU','VEN','SAM') NOT NULL,
  `coef_dense`          DECIMAL(7,4)   NOT NULL,
  `coef_faible1`        DECIMAL(7,4)   NOT NULL,
  `coef_faible2`        DECIMAL(7,4)   NOT NULL,
  `est_modifie`         TINYINT(1)     NOT NULL DEFAULT 0,
  `created_at`          DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`          DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                       ON UPDATE CURRENT_TIMESTAMP,
  `created_by`          VARCHAR(40)    DEFAULT NULL,
  `updated_by`          VARCHAR(40)    DEFAULT NULL,
  PRIMARY KEY (`id_scen_pic_coef`),
  UNIQUE KEY `uq_spc` (`id_scenario`, `co_produit`, `jour_semaine`),
  KEY `idx_spc_scen`     (`id_scenario`),
  KEY `idx_spc_modifie`  (`id_scenario`, `est_modifie`),
  CONSTRAINT `fk_spc_scen`    FOREIGN KEY (`id_scenario`)
    REFERENCES `trppu_scenario`(`id_scenario`) ON DELETE CASCADE,
  CONSTRAINT `fk_spc_produit` FOREIGN KEY (`co_produit`)
    REFERENCES `trppu_produit`(`co_produit`)   ON DELETE RESTRICT,
  CONSTRAINT `chk_spc_coefs`
    CHECK (`coef_dense` >= 0 AND `coef_faible1` >= 0 AND `coef_faible2` >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
--  09  trppu_scenario_variations_prev
-- =============================================================================
DROP TABLE IF EXISTS `trppu_scenario_variations_prev`;
CREATE TABLE `trppu_scenario_variations_prev` (
  `id_variation`        BIGINT         NOT NULL AUTO_INCREMENT,
  `id_scenario`         BIGINT         NOT NULL,
  `co_produit`          CHAR(2)        NOT NULL,
  `variation_pct`       DECIMAL(6,2)   NOT NULL,
  `motif`               VARCHAR(500)   DEFAULT NULL,
  `created_at`          DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_by`          VARCHAR(40)    DEFAULT NULL,
  PRIMARY KEY (`id_variation`),
  UNIQUE KEY `uq_var_scen_prod` (`id_scenario`, `co_produit`),
  KEY `idx_var_produit` (`co_produit`),
  CONSTRAINT `fk_var_scen`    FOREIGN KEY (`id_scenario`)
    REFERENCES `trppu_scenario`(`id_scenario`) ON DELETE CASCADE,
  CONSTRAINT `fk_var_produit` FOREIGN KEY (`co_produit`)
    REFERENCES `trppu_produit`(`co_produit`)   ON DELETE RESTRICT,
  CONSTRAINT `chk_var_borne`
    CHECK (`variation_pct` BETWEEN -100 AND 100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
--  10  trppu_scenario_comptages_manuels
-- =============================================================================
DROP TABLE IF EXISTS `trppu_scenario_comptages_manuels`;
CREATE TABLE `trppu_scenario_comptages_manuels` (
  `id_comptage`         BIGINT         NOT NULL AUTO_INCREMENT,
  `id_scenario`         BIGINT         NOT NULL,
  `dt_comptage`         DATE           NOT NULL,
  `co_produit`          CHAR(2)        NOT NULL,
  `nb_produit`          INT            NOT NULL,
  `source_donnee`       ENUM('MANUEL','IMPORT','CORRECTION') NOT NULL DEFAULT 'MANUEL',
  `motif`               VARCHAR(500)   DEFAULT NULL,
  `dt_saisie`           DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_by`          VARCHAR(40)    DEFAULT NULL,
  PRIMARY KEY (`id_comptage`),
  UNIQUE KEY `uq_compt` (`id_scenario`, `dt_comptage`, `co_produit`),
  KEY `idx_compt_dt` (`dt_comptage`),
  CONSTRAINT `fk_compt_scen`    FOREIGN KEY (`id_scenario`)
    REFERENCES `trppu_scenario`(`id_scenario`) ON DELETE CASCADE,
  CONSTRAINT `fk_compt_produit` FOREIGN KEY (`co_produit`)
    REFERENCES `trppu_produit`(`co_produit`)   ON DELETE RESTRICT,
  CONSTRAINT `chk_compt_nb` CHECK (`nb_produit` >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
--  11  trppu_neutralisations
-- =============================================================================
DROP TABLE IF EXISTS `trppu_neutralisations`;
CREATE TABLE `trppu_neutralisations` (
  `id_neutralisation`   BIGINT         NOT NULL AUTO_INCREMENT,
  `id_scenario`         BIGINT         NOT NULL,
  `dt_debut`            DATE           NOT NULL,
  `dt_fin`              DATE           NOT NULL,
  `nb_jour`             INT            NOT NULL,
  `type`                ENUM('FERIE','PEAK','LOCAL') NOT NULL,
  `motif`               VARCHAR(500)   DEFAULT NULL,
  `created_at`          DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_by`          VARCHAR(40)    DEFAULT NULL,
  PRIMARY KEY (`id_neutralisation`),
  UNIQUE KEY `uq_neutre` (`id_scenario`, `dt_debut`, `type`),
  KEY `idx_neutre_periode` (`id_scenario`, `dt_debut`, `dt_fin`),
  CONSTRAINT `fk_neutre_scen` FOREIGN KEY (`id_scenario`)
    REFERENCES `trppu_scenario`(`id_scenario`) ON DELETE CASCADE,
  CONSTRAINT `chk_neutre_dates` CHECK (`dt_debut` <= `dt_fin`),
  CONSTRAINT `chk_neutre_jour`  CHECK (`nb_jour` > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
--  12  trppu_tmh
-- =============================================================================
DROP TABLE IF EXISTS `trppu_tmh`;
CREATE TABLE `trppu_tmh` (
  `id_tmh`                BIGINT         NOT NULL AUTO_INCREMENT,
  `id_scenario`           BIGINT         NOT NULL,
  `co_produit`            CHAR(2)        NOT NULL,
  `volume_realise`        INT            DEFAULT NULL,
  `volume_previsionnel`   INT            DEFAULT NULL,
  `moyenne_journaliere`   DECIMAL(12,2)  NOT NULL,
  `semaine_moyenne`       DECIMAL(12,2)  NOT NULL,
  `dt_calcul`             DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `version_calcul`        INT            NOT NULL DEFAULT 1,
  `commentaire_calcul`    VARCHAR(500)   DEFAULT NULL,
  PRIMARY KEY (`id_tmh`),
  UNIQUE KEY `uq_tmh` (`id_scenario`, `co_produit`),
  KEY `idx_tmh_produit` (`co_produit`, `id_scenario`),
  CONSTRAINT `fk_tmh_scen`    FOREIGN KEY (`id_scenario`)
    REFERENCES `trppu_scenario`(`id_scenario`) ON DELETE CASCADE,
  CONSTRAINT `fk_tmh_produit` FOREIGN KEY (`co_produit`)
    REFERENCES `trppu_produit`(`co_produit`)   ON DELETE RESTRICT,
  CONSTRAINT `chk_tmh_volumes`
    CHECK ((`volume_realise`      IS NULL OR `volume_realise`      >= 0)
       AND (`volume_previsionnel` IS NULL OR `volume_previsionnel` >= 0))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
--  13  trppu_calcul_batch  (NOUVELLE — référence des runs de calcul)
-- =============================================================================
DROP TABLE IF EXISTS `trppu_calcul_batch`;
CREATE TABLE `trppu_calcul_batch` (
  `id_calcul_batch`     BIGINT         NOT NULL AUTO_INCREMENT,
  `id_scenario`         BIGINT         NOT NULL,
  `dt_debut`            DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `dt_fin`              DATETIME       DEFAULT NULL,
  `statut`              ENUM('EN_COURS','SUCCES','ECHEC','PARTIEL') NOT NULL,
  `nb_lignes_agrebal`   BIGINT         DEFAULT NULL,
  `nb_lignes_pdi`       BIGINT         DEFAULT NULL,
  `commentaire`         VARCHAR(500)   DEFAULT NULL,
  `created_by`          VARCHAR(40)    DEFAULT NULL,
  PRIMARY KEY (`id_calcul_batch`),
  KEY `idx_batch_scen` (`id_scenario`, `dt_debut`),
  CONSTRAINT `fk_batch_scen` FOREIGN KEY (`id_scenario`)
    REFERENCES `trppu_scenario`(`id_scenario`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
--  14  trppu_trafic_agrebal  [PARTITIONNÉE par HASH(id_scenario)]
-- -----------------------------------------------------------------------------
--  NB : les FK ne sont pas autorisées sur les tables partitionnées.
--       L'intégrité est garantie par l'application (insertion via le moteur
--       de calcul, jamais saisie manuelle).
-- =============================================================================
DROP TABLE IF EXISTS `trppu_trafic_agrebal`;
CREATE TABLE `trppu_trafic_agrebal` (
  `id_trafic_agrebal`   BIGINT         NOT NULL AUTO_INCREMENT,
  `id_scenario`         BIGINT         NOT NULL,
  `co_regate`           CHAR(6)        NOT NULL,
  `id_agrebal`          BIGINT         NOT NULL,
  `co_produit`          CHAR(2)        NOT NULL,
  `jour_semaine`        ENUM('LUN','MAR','MER','JEU','VEN','SAM') NOT NULL,
  `couleur_pic`         ENUM('DENSE','FAIBLE1','FAIBLE2') NOT NULL,
  `volume`              DECIMAL(12,2)  NOT NULL,
  `dt_calcul`           DATETIME       NOT NULL,
  `id_calcul_batch`     BIGINT         NOT NULL,
  PRIMARY KEY (`id_trafic_agrebal`, `id_scenario`),
  KEY `idx_ta_scen_site_agr` (`id_scenario`, `co_regate`, `id_agrebal`),
  KEY `idx_ta_agr_prod`      (`id_agrebal`, `co_produit`),
  KEY `idx_ta_batch`         (`id_calcul_batch`),
  CONSTRAINT `chk_ta_volume` CHECK (`volume` >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
PARTITION BY HASH(`id_scenario`) PARTITIONS 16;


-- =============================================================================
--  15  trppu_trafic_pdi  [PARTITIONNÉE par HASH(id_scenario), 32 partitions]
-- =============================================================================
DROP TABLE IF EXISTS `trppu_trafic_pdi`;
CREATE TABLE `trppu_trafic_pdi` (
  `id_trafic_pdi`       BIGINT         NOT NULL AUTO_INCREMENT,
  `id_scenario`         BIGINT         NOT NULL,
  `co_regate`           CHAR(6)        NOT NULL,
  `id_agrebal`          BIGINT         NOT NULL,
  `id_pdi`              BIGINT         NOT NULL,
  `co_produit`          CHAR(2)        NOT NULL,
  `jour_semaine`        ENUM('LUN','MAR','MER','JEU','VEN','SAM') NOT NULL,
  `couleur_pic`         ENUM('DENSE','FAIBLE1','FAIBLE2') NOT NULL,
  `volume`              DECIMAL(12,2)  NOT NULL,
  `dt_calcul`           DATETIME       NOT NULL,
  `id_calcul_batch`     BIGINT         NOT NULL,
  PRIMARY KEY (`id_trafic_pdi`, `id_scenario`),
  KEY `idx_tp_scen_agr_prod_jour` (`id_scenario`, `id_agrebal`, `co_produit`, `jour_semaine`),
  KEY `idx_tp_pdi_agr`            (`id_pdi`, `id_agrebal`),
  KEY `idx_tp_site`               (`co_regate`),
  KEY `idx_tp_batch`              (`id_calcul_batch`),
  CONSTRAINT `chk_tp_volume` CHECK (`volume` >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
PARTITION BY HASH(`id_scenario`) PARTITIONS 32;


-- =============================================================================
--  16  trppu_recalcul_log
-- =============================================================================
DROP TABLE IF EXISTS `trppu_recalcul_log`;
CREATE TABLE `trppu_recalcul_log` (
  `id_log`               BIGINT         NOT NULL AUTO_INCREMENT,
  `id_scenario`          BIGINT         NOT NULL,
  `id_agrebal`           BIGINT         DEFAULT NULL,
  `dt_recalcul`          DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `raison`               ENUM('TOPIC_AGREBAL','MANUEL','SYSTEME') NOT NULL,
  `statut_recalcul`      ENUM('SUCCES','ECHEC','PARTIEL') NOT NULL,
  `duree_ms`             INT            DEFAULT NULL,
  `nb_lignes_impactees`  BIGINT         DEFAULT NULL,
  `user_declencheur`     VARCHAR(40)    DEFAULT NULL,
  `commentaire`          VARCHAR(500)   DEFAULT NULL,
  PRIMARY KEY (`id_log`),
  KEY `idx_recalc_scen`   (`id_scenario`, `dt_recalcul`),
  KEY `idx_recalc_raison` (`raison`, `dt_recalcul`),
  CONSTRAINT `fk_recalc_scen` FOREIGN KEY (`id_scenario`)
    REFERENCES `trppu_scenario`(`id_scenario`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- =============================================================================
--  17  trppu_api_log  [PARTITIONNÉE par mois — purge > 90 jours]
-- =============================================================================
DROP TABLE IF EXISTS `trppu_api_log`;
CREATE TABLE `trppu_api_log` (
  `id_log`              BIGINT         NOT NULL AUTO_INCREMENT,
  `dt_appel`            DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `id_rh`               VARCHAR(40)    DEFAULT NULL,
  `profil`              VARCHAR(20)    DEFAULT NULL,
  `api_name`            VARCHAR(80)    NOT NULL,
  `id_scenario`         BIGINT         DEFAULT NULL,
  `co_regate`           CHAR(6)        DEFAULT NULL,
  `caller`              VARCHAR(120)   DEFAULT NULL,
  `http_status`         SMALLINT       DEFAULT NULL,
  `duree_ms`            INT            DEFAULT NULL,
  `response_size`       INT            DEFAULT NULL,
  `ip_source`           VARCHAR(45)    DEFAULT NULL,
  `params`              JSON           DEFAULT NULL,
  PRIMARY KEY (`id_log`, `dt_appel`),
  KEY `idx_api_when`     (`api_name`, `dt_appel`),
  KEY `idx_api_user`     (`id_rh`, `dt_appel`),
  KEY `idx_api_scenario` (`id_scenario`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
PARTITION BY RANGE (TO_DAYS(`dt_appel`)) (
  PARTITION p2026_05 VALUES LESS THAN (TO_DAYS('2026-06-01')),
  PARTITION p2026_06 VALUES LESS THAN (TO_DAYS('2026-07-01')),
  PARTITION p2026_07 VALUES LESS THAN (TO_DAYS('2026-08-01')),
  PARTITION p2026_08 VALUES LESS THAN (TO_DAYS('2026-09-01')),
  PARTITION p_future VALUES LESS THAN MAXVALUE
);


SET FOREIGN_KEY_CHECKS = 1;

-- =============================================================================
--  Fin du script
-- =============================================================================