

DROP TABLE IF EXISTS trppu_scenario;
CREATE TABLE trppu_scenario (
    id_scenario BIGINT AUTO_INCREMENT PRIMARY KEY,
    co_roc CHAR(6) NOT NULL,
    co_regate CHAR(6) NOT NULL,
    lb_scenario VARCHAR(20) NOT NULL,
    statut ENUM('EN COURS','SIMULATION','VALIDE','VERROUILLE','ARCHIVE') NOT NULL,
    dt_creation DATETIME NOT NULL,
    dt_validation DATETIME NULL,
    dt_mise_en_prod DATETIME NULL,

    /* Fenêtre CTR (≤ 24 mois) */
    periode_debut DATE NOT NULL,
    periode_fin DATE NOT NULL,
    periode_realise_debut DATE NULL,
    periode_realise_fin DATE NULL,
    periode_prev_debut DATE NULL,
    periode_prev_fin DATE NULL, 

    /* PIC utilisé dans le scénario (scénarisé puis figé) */
    id_pic_version INT NOT NULL,

    /* Informations complémentaires */
    version_scenario INT NOT NULL,
    est_fige BOOLEAN DEFAULT FALSE,

    INDEX idx_scenario_site_statut (co_regate, statut)
);


DROP TABLE IF EXISTS trppu_scenario_variations_prev;
CREATE TABLE trppu_scenario_variations_prev (
  id_variation       INT AUTO_INCREMENT PRIMARY KEY,
  id_scenario  BIGINT NOT NULL,
  co_produit CHAR(2) NOT NULL,
  variation_pct DECIMAL(5,2) NOT NULL,
  FOREIGN KEY (id_scenario) REFERENCES trppu_scenario(id_scenario),
  UNIQUE KEY uq_scen_prod (id_scenario, co_produit)
);

DROP TABLE IF EXISTS trppu_scenario_comptages_manuels;
CREATE TABLE trppu_scenario_comptages_manuels (
 id_comptage INT AUTO_INCREMENT PRIMARY KEY,
 id_scenario BIGINT NOT NULL,
 dt_comptage DATE NOT NULL,
 co_produit CHAR(2) NOT NULL,
 nb_produit INT NOT NULL,
 FOREIGN KEY (id_scenario) REFERENCES trppu_scenario(id_scenario),
 INDEX idx_scm (id_scenario, dt_comptage, co_produit)
);

DROP TABLE IF EXISTS trppu_pic_version;
CREATE TABLE trppu_pic_version (
id_pic_version INT AUTO_INCREMENT PRIMARY KEY,
lb_pic_version VARCHAR(50),
niveau ENUM('NATIONAL','DEX','SITE') NOT NULL,
co_regate CHAR(6) NOT NULL,
dt_activation DATETIME NOT NULL,
dt_desactivation DATETIME NULL,
INDEX idx_pic_site (co_regate)
);


DROP TABLE IF EXISTS trppu_pic_coefficients;
CREATE TABLE trppu_pic_coefficients (
 id_pic_coef       BIGINT AUTO_INCREMENT PRIMARY KEY,
 id_pic_version INT NOT NULL,
 co_produit  CHAR(2) NOT NULL,
 jour_semaine ENUM('LUN','MAR','MER','JEU','VEN','SAM') NOT NULL,
 dt_effet DATE NOT NULL, -- ajouté : date pour le jour/produit   – ??
 coef_dense DECIMAL(8,5) NOT NULL,
 coef_faible1 DECIMAL(8,5) NOT NULL,
 coef_faible2 DECIMAL(8,5) NOT NULL,
 FOREIGN KEY (id_pic_version) REFERENCES trppu_pic_version(id_pic_version),
 UNIQUE KEY uq_tpc (id_pic_version, co_produit, jour_semaine, dt_effet)
);



DROP TABLE IF EXISTS trppu_scenario_pic_coeffs;
CREATE TABLE trppu_scenario_pic_coeffs (
id BIGINT AUTO_INCREMENT PRIMARY KEY,
id_scenario BIGINT NOT NULL,
co_produit CHAR(2) NOT NULL,
jour_semaine ENUM('LUN','MAR','MER','JEU','VEN','SAM') NOT NULL,
coef_dense DECIMAL(8,5) NOT NULL,
coef_faible1 DECIMAL(8,5) NOT NULL,
coef_faible2 DECIMAL(8,5) NOT NULL,
FOREIGN KEY (id_scenario) REFERENCES trppu_scenario(id_scenario),
UNIQUE KEY uq_tspc (id_scenario, co_produit, jour_semaine)
);


DROP TABLE IF EXISTS trppu_neutralisations;
CREATE TABLE trppu_neutralisations (
id BIGINT AUTO_INCREMENT PRIMARY KEY,
id_scenario BIGINT NOT NULL,
dt_debut DATE NOT NULL,
dt_fin DATE NOT NULL,
nb_jour INT NOT NULL,
type ENUM('FERIE','PEAK','LOCAL') NOT NULL,
FOREIGN KEY (id_scenario) REFERENCES trppu_scenario(id_scenario),
UNIQUE KEY uq_neutre (id_scenario, dt_debut, type)
);



DROP TABLE IF EXISTS trppu_scenario_exclusions;
CREATE TABLE trppu_scenario_exclusions (
 id BIGINT AUTO_INCREMENT PRIMARY KEY,
 id_scenario BIGINT NOT NULL,
 co_produit CHAR(2) NOT NULL,
 motif VARCHAR(255) NULL,
 FOREIGN KEY (id_scenario) REFERENCES trppu_scenario(id_scenario),
 UNIQUE KEY uq_exclusion (id_scenario, co_produit)
);



DROP TABLE IF EXISTS trppu_tmh;
CREATE TABLE trppu_tmh (
 id_tmh BIGINT AUTO_INCREMENT PRIMARY KEY,
 id_scenario BIGINT NOT NULL,
 co_produit   CHAR(2) NOT NULL,
 volume_realise INT NULL,
 volume_previsionnel INT NULL,
 moyenne_journaliere DECIMAL(12,4) NOT NULL,
 semaine_moyenne DECIMAL(12,4) NOT NULL,
 FOREIGN KEY (id_scenario) REFERENCES trppu_scenario(id_scenario),
 UNIQUE KEY uq_tmh (id_scenario, co_produit)
);


DROP TABLE IF EXISTS trppu_cles_repartition;
CREATE TABLE trppu_cles_repartition (
 id_pdi      BIGINT PRIMARY KEY,
 pct_nature    DECIMAL(10,8) NOT NULL,
 pct_oo      DECIMAL(10,8) NOT NULL,
 pct_os_suivi   DECIMAL(10,8) NOT NULL,
 pct_os_rmp    DECIMAL(10,8) NOT NULL,
 pct_os_rmp2   DECIMAL(10,8) NOT NULL,
 pct_colis_suivi DECIMAL(10,8) NOT NULL,
 pct_colis_signe DECIMAL(10,8) NOT NULL,
 pct_colis    DECIMAL(10,8) NOT NULL,
 pct_pf      DECIMAL(10,8) NOT NULL,
 pct_gf      DECIMAL(10,8) NOT NULL,
 pct_suivi    DECIMAL(10,8) NOT NULL,
 pct_signe    DECIMAL(10,8) NOT NULL,
 pct_3s      DECIMAL(10,8) NOT NULL,
 pct_ppi     DECIMAL(10,8) NOT NULL,
 pct_ip      DECIMAL(10,8) NOT NULL
);


DROP TABLE IF EXISTS trppu_amas_pdi;
CREATE TABLE trppu_amas_pdi (
 id_amas INT NOT NULL,
 id_pdi BIGINT NOT NULL,
 co_regate CHAR(6) NOT NULL,           
 PRIMARY KEY (id_amas, id_pdi),
 INDEX idx_regate (co_regate),
 INDEX idx_pdi (id_pdi)
);

DROP TABLE IF EXISTS trppu_produit;
CREATE TABLE trppu_produit (
 co_produit  CHAR(2) PRIMARY KEY,
 lb_produit  VARCHAR(50)
);


DROP TABLE IF EXISTS trppu_trafic_pdi;
CREATE TABLE trppu_trafic_pdi (
 id BIGINT AUTO_INCREMENT PRIMARY KEY,
 id_scenario BIGINT NOT NULL,
 co_regate CHAR(6) NOT NULL,     
 id_amas INT NOT NULL,           
 id_pdi BIGINT NOT NULL,
 co_produit CHAR(2) NOT NULL,
 jour_semaine ENUM('LUN','MAR','MER','JEU','VEN','SAM') NOT NULL,
 couleur_pic ENUM('DENSE','FAIBLE1','FAIBLE2') NOT NULL,
 volume DECIMAL(12,4) NOT NULL,
 FOREIGN KEY (id_scenario) REFERENCES trppu_scenario(id_scenario),
 INDEX idx_pdi_amas (id_pdi, id_amas),
 INDEX idx_scen_amas_prod_jour (id_scenario, id_amas, co_produit, jour_semaine),
 INDEX idx_site (co_regate)
);


DROP TABLE IF EXISTS trppu_trafic_amas;
CREATE TABLE trppu_trafic_amas (
 id BIGINT AUTO_INCREMENT PRIMARY KEY,
 id_scenario BIGINT NOT NULL,
 co_regate CHAR(6) NOT NULL, -- ajouté
 id_amas   INT NOT NULL,
 co_produit CHAR(2) NOT NULL,
 jour_semaine ENUM('LUN','MAR','MER','JEU','VEN','SAM') NOT NULL,
 couleur_pic ENUM('DENSE','FAIBLE1','FAIBLE2') NOT NULL,
 volume DECIMAL(12,4) NOT NULL,
 FOREIGN KEY (id_scenario) REFERENCES trppu_scenario(id_scenario),
 INDEX idx_scen_site_amas (id_scenario, co_regate, id_amas),
 INDEX idx_amas_prod (id_amas, co_produit)
);


DROP TABLE IF EXISTS trppu_recalcul_log;
CREATE TABLE trppu_recalcul_log (
 id_log BIGINT AUTO_INCREMENT PRIMARY KEY,
 id_scenario BIGINT NOT NULL,
 id_amas INT NULL,
 dt_recalcul DATETIME NOT NULL,
 raison ENUM('TOPIC_AMAS','MANUEL','SYSTEME') NOT NULL,
 commentaire VARCHAR(255) NULL, 
 FOREIGN KEY (id_scenario) REFERENCES trppu_scenario(id_scenario),
 INDEX idx_log_scenario (id_scenario, dt_recalcul)
);


DROP TABLE IF EXISTS trppu_api_log;
CREATE TABLE trppu_api_log (
 id_log BIGINT AUTO_INCREMENT PRIMARY KEY,
 api_name VARCHAR(50),
 id_scenario BIGINT NULL,
 regate CHAR(6) NULL,
 dt_appel DATETIME NOT NULL,
 caller VARCHAR(120) NULL,
 params JSON NULL,
 FOREIGN KEY (id_scenario) REFERENCES trppu_scenario(id_scenario),
 INDEX idx_api_when (api_name, dt_appel)
);
