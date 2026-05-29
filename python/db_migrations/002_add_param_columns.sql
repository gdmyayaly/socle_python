-- DSR — Socle (NEW-1) : colonnes manquantes + enum SAISON sur les tables de paramètres.
-- DSR-644 (comptages), DSR-645 (neutralisations), DSR-646 (variations).
-- Base : trppu

USE `trppu`;

-- DSR-644 : comptages manuels — traçabilité utilisateur (id_rh crypté)
ALTER TABLE `trppu_scenario_comptages_manuels`
  ADD COLUMN `id_rh` VARCHAR(255) NULL AFTER `nb_produit`;

-- DSR-646 : variations prévisionnelles — date + id_rh crypté
ALTER TABLE `trppu_scenario_variations_prev`
  ADD COLUMN `dt_creation` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER `variation_pct`,
  ADD COLUMN `id_rh` VARCHAR(255) NULL AFTER `dt_creation`;

-- DSR-645 : neutralisations — type SAISON (remplace LOCAL), date + id_rh crypté
-- NB : valider la reprise d'éventuelles données existantes 'LOCAL' avant migration.
ALTER TABLE `trppu_neutralisations`
  MODIFY COLUMN `type` ENUM('FERIE','PEAK','SAISON') NOT NULL,
  ADD COLUMN `dt_creation` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER `type`,
  ADD COLUMN `id_rh` VARCHAR(255) NULL AFTER `dt_creation`;
