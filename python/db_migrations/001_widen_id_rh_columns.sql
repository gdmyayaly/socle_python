-- DSR — Socle (NEW-1) : élargissement des colonnes id_rh pour héberger un token Fernet.
-- Un token Fernet (~100+ caractères) ne tient pas dans VARCHAR(40).
-- À appliquer AVANT toute écriture d'id_rh crypté.
-- Base : trppu

USE `trppu`;

ALTER TABLE `trppu_scenario`
  MODIFY COLUMN `id_rh_creation` VARCHAR(255) NULL,
  MODIFY COLUMN `id_rh_maj`      VARCHAR(255) NULL;

ALTER TABLE `trppu_pic_version`
  MODIFY COLUMN `id_rh_creation` VARCHAR(255) NULL,
  MODIFY COLUMN `id_rh_maj`      VARCHAR(255) NULL;

ALTER TABLE `trppu_pic_coefficients`
  MODIFY COLUMN `id_rh` VARCHAR(255) NULL;
