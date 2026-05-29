-- DSR — Socle (NEW-3) : table des jours fériés nationaux (source pour DSR-613 / DSR-645).
-- Alimentée par db_migrations/004_seed_trppu_jours_feries.sql (généré).
-- Base : trppu

USE `trppu`;

CREATE TABLE IF NOT EXISTS `trppu_jours_feries` (
  `dt` DATE NOT NULL,
  `libelle` VARCHAR(80) NOT NULL,
  `est_national` TINYINT(1) NOT NULL DEFAULT 1,
  PRIMARY KEY (`dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
