-- DSR-646 : restaure la traçabilité des variations du trafic prévisionnel.
-- Le ticket DSR-646 demande de tracer `dt_creation` et `id_rh` (crypté) à chaque
-- ajout / modification d'une variation, mais la table `trppu_scenario_variations_prev`
-- (dump db_10_09_2026) ne possède aucune de ces colonnes. On les ajoute pour permettre
-- le stockage : `id_rh` crypté (Fernet) côté applicatif, `dt_creation` = date du jour.
-- Mêmes types que `trppu_neutralisations` (cohérence : datetime DEFAULT NOW + varchar(255)).
-- Base : trppu

USE `trppu`;

ALTER TABLE `trppu_scenario_variations_prev`
  ADD COLUMN `dt_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN `id_rh` varchar(255) DEFAULT NULL;
