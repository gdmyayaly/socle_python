-- Élargit les colonnes de trafic de trppu_trafic_pdi : smallint unsigned (max 65 535) trop
-- étroit pour un gros site (prod 12/10/2026 : scénario 15, site 133950, OO/LUNDI = 108 716).
-- À jouer avant de déployer le YB05 où le plafond vient de la variable TRAFIC_MAX.
ALTER TABLE `trppu_trafic_pdi`
  MODIFY COLUMN `dense`   decimal(12,4) DEFAULT NULL,
  MODIFY COLUMN `faible1` decimal(12,4) DEFAULT NULL,
  MODIFY COLUMN `faible2` decimal(12,4) DEFAULT NULL;
