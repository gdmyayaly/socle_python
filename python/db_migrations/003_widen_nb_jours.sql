-- DSR — Socle : élargissement des compteurs de jours d'un scénario.
-- `nb_jours_ouvres` / `nb_jours_ouvrables` / `nb_jours_scenario` sont en TINYINT
-- (max 127) mais comptent les jours sur TOUTE la période du scénario : sur une
-- période longue (jusqu'à 730 j ≈ ~520 jours ouvrés) la valeur dépasse 127.
-- Symptôme corrigé : `DataError (1264) Out of range value for column 'nb_jours_ouvres'`
-- à la création/MAJ d'un scénario (période par défaut = today-1an / today+1an).
-- Passage en SMALLINT (max 32767). `nb_jours_semaine` reste TINYINT (valeurs 5/6).
-- Base : trppu

USE `trppu`;

ALTER TABLE `trppu_scenario`
  MODIFY COLUMN `nb_jours_ouvres`    SMALLINT NULL,
  MODIFY COLUMN `nb_jours_ouvrables` SMALLINT NULL,
  MODIFY COLUMN `nb_jours_scenario`  SMALLINT NULL;
