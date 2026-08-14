-- =====================================================================================
-- Purge des données produites par les modules TRPPU (YS04) — base `dsr_mercure_aa`
-- =====================================================================================
-- Objet : remettre à plat les données créées/modifiées par l'API entre deux campagnes de
-- test, SANS toucher aux référentiels alimentés par les autres chaînes (clés de
-- répartition, agrebal, staging…).
--
-- Le périmètre reprend `SCENARIO_CHILD_TABLES` de `app/routes/trppu_scenario/helpers.py` :
-- c'est la même liste que celle utilisée par la suppression en cascade d'un scénario, donc
-- la purge ne peut pas diverger du comportement de l'application.
--
-- ORDRE DE LECTURE
--   §0 paramètres        §1 état avant       §2 purge (périmètre par défaut)
--   §3 AUTO_INCREMENT    §4 état après       §5 purges optionnelles (référentiels)
--   §6 tables hors périmètre — à ne pas purger
--
-- USAGE
--   mysql -h <hote> -u <user> -p dsr_mercure_aa < db/purge_test.sql
--   (les §2 à §4 sont dans une transaction : tant que le COMMIT n'est pas atteint,
--    un ROLLBACK annule tout.)
-- =====================================================================================

USE `dsr_mercure_aa`;


-- =====================================================================================
-- §0. PARAMÈTRES — laisser les trois à NULL pour une purge totale du périmètre module
-- =====================================================================================
-- Les filtres se combinent (ET). Ils ne s'appliquent qu'aux scénarios et à leurs enfants ;
-- les référentiels du §5 ne sont pas filtrés.

SET @co_regate  := NULL;   -- ex. '400300' — ne purger que ce site
SET @statut     := NULL;   -- ex. 'ARCHIVE' — 'EN COURS' | 'SIMULATION' | 'VALIDE' | 'EN PRODUCTION' | 'ARCHIVE'
SET @cree_avant := NULL;   -- ex. '2026-08-01' — ne purger que les scénarios créés avant cette date

-- Sélection figée une bonne fois : tous les DELETE du §2 s'y rattachent, donc un scénario
-- créé pendant l'exécution ne peut pas être à moitié purgé.
DROP TEMPORARY TABLE IF EXISTS `purge_scenarios`;
CREATE TEMPORARY TABLE `purge_scenarios` (
  `id_scenario` bigint NOT NULL,
  PRIMARY KEY (`id_scenario`)
) ENGINE=InnoDB;

INSERT INTO `purge_scenarios` (`id_scenario`)
SELECT `id_scenario`
FROM `trppu_scenario`
WHERE (@co_regate  IS NULL OR `co_regate`   = @co_regate)
  AND (@statut     IS NULL OR `statut`      = @statut)
  AND (@cree_avant IS NULL OR `dt_creation` < @cree_avant);

SELECT COUNT(*) AS scenarios_a_purger FROM `purge_scenarios`;


-- =====================================================================================
-- §1. ÉTAT AVANT
-- =====================================================================================
-- `COUNT(*)` exact, et non `information_schema.TABLE_ROWS` : sur InnoDB cette dernière est
-- une estimation (db/count.json annonce 22 395 341 clés de répartition pour un
-- AUTO_INCREMENT à 24 217 441).

SELECT 'trppu_scenario'                   AS `table`, COUNT(*) AS `avant` FROM `trppu_scenario`
UNION ALL SELECT 'trppu_tmh',                         COUNT(*) FROM `trppu_tmh`
UNION ALL SELECT 'trppu_neutralisations',             COUNT(*) FROM `trppu_neutralisations`
UNION ALL SELECT 'trppu_scenario_comptages_manuels',  COUNT(*) FROM `trppu_scenario_comptages_manuels`
UNION ALL SELECT 'trppu_scenario_exclusions',         COUNT(*) FROM `trppu_scenario_exclusions`
UNION ALL SELECT 'trppu_scenario_pic_coeffs',         COUNT(*) FROM `trppu_scenario_pic_coeffs`
UNION ALL SELECT 'trppu_scenario_variations_prev',    COUNT(*) FROM `trppu_scenario_variations_prev`
UNION ALL SELECT 'trppu_trafic_agrebal',              COUNT(*) FROM `trppu_trafic_agrebal`
UNION ALL SELECT 'trppu_trafic_pdi',                  COUNT(*) FROM `trppu_trafic_pdi`
UNION ALL SELECT 'trppu_pic_version',                 COUNT(*) FROM `trppu_pic_version`
UNION ALL SELECT 'trppu_pic_coefficients',            COUNT(*) FROM `trppu_pic_coefficients`
UNION ALL SELECT 'trppu_api_log',                     COUNT(*) FROM `trppu_api_log`
UNION ALL SELECT 'trppu_recalcul_log',                COUNT(*) FROM `trppu_recalcul_log`
UNION ALL SELECT 'trppu_produit    (§5, conservé)',   COUNT(*) FROM `trppu_produit`
UNION ALL SELECT 'trppu_site       (§5, conservé)',   COUNT(*) FROM `trppu_site`;


-- =====================================================================================
-- §2. PURGE DU PÉRIMÈTRE MODULE
-- =====================================================================================
-- Suppression enfants -> parent. Nécessaire pour les FK en ON DELETE RESTRICT (comptages,
-- exclusions, scenario_pic_coeffs, trafic_agrebal, api_log, recalcul_log) et sans effet de
-- bord pour celles en CASCADE (neutralisations, tmh, variations) : le DELETE explicite est
-- alors simplement un no-op.
--
-- `FOREIGN_KEY_CHECKS` n'est volontairement PAS désactivé : si un DELETE échoue sur une FK,
-- c'est qu'une table hors de cette liste référence le scénario — il faut le voir, pas le
-- contourner.

START TRANSACTION;

-- 2.1 Logs applicatifs rattachés au scénario.
-- L'application les préserve pour la traçabilité (cf. le commentaire de
-- SCENARIO_CHILD_TABLES), mais leurs FK sont en RESTRICT : sans ce DELETE, la suppression
-- du scénario échoue. Sur une base de test, l'historique n'a pas de valeur.
DELETE l FROM `trppu_api_log`      l JOIN `purge_scenarios` p ON p.`id_scenario` = l.`id_scenario`;
DELETE l FROM `trppu_recalcul_log` l JOIN `purge_scenarios` p ON p.`id_scenario` = l.`id_scenario`;

-- 2.2 Coefficients PIC portés par les versions des scénarios purgés.
-- La FK `fk_picc_version` est en CASCADE, mais on supprime explicitement pour que le
-- décompte des lignes touchées soit lisible.
DELETE c
FROM `trppu_pic_coefficients` c
JOIN `trppu_pic_version` v ON v.`id_pic_version` = c.`id_pic_version`
JOIN `purge_scenarios`   p ON p.`id_scenario`    = v.`id_scenario`;

-- 2.3 Données opérationnelles du scénario (ordre de SCENARIO_CHILD_TABLES).
DELETE t FROM `trppu_neutralisations`            t JOIN `purge_scenarios` p ON p.`id_scenario` = t.`id_scenario`;
DELETE t FROM `trppu_tmh`                        t JOIN `purge_scenarios` p ON p.`id_scenario` = t.`id_scenario`;
DELETE t FROM `trppu_scenario_comptages_manuels` t JOIN `purge_scenarios` p ON p.`id_scenario` = t.`id_scenario`;
DELETE t FROM `trppu_scenario_exclusions`        t JOIN `purge_scenarios` p ON p.`id_scenario` = t.`id_scenario`;
DELETE t FROM `trppu_scenario_pic_coeffs`        t JOIN `purge_scenarios` p ON p.`id_scenario` = t.`id_scenario`;
DELETE t FROM `trppu_scenario_variations_prev`   t JOIN `purge_scenarios` p ON p.`id_scenario` = t.`id_scenario`;
DELETE t FROM `trppu_trafic_agrebal`             t JOIN `purge_scenarios` p ON p.`id_scenario` = t.`id_scenario`;
DELETE t FROM `trppu_trafic_pdi`                 t JOIN `purge_scenarios` p ON p.`id_scenario` = t.`id_scenario`;
DELETE t FROM `trppu_pic_version`                t JOIN `purge_scenarios` p ON p.`id_scenario` = t.`id_scenario`;

-- 2.4 Le scénario lui-même.
DELETE s FROM `trppu_scenario` s JOIN `purge_scenarios` p ON p.`id_scenario` = s.`id_scenario`;

COMMIT;
-- Pour annuler avant d'en arriver là : remplacer ce COMMIT par ROLLBACK.


-- =====================================================================================
-- §3. REMISE À ZÉRO DES COMPTEURS AUTO_INCREMENT
-- =====================================================================================
-- À jouer après une purge totale, pour repartir d'identifiants 1, 2, 3… lisibles dans les
-- tests. Sans danger si des lignes subsistent (purge filtrée) : MySQL relève
-- automatiquement le compteur au max(id) + 1 existant.
-- Ces ALTER provoquent un COMMIT implicite : ils sont donc hors de la transaction du §2.

ALTER TABLE `trppu_scenario`                   AUTO_INCREMENT = 1;
ALTER TABLE `trppu_tmh`                        AUTO_INCREMENT = 1;
ALTER TABLE `trppu_neutralisations`            AUTO_INCREMENT = 1;
ALTER TABLE `trppu_scenario_comptages_manuels` AUTO_INCREMENT = 1;
ALTER TABLE `trppu_scenario_exclusions`        AUTO_INCREMENT = 1;
ALTER TABLE `trppu_scenario_pic_coeffs`        AUTO_INCREMENT = 1;
ALTER TABLE `trppu_scenario_variations_prev`   AUTO_INCREMENT = 1;
ALTER TABLE `trppu_trafic_agrebal`             AUTO_INCREMENT = 1;
ALTER TABLE `trppu_trafic_pdi`                 AUTO_INCREMENT = 1;
ALTER TABLE `trppu_pic_version`                AUTO_INCREMENT = 1;
ALTER TABLE `trppu_pic_coefficients`           AUTO_INCREMENT = 1;
ALTER TABLE `trppu_api_log`                    AUTO_INCREMENT = 1;
ALTER TABLE `trppu_recalcul_log`               AUTO_INCREMENT = 1;


-- =====================================================================================
-- §4. ÉTAT APRÈS — tout doit être à 0 sur une purge totale
-- =====================================================================================

SELECT 'trppu_scenario'                   AS `table`, COUNT(*) AS `apres` FROM `trppu_scenario`
UNION ALL SELECT 'trppu_tmh',                         COUNT(*) FROM `trppu_tmh`
UNION ALL SELECT 'trppu_neutralisations',             COUNT(*) FROM `trppu_neutralisations`
UNION ALL SELECT 'trppu_scenario_comptages_manuels',  COUNT(*) FROM `trppu_scenario_comptages_manuels`
UNION ALL SELECT 'trppu_scenario_exclusions',         COUNT(*) FROM `trppu_scenario_exclusions`
UNION ALL SELECT 'trppu_scenario_pic_coeffs',         COUNT(*) FROM `trppu_scenario_pic_coeffs`
UNION ALL SELECT 'trppu_scenario_variations_prev',    COUNT(*) FROM `trppu_scenario_variations_prev`
UNION ALL SELECT 'trppu_trafic_agrebal',              COUNT(*) FROM `trppu_trafic_agrebal`
UNION ALL SELECT 'trppu_trafic_pdi',                  COUNT(*) FROM `trppu_trafic_pdi`
UNION ALL SELECT 'trppu_pic_version',                 COUNT(*) FROM `trppu_pic_version`
UNION ALL SELECT 'trppu_pic_coefficients',            COUNT(*) FROM `trppu_pic_coefficients`
UNION ALL SELECT 'trppu_api_log',                     COUNT(*) FROM `trppu_api_log`
UNION ALL SELECT 'trppu_recalcul_log',                COUNT(*) FROM `trppu_recalcul_log`;

-- Contrôle d'orphelins : doit renvoyer 0 ligne. Les tables enfants n'ont pas toutes une FK
-- (trppu_trafic_pdi n'en a aucune), donc rien ne garantit structurellement l'absence de
-- restes — d'où ce contrôle explicite.
SELECT 'trppu_tmh'                      AS `table`, COUNT(*) AS `orphelins` FROM `trppu_tmh`                      t LEFT JOIN `trppu_scenario` s ON s.`id_scenario` = t.`id_scenario` WHERE s.`id_scenario` IS NULL
UNION ALL SELECT 'trppu_neutralisations',           COUNT(*) FROM `trppu_neutralisations`            t LEFT JOIN `trppu_scenario` s ON s.`id_scenario` = t.`id_scenario` WHERE s.`id_scenario` IS NULL
UNION ALL SELECT 'trppu_scenario_comptages_manuels',COUNT(*) FROM `trppu_scenario_comptages_manuels` t LEFT JOIN `trppu_scenario` s ON s.`id_scenario` = t.`id_scenario` WHERE s.`id_scenario` IS NULL
UNION ALL SELECT 'trppu_scenario_exclusions',       COUNT(*) FROM `trppu_scenario_exclusions`        t LEFT JOIN `trppu_scenario` s ON s.`id_scenario` = t.`id_scenario` WHERE s.`id_scenario` IS NULL
UNION ALL SELECT 'trppu_scenario_pic_coeffs',       COUNT(*) FROM `trppu_scenario_pic_coeffs`        t LEFT JOIN `trppu_scenario` s ON s.`id_scenario` = t.`id_scenario` WHERE s.`id_scenario` IS NULL
UNION ALL SELECT 'trppu_scenario_variations_prev',  COUNT(*) FROM `trppu_scenario_variations_prev`   t LEFT JOIN `trppu_scenario` s ON s.`id_scenario` = t.`id_scenario` WHERE s.`id_scenario` IS NULL
UNION ALL SELECT 'trppu_trafic_agrebal',            COUNT(*) FROM `trppu_trafic_agrebal`             t LEFT JOIN `trppu_scenario` s ON s.`id_scenario` = t.`id_scenario` WHERE s.`id_scenario` IS NULL
UNION ALL SELECT 'trppu_trafic_pdi',                COUNT(*) FROM `trppu_trafic_pdi`                 t LEFT JOIN `trppu_scenario` s ON s.`id_scenario` = t.`id_scenario` WHERE s.`id_scenario` IS NULL
UNION ALL SELECT 'trppu_pic_version',               COUNT(*) FROM `trppu_pic_version`                t LEFT JOIN `trppu_scenario` s ON s.`id_scenario` = t.`id_scenario` WHERE s.`id_scenario` IS NULL
UNION ALL SELECT 'trppu_pic_coefficients',          COUNT(*) FROM `trppu_pic_coefficients`           c LEFT JOIN `trppu_pic_version` v ON v.`id_pic_version` = c.`id_pic_version` WHERE v.`id_pic_version` IS NULL;

DROP TEMPORARY TABLE IF EXISTS `purge_scenarios`;


-- =====================================================================================
-- §5. PURGES OPTIONNELLES — référentiels alimentés par l'API (décommenter au besoin)
-- =====================================================================================
-- Ces tables ne sont PAS vidées par défaut : le §2 les laisse intactes, un jeu de test
-- reste donc immédiatement rejouable. Elles sont bien écrites par les modules
-- (POST/PUT/upload-excel), d'où leur présence ici.

-- 5.1 Versions PIC et coefficients devenus orphelins (scénario disparu avant cette purge).
--     Le §2 n'atteint que les versions dont le scénario était encore présent.
-- DELETE c FROM `trppu_pic_coefficients` c
--   LEFT JOIN `trppu_pic_version` v ON v.`id_pic_version` = c.`id_pic_version`
--   WHERE v.`id_pic_version` IS NULL;
-- DELETE v FROM `trppu_pic_version` v
--   LEFT JOIN `trppu_scenario` s ON s.`id_scenario` = v.`id_scenario`
--   WHERE s.`id_scenario` IS NULL;

-- 5.2 Produits créés automatiquement par le TMH et jamais repris à la main.
--     `ensure_produits_exist` crée le produit manquant avec le libellé de
--     `g_trppu_obj_mapping`, ou son propre code en libellé quand l'objet est absent du
--     mapping (PR, PPI — cf. DSR-679). `lb_produit = co_produit` est donc la marque d'une
--     création automatique jamais corrigée : ce filtre épargne les produits saisis par le
--     métier.
-- DELETE FROM `trppu_produit`
--  WHERE `lb_produit` = `co_produit`
--    AND `co_produit` NOT IN (SELECT `co_produit` FROM `trppu_tmh`)
--    AND `co_produit` NOT IN (SELECT `co_produit` FROM `trppu_scenario_variations_prev`)
--    AND `co_produit` NOT IN (SELECT `co_produit` FROM `trppu_pic_coefficients`);

-- 5.3 Table rase du référentiel produits (tous les produits sans aucune référence).
--     Après une purge totale du §2, plus rien ne référence les produits : ceci vide donc la
--     table, libellés métier compris. À ne faire que si le référentiel est réimporté juste
--     après (POST /trppu-api/produits/upload-excel).
-- DELETE FROM `trppu_produit`
--  WHERE `co_produit` NOT IN (SELECT `co_produit` FROM `trppu_tmh`)
--    AND `co_produit` NOT IN (SELECT `co_produit` FROM `trppu_scenario_variations_prev`)
--    AND `co_produit` NOT IN (SELECT `co_produit` FROM `trppu_pic_coefficients`);

-- 5.4 Référentiel des sites (77 lignes au dernier relevé).
--     Aucune FK ne pointe vers `trppu_site` dans ce schéma : la suppression passe sans
--     erreur, mais tout scénario recréé ensuite exigera un site existant. À réimporter via
--     POST /trppu-api/sites/upload-excel.
-- DELETE FROM `trppu_site`;
--     (pas d'AUTO_INCREMENT à remettre à zéro ici : `trppu_site` et `trppu_produit` ont une
--      PK saisie, `co_regate` / `co_produit`.)


-- =====================================================================================
-- §6. HORS PÉRIMÈTRE — tables qu'aucun module TRPPU n'écrit (ne pas purger)
-- =====================================================================================
-- Vérifié par recherche des INSERT/UPDATE/DELETE dans `app/` : aucune de ces tables n'est
-- écrite par l'API. Elles proviennent d'autres chaînes (Mercure/DSR, chargements batch) et
-- les vider casserait des jeux de données coûteux à reconstituer.
--
--   trppu_cles_repartition           ~22 395 341 lignes  <-- chargement externe, JAMAIS purger
--   trppu_agrebal_pdi                      9 505 lignes  <-- référentiel agrebal
--   trppu_pic_coefficients_ko                 42 lignes  <-- table héritée, hors code YS04
--   demande_dsr                                2 lignes  <-- module DSR
--   trafic_staging                             0 ligne   <-- table de staging d'import
--   trppu_cles_repartition_calcule             0 ligne
--   trppu_referentiel                          0 ligne
--   trppu_site_trafic                          0 ligne
--   trppu_suivi_batch                          0 ligne   <-- suivi des batchs
--   trppu_version_cle                          0 ligne
--
-- (volumes = db/count.json, relevé via information_schema — estimations InnoDB)
