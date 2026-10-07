-- =====================================================================================
-- FIX — `ERROR 1264 (Out of range value for column 'trafic_oo_total')` sur DSR-696
-- =====================================================================================
-- Élargit les trois totaux de `trppu_trafic_site` de decimal(24,18) (6 chiffres entiers,
-- trop peu pour une somme de sources decimal(25,19)) à decimal(35,19) : 16 chiffres
-- entiers, et l'échelle de la source pour que les contrôles CA1+CA3 ne voient pas d'écart
-- d'arrondi. `potentielip_total` (bigint) n'est pas concerné.
--
-- ATTENTION — DDL : COMMIT IMPLICITE, ALTER dans PREPARE/EXECUTE invisible à `is_ddl` :
-- jouer avec `transactional=False`. Reconstruit la table (ALGORITHM=COPY).
-- REJOUABLE : l'ALTER teste la définition courante des colonnes.
-- USAGE : mysql -h <hote> -u <user> -p dsr_mercure_aa < db/fix_error.sql
-- APRÈS : rejouer DSR-696 entièrement (son DELETE a été validé), puis DSR-698 et DSR-699.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Paramètre — sert aux deux constats ci-dessous, pas à l'ALTER
-- -------------------------------------------------------------------------------------
SET @id_referentiel := 1;      -- le référentiel sur lequel DSR-696 a échoué


-- -------------------------------------------------------------------------------------
-- Constat 1 — la définition actuelle des colonnes
-- -------------------------------------------------------------------------------------
-- `chiffres_avant_virgule` borne la valeur stockable : 6 avant correction, 16 après.
SELECT COLUMN_NAME,
       COLUMN_TYPE,
       NUMERIC_PRECISION                          AS precision_totale,
       NUMERIC_SCALE                              AS decimales,
       NUMERIC_PRECISION - NUMERIC_SCALE          AS chiffres_avant_virgule
  FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA = DATABASE()
   AND TABLE_NAME = 'trppu_trafic_site'
   AND COLUMN_NAME IN ('trafic_colis_total', 'trafic_oo_total', 'trafic_3s_total',
                       'potentielip_total')
 ORDER BY ORDINAL_POSITION;


-- -------------------------------------------------------------------------------------
-- Constat 2 — de combien ça déborde, et sur quels sites
-- -------------------------------------------------------------------------------------
-- Les dix plus gros sites et le nombre de chiffres entiers par famille de trafic.
SELECT co_regate_site,
       COUNT(*)                                        AS nb_pdi_actifs,
       SUM(trafic_colis)                               AS somme_colis,
       SUM(trafic_oo)                                  AS somme_oo,
       SUM(trafic_3s)                                  AS somme_3s,
       LENGTH(TRUNCATE(SUM(trafic_colis), 0))          AS chiffres_colis,
       LENGTH(TRUNCATE(SUM(trafic_oo), 0))             AS chiffres_oo,
       LENGTH(TRUNCATE(SUM(trafic_3s), 0))             AS chiffres_3s
  FROM trppu_cles_repartition
 WHERE id_referentiel = @id_referentiel
   AND date_fin_validite IS NULL
 GROUP BY co_regate_site
 ORDER BY GREATEST(SUM(trafic_colis), SUM(trafic_oo), SUM(trafic_3s)) DESC
 LIMIT 10;


-- -------------------------------------------------------------------------------------
-- Correction — élargissement des trois colonnes de totaux
-- -------------------------------------------------------------------------------------
-- Un seul ALTER (une reconstruction). Le garde-fou teste la définition, pas le nom.
-- `NOT NULL` est répété : MODIFY ne conserve que ce qui est réécrit.

SET @sql := IF(
    (SELECT NUMERIC_PRECISION FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'trppu_trafic_site'
        AND COLUMN_NAME = 'trafic_oo_total') >= 35,
    'SELECT ''trppu_trafic_site : totaux déjà élargis'' AS resultat',
    'ALTER TABLE `trppu_trafic_site`
       MODIFY COLUMN `trafic_colis_total` decimal(35,19) NOT NULL,
       MODIFY COLUMN `trafic_oo_total`    decimal(35,19) NOT NULL,
       MODIFY COLUMN `trafic_3s_total`    decimal(35,19) NOT NULL'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


-- -------------------------------------------------------------------------------------
-- Contrôle — la nouvelle définition
-- -------------------------------------------------------------------------------------
SELECT COLUMN_NAME,
       COLUMN_TYPE,
       NUMERIC_PRECISION - NUMERIC_SCALE                       AS chiffres_avant_virgule,
       NUMERIC_SCALE                                           AS decimales,
       IF(COLUMN_NAME = 'potentielip_total'
          OR (NUMERIC_PRECISION = 35 AND NUMERIC_SCALE = 19),
          'OK', 'ANOMALIE')                                    AS verdict
  FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA = DATABASE()
   AND TABLE_NAME = 'trppu_trafic_site'
   AND COLUMN_NAME IN ('trafic_colis_total', 'trafic_oo_total', 'trafic_3s_total',
                       'potentielip_total')
 ORDER BY ORDINAL_POSITION;

-- `nb_sites` à 0 pour le référentiel concerné est normal : DSR-696 est à rejouer.
SELECT id_referentiel,
       COUNT(*)           AS nb_sites,
       MAX(date_creation) AS dernier_calcul
  FROM trppu_trafic_site
 GROUP BY id_referentiel
 ORDER BY id_referentiel;
