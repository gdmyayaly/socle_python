-- =====================================================================================
-- FIX — `ERROR 1264 (Out of range value for column 'trafic_oo_total')` sur DSR-696
-- =====================================================================================
-- Élargit les trois colonnes de totaux de `trppu_trafic_site`, qui ne peuvent pas contenir
-- les sommes qu'on leur demande de porter.
--
-- LE PROBLÈME
--   source  `trppu_cles_repartition.trafic_oo`        decimal(25,19)
--   cible   `trppu_trafic_site.trafic_oo_total`       decimal(24,18)
--
-- `decimal(24,18)` = 24 chiffres au total dont 18 après la virgule, soit **six chiffres
-- avant** : la plus grande valeur stockable est 999999,999999999999999999. Or la cible reçoit
-- la SOMME des trafics d'un site, alors que chaque ligne source peut à elle seule atteindre
-- ce plafond — six chiffres entiers aussi. Deux PDI suffisent donc, en théorie, à déborder.
-- L'`INSERT` de `DSR-696_site_trafic.sql` échoue en `ERROR 1264` sur la première famille de
-- trafic qui dépasse ; ici `trafic_oo`, mais les trois colonnes ont le même défaut.
--
-- Ce n'est pas une erreur du script : le calcul est juste, c'est la colonne qui est trop
-- étroite. Le constat était ouvert depuis l'analyse du ticket — `docs/INCOHERENCES.md` n° 5 et
-- constat 9 de `docs/DIAGNOSTIC-DSR-696-699.md` — avec le contrôle qui l'annonçait déjà dans
-- `DSR-697_chargement_cles_repartition.sql`.
--
-- LE CORRECTIF — `decimal(35,19)`
--   * 35 - 19 = **16 chiffres avant la virgule**. La somme de TOUTES les lignes de la table
--     (22,4 M) au plafond de la source vaudrait 2,24 × 10¹³, soit 14 chiffres : la marge
--     couvre n'importe quel site, y compris un référentiel national d'un seul tenant.
--   * 19 décimales, et non 18 : c'est l'échelle de la SOURCE. À 18, MySQL arrondit la somme
--     au dernier chiffre — assez pour que le contrôle CA1+CA3 de DSR-696, qui compare le
--     total stocké à la somme recalculée, affiche des `ecart_*` non nuls de l'ordre de 10⁻¹⁹.
--     Aligner les deux échelles supprime l'écart au lieu de le rendre négligeable.
--
-- `potentielip_total` n'est pas touchée : `bigint` contient largement la somme des
-- `smallint` de la source (32767 × 22,4 M ≈ 7,3 × 10¹¹).
-- Les clés de `trppu_cles_repartition_calcule` non plus : une clé vaut au plus 1, et
-- `decimal(24,18)` lui va.
--
-- ATTENTION — `ALTER TABLE` est du DDL : COMMIT IMPLICITE, aucun ROLLBACK possible. Si ce
-- fichier est joué par le socle, passer explicitement `transactional=False`, comme pour
-- `DSR-696-699_migration.sql` et pour la même raison : l'ALTER voyage dans une chaîne
-- exécutée par PREPARE/EXECUTE, invisible à la détection de DDL (`is_ddl`).
--
-- La modification reconstruit la table (`ALGORITHM=COPY`) : la jouer tant que
-- `trppu_trafic_site` est vide ou presque coûte quelques millisecondes.
--
-- Le fichier est REJOUABLE : l'ALTER est précédé d'un test de présence sur la définition
-- courante des colonnes.
--
-- USAGE
--   mysql -h <hote> -u <user> -p dsr_mercure_aa < db/fix_error.sql
--
-- APRÈS — la correction ne recalcule rien. Le `DELETE` de DSR-696 ayant, lui, été validé
-- avant l'échec de l'`INSERT`, le référentiel est à recalculer entièrement :
--   mysql … < db/DSR-696_site_trafic.sql
-- puis la suite de la chaîne (DSR-698, DSR-699) — cf. `db/README.md`.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Paramètre — sert aux deux constats ci-dessous, pas à l'ALTER
-- -------------------------------------------------------------------------------------
SET @id_referentiel := 1;      -- le référentiel sur lequel DSR-696 a échoué


-- -------------------------------------------------------------------------------------
-- Constat 1 — la définition actuelle des colonnes
-- -------------------------------------------------------------------------------------
-- `chiffres_avant_virgule` = NUMERIC_PRECISION - NUMERIC_SCALE. C'est ce nombre, et lui seul,
-- qui borne la valeur stockable : 6 aujourd'hui, 16 après correction.
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
-- Les dix plus gros sites du référentiel, et pour chacun le nombre de chiffres entiers que
-- réclame chaque famille de trafic. Tant qu'une seule colonne y dépasse 6, DSR-696 échoue ;
-- après correction, la limite est 16.
--
-- À LIRE AUSSI COMME UNE QUESTION MÉTIER. Si ces totaux affichent douze ou quinze chiffres
-- entiers, la question posée à l'équipe data reste entière : des colonnes à dix-neuf
-- décimales portent-elles des volumes, ou déjà des ratios chargés comme des volumes ? Le
-- présent correctif permet de stocker la somme, il ne dit pas qu'elle a un sens.
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
-- Un SEUL `ALTER`, avec ses trois `MODIFY` : la table n'est reconstruite qu'une fois.
--
-- Le garde-fou teste la DÉFINITION (`NUMERIC_PRECISION`), pas le nom de la colonne : rejoué
-- sur une base déjà corrigée, le fichier ne reconstruit rien. C'est la leçon de l'index
-- `idx_regate_actif` de la migration, dont le garde-fou testait le nom et serait passé à côté.
--
-- `NOT NULL` est répété dans chaque `MODIFY` : une clause omise vaut suppression de la
-- contrainte, MySQL ne conservant que ce qui est réécrit.

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
-- `chiffres_avant_virgule` doit valoir 16 sur les trois colonnes de trafic, et `decimales`
-- 19 — l'échelle de la source, pour que les contrôles de DSR-696 ne voient plus d'écart
-- d'arrondi.
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

-- État de la table après correction. `nb_sites` est très probablement à 0 pour le
-- référentiel concerné : le `DELETE` de DSR-696 a été validé, son `INSERT` a échoué. C'est
-- normal, et c'est la raison pour laquelle DSR-696 est à rejouer entièrement.
SELECT id_referentiel,
       COUNT(*)           AS nb_sites,
       MAX(date_creation) AS dernier_calcul
  FROM trppu_trafic_site
 GROUP BY id_referentiel
 ORDER BY id_referentiel;
