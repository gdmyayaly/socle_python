-- =====================================================================================
-- DSR-699 — Calcul des clés de répartition des PDI
-- =====================================================================================
-- Clé = trafic du PDI (`trppu_cles_repartition`) / total du site (`trppu_trafic_site`),
-- rattachée à la version active (`trppu_version_cle`), dans `trppu_cles_repartition_calcule`.
--   CA1  une ligne par PDI actif             CA2  toute clé rattachée à une version
--   CA3  somme d'un site = 1 (0,9999-1,0001) CA4  une version calculée n'est jamais modifiée
-- Prérequis : `DSR-696-699_migration.sql`, puis `DSR-696_site_trafic.sql` et
-- `DSR-698_version_cle.sql` sur le même référentiel (ordre : `db/README.md`).
-- Usage : mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-699_cles_calculees.sql
-- REJOUABLE : ne recalcule JAMAIS une version déjà calculée (CA4, `@deja`).
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Paramètres
-- -------------------------------------------------------------------------------------
-- Mêmes paramètres que `DSR-696_site_trafic.sql` (même périmètre).

SET @id_referentiel := 1;      -- référentiel à calculer — obligatoire
SET @co_regate      := NULL;   -- '123456' pour un seul site, NULL pour tout le référentiel


-- -------------------------------------------------------------------------------------
-- Durcissement du mode SQL — l'échec doit être garanti, pas dépendre du serveur
-- -------------------------------------------------------------------------------------
-- Total de site à zéro : sur un serveur non strict, la division par zéro passerait avec une
-- clé fausse. On veut l'échec explicite (`ERROR 1365`). Portée : la session seule.

SET SESSION sql_mode = CONCAT(@@sql_mode, ',STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO');


-- -------------------------------------------------------------------------------------
-- Garde-fou 1 — état du périmètre avant calcul
-- -------------------------------------------------------------------------------------
-- `nb_sites_sans_agregat` et `nb_sites_sans_version` doivent valoir 0 : sinon la jointure
-- du calcul écarte silencieusement ces PDI (CA1).
SELECT @id_referentiel                                          AS id_referentiel_demande,
       COUNT(*)                                                 AS nb_pdi_actifs,
       COUNT(DISTINCT c.co_regate_site)                         AS nb_sites,
       SUM(s.co_regate_site IS NULL)                            AS nb_pdi_sans_agregat,
       COUNT(DISTINCT IF(s.co_regate_site IS NULL,
                         c.co_regate_site, NULL))               AS nb_sites_sans_agregat,
       COUNT(DISTINCT IF(v.id_version_cle IS NULL,
                         c.co_regate_site, NULL))               AS nb_sites_sans_version
  FROM trppu_cles_repartition c
  LEFT JOIN trppu_trafic_site s ON s.id_referentiel = c.id_referentiel
                               AND s.co_regate_site = c.co_regate_site
  LEFT JOIN trppu_version_cle v ON v.id_referentiel = c.id_referentiel
                               AND v.co_regate      = c.co_regate_site
                               AND v.actif = 'O'
 WHERE c.id_referentiel = @id_referentiel
   AND c.date_fin_validite IS NULL
   AND (@co_regate IS NULL OR c.co_regate_site = @co_regate);


-- -------------------------------------------------------------------------------------
-- Garde-fou 2 — dénominateurs nuls
-- -------------------------------------------------------------------------------------
-- Doit renvoyer 0 ligne : toute ligne annonce l'`ERROR 1365` du calcul (souvent
-- `potentielip_total` à zéro). Recalculer l'agrégat DSR-696 s'il est périmé, sinon arbitrage métier.
SELECT co_regate_site,
       trafic_colis_total,
       trafic_oo_total,
       trafic_3s_total,
       potentielip_total
  FROM trppu_trafic_site
 WHERE id_referentiel = @id_referentiel
   AND (@co_regate IS NULL OR co_regate_site = @co_regate)
   AND (trafic_colis_total = 0
     OR trafic_oo_total    = 0
     OR trafic_3s_total    = 0
     OR potentielip_total  = 0)
 ORDER BY co_regate_site;


-- -------------------------------------------------------------------------------------
-- Garde-fou 3 — le périmètre a-t-il déjà été calculé ? (CA4)
-- -------------------------------------------------------------------------------------
-- Mémorisé AVANT toute écriture (erreur 1093 : pas de lecture de la table cible dans
-- l'INSERT). Si une seule version du périmètre a des clés, rien n'est chargé : relancer
-- site par site avec `@co_regate`.
SET @deja := (SELECT COUNT(*)
                FROM trppu_cles_repartition_calcule k
                JOIN trppu_version_cle v ON v.id_version_cle = k.id_version_cle
               WHERE v.id_referentiel = @id_referentiel
                 AND (@co_regate IS NULL OR v.co_regate = @co_regate));


-- -------------------------------------------------------------------------------------
-- Calcul des clés
-- -------------------------------------------------------------------------------------
-- La jointure sur `trppu_version_cle` porte le CA2. Index : `idx_cr_ref_actif`,
-- `uq_site_trafic`, `idx_regate_actif`.
-- `CAST` de la clé potentiel IP : `potentielip` est un smallint, la division ne donnerait que
-- 4 décimales (`div_precision_increment`).
-- `COALESCE` sur le seul numérateur ; dénominateurs volontairement non protégés (sql_mode).

INSERT INTO trppu_cles_repartition_calcule
    (id_version_cle,
     id_referentiel,
     id_pdi,
     co_regate_site,
     cle_colis,
     cle_oo,
     cle_3s,
     cle_potentielip)
SELECT v.id_version_cle,
       c.id_referentiel,
       c.id_pdi,
       c.co_regate_site,
       c.trafic_colis / s.trafic_colis_total,
       c.trafic_oo    / s.trafic_oo_total,
       c.trafic_3s    / s.trafic_3s_total,
       CAST(COALESCE(c.potentielip, 0) AS DECIMAL(24,18)) / s.potentielip_total
  FROM trppu_cles_repartition c
  JOIN trppu_trafic_site  s ON s.id_referentiel = c.id_referentiel
                           AND s.co_regate_site = c.co_regate_site
  JOIN trppu_version_cle  v ON v.id_referentiel = c.id_referentiel
                           AND v.co_regate      = c.co_regate_site
                           AND v.actif = 'O'
 WHERE c.id_referentiel = @id_referentiel
   AND c.date_fin_validite IS NULL
   AND (@co_regate IS NULL OR c.co_regate_site = @co_regate)
   AND @deja = 0;


-- -------------------------------------------------------------------------------------
-- Contrôles — critères d'acceptation
-- -------------------------------------------------------------------------------------

-- CA1 : chaque PDI actif a sa ligne de clés — doit renvoyer 0 ligne.
-- `NOT EXISTS` non indexé, à dessein : passer par la version rendrait le contrôle aveugle
-- aux sites sans version. Sur un gros référentiel, restreindre `@co_regate`.
SELECT c.co_regate_site,
       c.id_pdi
  FROM trppu_cles_repartition c
 WHERE c.id_referentiel = @id_referentiel
   AND c.date_fin_validite IS NULL
   AND (@co_regate IS NULL OR c.co_regate_site = @co_regate)
   AND NOT EXISTS (SELECT 1
                     FROM trppu_cles_repartition_calcule k
                    WHERE k.id_referentiel = c.id_referentiel
                      AND k.id_pdi         = c.id_pdi)
 ORDER BY c.co_regate_site, c.id_pdi;

-- CA2 : aucune clé orpheline de version, ou rattachée à une version inactive — 0 ligne.
SELECT k.id_version_cle,
       COUNT(*) AS nb_cles
  FROM trppu_cles_repartition_calcule k
  LEFT JOIN trppu_version_cle v ON v.id_version_cle = k.id_version_cle
                               AND v.actif = 'O'
 WHERE k.id_referentiel = @id_referentiel
   AND v.id_version_cle IS NULL
 GROUP BY k.id_version_cle;

-- CA3 : la somme des clés d'un site vaut 1 à 10⁻⁴ près ; `verdict` doit valoir OK.
-- L'alerte dans les logs demandée par le ticket relève de l'appelant (lignes en ANOMALIE).
SELECT k.co_regate_site,
       COUNT(*)                AS nb_pdi,
       SUM(k.cle_colis)        AS somme_colis,
       SUM(k.cle_oo)           AS somme_oo,
       SUM(k.cle_3s)           AS somme_3s,
       SUM(k.cle_potentielip)  AS somme_potentielip,
       IF(SUM(k.cle_colis)       BETWEEN 0.9999 AND 1.0001
      AND SUM(k.cle_oo)          BETWEEN 0.9999 AND 1.0001
      AND SUM(k.cle_3s)          BETWEEN 0.9999 AND 1.0001
      AND SUM(k.cle_potentielip) BETWEEN 0.9999 AND 1.0001,
          'OK', 'ANOMALIE')   AS verdict
  FROM trppu_cles_repartition_calcule k
 WHERE k.id_referentiel = @id_referentiel
   AND (@co_regate IS NULL OR k.co_regate_site = @co_regate)
 GROUP BY k.co_regate_site
 ORDER BY verdict DESC, k.co_regate_site;

-- CA4 : un PDI n'a qu'une clé par version — doit renvoyer 0 ligne (cf. `uq_crc_version_pdi`).
SELECT id_version_cle,
       id_pdi,
       COUNT(*) AS nb_lignes
  FROM trppu_cles_repartition_calcule
 GROUP BY id_version_cle, id_pdi
HAVING COUNT(*) > 1;
