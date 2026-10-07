-- =====================================================================================
-- DSR-699 — Calcul des clés de répartition des PDI
-- =====================================================================================
-- Clé = trafic du PDI / total du site, dans `trppu_cles_repartition_calcule`. Total de site
-- nul : clé à 0 (règle métier), sites listés dans le rapport de `init`.
--
-- CA1 une ligne par PDI actif, CA2 toute clé a une version, CA3 somme par site = 1 à 10⁻⁴,
-- CA4 aucune clé existante modifiée.
--
-- Prérequis : migration, puis `DSR-696_site_trafic.sql` et `DSR-698_version_cle.sql`.
-- USAGE : mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-699_cles_calculees.sql
-- REJOUABLE (CA4) : n'insère que les couples (version, PDI) absents ; une relance complète.
--
-- YB07 (étape « cles ») le joue PAR LOTS ]@id_debut ; @id_fin], commités un à un : une
-- transaction unique de 22 M lignes dépasse `group_replication_transaction_size_limit`
-- (erreur 3231). À la main, les bornes par défaut couvrent toute la table.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Paramètres
-- -------------------------------------------------------------------------------------
-- Mêmes paramètres que `DSR-696_site_trafic.sql`.

SET @id_referentiel := 1;      -- référentiel à calculer — obligatoire
SET @co_regate      := NULL;   -- '123456' pour un seul site, NULL pour tout le référentiel
SET @id_debut       := 0;      -- lot : id de trppu_cles_repartition strictement supérieur à
SET @id_fin         := 9223372036854775807;  -- … et inférieur ou égal à (défaut : tout)


-- -------------------------------------------------------------------------------------
-- Durcissement du mode SQL (session seulement)
-- -------------------------------------------------------------------------------------
-- Sans mode strict, une valeur invalide passerait en avertissement avec une clé fausse.

SET SESSION sql_mode = CONCAT(@@sql_mode, ',STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO');


-- -------------------------------------------------------------------------------------
-- Garde-fou 1 — état du périmètre avant calcul
-- -------------------------------------------------------------------------------------
-- `nb_sites_sans_agregat` et `nb_sites_sans_version` doivent valoir 0 : sinon la jointure
-- du calcul écarte ces PDI sans rien dire et le CA1 échoue.
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
-- Sites et familles de trafic dont le total est nul : leurs clés vaudront 0.
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
-- Calcul des clés
-- -------------------------------------------------------------------------------------
-- CA4 : `NOT EXISTS` sur (version, PDI), servi par `uq_crc_version_pdi` — seules les clés
-- absentes sont écrites, ce qui rend les lots reprenables. La jointure sur la version porte
-- le CA2.
--
-- `CAST` de potentielip : smallint / bigint donnerait une échelle de 4 décimales
-- (`div_precision_increment`) ; les autres numérateurs sont déjà en decimal(25,19).

INSERT INTO trppu_cles_repartition_calcule
    (id_version_cle,
     id_referentiel,
     id_pdi,
     co_regate_site,
     cle_colis,
     cle_oo,
     cle_3s,
     cle_potentielip)
-- STRAIGHT_JOIN + IGNORE INDEX : lecture de la grosse table dans l'ordre de sa PK (la
-- tranche du lot), site et version cherchés par index dans les petites tables. Sinon
-- l'optimiseur peut partir des sites : 22 M lectures aléatoires par `idx_cr_ref_actif`.
SELECT STRAIGHT_JOIN
       v.id_version_cle,
       c.id_referentiel,
       c.id_pdi,
       c.co_regate_site,
       -- Total de site nul : clé à 0 (règle métier), jamais de division par zéro.
       IF(s.trafic_colis_total = 0, 0, c.trafic_colis / s.trafic_colis_total),
       IF(s.trafic_oo_total    = 0, 0, c.trafic_oo    / s.trafic_oo_total),
       IF(s.trafic_3s_total    = 0, 0, c.trafic_3s    / s.trafic_3s_total),
       IF(s.potentielip_total  = 0, 0,
          CAST(COALESCE(c.potentielip, 0) AS DECIMAL(24,18)) / s.potentielip_total)
  FROM trppu_cles_repartition c IGNORE INDEX (idx_cr_ref_actif)
  JOIN trppu_trafic_site  s ON s.id_referentiel = c.id_referentiel
                           AND s.co_regate_site = c.co_regate_site
  JOIN trppu_version_cle  v ON v.id_referentiel = c.id_referentiel
                           AND v.co_regate      = c.co_regate_site
                           AND v.actif = 'O'
 WHERE c.id > @id_debut
   AND c.id <= @id_fin
   AND c.id_referentiel = @id_referentiel
   AND c.date_fin_validite IS NULL
   AND (@co_regate IS NULL OR c.co_regate_site = @co_regate)
   AND NOT EXISTS (SELECT 1
                     FROM trppu_cles_repartition_calcule k
                    WHERE k.id_version_cle = v.id_version_cle
                      AND k.id_pdi         = c.id_pdi);


-- -------------------------------------------------------------------------------------
-- Contrôles — critères d'acceptation
-- -------------------------------------------------------------------------------------

-- CA1 : chaque PDI actif a sa ligne de clés — doit renvoyer 0 ligne.
-- Filtre sur (référentiel, PDI), sans index, pour voir aussi les sites SANS version ;
-- sur un gros référentiel, restreindre `@co_regate`.
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

-- CA3 : la somme des clés d'un site vaut 1 à 10⁻⁴ près — `verdict` OK partout. Les lignes
-- en ANOMALIE sont remontées dans les logs par l'appelant.
SELECT k.co_regate_site,
       COUNT(*)                AS nb_pdi,
       SUM(k.cle_colis)        AS somme_colis,
       SUM(k.cle_oo)           AS somme_oo,
       SUM(k.cle_3s)           AS somme_3s,
       SUM(k.cle_potentielip)  AS somme_potentielip,
       -- Somme attendue : 1, ou 0 pour une composante dont le total de site est nul.
       IF(ABS(SUM(k.cle_colis)       - IF(MAX(s.trafic_colis_total) = 0, 0, 1)) <= 0.0001
      AND ABS(SUM(k.cle_oo)          - IF(MAX(s.trafic_oo_total)    = 0, 0, 1)) <= 0.0001
      AND ABS(SUM(k.cle_3s)          - IF(MAX(s.trafic_3s_total)    = 0, 0, 1)) <= 0.0001
      AND ABS(SUM(k.cle_potentielip) - IF(MAX(s.potentielip_total)  = 0, 0, 1)) <= 0.0001,
          'OK', 'ANOMALIE')   AS verdict
  FROM trppu_cles_repartition_calcule k
  JOIN trppu_trafic_site s ON s.id_referentiel = k.id_referentiel
                          AND s.co_regate_site = k.co_regate_site
 WHERE k.id_referentiel = @id_referentiel
   AND (@co_regate IS NULL OR k.co_regate_site = @co_regate)
 GROUP BY k.co_regate_site
 ORDER BY verdict DESC, k.co_regate_site;

-- CA4 : un PDI n'a qu'une clé par version — doit renvoyer 0 ligne.
SELECT id_version_cle,
       id_pdi,
       COUNT(*) AS nb_lignes
  FROM trppu_cles_repartition_calcule
 GROUP BY id_version_cle, id_pdi
HAVING COUNT(*) > 1;
