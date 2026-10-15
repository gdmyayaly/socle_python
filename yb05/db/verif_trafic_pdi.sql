-- =====================================================================================
-- Vérification manuelle du calcul des trafics PDI d'un scénario (DSR-702) — LECTURE SEULE
-- =====================================================================================
-- Rejoue en SQL la formule de `app/traitements/trafic_pdi.py` :
--
--   trafic(PDI, produit, jour, densité) =
--       ARRONDI( TMH hebdo du produit
--              × coef de rétention / 100 (version PIC du scénario, produit, jour, densité ;
--                stocké en pourcentage : 52 = 52 %)
--              × clé de répartition du PDI (version de clés du scénario, famille du produit) )
--
--   * TMH hebdo   = SUM(trppu_tmh.moyenne_hebdo) du produit, lignes bl_exclu = 0, hors
--                   produits de trppu_scenario_exclusions ; TMH nul => produit ignoré.
--   * densité     = 0 -> dense, 1 -> faible1, 2 -> faible2 (coef absent = 0).
--   * clé         = colonne de trppu_cles_repartition_calcule choisie par CLES_PAR_PRODUIT
--                   (.env) ; ci-dessous la valeur par défaut — À ADAPTER si le .env diffère.
--   * arrondi     = à l'entier, demi vers le haut (ROUND MySQL sur un DECIMAL, identique).
--   * PDI retenus = ceux qui ont une clé ET un Agrébal actif sur le site (le rattachement
--                   Agrébal est lu dans un JSON par le batch : ici on part des lignes écrites).
--
-- Les mêmes facteurs sont dans les logs du batch (`Avancement calcul trafics PDI`) :
--   phase=entrées  -> id_pic_version, id_version_cle, tmh_hebdo, cle_par_produit
--   phase=détail   -> par produit/jour : coefs, nb_pdi, somme_cles, totaux, plus gros PDI
--   phase=calcul PDI (avec -v) -> chaque PDI, chaque facteur, chaque trafic
-- et un échec « hors capacité » donne la formule complète du trafic fautif.
--
-- Usage : renseigner @id_scenario, puis
--   mysql -h <hote> -u <user> -p dsr_mercure_aa < db/verif_trafic_pdi.sql
-- =====================================================================================

SET @id_scenario := 15;


-- -------------------------------------------------------------------------------------
-- 1. Paramètres du scénario
-- -------------------------------------------------------------------------------------
-- `id_version_cle` est tracé AVANT le calcul (commit séparé) : il est renseigné même
-- quand le calcul a échoué ensuite.
SELECT id_scenario, co_regate, statut, id_pic_version, id_version_cle, id_referentiel,
       nb_jours_semaine, trafic_pdi_calcule, trafic_agrebal_calcule, calcul_trafic_en_cours
  FROM trppu_scenario
 WHERE id_scenario = @id_scenario;

SET @co_regate      := (SELECT co_regate      FROM trppu_scenario WHERE id_scenario = @id_scenario);
SET @id_pic_version := (SELECT id_pic_version FROM trppu_scenario WHERE id_scenario = @id_scenario);
SET @id_version_cle := (SELECT id_version_cle FROM trppu_scenario WHERE id_scenario = @id_scenario);


-- -------------------------------------------------------------------------------------
-- 2. TMH retenu par produit (= tmh_hebdo du log phase=entrées)
-- -------------------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS verif_tmh;
CREATE TEMPORARY TABLE verif_tmh AS
SELECT t.co_produit,
       SUM(t.moyenne_hebdo) AS tmh_hebdo,
       -- CLES_PAR_PRODUIT par défaut : CO:colis,OO:oo,IP:potentielip,OS:3s,PR:3s,PPI:3s
       CASE UPPER(t.co_produit)
            WHEN 'CO'  THEN 'cle_colis'
            WHEN 'OO'  THEN 'cle_oo'
            WHEN 'IP'  THEN 'cle_potentielip'
            WHEN 'OS'  THEN 'cle_3s'
            WHEN 'PR'  THEN 'cle_3s'
            WHEN 'PPI' THEN 'cle_3s'
       END AS colonne_cle
  FROM trppu_tmh t
 WHERE t.id_scenario = @id_scenario
   AND t.bl_exclu = 0
   AND NOT EXISTS (SELECT 1 FROM trppu_scenario_exclusions e
                    WHERE e.id_scenario = t.id_scenario AND e.co_produit = t.co_produit)
 GROUP BY t.co_produit
HAVING SUM(t.moyenne_hebdo) > 0;

SELECT * FROM verif_tmh ORDER BY co_produit;


-- -------------------------------------------------------------------------------------
-- 3. Coefficients de rétention par produit/jour (= coefs du log phase=détail)
--    Ramenés en fraction (coef / 100) : toutes les formules ci-dessous les utilisent tels quels.
-- -------------------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS verif_coef;
CREATE TEMPORARY TABLE verif_coef AS
SELECT co_produit, jour_semaine,
       COALESCE(MAX(CASE WHEN densite = 0 THEN coef END), 0) / 100 AS coef_dense,
       COALESCE(MAX(CASE WHEN densite = 1 THEN coef END), 0) / 100 AS coef_faible1,
       COALESCE(MAX(CASE WHEN densite = 2 THEN coef END), 0) / 100 AS coef_faible2
  FROM trppu_pic_coefficients
 WHERE id_pic_version = @id_pic_version
 GROUP BY co_produit, jour_semaine;

SELECT c.*
  FROM verif_coef c
  JOIN verif_tmh t ON t.co_produit = c.co_produit
 ORDER BY c.co_produit, FIELD(c.jour_semaine,
          'LUNDI','MARDI','MERCREDI','JEUDI','VENDREDI','SAMEDI');


-- -------------------------------------------------------------------------------------
-- 4. Recalcul ligne à ligne des trafics écrits — doit renvoyer 0 ligne
-- -------------------------------------------------------------------------------------
-- Toute ligne affichée est un trafic écrit qui ne correspond pas à la formule.
SELECT p.id_pdi, p.co_produit, p.jour_semaine,
       p.dense,   ROUND(t.tmh_hebdo * c.coef_dense   * k.cle) AS dense_attendu,
       p.faible1, ROUND(t.tmh_hebdo * c.coef_faible1 * k.cle) AS faible1_attendu,
       p.faible2, ROUND(t.tmh_hebdo * c.coef_faible2 * k.cle) AS faible2_attendu,
       t.tmh_hebdo, k.cle
  FROM trppu_trafic_pdi p
  JOIN verif_tmh  t ON t.co_produit = p.co_produit
  JOIN verif_coef c ON c.co_produit = p.co_produit AND c.jour_semaine = p.jour_semaine
  JOIN (SELECT id_pdi, cle_colis, cle_oo, cle_3s, cle_potentielip
          FROM trppu_cles_repartition_calcule
         WHERE id_version_cle = @id_version_cle AND co_regate_site = @co_regate) r
    ON r.id_pdi = p.id_pdi
  CROSS JOIN LATERAL (SELECT CASE t.colonne_cle
                                  WHEN 'cle_colis'       THEN r.cle_colis
                                  WHEN 'cle_oo'          THEN r.cle_oo
                                  WHEN 'cle_3s'          THEN r.cle_3s
                                  WHEN 'cle_potentielip' THEN r.cle_potentielip
                             END AS cle) k
 WHERE p.id_scenario = @id_scenario
   AND (   p.dense   <> ROUND(t.tmh_hebdo * c.coef_dense   * k.cle)
        OR p.faible1 <> ROUND(t.tmh_hebdo * c.coef_faible1 * k.cle)
        OR p.faible2 <> ROUND(t.tmh_hebdo * c.coef_faible2 * k.cle))
 LIMIT 100;


-- -------------------------------------------------------------------------------------
-- 5. Totaux par produit/jour (= totaux et somme_cles du log phase=détail)
-- -------------------------------------------------------------------------------------
-- total écrit ≈ TMH × coef × somme des clés des PDI écrits ; l'écart ne vient que des
-- arrondis (au plus 0,5 par PDI).
SELECT p.co_produit, p.jour_semaine,
       COUNT(*)                                       AS nb_pdi,
       SUM(p.dense)                                   AS total_dense,
       ROUND(t.tmh_hebdo * c.coef_dense   * SUM(k.cle), 2) AS theorique_dense,
       SUM(p.faible1)                                 AS total_faible1,
       ROUND(t.tmh_hebdo * c.coef_faible1 * SUM(k.cle), 2) AS theorique_faible1,
       SUM(p.faible2)                                 AS total_faible2,
       ROUND(t.tmh_hebdo * c.coef_faible2 * SUM(k.cle), 2) AS theorique_faible2,
       SUM(k.cle)                                     AS somme_cles
  FROM trppu_trafic_pdi p
  JOIN verif_tmh  t ON t.co_produit = p.co_produit
  JOIN verif_coef c ON c.co_produit = p.co_produit AND c.jour_semaine = p.jour_semaine
  JOIN (SELECT id_pdi, cle_colis, cle_oo, cle_3s, cle_potentielip
          FROM trppu_cles_repartition_calcule
         WHERE id_version_cle = @id_version_cle AND co_regate_site = @co_regate) r
    ON r.id_pdi = p.id_pdi
  CROSS JOIN LATERAL (SELECT CASE t.colonne_cle
                                  WHEN 'cle_colis'       THEN r.cle_colis
                                  WHEN 'cle_oo'          THEN r.cle_oo
                                  WHEN 'cle_3s'          THEN r.cle_3s
                                  WHEN 'cle_potentielip' THEN r.cle_potentielip
                             END AS cle) k
 WHERE p.id_scenario = @id_scenario
 GROUP BY p.co_produit, p.jour_semaine, t.tmh_hebdo,
          c.coef_dense, c.coef_faible1, c.coef_faible2
 ORDER BY p.co_produit, FIELD(p.jour_semaine,
          'LUNDI','MARDI','MERCREDI','JEUDI','VENDREDI','SAMEDI');


-- -------------------------------------------------------------------------------------
-- 6. Plus gros trafics calculables — utile quand le calcul a ÉCHOUÉ (rien d'écrit)
-- -------------------------------------------------------------------------------------
-- Part des clés (pas des lignes écrites) : tous les PDI du site, Agrébal ou non. Le trafic
-- en tête doit correspondre à celui du message « hors capacité » du batch.
SELECT r.id_pdi, t.co_produit, c.jour_semaine,
       t.tmh_hebdo, k.cle,
       c.coef_dense,   ROUND(t.tmh_hebdo * c.coef_dense   * k.cle) AS dense,
       c.coef_faible1, ROUND(t.tmh_hebdo * c.coef_faible1 * k.cle) AS faible1,
       c.coef_faible2, ROUND(t.tmh_hebdo * c.coef_faible2 * k.cle) AS faible2
  FROM verif_tmh t
  JOIN verif_coef c ON c.co_produit = t.co_produit
  JOIN trppu_cles_repartition_calcule r
    ON r.id_version_cle = @id_version_cle AND r.co_regate_site = @co_regate
  CROSS JOIN LATERAL (SELECT CASE t.colonne_cle
                                  WHEN 'cle_colis'       THEN r.cle_colis
                                  WHEN 'cle_oo'          THEN r.cle_oo
                                  WHEN 'cle_3s'          THEN r.cle_3s
                                  WHEN 'cle_potentielip' THEN r.cle_potentielip
                             END AS cle) k
 ORDER BY GREATEST(t.tmh_hebdo * c.coef_dense   * k.cle,
                   t.tmh_hebdo * c.coef_faible1 * k.cle,
                   t.tmh_hebdo * c.coef_faible2 * k.cle) DESC
 LIMIT 20;


DROP TEMPORARY TABLE IF EXISTS verif_tmh;
DROP TEMPORARY TABLE IF EXISTS verif_coef;
