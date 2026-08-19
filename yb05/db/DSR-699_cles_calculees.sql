-- =====================================================================================
-- DSR-699 — Calcul des clés de répartition des PDI
-- =====================================================================================
-- Dernier maillon de la chaîne d'initialisation : divise le trafic de chaque PDI par le
-- total de son site pour produire les clés de `trppu_cles_repartition_calcule`.
--
--   clé colis = trafic_colis du PDI / trafic_colis_total du site
--
-- Le numérateur vient de `trppu_cles_repartition` (DSR-696 source), le dénominateur de
-- `trppu_trafic_site` (DSR-696 cible), et la version de rattachement de `trppu_version_cle`
-- (DSR-698). Les trois doivent donc être en place — cf. `db/README.md` pour l'ordre.
--
-- Critères d'acceptation couverts
--   CA1  une ligne de clés par PDI actif du référentiel
--   CA2  toutes les clés sont rattachées à une version
--   CA3  la somme des clés d'un site vaut 1 (tolérance 0,9999 — 1,0001)
--   CA4  aucune clé d'une version existante n'est modifiée
--
-- Prérequis : `DSR-696-699_migration.sql` (unicité (version, PDI), index d'agrégation),
-- puis `DSR-696_site_trafic.sql` et `DSR-698_version_cle.sql` pour le même référentiel.
--
-- USAGE — renseigner les paramètres ci-dessous, puis :
--   mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-699_cles_calculees.sql
--
-- Le script est REJOUABLE, mais pas au même sens que les deux autres : il ne recalcule
-- JAMAIS une version déjà calculée (CA4). Relancé sur un périmètre déjà chargé, il ne fait
-- rien — voir `@deja` plus bas.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Paramètres
-- -------------------------------------------------------------------------------------
-- Mêmes paramètres que `DSR-696_site_trafic.sql`, volontairement : les deux scripts se
-- jouent l'un après l'autre sur le même périmètre.

SET @id_referentiel := 1;      -- référentiel à calculer — obligatoire
SET @co_regate      := NULL;   -- '123456' pour un seul site, NULL pour tout le référentiel


-- -------------------------------------------------------------------------------------
-- Durcissement du mode SQL — l'échec doit être garanti, pas dépendre du serveur
-- -------------------------------------------------------------------------------------
-- Un site dont le total de trafic est à zéro produirait une division par zéro. En MySQL,
-- celle-ci vaut NULL, refusé par les quatre colonnes cibles `NOT NULL` — mais uniquement si
-- le serveur est en mode strict. Sur un serveur laxiste, la même ligne passerait avec un
-- avertissement et une clé fausse. Le choix retenu est l'ÉCHEC EXPLICITE : mieux vaut une
-- `ERROR 1365` qu'un jeu de clés silencieusement faux, sur lequel tout le calcul de trafic
-- des scénarios s'appuiera ensuite.
--
-- La modification ne vaut que pour la session du script ; elle ne touche pas la
-- configuration du serveur ni les connexions applicatives.

SET SESSION sql_mode = CONCAT(@@sql_mode, ',STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO');


-- -------------------------------------------------------------------------------------
-- Garde-fou 1 — état du périmètre avant calcul
-- -------------------------------------------------------------------------------------
-- `nb_sites_sans_agregat` et `nb_sites_sans_version` doivent valoir 0 : ce sont les deux
-- façons de perdre silencieusement des PDI. Un site sans agrégat DSR-696 n'a pas de
-- dénominateur, un site sans version active DSR-698 n'a pas de conteneur — dans les deux cas
-- la jointure du calcul l'écarte sans rien dire, et le CA1 échoue au contrôle final.
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
-- Doit renvoyer 0 ligne. Toute ligne ici annonce l'`ERROR 1365` de l'étape suivante, et
-- désigne le site et la famille de trafic fautifs. Le cas le plus courant est
-- `potentielip_total` : un site dont aucun PDI ne porte de potentiel IP a un total à zéro.
--
-- Que faire d'une ligne qui remonte : soit le site n'a effectivement aucun trafic de cette
-- famille et il faut décider ce que vaut « sa part » (question métier, pas technique), soit
-- l'agrégat DSR-696 est périmé et il suffit de le recalculer.
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
-- Mémorisé AVANT toute écriture, comme dans `DSR-698_version_cle.sql`, et pour la même
-- raison technique : MySQL refuse de lire la table cible d'un INSERT dans son propre SELECT
-- (erreur 1093), le test est donc déporté sur une variable calculée en amont.
--
-- Le CA4 est ABSOLU : une version déjà calculée n'est jamais retouchée. Si une seule version
-- du périmètre porte des clés, le script ne fait rien du tout — il ne charge pas non plus
-- les autres, pour ne pas laisser un référentiel à moitié calculé sans que rien ne le dise.
-- Pour calculer les sites restants, relancer site par site avec `@co_regate`.
SET @deja := (SELECT COUNT(*)
                FROM trppu_cles_repartition_calcule k
                JOIN trppu_version_cle v ON v.id_version_cle = k.id_version_cle
               WHERE v.id_referentiel = @id_referentiel
                 AND (@co_regate IS NULL OR v.co_regate = @co_regate));


-- -------------------------------------------------------------------------------------
-- Calcul des clés
-- -------------------------------------------------------------------------------------
-- Un seul INSERT : la jointure sur `trppu_version_cle` porte le CA2 (aucune clé ne peut
-- exister sans version), celle sur `trppu_trafic_site` fournit les dénominateurs. Les trois
-- accès sont servis par un index — `idx_cr_ref_actif`, `uq_site_trafic`, `idx_regate_actif`.
--
-- Non listées : `id_cle_repartition` (AUTO_INCREMENT) et `date_creation` (DEFAULT
-- CURRENT_TIMESTAMP, qui horodate le calcul).
--
-- Le `CAST` de la clé potentiel IP n'est pas décoratif. `potentielip` est un `smallint` et
-- `potentielip_total` un `bigint` : pour MySQL, l'échelle du résultat d'une division est
-- celle du premier opérande augmentée de `div_precision_increment` (4 par défaut). Sans
-- CAST, la clé serait donc calculée à 10⁻⁴ près, puis stockée dans un `decimal(24,18)` qui
-- ferait croire à dix-huit décimales significatives. Les trois autres clés partent d'un
-- `decimal(25,19)` et ne sont pas concernées.
--
-- `COALESCE` ne porte que sur le NUMÉRATEUR, seule colonne source nullable. Aucun COALESCE
-- ni NULLIF ne protège les dénominateurs : c'est délibéré, cf. le durcissement du sql_mode.

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

-- CA1 : chaque PDI actif du référentiel a sa ligne de clés — doit renvoyer 0 ligne.
-- Ce qui remonte ici est un PDI perdu par une jointure : site sans agrégat DSR-696, ou site
-- sans version active DSR-698. Les deux garde-fous du début l'annonçaient.
--
-- Le `NOT EXISTS` porte sur (id_referentiel, id_pdi), qu'aucun index ne sert — `uq_crc_
-- version_pdi` commence par `id_version_cle`. C'est délibéré : passer par la version rendrait
-- le contrôle index-friendly, mais aveugle aux sites SANS version, c'est-à-dire au cas même
-- que le CA1 doit détecter. Sur un gros référentiel, restreindre `@co_regate`.
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

-- CA3 : la somme des clés d'un site vaut 1, à 10⁻⁴ près (tolérance du ticket).
-- `verdict` doit valoir OK sur toutes les lignes. Une somme à 0 désigne un dénominateur nul
-- accepté par un serveur laxiste ; une somme franchement supérieure à 1 signale un doublon
-- de PDI ou un agrégat DSR-696 calculé sur un périmètre plus étroit que les clés.
--
-- Le ticket demande une alerte dans les logs : le SQL ne sait pas journaliser. C'est
-- l'appelant — socle ou exploitant — qui remonte les lignes en ANOMALIE, avec le site et la
-- famille de clés concernés.
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

-- CA4 : un PDI n'a qu'une clé par version — doit renvoyer 0 ligne.
-- Garanti en base par `uq_crc_version_pdi` depuis la migration ; le contrôle reste utile là
-- où elle n'aurait pas été jouée.
SELECT id_version_cle,
       id_pdi,
       COUNT(*) AS nb_lignes
  FROM trppu_cles_repartition_calcule
 GROUP BY id_version_cle, id_pdi
HAVING COUNT(*) > 1;
