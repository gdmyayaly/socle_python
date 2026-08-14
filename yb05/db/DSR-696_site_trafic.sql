-- =====================================================================================
-- DSR-696 — Calcul et alimentation des trafics agrégés par site
-- =====================================================================================
-- Alimente `trppu_site_trafic` à partir de `trppu_cles_repartition` : pour un référentiel
-- (et optionnellement un seul site), la somme des trafics des PDI actifs. Ces agrégats sont
-- le dénominateur des clés de répartition — clé = trafic du PDI / total du site.
--
-- Règles de gestion couvertes
--   RG1  seules les lignes actives (`date_fin_validite IS NULL`) sont prises en compte
--   RG2  agrégation par site + référentiel
--   RG3  sommes de trafic_colis, trafic_oo, trafic_3s, potentielip
--   RG4  historisation : chaque référentiel produit son propre jeu d'agrégats
--
-- Prérequis : `DSR-696-698_migration.sql` (clé unique + index de l'agrégation).
--
-- USAGE — renseigner les paramètres ci-dessous, puis :
--   mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-696_site_trafic.sql
--
-- Le script est REJOUABLE : le DELETE ciblé le rend idempotent, deux exécutions
-- consécutives laissent la table dans le même état.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Paramètres
-- -------------------------------------------------------------------------------------
-- Les deux scripts sont joués sur une connexion unique (client `mysql` comme
-- `Database.execute_sql_files`, qui ouvre une connexion dédiée hors pool) : ces variables
-- de session restent donc visibles par toutes les instructions du fichier.

SET @id_referentiel := 1;      -- référentiel à (re)calculer — obligatoire
SET @co_regate      := NULL;   -- '123456' pour un seul site, NULL pour tout le référentiel


-- -------------------------------------------------------------------------------------
-- Garde-fou — un référentiel inexistant produirait un résultat vide silencieux
-- -------------------------------------------------------------------------------------
SELECT @id_referentiel                                        AS id_referentiel_demande,
       COUNT(*)                                               AS nb_pdi_actifs,
       COUNT(DISTINCT co_regate_site)                         AS nb_sites_attendus,
       MIN(date_debut_validite)                               AS debut_validite_min
  FROM trppu_cles_repartition
 WHERE id_referentiel = @id_referentiel
   AND date_fin_validite IS NULL
   AND (@co_regate IS NULL OR co_regate_site = @co_regate);


-- -------------------------------------------------------------------------------------
-- Étape 1 — purge des agrégats déjà chargés pour ce périmètre
-- -------------------------------------------------------------------------------------
-- DELETE puis INSERT, et non `INSERT … ON DUPLICATE KEY UPDATE` : c'est cette séquence qui
-- satisfait le CA4 (« les sites sans PDI actif ne sont pas chargés »). Un site dont tous les
-- PDI ont été clôturés depuis le dernier calcul doit DISPARAÎTRE de la table ; un upsert y
-- laisserait sa ligne périmée, avec un total qui ne correspond plus à rien.
--
-- Le filtre sur le référentiel garantit l'historisation (RG4) : les autres référentiels ne
-- sont jamais touchés.

DELETE FROM trppu_site_trafic
 WHERE id_referentiel = @id_referentiel
   AND (@co_regate IS NULL OR co_regate_site = @co_regate);


-- -------------------------------------------------------------------------------------
-- Étape 2 — calcul des agrégats
-- -------------------------------------------------------------------------------------
-- Colonnes : le ticket annonce `id_site` dans sa liste d'INSERT, mais cette colonne n'existe
-- pas — la table porte `co_regate_site`, que le SELECT du ticket nomme d'ailleurs
-- correctement. Ce sont les noms réels qui sont utilisés ici.
--
-- Non listées volontairement : `id_site_trafic` (AUTO_INCREMENT) et `date_creation`
-- (DEFAULT CURRENT_TIMESTAMP, qui horodate le calcul).
--
-- `COALESCE(potentielip, 0)` : c'est la seule des quatre colonnes sources qui soit nullable,
-- et la cible `potentielip_total` est NOT NULL. Sans cela, un site dont tous les PDI ont un
-- potentiel IP nul ferait échouer l'insertion.
--
-- `MIN(date_debut_validite)` reprend la règle du ticket : l'agrégat est valide depuis la plus
-- ancienne des dates de ses PDI. `date_fin_validite` à NULL marque l'agrégat courant.

INSERT INTO trppu_site_trafic
    (id_referentiel,
     co_regate_site,
     trafic_colis_total,
     trafic_oo_total,
     trafic_3s_total,
     potentielip_total,
     date_debut_validite,
     date_fin_validite)
SELECT id_referentiel,
       co_regate_site,
       SUM(trafic_colis),
       SUM(trafic_oo),
       SUM(trafic_3s),
       SUM(COALESCE(potentielip, 0)),
       MIN(date_debut_validite),
       NULL
  FROM trppu_cles_repartition
 WHERE id_referentiel = @id_referentiel
   AND date_fin_validite IS NULL                              -- RG1
   AND (@co_regate IS NULL OR co_regate_site = @co_regate)
 GROUP BY id_referentiel, co_regate_site;                     -- RG2


-- -------------------------------------------------------------------------------------
-- Contrôles — critères d'acceptation
-- -------------------------------------------------------------------------------------

-- CA1 + CA3 : autant de sites qu'en source, et sommes identiques au calcul direct.
-- `ecart_*` doit valoir 0 partout ; toute ligne non nulle signale un agrégat faux.
SELECT s.co_regate_site,
       s.trafic_colis_total,
       s.trafic_oo_total,
       s.trafic_3s_total,
       s.potentielip_total,
       s.trafic_colis_total - c.somme_colis  AS ecart_colis,
       s.trafic_oo_total    - c.somme_oo     AS ecart_oo,
       s.trafic_3s_total    - c.somme_3s     AS ecart_3s,
       s.potentielip_total  - c.somme_ip     AS ecart_ip
  FROM trppu_site_trafic s
  JOIN (SELECT co_regate_site,
               SUM(trafic_colis)             AS somme_colis,
               SUM(trafic_oo)                AS somme_oo,
               SUM(trafic_3s)                AS somme_3s,
               SUM(COALESCE(potentielip, 0)) AS somme_ip
          FROM trppu_cles_repartition
         WHERE id_referentiel = @id_referentiel
           AND date_fin_validite IS NULL
         GROUP BY co_regate_site) c
    ON c.co_regate_site = s.co_regate_site
 WHERE s.id_referentiel = @id_referentiel
 ORDER BY s.co_regate_site;

-- CA2 : une seule ligne par (site, référentiel) — doit renvoyer 0 ligne.
SELECT id_referentiel, co_regate_site, COUNT(*) AS nb_lignes
  FROM trppu_site_trafic
 GROUP BY id_referentiel, co_regate_site
HAVING COUNT(*) > 1;

-- CA4 : aucun site chargé sans PDI actif — doit renvoyer 0 ligne.
SELECT s.co_regate_site
  FROM trppu_site_trafic s
 WHERE s.id_referentiel = @id_referentiel
   AND NOT EXISTS (SELECT 1
                     FROM trppu_cles_repartition c
                    WHERE c.id_referentiel = s.id_referentiel
                      AND c.co_regate_site = s.co_regate_site
                      AND c.date_fin_validite IS NULL);

-- CA5 : historisation — un jeu d'agrégats par référentiel, les autres sont intacts.
SELECT id_referentiel,
       COUNT(*)           AS nb_sites,
       MIN(date_creation) AS premier_calcul,
       MAX(date_creation) AS dernier_calcul
  FROM trppu_site_trafic
 GROUP BY id_referentiel
 ORDER BY id_referentiel;
