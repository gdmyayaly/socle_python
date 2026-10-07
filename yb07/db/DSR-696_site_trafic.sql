-- =====================================================================================
-- DSR-696 — Calcul et alimentation des trafics agrégés par site
-- =====================================================================================
-- Alimente `trppu_trafic_site` : somme des trafics des PDI actifs par site, pour un
-- référentiel (et optionnellement un site). Dénominateur des clés de répartition.
--
-- Règles : RG1 lignes actives seules, RG2 agrégation site + référentiel, RG3 sommes des
-- quatre trafics, RG4 un jeu d'agrégats par référentiel.
--
-- Prérequis : `DSR-696-699_migration.sql`.
-- USAGE : mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-696_site_trafic.sql
-- REJOUABLE : le DELETE ciblé rend le script idempotent.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Paramètres
-- -------------------------------------------------------------------------------------
-- Variables de session : le script est joué sur une connexion unique.

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
-- DELETE puis INSERT (pas d'upsert) : un site sans plus aucun PDI actif doit disparaître
-- (CA4). Le filtre sur le référentiel préserve les autres jeux (RG4).

DELETE FROM trppu_trafic_site
 WHERE id_referentiel = @id_referentiel
   AND (@co_regate IS NULL OR co_regate_site = @co_regate);


-- -------------------------------------------------------------------------------------
-- Étape 2 — calcul des agrégats
-- -------------------------------------------------------------------------------------
-- Noms de colonnes réels (`co_regate_site`), non ceux du ticket. `COALESCE(potentielip, 0)` :
-- seule source nullable, cible NOT NULL. `date_fin_validite` reste NULL partout : le jeu
-- courant se filtre par `id_referentiel`, pas par `date_fin_validite IS NULL`.

INSERT INTO trppu_trafic_site
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
  -- IGNORE INDEX : lecture séquentielle plutôt que 22 M lectures aléatoires par
  -- `idx_cr_ref_actif` (des heures avec un petit cache InnoDB). Un calcul mono-site lit
  -- donc toute la table — `init` n'en fait jamais.
  FROM trppu_cles_repartition IGNORE INDEX (idx_cr_ref_actif)
 WHERE id_referentiel = @id_referentiel
   AND date_fin_validite IS NULL                              -- RG1
   AND (@co_regate IS NULL OR co_regate_site = @co_regate)
 GROUP BY id_referentiel, co_regate_site;                     -- RG2


-- -------------------------------------------------------------------------------------
-- Contrôles — critères d'acceptation
-- -------------------------------------------------------------------------------------

-- CA1 + CA3 : mêmes sites (ayant au moins un PDI actif) et mêmes sommes qu'en source.
-- `ecart_*` doit valoir 0 partout.
SELECT s.co_regate_site,
       s.trafic_colis_total,
       s.trafic_oo_total,
       s.trafic_3s_total,
       s.potentielip_total,
       s.trafic_colis_total - c.somme_colis  AS ecart_colis,
       s.trafic_oo_total    - c.somme_oo     AS ecart_oo,
       s.trafic_3s_total    - c.somme_3s     AS ecart_3s,
       s.potentielip_total  - c.somme_ip     AS ecart_ip
  FROM trppu_trafic_site s
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
  FROM trppu_trafic_site
 GROUP BY id_referentiel, co_regate_site
HAVING COUNT(*) > 1;

-- CA4 : aucun site chargé sans PDI actif — doit renvoyer 0 ligne.
SELECT s.co_regate_site
  FROM trppu_trafic_site s
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
  FROM trppu_trafic_site
 GROUP BY id_referentiel
 ORDER BY id_referentiel;
