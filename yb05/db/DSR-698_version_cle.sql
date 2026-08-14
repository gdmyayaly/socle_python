-- =====================================================================================
-- DSR-698 — Création d'une version de clés pour un site
-- =====================================================================================
-- Crée dans `trppu_version_cle` le conteneur des clés de répartition d'un site pour un
-- référentiel donné. À jouer dès qu'un nouveau référentiel apparaît pour ce site : arrivée
-- de PDI, changement de rattachement, évolution de trafic.
--
-- Règle de gestion : une modification de périmètre PDI ou de trafic n'entraîne la création
-- d'une version QUE pour le site concerné — d'où le paramètre `@co_regate`, obligatoire.
--
-- Prérequis : `DSR-696-698_migration.sql` (index (co_regate, actif)).
--
-- USAGE — renseigner les paramètres ci-dessous, puis :
--   mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-698_version_cle.sql
--
-- Le script est REJOUABLE : si le site possède déjà une version active sur ce référentiel,
-- il ne fait rien (ni désactivation, ni création). Cf. `@deja` ci-dessous.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Paramètres
-- -------------------------------------------------------------------------------------
SET @id_referentiel := 2;                       -- référentiel rattaché — obligatoire
SET @co_regate      := '123456';                -- code régate du site — obligatoire
SET @commentaire    := 'Réorganisation DEX';    -- motif métier, visible en recette
SET @libelle        := NULL;                    -- colonne présente en base, absente du ticket


-- -------------------------------------------------------------------------------------
-- Garde-fous
-- -------------------------------------------------------------------------------------
-- État courant du site : version active éventuelle, et dernier référentiel connu. Un écart
-- entre les deux est ce que contrôle DSR-701 règle 10 avant d'autoriser un calcul.
SELECT @co_regate                                              AS co_regate,
       @id_referentiel                                         AS id_referentiel_demande,
       (SELECT id_version_cle FROM trppu_version_cle
         WHERE co_regate = @co_regate AND actif = 'O'
         ORDER BY id_version_cle DESC LIMIT 1)                 AS version_active_actuelle,
       (SELECT id_referentiel FROM trppu_version_cle
         WHERE co_regate = @co_regate AND actif = 'O'
         ORDER BY id_version_cle DESC LIMIT 1)                 AS referentiel_de_cette_version,
       (SELECT MAX(id_referentiel) FROM trppu_referentiel
         WHERE co_regate = @co_regate)                         AS dernier_referentiel_du_site,
       (SELECT COUNT(*) FROM trppu_site_trafic
         WHERE id_referentiel = @id_referentiel
           AND co_regate_site = @co_regate)                    AS agregats_dsr696_presents;

-- Rejouabilité — mémorisé AVANT toute écriture : le site a-t-il déjà une version active sur
-- ce référentiel ? Si oui (`@deja` > 0), les deux instructions suivantes sont neutralisées.
-- Sans ce garde-fou, relancer le script créerait une seconde version pour le même
-- référentiel en désactivant la première, ce qui fausserait la traçabilité.
SET @deja := (SELECT COUNT(*)
                FROM trppu_version_cle
               WHERE co_regate = @co_regate
                 AND id_referentiel = @id_referentiel
                 AND actif = 'O');


-- -------------------------------------------------------------------------------------
-- Étape 1 — désactivation de la version active précédente du site
-- -------------------------------------------------------------------------------------
-- Une nouvelle version remplace la précédente : le site ne doit avoir qu'une seule version
-- active à un instant donné, faute de quoi la lecture d'éligibilité de DSR-701 règle 9
-- (`WHERE co_regate = ? AND actif = 'O'`) renverrait plusieurs lignes et deviendrait
-- ambiguë. Restreint au seul site concerné, conformément à la RG du ticket.

UPDATE trppu_version_cle
   SET actif = 'N'
 WHERE co_regate = @co_regate
   AND actif = 'O'
   AND @deja = 0;


-- -------------------------------------------------------------------------------------
-- Étape 2 — création de la nouvelle version
-- -------------------------------------------------------------------------------------
-- `SELECT … FROM DUAL WHERE @deja = 0` plutôt qu'un `INSERT … WHERE NOT EXISTS (SELECT …
-- FROM trppu_version_cle)` : MySQL refuse de lire la table cible d'un INSERT dans son propre
-- SELECT (erreur 1093). D'où le test déporté sur la variable calculée plus haut.
--
-- Non listées : `id_version_cle` (AUTO_INCREMENT — CA2, identifiant unique) et
-- `date_creation` (DEFAULT CURRENT_TIMESTAMP).

INSERT INTO trppu_version_cle
    (id_referentiel, libelle, co_regate, actif, commentaire)
SELECT @id_referentiel, @libelle, @co_regate, 'O', @commentaire
  FROM DUAL
 WHERE @deja = 0;


-- -------------------------------------------------------------------------------------
-- Contrôles — critères d'acceptation
-- -------------------------------------------------------------------------------------

-- CA1 + CA2 : la version créée (ou celle déjà en place si le script n'a rien fait).
SELECT id_version_cle, id_referentiel, co_regate, libelle, actif, commentaire, date_creation
  FROM trppu_version_cle
 WHERE co_regate = @co_regate
 ORDER BY id_version_cle DESC;

-- CA3 : exactement une version active pour ce site, et elle porte le référentiel demandé.
-- `nb_versions_actives` doit valoir 1 et `referentiel_actif` être égal au paramètre.
SELECT COUNT(*)                                          AS nb_versions_actives,
       MAX(id_version_cle)                               AS id_version_active,
       MAX(id_referentiel)                               AS referentiel_actif,
       IF(COUNT(*) = 1 AND MAX(id_referentiel) = @id_referentiel, 'OK', 'ANOMALIE') AS verdict
  FROM trppu_version_cle
 WHERE co_regate = @co_regate
   AND actif = 'O';
