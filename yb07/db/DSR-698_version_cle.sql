-- =====================================================================================
-- DSR-698 — Création d'une version de clés pour un site
-- =====================================================================================
-- Crée dans `trppu_version_cle` le conteneur des clés d'un site pour un référentiel. Une
-- version n'est créée QUE pour le site concerné, d'où `@co_regate` obligatoire.
--
-- Prérequis : `DSR-696-699_migration.sql` (colonne `date_creation`).
-- USAGE : mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-698_version_cle.sql
-- REJOUABLE : si le site a déjà une version active sur ce référentiel, rien n'est fait
-- (cf. `@deja`).
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
-- État courant du site : version active éventuelle et son référentiel.
SELECT @co_regate                                              AS co_regate,
       @id_referentiel                                         AS id_referentiel_demande,
       (SELECT id_version_cle FROM trppu_version_cle
         WHERE co_regate = @co_regate AND actif = 'O'
         ORDER BY id_version_cle DESC LIMIT 1)                 AS version_active_actuelle,
       (SELECT id_referentiel FROM trppu_version_cle
         WHERE co_regate = @co_regate AND actif = 'O'
         ORDER BY id_version_cle DESC LIMIT 1)                 AS referentiel_de_cette_version,
       (SELECT COUNT(*) FROM trppu_trafic_site
         WHERE id_referentiel = @id_referentiel
           AND co_regate_site = @co_regate)                    AS agregats_dsr696_presents;

-- Rejouabilité — mémorisé AVANT toute écriture : si `@deja` > 0, l'UPDATE et l'INSERT
-- suivants sont neutralisés (sinon une relance créerait une seconde version).
SET @deja := (SELECT COUNT(*)
                FROM trppu_version_cle
               WHERE co_regate = @co_regate
                 AND id_referentiel = @id_referentiel
                 AND actif = 'O');


-- -------------------------------------------------------------------------------------
-- Étape 1 — désactivation de la version active précédente du site
-- -------------------------------------------------------------------------------------
-- Une seule version active par site, sinon la lecture d'éligibilité de DSR-701 règle 9
-- devient ambiguë. `date_fin_validite` est posée avec `actif = 'N'` pour rester cohérente.

UPDATE trppu_version_cle
   SET actif = 'N',
       date_fin_validite = NOW()
 WHERE co_regate = @co_regate
   AND actif = 'O'
   AND @deja = 0;


-- -------------------------------------------------------------------------------------
-- Étape 2 — création de la nouvelle version
-- -------------------------------------------------------------------------------------
-- `FROM DUAL WHERE @deja = 0` : MySQL refuse de lire la table cible d'un INSERT dans son
-- propre SELECT (erreur 1093). `id_version_cle`, `date_creation` et `date_debut_validite`
-- sont alimentées par la base.

INSERT INTO trppu_version_cle
    (id_referentiel, libelle, co_regate, actif, commentaire)
SELECT @id_referentiel, @libelle, @co_regate, 'O', @commentaire
  FROM DUAL
 WHERE @deja = 0;


-- -------------------------------------------------------------------------------------
-- Contrôles — critères d'acceptation
-- -------------------------------------------------------------------------------------

-- CA1 + CA2 : la version créée (ou celle déjà en place si le script n'a rien fait).
SELECT id_version_cle, id_referentiel, co_regate, libelle, actif, commentaire,
       date_creation, date_debut_validite, date_fin_validite
  FROM trppu_version_cle
 WHERE co_regate = @co_regate
 ORDER BY id_version_cle DESC;

-- CA3 : exactement une version active pour ce site, et elle porte le référentiel demandé.
SELECT COUNT(*)                                          AS nb_versions_actives,
       MAX(id_version_cle)                               AS id_version_active,
       MAX(id_referentiel)                               AS referentiel_actif,
       IF(COUNT(*) = 1 AND MAX(id_referentiel) = @id_referentiel, 'OK', 'ANOMALIE') AS verdict
  FROM trppu_version_cle
 WHERE co_regate = @co_regate
   AND actif = 'O';
