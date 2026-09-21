-- =====================================================================================
-- DSR-697 — Chargement initial du référentiel des PDI (fichier CSV métier)
-- =====================================================================================
-- PREMIER maillon de la chaîne : alimente `trppu_cles_repartition` à partir du fichier CSV
-- fourni par le métier. Tout le reste en découle — les agrégats DSR-696, les versions
-- DSR-698 et les clés DSR-699 ne lisent que cette table.
--
--   fichier CSV métier  →  trppu_cles_repartition  →  trppu_trafic_site  →  …
--
-- Règles de gestion couvertes
--   RG1  toutes les lignes chargées portent le même `id_referentiel`
--   RG2  lignes actives : `date_debut_validite` = date du chargement, `date_fin_validite` NULL
--   RG3  champs vides convertis en NULL — co_regate_etablissement, lb_etablissement,
--        nb_pre, potentielip
--   RG4  doublons stricts éliminés — HORS de ce script, cf. `db/README.md`
--   RG5  unicité (id_pdi, id_referentiel)
--   RG6  purge du référentiel cible avant chargement
--
-- Prérequis
--   * le fichier est DÉDOUBLONNÉ (RG4) et déposé dans le répertoire autorisé du serveur
--     MySQL — `@@secure_file_priv`, typiquement `/var/lib/mysql-files/` ;
--   * le compte utilisé porte le privilège `FILE` ;
--   * l'unicité `uk_pdi_ref (id_pdi, id_referentiel)` est en place — elle l'est dans le
--     schéma livré, et c'est elle qui rend la RG5 vraie en base.
--
-- `DSR-696-699_migration.sql` n'est PAS un prérequis de ce script : aucun des quatre objets
-- qu'elle pose n'est lu ici. L'ordre recommandé est même l'inverse — charger, PUIS migrer,
-- pour que `idx_cr_ref_actif` soit construit une fois sur la table pleine plutôt que
-- maintenu ligne à ligne pendant le chargement. Cf. `db/README.md`.
--
-- ATTENTION — le chemin du fichier ne peut pas être paramétré. `LOAD DATA` n'accepte qu'un
-- littéral, et la voie de contournement habituelle (PREPARE/EXECUTE, comme la migration)
-- n'est pas ouverte : cette instruction ne figure pas parmi les instructions préparables.
-- Le chemin de l'étape 2 est donc À ÉDITER À LA MAIN, en même temps que `@id_referentiel`.
--
-- USAGE — renseigner le paramètre et le chemin, puis :
--   mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-697_chargement_cles_repartition.sql
--
-- Le script est REJOUABLE : la purge RG6 le rend idempotent, deux exécutions consécutives
-- laissent la table dans le même état. Il ne contient aucun DDL et reste donc entièrement
-- annulable — mais sur 22 M de lignes, le ROLLBACK a un coût, cf. `db/README.md`.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Paramètres
-- -------------------------------------------------------------------------------------
-- Comme les trois autres scripts, le fichier tourne sur une connexion unique : cette
-- variable de session reste visible par toutes ses instructions. Elle alimente la purge et
-- la colonne `id_referentiel` des lignes chargées (RG1), mais PAS le chemin du fichier.

SET @id_referentiel := 1;      -- référentiel chargé — obligatoire


-- -------------------------------------------------------------------------------------
-- Durcissement du mode SQL — un fichier mal formé doit échouer, pas se tronquer
-- -------------------------------------------------------------------------------------
-- Hors mode strict, `LOAD DATA` ne rejette presque rien : une valeur non numérique devient
-- 0, une chaîne trop longue est tronquée, une date invalide devient '0000-00-00', et le
-- chargement se termine « avec succès » sur un avertissement. C'est exactement ce qu'il ne
-- faut pas pour une photographie du référentiel dont tout le calcul des clés dépend : mieux
-- vaut une erreur au chargement qu'un trafic silencieusement ramené à zéro.
--
-- La modification ne vaut que pour la session du script ; elle ne touche ni la configuration
-- du serveur, ni les connexions applicatives.

SET SESSION sql_mode = CONCAT(@@sql_mode, ',STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE');


-- -------------------------------------------------------------------------------------
-- Garde-fou — état du référentiel cible et accès au fichier, AVANT toute écriture
-- -------------------------------------------------------------------------------------
-- `repertoire_autorise` donne le seul répertoire depuis lequel le serveur acceptera de lire.
-- Trois valeurs possibles : un chemin (déposer le fichier dedans), une chaîne vide (aucune
-- restriction) ou NULL (`LOAD DATA INFILE` interdit — il faut alors passer par la variante
-- `LOCAL`, cf. `db/README.md`).
--
-- `nb_lignes_deja_presentes` annonce ce que la purge RG6 va supprimer. Sur un rechargement,
-- ce nombre doit correspondre au chargement précédent ; sur un premier chargement, il vaut 0.
--
-- `referentiel_declare` doit valoir 1 : `trppu_referentiel` ne porte aucune clé étrangère
-- vers cette table, rien n'empêche donc de charger 22 M de lignes sous un identifiant de
-- référentiel qui n'existe pas. C'est ici, et seulement ici, que l'écart se voit.
SELECT @id_referentiel                                          AS id_referentiel_demande,
       @@secure_file_priv                                       AS repertoire_autorise,
       (SELECT COUNT(*) FROM trppu_cles_repartition
         WHERE id_referentiel = @id_referentiel)                AS nb_lignes_deja_presentes,
       (SELECT COUNT(*) FROM trppu_referentiel
         WHERE id_referentiel = @id_referentiel)                AS referentiel_declare,
       (SELECT COUNT(*) FROM trppu_trafic_site
         WHERE id_referentiel = @id_referentiel)                AS agregats_dsr696_a_recalculer;


-- -------------------------------------------------------------------------------------
-- Étape 1 — purge du référentiel cible (RG6)
-- -------------------------------------------------------------------------------------
-- « Le chargement est réalisé dans le référentiel cible après suppression des données
-- éventuellement déjà présentes pour le référentiel à charger. » Le filtre porte sur le seul
-- référentiel chargé : les autres sont intacts, c'est ce qui fait l'historisation.
--
-- DELETE et non TRUNCATE, bien que la table soit volumineuse et que TRUNCATE soit
-- instantané : TRUNCATE viderait TOUS les référentiels, remettrait l'AUTO_INCREMENT à zéro
-- et, étant du DDL, provoquerait un commit implicite qui interdirait tout retour arrière.
--
-- À savoir sur un rechargement complet : ce DELETE est la partie la plus lente du script et
-- construit un journal d'annulation à la mesure du nombre de lignes supprimées. Cf.
-- `db/README.md`, section « Rejouabilité », pour la marche à suivre quand il devient
-- problématique.
--
-- Les agrégats DSR-696 déjà calculés sur ce référentiel ne sont PAS purgés ici : ils
-- deviennent périmés, et c'est `DSR-696_site_trafic.sql` — dont la première écriture est
-- précisément un DELETE ciblé — qui les remplace. D'où le compteur du garde-fou ci-dessus :
-- s'il est non nul, DSR-696 est à rejouer après ce chargement.

DELETE FROM trppu_cles_repartition
 WHERE id_referentiel = @id_referentiel;


-- -------------------------------------------------------------------------------------
-- Étape 2 — chargement du fichier
-- -------------------------------------------------------------------------------------
-- CHEMIN À ÉDITER — il ne peut pas venir d'une variable, cf. l'en-tête du fichier.
--
-- La liste de colonnes décrit l'ordre des champs DU FICHIER, pas celui de la table : le
-- mapping est positionnel. Elle suit donc exactement l'ordre du ticket. Quatre champs sont
-- captés dans des variables `@…` pour être convertis avant écriture (RG3) : sans ce détour,
-- une chaîne vide deviendrait `''` sur les deux colonnes texte et 0 sur les deux colonnes
-- numériques — or 0 et « inconnu » ne se confondent pas pour un potentiel IP.
--
-- Les quatre colonnes restantes de la table ne viennent pas du fichier :
--   * `id` est AUTO_INCREMENT ;
--   * `id_referentiel`, `date_debut_validite` et `date_fin_validite` sont posées par le SET
--     ci-dessous (RG1, RG2).
--
-- `LINES TERMINATED BY '\n'` suppose un fichier à fins de ligne Unix. Un CSV produit sous
-- Windows se termine par `\r\n` : le `\r` resterait collé au dernier champ de chaque ligne —
-- ici `@potentielip`, dont la conversion numérique échouerait alors en mode strict.
-- Remplacer par `'\r\n'` le cas échéant ; c'est le premier réflexe devant une erreur 1265
-- sur la dernière colonne.
--
-- Pas de `IGNORE` ni de `REPLACE` : un doublon de `(id_pdi, id_referentiel)` doit faire
-- ÉCHOUER le chargement (erreur 1062) et non se voir silencieusement écarté ou écrasé. Un
-- tel doublon signale un fichier dont la déduplication RG4 n'a pas suffi — elle porte sur la
-- ligne entière, pas sur le PDI : deux lignes d'un même PDI aux trafics différents y
-- survivent toutes les deux. Cf. `db/README.md`, contrôle du fichier avant dépôt.

LOAD DATA INFILE '/var/lib/mysql-files/cles_repartitions_final_joined_ref1.csv'
  INTO TABLE trppu_cles_repartition
  CHARACTER SET utf8mb4
  FIELDS TERMINATED BY ';' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  IGNORE 1 ROWS
    (id_pdi,
     pdi_rattache,
     trafic_colis,
     trafic_oo,
     trafic_3s,
     nature,
     co_regate_site,
     type_site,
     lb_regate,
     @regate_etab,
     @libelle_etab,
     co_regate_dex,
     lb_dex,
     @nb_pre,
     @potentielip)
  SET co_regate_etablissement = NULLIF(@regate_etab, ''),
      lb_etablissement        = NULLIF(@libelle_etab, ''),
      nb_pre                  = NULLIF(@nb_pre, ''),
      potentielip             = NULLIF(@potentielip, ''),
      id_referentiel          = @id_referentiel,
      date_debut_validite     = CURRENT_DATE(),
      date_fin_validite       = NULL;


-- -------------------------------------------------------------------------------------
-- Contrôles — critères d'acceptation
-- -------------------------------------------------------------------------------------

-- Contrôle 1 (CA1 + CA3) : volumétrie chargée.
-- `nb_lignes` doit être égal au nombre de lignes du fichier dédoublonné, en-tête déduit, et
-- `nb_pdi_distincts` lui être égal — sinon le chargement aurait échoué sur `uk_pdi_ref`.
--
-- Le ticket écrit `SELECT COUNT FROM …` : sans parenthèses, `COUNT` est lu comme un nom de
-- colonne et l'instruction échoue en `ERROR 1054`. Corrigé ici, comme dans les contrôles
-- suivants.
SELECT @id_referentiel                AS id_referentiel,
       COUNT(*)                       AS nb_lignes,
       COUNT(DISTINCT id_pdi)         AS nb_pdi_distincts,
       COUNT(DISTINCT co_regate_site) AS nb_sites,
       COUNT(DISTINCT co_regate_dex)  AS nb_dex,
       MIN(date_debut_validite)       AS date_chargement
  FROM trppu_cles_repartition
 WHERE id_referentiel = @id_referentiel;

-- Contrôle 2 (CA3 + RG5) : aucun PDI chargé deux fois — doit renvoyer 0 ligne.
-- Garanti en base par `uk_pdi_ref` ; le contrôle reste utile là où cet index aurait été
-- retiré pour accélérer un chargement de masse.
SELECT id_pdi,
       COUNT(*) AS nb_lignes
  FROM trppu_cles_repartition
 WHERE id_referentiel = @id_referentiel
 GROUP BY id_pdi
HAVING COUNT(*) > 1;

-- Contrôle 3 (CA5 + CA6) : toutes les lignes sont actives — doit renvoyer 0 ligne.
SELECT id_pdi,
       date_debut_validite,
       date_fin_validite
  FROM trppu_cles_repartition
 WHERE id_referentiel = @id_referentiel
   AND date_fin_validite IS NOT NULL
 LIMIT 50;

-- Contrôle 4 (CA4) : les champs métier vides sont stockés à NULL.
-- `nb_chaines_vides` doit valoir 0 — une seule chaîne vide restante signerait un `SET` de
-- l'étape 2 oublié, et se propagerait en clé de jointure fantôme pour l'établissement.
-- Les compteurs de NULL sont donnés pour mémoire : ils ne sont pas anormaux, ils mesurent la
-- part du fichier qui ne renseigne pas ces champs.
-- Le `COALESCE` du premier compteur n'est pas décoratif : sur une ligne dont les deux
-- colonnes sont NULL, la comparaison rend NULL, et `SUM` l'ignorerait — un référentiel sans
-- aucun établissement renseigné afficherait alors NULL là où l'on attend 0.
SELECT SUM(COALESCE(co_regate_etablissement = ''
                 OR lb_etablissement = '', 0))                     AS nb_chaines_vides,
       SUM(co_regate_etablissement IS NULL)                        AS nb_etablissement_null,
       SUM(lb_etablissement IS NULL)                               AS nb_libelle_etab_null,
       SUM(nb_pre IS NULL)                                         AS nb_pre_null,
       SUM(potentielip IS NULL)                                    AS nb_potentielip_null,
       SUM(potentielip = 0)                                        AS nb_potentielip_zero
  FROM trppu_cles_repartition
 WHERE id_referentiel = @id_referentiel;

-- Contrôle 5 — débordement décimal, à lire AVANT de jouer DSR-696.
-- Les trafics sources sont en `decimal(25,19)`, les totaux de `trppu_trafic_site` en
-- `decimal(24,18)` : six chiffres avant la virgule, soit 999999 au maximum. Or DSR-696 y
-- écrit la SOMME des trafics d'un site. `verdict` doit valoir OK ; en ANOMALIE, élargir les
-- trois colonnes `trafic_*_total` avant de lancer l'agrégation, qui échouerait sinon en
-- `ERROR 1264 (Out of range value)`.
SELECT MAX(somme_colis)                             AS max_somme_colis,
       MAX(somme_oo)                                AS max_somme_oo,
       MAX(somme_3s)                                AS max_somme_3s,
       IF(GREATEST(MAX(somme_colis), MAX(somme_oo), MAX(somme_3s)) >= 999999,
          'ANOMALIE', 'OK')                         AS verdict
  FROM (SELECT co_regate_site,
               SUM(trafic_colis) AS somme_colis,
               SUM(trafic_oo)    AS somme_oo,
               SUM(trafic_3s)    AS somme_3s
          FROM trppu_cles_repartition
         WHERE id_referentiel = @id_referentiel
           AND date_fin_validite IS NULL
         GROUP BY co_regate_site) x;

-- Contrôle 6 (CA7) : historisation — un jeu de lignes par référentiel, les autres intacts.
-- C'est aussi le périmètre que DSR-696 puis DSR-699 vont traiter : `nb_sites` du référentiel
-- chargé est le nombre de lignes attendu dans `trppu_trafic_site`, et le nombre de versions
-- à créer par DSR-698.
SELECT id_referentiel,
       COUNT(*)                       AS nb_lignes,
       COUNT(DISTINCT co_regate_site) AS nb_sites,
       MIN(date_debut_validite)       AS debut_validite_min,
       SUM(date_fin_validite IS NULL) AS nb_lignes_actives
  FROM trppu_cles_repartition
 GROUP BY id_referentiel
 ORDER BY id_referentiel;
