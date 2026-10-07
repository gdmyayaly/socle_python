-- =====================================================================================
-- DSR-697 — Chargement initial du référentiel des PDI (fichier CSV métier)
-- =====================================================================================
-- PREMIER maillon de la chaîne : alimente `trppu_cles_repartition` depuis le CSV métier,
-- seule source de DSR-696, DSR-698 et DSR-699.
-- Règles de gestion couvertes
--   RG1  toutes les lignes chargées portent le même `id_referentiel`
--   RG2  lignes actives : `date_debut_validite` = date du chargement, `date_fin_validite` NULL
--   RG3  champs vides convertis en NULL — co_regate_etablissement, lb_etablissement,
--        nb_pre, potentielip
--   RG4  doublons stricts éliminés — HORS de ce script, cf. `db/README.md`
--   RG5  unicité (id_pdi, id_referentiel)
--   RG6  purge du référentiel cible avant chargement
--
-- Prérequis : fichier DÉDOUBLONNÉ (RG4) dans `@@secure_file_priv` ; privilège `FILE` ;
-- unicité `uk_pdi_ref (id_pdi, id_referentiel)` (RG5). La migration se joue APRÈS, pour
-- construire `idx_cr_ref_actif` une fois sur la table pleine.
-- ATTENTION — chemin du fichier À ÉDITER À LA MAIN : `LOAD DATA` n'accepte qu'un littéral
-- et n'est pas préparable.
-- Usage : mysql -h <hote> -u <user> -p dsr_mercure_aa < db/DSR-697_chargement_cles_repartition.sql
-- REJOUABLE (purge RG6), sans DDL donc annulable — ROLLBACK coûteux sur 22 M de lignes.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Paramètres
-- -------------------------------------------------------------------------------------
-- Alimente la purge et `id_referentiel` (RG1), mais PAS le chemin du fichier.

SET @id_referentiel := 1;      -- référentiel chargé — obligatoire


-- -------------------------------------------------------------------------------------
-- Durcissement du mode SQL — un fichier mal formé doit échouer, pas se tronquer
-- -------------------------------------------------------------------------------------
-- Hors mode strict, `LOAD DATA` tronque ou met à 0 sur simple avertissement : on veut une
-- erreur plutôt qu'un trafic faux. Portée : la session seule.

SET SESSION sql_mode = CONCAT(@@sql_mode, ',STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE');


-- -------------------------------------------------------------------------------------
-- Garde-fou — état du référentiel cible et accès au fichier, AVANT toute écriture
-- -------------------------------------------------------------------------------------
-- `repertoire_autorise` : chemin où déposer le fichier, vide = libre, NULL = passer par
-- `LOCAL` (cf. `db/README.md`). `nb_lignes_deja_presentes` : ce que la purge RG6 supprimera.
-- `trppu_referentiel` n'est pas interrogée : le référentiel est porté par `trppu_version_cle`.
SELECT @id_referentiel                                          AS id_referentiel_demande,
       @@secure_file_priv                                       AS repertoire_autorise,
       (SELECT COUNT(*) FROM trppu_cles_repartition
         WHERE id_referentiel = @id_referentiel)                AS nb_lignes_deja_presentes,
       (SELECT COUNT(*) FROM trppu_trafic_site
         WHERE id_referentiel = @id_referentiel)                AS agregats_dsr696_a_recalculer;


-- -------------------------------------------------------------------------------------
-- Étape 1 — purge du référentiel cible (RG6)
-- -------------------------------------------------------------------------------------
-- Seul le référentiel chargé est purgé (historisation). DELETE et non TRUNCATE : TRUNCATE
-- viderait tous les référentiels et, DDL, commiterait implicitement. Partie la plus lente
-- (cf. `db/README.md`, « Rejouabilité »).
-- Les agrégats DSR-696 ne sont pas purgés ici : rejouer DSR-696 après ce chargement.

DELETE FROM trppu_cles_repartition
 WHERE id_referentiel = @id_referentiel;


-- -------------------------------------------------------------------------------------
-- Étape 2 — chargement du fichier
-- -------------------------------------------------------------------------------------
-- CHEMIN À ÉDITER (littéral obligatoire, cf. en-tête).
-- Colonnes dans l'ordre DU FICHIER (mapping positionnel). Champs `@…` convertis en NULL si
-- vides (RG3) : 0 et « inconnu » ne se confondent pas.
-- `'\n'` suppose un CSV Unix : sous Windows, passer à `'\r\n'` (sinon erreur 1265 sur
-- `@potentielip` en mode strict).
-- Ni `IGNORE` ni `REPLACE` : un doublon (id_pdi, id_referentiel) doit échouer (erreur 1062).

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
-- `nb_lignes` = lignes du fichier dédoublonné hors en-tête = `nb_pdi_distincts`.
-- (`COUNT` du ticket corrigé en `COUNT(*)` : ERROR 1054 sinon.)
SELECT @id_referentiel                AS id_referentiel,
       COUNT(*)                       AS nb_lignes,
       COUNT(DISTINCT id_pdi)         AS nb_pdi_distincts,
       COUNT(DISTINCT co_regate_site) AS nb_sites,
       COUNT(DISTINCT co_regate_dex)  AS nb_dex,
       MIN(date_debut_validite)       AS date_chargement
  FROM trppu_cles_repartition
 WHERE id_referentiel = @id_referentiel;

-- Contrôle 2 (CA3 + RG5) : aucun PDI chargé deux fois — doit renvoyer 0 ligne.
-- Garanti par `uk_pdi_ref`, sauf si l'index a été retiré pour un chargement de masse.
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
-- `nb_chaines_vides` doit valoir 0 ; les compteurs de NULL sont pour mémoire.
-- `COALESCE` : sans lui, un référentiel sans établissement afficherait NULL au lieu de 0.
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
-- Totaux en `decimal(24,18)` : 999999 au plus par site. En ANOMALIE, jouer
-- `fix_error.sql` avant DSR-696 (sinon `ERROR 1264`).
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
-- `nb_sites` = lignes attendues dans `trppu_trafic_site` et versions à créer (DSR-698).
SELECT id_referentiel,
       COUNT(*)                       AS nb_lignes,
       COUNT(DISTINCT co_regate_site) AS nb_sites,
       MIN(date_debut_validite)       AS debut_validite_min,
       SUM(date_fin_validite IS NULL) AS nb_lignes_actives
  FROM trppu_cles_repartition
 GROUP BY id_referentiel
 ORDER BY id_referentiel;
