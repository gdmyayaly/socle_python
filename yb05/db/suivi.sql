-- =====================================================================================
-- SUIVI — où en est le traitement en cours, vu depuis une AUTRE session
-- =====================================================================================
-- À lancer dans un second terminal pendant qu'un script de la chaîne travaille : chargement
-- DSR-697 (24 M de lignes), `ALTER` de `fix_error.sql`, agrégation DSR-696, calcul DSR-699.
--
-- LE CAS COURANT — l'`INSERT INTO trppu_trafic_site … SELECT … GROUP BY` de DSR-696. Cette
-- instruction balaie les 24 M de lignes du référentiel pour n'en écrire que quelques milliers.
-- Le compteur à regarder est donc `lignes_lues` du bloc 1 (`ROWS_EXAMINED`), rapporté aux
-- 24 M de `@lignes_attendues` : c'est lui qui avance. `lignes_ecrites` restera à 0 jusqu'aux
-- toutes dernières secondes, et `agregats_696` (bloc 5) à 0 jusqu'au commit — c'est normal,
-- pas un blocage.
--
-- **Ce script ne modifie RIEN.** Uniquement des `SET` de variables de session et des `SELECT`
-- sur `information_schema` et `performance_schema` : il peut tourner en boucle sans risque,
-- y compris pendant une transaction de 24 M de lignes. Verrouillé par
-- `tests/test_scripts_dsr.py` (`test_le_suivi_est_strictement_en_lecture`).
--
-- CE QU'ON PEUT VOIR, ET CE QU'ON NE PEUT PAS
--   Un `INSERT … SELECT`, un `LOAD DATA` ou un `DELETE` n'est visible par les autres sessions
--   qu'une fois VALIDÉ : compter les lignes de la table cible renvoie donc l'état d'AVANT
--   jusqu'à la toute fin. La progression réelle se lit ailleurs — sur le nombre de lignes
--   déjà LUES par l'instruction (`ROWS_EXAMINED`) et sur le nombre de lignes déjà MODIFIÉES
--   par sa transaction (`trx_rows_modified`), qui grandissent tous deux en direct.
--
-- USAGE — rafraîchissement toutes les 5 secondes :
--   while true; do clear; mysql -h <hote> -u <user> -p<mdp> dsr_mercure_aa < db/suivi.sql; sleep 5; done
--
--   Sans boucle, une photo à la demande :
--     mysql -h <hote> -u <user> -p dsr_mercure_aa --table < db/suivi.sql
--
--   Pour la seule liste des requêtes en cours, le client fait déjà le travail :
--     mysqladmin -h <hote> -u <user> -p -i 5 processlist
--
-- DROITS REQUIS — `PROCESS` (voir les requêtes des autres sessions et `INNODB_TRX`) et
-- `SELECT` sur `performance_schema`. Sans `PROCESS`, les blocs 1, 2 et 3 ne montreront que
-- vos propres sessions : le suivi doit se faire avec le compte qui joue le traitement, ou un
-- compte d'exploitation.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Paramètres
-- -------------------------------------------------------------------------------------
-- `@lignes_attendues` sert au pourcentage et à l'estimation de fin. Le renseigner selon ce
-- qu'on surveille :
--   * agrégation DSR-696  → nombre de lignes ACTIVES du référentiel balayé (le cas courant) ;
--   * chargement DSR-697  → nombre de lignes du fichier CSV, en-tête déduit ;
--   * calcul DSR-699      → comme DSR-696.
--
-- Où le trouver sans relancer un `COUNT(*)` de plusieurs secondes sur la table : dans le
-- contrôle 1 de DSR-697 (`nb_lignes`), ou dans `lignes_estimees` du bloc 4 ci-dessous. La
-- valeur n'a pas besoin d'être exacte — elle ne sert qu'à l'estimation.
--
-- À 0, les colonnes de progression restent à NULL : le reste du suivi fonctionne quand même.

SET @lignes_attendues := 24217441;
SET @id_referentiel   := 1;


-- -------------------------------------------------------------------------------------
-- 1. L'instruction en cours, et sa progression
-- -------------------------------------------------------------------------------------
-- `lignes_lues` est le compteur qui bouge pendant un balayage : c'est lui qui dit où en est
-- l'`INSERT INTO trppu_trafic_site` de DSR-696, dont l'écriture finale ne fait que quelques
-- milliers de lignes après en avoir lu 24 M. `lignes_ecrites` reste à 0 jusqu'à la fin sur la
-- plupart des instructions — sur cet `INSERT`, il ne bougera qu'à la toute dernière étape.
--
-- `etat` est l'étape interne du serveur : « Sending data » = balayage en cours, « copy to tmp
-- table » = reconstruction d'un ALTER, « updating » = écriture, « waiting for handler commit »
-- = validation finale, c'est presque fini.
--
-- `fin_estimee` est une simple règle de trois sur le rythme observé depuis le début de
-- l'instruction. Elle ne vaut que pour un traitement régulier — ce que sont un balayage et un
-- chargement, pas une validation finale.
SELECT t.PROCESSLIST_ID                                              AS session_id,
       t.PROCESSLIST_TIME                                            AS secondes,
       SEC_TO_TIME(t.PROCESSLIST_TIME)                               AS duree,
       t.PROCESSLIST_STATE                                           AS etat,
       s.ROWS_EXAMINED                                               AS lignes_lues,
       s.ROWS_AFFECTED                                               AS lignes_ecrites,
       IF(@lignes_attendues > 0,
          CONCAT(ROUND(100 * s.ROWS_EXAMINED / @lignes_attendues, 1), ' %'),
          NULL)                                                      AS avancement,
       IF(@lignes_attendues > 0 AND s.ROWS_EXAMINED > 0 AND t.PROCESSLIST_TIME > 0,
          SEC_TO_TIME(GREATEST(@lignes_attendues - s.ROWS_EXAMINED, 0)
                      / (s.ROWS_EXAMINED / t.PROCESSLIST_TIME)),
          NULL)                                                      AS reste_estime,
       LEFT(REPLACE(REPLACE(t.PROCESSLIST_INFO, '\n', ' '), '  ', ' '), 70) AS instruction
  FROM performance_schema.threads t
  LEFT JOIN performance_schema.events_statements_current s
         ON s.THREAD_ID = t.THREAD_ID
        AND s.END_EVENT_ID IS NULL
 WHERE t.PROCESSLIST_ID IS NOT NULL
   AND t.PROCESSLIST_ID <> CONNECTION_ID()
   AND t.PROCESSLIST_COMMAND <> 'Sleep'
 ORDER BY t.PROCESSLIST_TIME DESC;


-- -------------------------------------------------------------------------------------
-- 2. La transaction, et ce qu'elle a déjà écrit
-- -------------------------------------------------------------------------------------
-- `lignes_modifiees` (`trx_rows_modified`) est LE compteur d'un chargement : il grandit ligne
-- à ligne pendant le `LOAD DATA` de DSR-697 et pendant la purge RG6, alors que la table reste
-- vide pour tout le monde jusqu'au commit.
--
-- `verrous` dit aussi le prix d'un retour arrière : une transaction à plusieurs millions de
-- lignes modifiées mettra, annulée, un temps comparable à celui qu'elle a déjà passé. C'est
-- l'argument contre le `Ctrl-C` réflexe sur un chargement qui traîne.
SELECT trx_id,
       trx_state                                                     AS etat,
       trx_started                                                   AS debut,
       SEC_TO_TIME(TIMESTAMPDIFF(SECOND, trx_started, NOW()))        AS age,
       trx_rows_modified                                             AS lignes_modifiees,
       trx_rows_locked                                               AS verrous,
       IF(@lignes_attendues > 0,
          CONCAT(ROUND(100 * trx_rows_modified / @lignes_attendues, 1), ' %'),
          NULL)                                                      AS avancement_ecriture,
       trx_isolation_level                                           AS isolation,
       LEFT(REPLACE(trx_query, '\n', ' '), 60)                       AS requete
  FROM information_schema.INNODB_TRX
 ORDER BY trx_started;


-- -------------------------------------------------------------------------------------
-- 3. Est-on bloqué par quelqu'un d'autre ?
-- -------------------------------------------------------------------------------------
-- Doit renvoyer 0 ligne. Une ligne ici signifie que le traitement n'avance plus parce qu'une
-- autre session tient un verrou — typiquement deux scripts de la chaîne lancés en parallèle
-- sur le même référentiel. `id_bloqueur` donne la session à arrêter.
-- La jointure passe par `performance_schema.threads` et non par les identifiants de
-- transaction : ceux-ci ont changé de type entre versions de MySQL, l'identifiant de thread
-- non.
SELECT tr.PROCESSLIST_ID                                             AS id_bloque,
       tr.PROCESSLIST_TIME                                           AS attend_depuis_s,
       LEFT(REPLACE(tr.PROCESSLIST_INFO, '\n', ' '), 50)             AS requete_bloquee,
       tb.PROCESSLIST_ID                                             AS id_bloqueur,
       tb.PROCESSLIST_TIME                                           AS bloqueur_depuis_s,
       LEFT(REPLACE(tb.PROCESSLIST_INFO, '\n', ' '), 50)             AS requete_bloqueuse
  FROM performance_schema.data_lock_waits w
  JOIN performance_schema.threads tr ON tr.THREAD_ID = w.REQUESTING_THREAD_ID
  JOIN performance_schema.threads tb ON tb.THREAD_ID = w.BLOCKING_THREAD_ID;


-- -------------------------------------------------------------------------------------
-- 4. Volumétrie stabilisée — ce qui est réellement validé en base
-- -------------------------------------------------------------------------------------
-- `lignes_estimees` vient des statistiques InnoDB (`information_schema.TABLES`) : instantané,
-- mais APPROXIMATIF, et volontairement — un `COUNT(*)` sur 24 M de lignes prendrait plusieurs
-- secondes à chaque rafraîchissement, et ferait du script de suivi une charge de plus sur un
-- serveur déjà occupé. L'ordre de grandeur suffit pour savoir si le chargement a été validé.
--
-- L'écart avec le compte exact peut atteindre quelques pour cent, et la valeur ne bouge PAS
-- pendant une transaction en cours : c'est le bloc 2 qui montre la progression.
SELECT TABLE_NAME                                                    AS "table",
       TABLE_ROWS                                                    AS lignes_estimees,
       ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024)             AS taille_mo,
       ROUND(DATA_FREE / 1024 / 1024)                                AS espace_libere_mo,
       UPDATE_TIME                                                   AS derniere_ecriture
  FROM information_schema.TABLES
 WHERE TABLE_SCHEMA = DATABASE()
   AND TABLE_NAME IN ('trppu_cles_repartition', 'trppu_trafic_site',
                      'trppu_version_cle', 'trppu_cles_repartition_calcule')
 ORDER BY TABLE_ROWS DESC;


-- -------------------------------------------------------------------------------------
-- 5. Où en est la chaîne, référentiel par référentiel
-- -------------------------------------------------------------------------------------
-- Comptes EXACTS, mais uniquement sur les tables filles, qui restent petites : quelques
-- milliers de sites, autant de versions et de clés — l'ordre de grandeur d'un référentiel,
-- pas celui de la table source. Ce bloc dit quelle étape est faite et laquelle reste à faire.
--
-- Lecture : `agregats_696` à 0 alors que le référentiel est chargé = DSR-696 reste à jouer ;
-- `versions_698` à 0 = DSR-698 ; `cles_699` à 0 = DSR-699. Une valeur qui n'avance plus d'un
-- rafraîchissement à l'autre, alors que le bloc 1 montre une instruction active, est normale :
-- rien n'est visible avant le commit.
SELECT @id_referentiel                                               AS id_referentiel,
       (SELECT COUNT(*) FROM trppu_trafic_site
         WHERE id_referentiel = @id_referentiel)                     AS agregats_696,
       (SELECT COUNT(*) FROM trppu_version_cle
         WHERE id_referentiel = @id_referentiel AND actif = 'O')     AS versions_698_actives,
       (SELECT COUNT(*) FROM trppu_cles_repartition_calcule
         WHERE id_referentiel = @id_referentiel)                     AS cles_699,
       (SELECT MAX(date_creation) FROM trppu_trafic_site
         WHERE id_referentiel = @id_referentiel)                     AS dernier_agregat,
       NOW()                                                         AS heure;


-- -------------------------------------------------------------------------------------
-- 6. Progression d'un `ALTER TABLE` — `fix_error.sql`, ou un index posé par la migration
-- -------------------------------------------------------------------------------------
-- Rend 0 ligne si aucun `ALTER` ne tourne… ou si l'instrumentation est désactivée, ce qui est
-- le cas par défaut : les étapes InnoDB ne sont pas suivies tant que les consommateurs
-- `events_stages` ne sont pas activés. Pour les activer — modification GLOBALE du serveur, à
-- faire en connaissance de cause et de préférence hors production :
--
--   UPDATE performance_schema.setup_instruments SET ENABLED = 'YES', TIMED = 'YES'
--    WHERE NAME LIKE 'stage/innodb/alter%';
--   UPDATE performance_schema.setup_consumers SET ENABLED = 'YES'
--    WHERE NAME LIKE '%events_stages%';
--
-- Sans cela, un `ALTER` reste visible par le bloc 1 : son `etat` affiche l'étape en cours
-- (« copy to tmp table »), ce qui suffit le plus souvent. Sur `trppu_trafic_site`, vide ou
-- presque, la reconstruction est de toute façon immédiate — l'instrumentation n'a d'intérêt
-- que sur un `ALTER` de `trppu_cles_repartition` et ses 24 M de lignes.
SELECT t.PROCESSLIST_ID                                              AS session_id,
       g.EVENT_NAME                                                  AS etape,
       g.WORK_COMPLETED                                              AS fait,
       g.WORK_ESTIMATED                                              AS total,
       IF(g.WORK_ESTIMATED > 0,
          CONCAT(ROUND(100 * g.WORK_COMPLETED / g.WORK_ESTIMATED, 1), ' %'),
          NULL)                                                      AS avancement
  FROM performance_schema.events_stages_current g
  JOIN performance_schema.threads t ON t.THREAD_ID = g.THREAD_ID
 WHERE t.PROCESSLIST_ID IS NOT NULL
   AND t.PROCESSLIST_ID <> CONNECTION_ID();
