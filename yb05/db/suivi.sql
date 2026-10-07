-- =====================================================================================
-- SUIVI — où en est le traitement en cours, vu depuis une AUTRE session
-- =====================================================================================
-- À lancer dans un second terminal pendant un script de la chaîne (DSR-697, DSR-696,
-- DSR-699, `fix_error.sql`). Rien n'est visible dans les tables cibles avant le commit : la
-- progression se lit sur `ROWS_EXAMINED` (bloc 1) et `trx_rows_modified` (bloc 2).
-- **Ce script ne modifie RIEN** (`SET` de session et `SELECT`) — verrouillé par
-- `tests/test_scripts_dsr.py` (`test_le_suivi_est_strictement_en_lecture`).
-- Usage : while true; do clear; mysql -h <hote> -u <user> -p<mdp> dsr_mercure_aa < db/suivi.sql; sleep 5; done
-- Droits : `PROCESS` (sinon blocs 1 à 3 limités à vos sessions) et `SELECT` sur
-- `performance_schema`.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Paramètres
-- -------------------------------------------------------------------------------------
-- `@lignes_attendues` (approximatif, 0 = pas d'estimation) : lignes actives du référentiel
-- pour DSR-696/699, lignes du CSV pour DSR-697 (cf. `lignes_estimees` du bloc 4).

SET @lignes_attendues := 24217441;
SET @id_referentiel   := 1;


-- -------------------------------------------------------------------------------------
-- 1. L'instruction en cours, et sa progression
-- -------------------------------------------------------------------------------------
-- `lignes_lues` avance pendant un balayage ; `lignes_ecrites` reste souvent à 0 jusqu'à la
-- fin. `reste_estime` : règle de trois sur le rythme observé.
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
-- `lignes_modifiees` est LE compteur d'un chargement (LOAD DATA, purge RG6). Annuler une
-- grosse transaction prend un temps comparable : pas de `Ctrl-C` réflexe.
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
-- Doit renvoyer 0 ligne ; sinon `id_bloqueur` donne la session qui tient le verrou.
-- Jointure par thread : le type des identifiants de transaction varie selon la version MySQL.
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
-- `lignes_estimees` : statistiques InnoDB, approximatives mais sans `COUNT(*)` coûteux ;
-- ne bouge pas avant le commit.
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
-- Comptes exacts sur les petites tables filles : un compteur à 0 = étape restant à jouer
-- (ou en cours, rien n'étant visible avant le commit).
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
-- 0 ligne si aucun `ALTER` ne tourne ou si l'instrumentation est désactivée (défaut). Pour
-- l'activer — modification GLOBALE du serveur, de préférence hors production :
--
--   UPDATE performance_schema.setup_instruments SET ENABLED = 'YES', TIMED = 'YES'
--    WHERE NAME LIKE 'stage/innodb/alter%';
--   UPDATE performance_schema.setup_consumers SET ENABLED = 'YES'
--    WHERE NAME LIKE '%events_stages%';
--
-- Sans cela, le bloc 1 (`etat` = « copy to tmp table ») suffit le plus souvent.
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
