-- =============================================================================
--  02_donnees.sql  --  Jeu de donnees de developpement
-- =============================================================================
--  Base : dsr_mercure_aa
--
--  ATTENTION -- CE N'EST PAS UN EXPORT DE PRODUCTION.
--  db/db_new.sql est un dump de schema sans aucune donnee ("0 ligne(s) au
--  total"). Les lignes ci-dessous sont un jeu de test ecrit a la main : il est
--  coherent, mais fictif. Ne jamais le jouer sur une base reelle.
--
--  Tous les identifiants sont explicites : rejouer ce script apres coup ne
--  decale aucune sequence, et les id cites dans la documentation et les tests
--  restent stables.
--
--  Rejouable : INSERT IGNORE. Une ligne deja presente (meme cle primaire ou
--  meme cle unique) est ignoree en silence. Corollaire a connaitre : ce script
--  ne met pas a jour une ligne existante -- pour repartir de zero, videz les
--  tables d'abord.
--
--  Ordre des sections = ordre des dependances de cles etrangeres. Il compte :
--  03_contraintes.sql valide les FK et les CHECK sur les lignes deja chargees,
--  donc une incoherence introduite ici ne se verra qu'a l'execution de 03.
--
--  Ordre d'execution : 01_structure -> 02_donnees -> 03_contraintes
-- =============================================================================

USE `dsr_mercure_aa`;

-- =============================================================================
--  1. Referentiels sans dependance
-- =============================================================================

-- ----- `trppu_produit` -----
-- Reference par trppu_tmh, trppu_pic_coefficients et trppu_scenario_variations_prev.
INSERT IGNORE INTO `trppu_produit`
  (`co_produit`, `lb_produit`, `dt_creation`, `dt_desactivation`, `motif_desactivation`)
VALUES
  ('OO',  'Objets ordinaires',              '2026-01-01 08:00:00', NULL, NULL),
  ('PPI', 'Petits paquets internationaux',  '2026-01-01 08:00:00', NULL, NULL),
  ('CO',  'Colis',                          '2026-01-01 08:00:00', NULL, NULL),
  ('EP',  'Envois de presse',               '2026-01-01 08:00:00', NULL, NULL),
  ('PQ',  'Paquets',                        '2026-01-01 08:00:00', NULL, NULL),
  ('IP',  'Imprimes publicitaires',         '2026-01-01 08:00:00', NULL, NULL),
  ('OS',  'Objets suivis',                  '2026-01-01 08:00:00', NULL, NULL),
  ('3S',  'Envois signes et suivis',        '2026-01-01 08:00:00', '2026-06-30',
          'Produit fusionne avec OS');

-- ----- `trppu_site` -----
INSERT IGNORE INTO `trppu_site`
  (`co_regate`, `lb_regate`, `type_site`, `co_roc`, `dt_maj`)
VALUES
  ('123456', 'PLUVENCE CENTRE', 'PPDC ', '012345', '2026-09-01 06:00:00'),
  ('654321', 'BEAUVAL BOURG',   'PDC  ', '012345', '2026-09-01 06:00:00'),
  ('789012', 'MONTBRAIS',       'PPDC ', '067890', '2026-09-01 06:00:00');

-- ----- `trppu_referentiel` -----
INSERT IGNORE INTO `trppu_referentiel`
  (`id_referentiel`, `co_regate`, `date_reference`, `commentaire`)
VALUES
  (1, NULL,     '2026-01-01', 'Referentiel national 2026'),
  (2, '123456', '2026-07-01', 'Referentiel site PLUVENCE, juillet 2026');

-- ----- `trppu_version_cle` -----
INSERT IGNORE INTO `trppu_version_cle`
  (`id_version_cle`, `id_referentiel`, `libelle`, `co_regate`, `actif`,
   `date_creation`, `date_debut_validite`, `date_fin_validite`, `commentaire`)
VALUES
  (1, 1, 'Cles initiales 2026',   '123456', 'N',
      '2026-01-05 09:00:00', '2026-01-05 00:00:00', '2026-06-30 23:59:59',
      'Remplacee par la version 2'),
  (2, 2, 'Cles juillet 2026',     '123456', 'O',
      '2026-07-01 09:00:00', '2026-07-01 00:00:00', NULL, NULL),
  (3, 1, 'Cles initiales 2026',   '654321', 'O',
      '2026-01-05 09:00:00', '2026-01-05 00:00:00', NULL, NULL);

-- ----- `trppu_pic_version` -----
-- `id_scenario` est NOT NULL sans cle etrangere : 0 signifie "hors scenario"
-- pour les niveaux NATIONAL, DEX et SITE.
INSERT IGNORE INTO `trppu_pic_version`
  (`id_pic_version`, `lb_pic_version`, `niveau`, `co_regate`, `id_scenario`,
   `dt_activation`, `dt_desactivation`, `motif_desactivation`, `commentaire`,
   `est_par_defaut`, `dt_creation`, `dt_maj`, `id_rh_creation`, `id_rh_maj`)
VALUES
  (1, 'PIC national 2026',    'NATIONAL', '000000', 0,
      '2026-01-01 00:00:00', NULL, NULL, 'Coefficients de reference',
      1, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001', NULL),
  (2, 'PIC PLUVENCE 2026',    'SITE',     '123456', 0,
      '2026-07-01 00:00:00', NULL, NULL, 'Surcharge locale du national',
      0, '2026-06-25 10:00:00', '2026-06-25 10:00:00', '0000002', NULL),
  (3, 'PIC PLUVENCE 2025',    'SITE',     '123456', 0,
      '2025-01-01 00:00:00', '2026-07-01 00:00:00', 'Millesime clos', NULL,
      0, '2025-01-01 08:00:00', '2026-07-01 00:00:00', '0000002', '0000002');

-- =============================================================================
--  2. Coefficients PIC   (FK -> trppu_pic_version, trppu_produit)
-- =============================================================================
-- `densite` : 0 = dense, 1 = faible1, 2 = faible2 (CHECK densite IN (0,1,2)).
-- Semaine complete pour OO, echantillon pour PPI.

INSERT IGNORE INTO `trppu_pic_coefficients`
  (`id_pic_coef`, `id_pic_version`, `co_produit`, `jour_semaine`,
   `dt_effet`, `dt_fin`, `coef`, `densite`, `dt_creation`, `dt_maj`, `id_rh`)
VALUES
  (1,  1, 'OO', 'LUNDI',    '2026-01-01 00:00:00', NULL, 1.2000, 0, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (2,  1, 'OO', 'LUNDI',    '2026-01-01 00:00:00', NULL, 0.8000, 1, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (3,  1, 'OO', 'LUNDI',    '2026-01-01 00:00:00', NULL, 0.5000, 2, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (4,  1, 'OO', 'MARDI',    '2026-01-01 00:00:00', NULL, 1.1000, 0, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (5,  1, 'OO', 'MARDI',    '2026-01-01 00:00:00', NULL, 0.7500, 1, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (6,  1, 'OO', 'MARDI',    '2026-01-01 00:00:00', NULL, 0.4500, 2, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (7,  1, 'OO', 'MERCREDI', '2026-01-01 00:00:00', NULL, 1.0500, 0, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (8,  1, 'OO', 'MERCREDI', '2026-01-01 00:00:00', NULL, 0.7000, 1, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (9,  1, 'OO', 'MERCREDI', '2026-01-01 00:00:00', NULL, 0.4000, 2, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (10, 1, 'OO', 'JEUDI',    '2026-01-01 00:00:00', NULL, 1.0000, 0, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (11, 1, 'OO', 'JEUDI',    '2026-01-01 00:00:00', NULL, 0.6500, 1, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (12, 1, 'OO', 'JEUDI',    '2026-01-01 00:00:00', NULL, 0.3500, 2, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (13, 1, 'OO', 'VENDREDI', '2026-01-01 00:00:00', NULL, 1.3000, 0, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (14, 1, 'OO', 'VENDREDI', '2026-01-01 00:00:00', NULL, 0.9000, 1, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (15, 1, 'OO', 'VENDREDI', '2026-01-01 00:00:00', NULL, 0.6000, 2, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (16, 1, 'OO', 'SAMEDI',   '2026-01-01 00:00:00', NULL, 0.9000, 0, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (17, 1, 'OO', 'SAMEDI',   '2026-01-01 00:00:00', NULL, 0.6000, 1, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (18, 1, 'OO', 'SAMEDI',   '2026-01-01 00:00:00', NULL, 0.3000, 2, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (19, 1, 'PPI','LUNDI',    '2026-01-01 00:00:00', NULL, 1.1500, 0, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (20, 1, 'PPI','LUNDI',    '2026-01-01 00:00:00', NULL, 0.7800, 1, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  (21, 1, 'PPI','LUNDI',    '2026-01-01 00:00:00', NULL, 0.4800, 2, '2026-01-01 08:00:00', '2026-01-01 08:00:00', '0000001'),
  -- Surcharge site : meme produit / meme jour, mais version PIC differente.
  (22, 2, 'OO', 'LUNDI',    '2026-07-01 00:00:00', NULL, 1.2500, 0, '2026-06-25 10:00:00', '2026-06-25 10:00:00', '0000002'),
  (23, 2, 'OO', 'LUNDI',    '2026-07-01 00:00:00', NULL, 0.8200, 1, '2026-06-25 10:00:00', '2026-06-25 10:00:00', '0000002'),
  (24, 2, 'OO', 'LUNDI',    '2026-07-01 00:00:00', NULL, 0.5100, 2, '2026-06-25 10:00:00', '2026-06-25 10:00:00', '0000002');

-- =============================================================================
--  3. Scenarios
-- =============================================================================
--  Jeu volontairement construit pour couvrir la visibilite OPTIPACC et la
--  machine a etats :
--
--   id  | site   | statut        | fige | pdi | agr | usage
--   ----+--------+---------------+------+-----+-----+--------------------------
--   125 | 123456 | VALIDE        |  1   |  1  |  1  | visible OPTIPACC, eligible
--       |        |               |      |     |     | a la mise en production
--   126 | 123456 | EN COURS      |  0   |  0  |  0  | editable, invisible
--   127 | 123456 | SIMULATION    |  1   |  1  |  0  | Agrebal non calcule
--   128 | 654321 | EN PRODUCTION |  1   |  1  |  1  | deja en production
--   129 | 123456 | ARCHIVE       |  1   |  1  |  1  | terminal
--
--  Le seul scenario EN PRODUCTION est sur le site 654321, jamais sur 123456 :
--  sinon le controle C5 de DSR-707 (un seul scenario en production par site)
--  refuserait toute mise en production du scenario 125.
--
--  `lb_scenario` est un varchar(20) : les libelles sont volontairement courts.
--  `Calcul_trafic_en_cours` est la seule colonne capitalisee du schema.

INSERT IGNORE INTO `trppu_scenario`
  (`id_scenario`, `co_roc`, `co_regate`, `lb_scenario`, `statut`,
   `dt_creation`, `dt_validation`, `dt_mise_en_oeuvre`, `dt_mise_en_prod`, `dt_pivot`,
   `periode_debut`, `periode_fin`,
   `periode_realise_debut`, `periode_realise_fin`,
   `periode_prev_debut`, `periode_prev_fin`,
   `nb_jours_semaine`, `nb_jours_ouvres`, `nb_jours_ouvrables`, `nb_jours_scenario`,
   `id_pic_version`, `version_scenario`, `est_fige`, `dt_maj`,
   `id_rh_creation`, `id_rh_maj`,
   `trafic_pdi_calcule`, `trafic_agrebal_calcule`, `Calcul_trafic_en_cours`,
   `id_referentiel`, `id_version_cle`)
VALUES
  (125, '012345', '123456', 'Scenario Sept 2026', 'VALIDE',
       '2026-09-01 09:00:00', '2026-09-05 14:30:00', NULL, NULL, '2026-09-11 00:00:00',
       '2026-06-01', '2026-11-30',
       '2026-06-01', '2026-09-11',
       '2026-09-11', '2026-11-30',
       6, 132, 156, 183,
       2, 4, 1, '2026-09-05 14:30:00',
       '0000001', '0000002',
       1, 1, 0,
       2, 2),

  (126, '012345', '123456', 'Brouillon Oct 2026', 'EN COURS',
       '2026-09-08 11:00:00', NULL, NULL, NULL, NULL,
       '2026-10-01', '2026-12-31',
       NULL, NULL,
       '2026-10-01', '2026-12-31',
       5, 65, 78, 92,
       2, 1, 0, '2026-09-08 11:00:00',
       '0000002', NULL,
       0, 0, 0,
       2, 2),

  (127, '012345', '123456', 'Simu vieillissement', 'SIMULATION',
       '2026-08-20 10:15:00', NULL, NULL, NULL, NULL,
       '2026-09-01', '2027-02-28',
       '2026-09-01', '2026-09-11',
       '2026-09-11', '2027-02-28',
       6, 150, 178, 181,
       2, 3, 1, '2026-08-25 16:00:00',
       '0000002', '0000002',
       1, 0, 0,
       2, 2),

  (128, '012345', '654321', 'Prod Avril 2027', 'EN PRODUCTION',
       '2026-05-02 09:30:00', '2026-06-15 10:00:00',
       '2027-04-01 00:00:00', '2027-04-01 00:00:00', NULL,
       '2026-04-01', '2026-09-30',
       '2026-04-01', '2026-09-11',
       '2026-09-11', '2026-09-30',
       6, 130, 154, 183,
       1, 7, 1, '2026-06-15 10:00:00',
       '0000003', '0000003',
       1, 1, 0,
       1, 3),

  (129, '012345', '123456', 'Archive 2025', 'ARCHIVE',
       '2025-03-01 08:00:00', '2025-04-01 09:00:00', NULL, NULL, NULL,
       '2025-03-01', '2025-08-31',
       '2025-03-01', '2025-08-31',
       NULL, NULL,
       6, 132, 157, 184,
       3, 12, 1, '2025-09-01 08:00:00',
       '0000001', '0000001',
       1, 1, 0,
       1, 1);

-- =============================================================================
--  4. Donnees rattachees aux scenarios   (FK -> trppu_scenario)
-- =============================================================================

-- ----- `trppu_tmh` -----
-- Jeu construit pour couvrir la formule du volume brut de DSR-689 :
--   volume_brut = volume_realise + COALESCE(volume_previsionnel_recalcule,
--                                            volume_previsionnel, 0)
--   somme par co_produit, lignes `bl_exclu = 1` ecartees.
--
--   OO  : une ligne calculee simple                    -> 1 250 000
--   IP  : deux lignes (calculee + ajout manuel)        -> 5 900 000
--   CO  : volume_previsionnel_recalcule NULL, repli    ->   791 000
--   EP  : volumes NULL                                 ->         0
--   PQ  : bl_exclu = 1, jamais restitue                ->    (exclu)
INSERT IGNORE INTO `trppu_tmh`
  (`id_tmh`, `id_scenario`, `co_produit`,
   `volume_realise`, `volume_previsionnel`, `moyenne_journaliere`, `moyenne_hebdo`,
   `dt_calcul`, `bl_exclu`, `bl_manuel`, `id_rh`, `motif`,
   `volume_previsionnel_recalcule`, `volume_brut`)
VALUES
  (1, 125, 'OO', 1000000,  200000, 6830.60, 40983.61, '2026-09-05 14:00:00', 0, 0, NULL, NULL,  250000, 1250000),
  (2, 125, 'IP', 5000000,  400000, 29508.20, 177049.18, '2026-09-05 14:00:00', 0, 0, NULL, NULL, 400000, 5400000),
  (3, 125, 'IP',  500000,       0,  2732.24, 16393.44, '2026-09-06 09:12:00', 0, 1, '0000002',
      'Ajout manuel apres comptage terrain', 0, 500000),
  (4, 125, 'CO',  700000,   91000,  4322.40, 25934.43, '2026-09-05 14:00:00', 0, 0, NULL, NULL,    NULL,  791000),
  (5, 125, 'EP',    NULL,    NULL,     NULL,     NULL, '2026-09-05 14:00:00', 0, 0, NULL, NULL,    NULL,       0),
  (6, 125, 'PQ',  250000,   30000,  1530.05,  9180.33, '2026-09-05 14:00:00', 1, 0, '0000002',
      'Produit hors perimetre du site', 30000, 280000),
  (7, 126, 'OO',  120000,   15000,   737.70,  4426.23, '2026-09-08 11:05:00', 0, 0, NULL, NULL,   15000,  135000),
  (8, 128, 'OO', 2100000,  310000, 13169.40, 79016.39, '2026-06-15 09:00:00', 0, 0, NULL, NULL,  310000, 2410000);

-- ----- `trppu_scenario_variations_prev` -----
INSERT IGNORE INTO `trppu_scenario_variations_prev`
  (`id_variation`, `id_scenario`, `co_produit`, `variation_pct`, `dt_creation`, `id_rh`)
VALUES
  (1, 125, 'OO',   2.50, '2026-09-03 10:20:00', '0000002'),
  (2, 125, 'IP', -12.00, '2026-09-03 10:21:00', '0000002'),
  (3, 125, 'CO',   0.00, '2026-09-03 10:22:00', '0000002'),
  (4, 126, 'OO',   5.00, '2026-09-08 11:10:00', '0000002');

-- ----- `trppu_scenario_exclusions` -----
INSERT IGNORE INTO `trppu_scenario_exclusions`
  (`id`, `id_scenario`, `co_produit`, `motif`)
VALUES
  (1, 125, 'PQ', 'Produit hors perimetre du site'),
  (2, 125, '3S', 'Produit desactive le 30/06/2026');

-- ----- `trppu_scenario_comptages_manuels` -----
INSERT IGNORE INTO `trppu_scenario_comptages_manuels`
  (`id_comptage`, `id_scenario`, `dt_comptage`, `co_produit`, `nb_produit`)
VALUES
  (1, 125, '2026-09-02', 'OO', 4820),
  (2, 125, '2026-09-03', 'OO', 5110),
  (3, 125, '2026-09-02', 'IP', 1240);

-- ----- `trppu_scenario_pic_coeffs` -----
-- Attention : l'enum des jours est ici abrege (LUN, MAR, ...), contrairement a
-- trppu_pic_coefficients et trppu_trafic_agrebal qui utilisent LUNDI, MARDI...
INSERT IGNORE INTO `trppu_scenario_pic_coeffs`
  (`id`, `id_scenario`, `co_produit`, `jour_semaine`,
   `coef_dense`, `coef_faible1`, `coef_faible2`)
VALUES
  (1, 125, 'OO', 'LUN', 1.25000, 0.82000, 0.51000),
  (2, 125, 'OO', 'MAR', 1.10000, 0.75000, 0.45000),
  (3, 125, 'OO', 'MER', 1.05000, 0.70000, 0.40000),
  (4, 125, 'OO', 'JEU', 1.00000, 0.65000, 0.35000),
  (5, 125, 'OO', 'VEN', 1.30000, 0.90000, 0.60000),
  (6, 125, 'OO', 'SAM', 0.90000, 0.60000, 0.30000),
  (7, 125, 'PPI','LUN', 1.15000, 0.78000, 0.48000),
  (8, 125, 'PPI','MAR', 1.08000, 0.72000, 0.44000);

-- ----- `trppu_neutralisations` -----
-- CHECK : dt_debut <= dt_fin et nb_jour > 0.
INSERT IGNORE INTO `trppu_neutralisations`
  (`id_neutralisation`, `id_scenario`, `dt_debut`, `dt_fin`, `nb_jour`,
   `motif`, `dt_creation`, `id_rh`)
VALUES
  (1, 125, '2026-07-14', '2026-07-14', 1, 'Fete nationale',        '2026-09-01 09:10:00', '0000001'),
  (2, 125, '2026-08-10', '2026-08-16', 7, 'Fermeture estivale',    '2026-09-01 09:11:00', '0000001'),
  (3, 128, '2026-05-01', '2026-05-01', 1, 'Fete du travail',       '2026-05-02 09:35:00', '0000003');

-- ----- `trppu_recalcul_log` -----
INSERT IGNORE INTO `trppu_recalcul_log`
  (`id_log`, `id_scenario`, `dt_recalcul`, `raison`, `commentaire`)
VALUES
  (1, 125, '2026-09-01 09:05:00', 'INITIAL',         'Creation du scenario'),
  (2, 125, '2026-09-03 10:25:00', 'MANUEL',          'Saisie des variations previsionnelles'),
  (3, 125, '2026-09-05 13:40:00', 'CLE_REPARTITION', 'Passage sur la version de cles 2'),
  (4, 125, '2026-09-05 14:00:00', 'AGREBAL',         'Calcul des trafics PDI puis Agrebal'),
  (5, 128, '2026-06-15 09:00:00', 'AGREBAL',         'Calcul avant mise en production');

-- ----- `trppu_api_log` -----
INSERT IGNORE INTO `trppu_api_log`
  (`id_log`, `api_name`, `id_scenario`, `regate`, `dt_appel`, `caller`, `params`)
VALUES
  (1, 'CREATION_SCENARIO',  125, '123456', '2026-09-01 09:00:00',
      'b1f0c2de-0001-4a00-9f00-000000000001',
      '{"lb_scenario": "Scenario Sept 2026"}'),
  (2, 'TRANSITION_STATUT',  125, '123456', '2026-09-05 14:30:00',
      'b1f0c2de-0001-4a00-9f00-000000000002',
      '{"statut_avant": "EN COURS", "statut_apres": "VALIDE"}'),
  (3, 'TRANSITION_STATUT',  128, '654321', '2026-06-15 10:00:00',
      'b1f0c2de-0001-4a00-9f00-000000000003',
      '{"statut_avant": "VALIDE", "statut_apres": "EN PRODUCTION", "origine": "OPTIPACC", "date_mise_en_oeuvre": "2027-04-01"}'),
  (4, 'ECRITURE_TMH',       125, '123456', '2026-09-06 09:12:00',
      'b1f0c2de-0001-4a00-9f00-000000000004',
      '{"co_produit": "IP", "bl_manuel": 1}');

-- =============================================================================
--  5. Amas (Agrebals) et trafics calcules
-- =============================================================================

-- ----- `trppu_agrebal_pdi` -----
-- Referentiel des amas. `agrebal_pdi_ids` est une colonne GENERATED : elle ne
-- doit jamais figurer dans un INSERT, MySQL la derive de `agrebal_pdiList`.
-- Cle unique : (agrebal_id, agrebal_code_regate).
INSERT IGNORE INTO `trppu_agrebal_pdi`
  (`agrebal_id_pdi`, `agrebal_id`, `agrebal_uuid`, `agrebal_nom`,
   `agrebal_code_roc`, `agrebal_code_regate`, `agrebal_pdiQuantity`,
   `agrebal_pdiList`, `agrebal_event`,
   `agrebal_createdAt`, `agrebal_updatedAt`, `agrebal_deleteddAt`)
VALUES
  (1, 7001, '0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234', 'PLUVENCE_2449',
      '012345', '123456', 2, '[{"pdi_id": 100001}, {"pdi_id": 100002}]', 'CREATED',
      '2026-02-01 08:00:00', '2026-08-20 11:00:00', NULL),
  (2, 7002, '77ad1e51-b9fe-490f-b9af-c1ff4f4b9876', 'PLUVENCE_2450',
      '012345', '123456', 1, '[{"pdi_id": 100003}]', 'CREATED',
      '2026-02-01 08:00:00', '2026-08-20 11:00:00', NULL),
  (3, 7003, '3c9d51a8-1f22-4b77-8e10-2a4b6c8d0e11', 'PLUVENCE_2451',
      '012345', '123456', 1, '[{"pdi_id": 100004}]', 'UPDATED',
      '2026-02-01 08:00:00', '2026-08-25 09:30:00', NULL),
  (4, 7101, '9f2e40b6-5c31-4d88-a903-7b1e2f5c6d22', 'BEAUVAL_1180',
      '012345', '654321', 1, '[{"pdi_id": 200001}]', 'CREATED',
      '2026-02-01 08:00:00', '2026-05-02 09:00:00', NULL);

-- Note : l'amas 7004 ci-dessous existe dans les trafics mais volontairement PAS
-- dans trppu_agrebal_pdi. Il couvre le LEFT JOIN de DSR-705 : ses trafics sont
-- restitues avec "nom_amas": null au lieu de disparaitre de la reponse.

-- ----- `trppu_trafic_pdi` -----
-- Produit par le batch YB05 avant l'agregation Agrebal. Les valeurs sont
-- choisies pour que la somme par amas retombe exactement sur
-- trppu_trafic_agrebal ci-dessous (amas 7001, LUNDI, OO : 7+5 = 12 dense,
-- 1+1 = 2 faible1, 1+0 = 1 faible2).
INSERT IGNORE INTO `trppu_trafic_pdi`
  (`id_trafic_pdi`, `id_scenario`, `co_regate`, `id_agrebal`, `agrebal_uuid`,
   `id_pdi`, `co_produit`, `jour_semaine`, `dense`, `faible1`, `faible2`, `dt_calcul`)
VALUES
  (1, 125, '123456', 7001, '0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234', 100001, 'OO',  'LUNDI', 7, 1, 1, '2026-09-05 13:55:00'),
  (2, 125, '123456', 7001, '0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234', 100002, 'OO',  'LUNDI', 5, 1, 0, '2026-09-05 13:55:00'),
  (3, 125, '123456', 7001, '0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234', 100001, 'PPI', 'LUNDI', 5, 1, 1, '2026-09-05 13:55:00'),
  (4, 125, '123456', 7001, '0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234', 100002, 'PPI', 'LUNDI', 3, 0, 0, '2026-09-05 13:55:00'),
  (5, 125, '123456', 7001, '0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234', 100001, 'OO',  'MARDI', 6, 2, 1, '2026-09-05 13:55:00'),
  (6, 125, '123456', 7001, '0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234', 100002, 'OO',  'MARDI', 4, 1, 1, '2026-09-05 13:55:00'),
  (7, 125, '123456', 7002, '77ad1e51-b9fe-490f-b9af-c1ff4f4b9876', 100003, 'OO',  'LUNDI', 5, 0, 0, '2026-09-05 13:55:00');

-- ----- `trppu_trafic_agrebal` -----
-- Table lue par POST /trppu-api/optipacc/trafic-amas (DSR-705).
-- Une ligne par (scenario, amas, produit, jour, DENSITE) : la densite est un
-- discriminant de ligne, l'API la pivote en fort / faible1 / faible2.
INSERT IGNORE INTO `trppu_trafic_agrebal`
  (`id`, `id_scenario`, `co_regate`, `id_agrebal`, `agrebal_uuid`,
   `co_produit`, `jour_semaine`, `couleur_pic`, `volume`)
VALUES
  -- amas 7001, LUNDI, OO   -> fort 12, faible1 2, faible2 1
  (1,  125, '123456', 7001, '0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234', 'OO',  'LUNDI', 'DENSE',   12.0000),
  (2,  125, '123456', 7001, '0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234', 'OO',  'LUNDI', 'FAIBLE1',  2.0000),
  (3,  125, '123456', 7001, '0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234', 'OO',  'LUNDI', 'FAIBLE2',  1.0000),
  -- amas 7001, LUNDI, PPI  -> fort 8, faible1 1, faible2 1
  (4,  125, '123456', 7001, '0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234', 'PPI', 'LUNDI', 'DENSE',    8.0000),
  (5,  125, '123456', 7001, '0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234', 'PPI', 'LUNDI', 'FAIBLE1',  1.0000),
  (6,  125, '123456', 7001, '0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234', 'PPI', 'LUNDI', 'FAIBLE2',  1.0000),
  -- amas 7001, MARDI, OO   -> fort 10, faible1 3, faible2 2
  (7,  125, '123456', 7001, '0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234', 'OO',  'MARDI', 'DENSE',   10.0000),
  (8,  125, '123456', 7001, '0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234', 'OO',  'MARDI', 'FAIBLE1',  3.0000),
  (9,  125, '123456', 7001, '0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234', 'OO',  'MARDI', 'FAIBLE2',  2.0000),
  -- amas 7002 : une seule densite presente, les deux autres sortent a 0
  (10, 125, '123456', 7002, '77ad1e51-b9fe-490f-b9af-c1ff4f4b9876', 'OO',  'LUNDI', 'DENSE',    5.0000),
  -- amas 7003 : semaine complete sur un seul produit
  (11, 125, '123456', 7003, '3c9d51a8-1f22-4b77-8e10-2a4b6c8d0e11', 'OO',  'MERCREDI', 'DENSE',  9.0000),
  (12, 125, '123456', 7003, '3c9d51a8-1f22-4b77-8e10-2a4b6c8d0e11', 'OO',  'JEUDI',    'DENSE',  7.0000),
  (13, 125, '123456', 7003, '3c9d51a8-1f22-4b77-8e10-2a4b6c8d0e11', 'OO',  'VENDREDI', 'DENSE', 14.0000),
  (14, 125, '123456', 7003, '3c9d51a8-1f22-4b77-8e10-2a4b6c8d0e11', 'OO',  'SAMEDI',   'DENSE',  6.0000),
  -- amas 7004 : absent de trppu_agrebal_pdi -> "nom_amas": null (LEFT JOIN)
  (15, 125, '123456', 7004, 'ffffffff-0000-4000-8000-aaaaaaaaaaaa', 'OO',  'LUNDI', 'DENSE',    3.0000),
  (16, 125, '123456', 7004, 'ffffffff-0000-4000-8000-aaaaaaaaaaaa', 'OO',  'LUNDI', 'FAIBLE1',  1.0000),
  -- scenario 128, autre site
  (17, 128, '654321', 7101, '9f2e40b6-5c31-4d88-a903-7b1e2f5c6d22', 'OO',  'LUNDI', 'DENSE',   21.0000),
  (18, 128, '654321', 7101, '9f2e40b6-5c31-4d88-a903-7b1e2f5c6d22', 'OO',  'LUNDI', 'FAIBLE1',  4.0000);

-- =============================================================================
--  6. Cles de repartition et trafics de site
-- =============================================================================

-- ----- `trppu_cles_repartition` -----
-- La vraie table compte ~22 millions de lignes (cf. db/count.json) : seules
-- quelques lignes representatives sont reprises ici.
-- Cle unique : (id_pdi, id_referentiel).
INSERT IGNORE INTO `trppu_cles_repartition`
  (`id`, `id_pdi`, `pdi_rattache`, `trafic_colis`, `trafic_oo`, `trafic_3s`,
   `nature`, `co_regate_site`, `type_site`, `lb_regate`,
   `co_regate_etablissement`, `lb_etablissement`, `co_regate_dex`, `lb_dex`,
   `nb_pre`, `potentielip`, `id_referentiel`, `date_debut_validite`, `date_fin_validite`)
VALUES
  (1, 100001, 100001, 0.0000412000000000000, 0.0003180000000000000, 0.0000091000000000000,
      'RES', '123456', 'PPDC', 'PLUVENCE CENTRE',
      '123400', 'ETABLISSEMENT PLUVENCE', '120000', 'DEX NORD',
      3, 18, 2, '2026-07-01', NULL),
  (2, 100002, 100001, 0.0000298000000000000, 0.0002740000000000000, 0.0000077000000000000,
      'RES', '123456', 'PPDC', 'PLUVENCE CENTRE',
      '123400', 'ETABLISSEMENT PLUVENCE', '120000', 'DEX NORD',
      2, 12, 2, '2026-07-01', NULL),
  (3, 100003, 100003, 0.0000155000000000000, 0.0001120000000000000, 0.0000034000000000000,
      'PRO', '123456', 'PPDC', 'PLUVENCE CENTRE',
      '123400', 'ETABLISSEMENT PLUVENCE', '120000', 'DEX NORD',
      1, 7, 2, '2026-07-01', NULL),
  (4, 200001, 200001, 0.0000901000000000000, 0.0006650000000000000, 0.0000188000000000000,
      'RES', '654321', 'PDC', 'BEAUVAL BOURG',
      NULL, NULL, '120000', 'DEX NORD',
      4, 26, 1, '2026-01-05', NULL);

-- ----- `trppu_cles_repartition_calcule` -----
-- Cle unique : (id_version_cle, id_pdi).
INSERT IGNORE INTO `trppu_cles_repartition_calcule`
  (`id_cle_repartition`, `id_version_cle`, `id_referentiel`, `id_pdi`, `co_regate_site`,
   `cle_colis`, `cle_oo`, `cle_3s`, `cle_potentielip`, `date_creation`)
VALUES
  (1, 2, 2, 100001, '123456',
      0.476190476190476190, 0.451923076923076923, 0.452261306532663316, 0.486486486486486486,
      '2026-07-01 09:05:00'),
  (2, 2, 2, 100002, '123456',
      0.344497607655502392, 0.389204545454545455, 0.382587064676616915, 0.324324324324324324,
      '2026-07-01 09:05:00'),
  (3, 2, 2, 100003, '123456',
      0.179311916153020446, 0.158872377622377622, 0.165151628790719769, 0.189189189189189189,
      '2026-07-01 09:05:00'),
  (4, 3, 1, 200001, '654321',
      1.000000000000000000, 1.000000000000000000, 1.000000000000000000, 1.000000000000000000,
      '2026-01-05 09:05:00');

-- ----- `trppu_trafic_site` -----
-- Cle unique : (id_referentiel, co_regate_site).
INSERT IGNORE INTO `trppu_trafic_site`
  (`id_site_trafic`, `id_referentiel`, `co_regate_site`,
   `trafic_colis_total`, `trafic_oo_total`, `trafic_3s_total`, `potentielip_total`,
   `date_debut_validite`, `date_fin_validite`, `date_creation`)
VALUES
  (1, 2, '123456',
      0.000086500000000000, 0.000704000000000000, 0.000020200000000000, 37,
      '2026-07-01', NULL, '2026-07-01 09:00:00'),
  (2, 1, '654321',
      0.000090100000000000, 0.000665000000000000, 0.000018800000000000, 26,
      '2026-01-05', NULL, '2026-01-05 09:00:00');

-- =============================================================================
--  7. Exploitation
-- =============================================================================

-- ----- `trppu_suivi_batch` -----
INSERT IGNORE INTO `trppu_suivi_batch`
  (`id_calcul_batch`, `lb_batch`, `dt_debut`, `dt_fin`, `statut`, `duree`,
   `commande`, `commentaire`)
VALUES
  (1, 'YB05_TRAFIC_PDI',     '2026-09-05 13:50:00', '2026-09-05 13:56:00', 'SUCCES',  '00:06:00',
      'python -m app.main --traitement trafic_pdi --scenario 125', NULL),
  (2, 'YB05_TRAFIC_AGREBAL', '2026-09-05 13:56:00', '2026-09-05 14:00:00', 'SUCCES',  '00:04:00',
      'python -m app.main --traitement trafic_agrebal --scenario 125', NULL),
  (3, 'YB05_TRAFIC_AGREBAL', '2026-09-08 11:20:00', '2026-09-08 11:21:00', 'ECHEC',   '00:01:00',
      'python -m app.main --traitement trafic_agrebal --scenario 126',
      'Scenario 126 non eligible : trafics PDI non calcules');

-- ----- `demande_dsr` -----
INSERT IGNORE INTO `demande_dsr`
  (`id`, `nomFichier`, `statut`, `idrh`, `codeRegate`, `message`, `bassins`,
   `simuOptiTheo`, `simuScenarDex`, `simuScenarRef`, `simuExistProj`,
   `forcerExec`, `restitTousBassins`, `optiTransport`, `choixPI`, `simulerACP`,
   `createdAt`, `updatedAt`)
VALUES
  (1, 'demande_pluvence_2026_09.xlsx', 'TERMINEE', '0000001', '123456',
      'Traitement termine sans erreur', '["BASSIN_NORD", "BASSIN_EST"]',
      1, 0, 1, 0, 0, 1, 0, 1, 0,
      '2026-09-01 09:00:00', '2026-09-01 09:42:00'),
  (2, 'demande_beauval_2026_05.xlsx', 'ERREUR',   '0000003', '654321',
      'Colonne codeRegate absente du fichier', NULL,
      0, 1, 0, 0, 1, 0, 1, 0, 0,
      '2026-05-02 09:20:00', '2026-05-02 09:21:00');

-- ----- `trafic_staging` -----
-- Table de chargement, volontairement laissee vide : elle est alimentee puis
-- videe par l'import de trafics. Elle n'a ni cle primaire ni cle unique, donc
-- INSERT IGNORE n'y serait pas idempotent.

-- FIN 02_donnees.sql
