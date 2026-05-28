# TRPPU / DSR — Incompréhensions & points à clarifier (PO / archi)

Liste consolidée des ambiguïtés relevées dans les tickets, **à arbitrer avant
implémentation**. Criticité : 🔴 bloquant · 🟠 important · 🟡 confort.

---

## 1. 🔴 Enum `SAISON` vs `LOCAL` — `trppu_neutralisations`

- **Constat** : le schéma définit `type ENUM('FERIE','PEAK','LOCAL')`. Les tickets
  DSR-645 (écriture) et DSR-652 (lecture) imposent **`SAISON`** (« Neutralisation
  saisonnière »). `LOCAL` n'est jamais cité côté métier.
- **Impact** : écriture/lecture incohérentes tant que l'enum n'est pas aligné.
- **Question** : `LOCAL` = `SAISON` (simple renommage) ? Existe-t-il des données
  `LOCAL` à migrer ? → DDL `MODIFY ENUM(...'SAISON')` proposé (DSR-645 §6).
- **Tickets** : 645, 652.

## 2. 🔴 Colonnes manquantes (`id_rh`, `dt_creation`)

| Table | Colonnes exigées par | Présentes ? |
| ----- | -------------------- | ----------- |
| `trppu_scenario_comptages_manuels` | `id_rh` (DSR-644) | ❌ |
| `trppu_neutralisations` | `id_rh`, `dt_creation` (DSR-645) | ❌ |
| `trppu_scenario_variations_prev` | `id_rh`, `dt_creation` (DSR-646) | ❌ |

- **Impact** : les services d'écriture ne peuvent pas remplir ces champs sans migration.
- **Question** : valide-t-on les `ALTER TABLE` (cf. `README_ameliorations.md`, ticket migration) ?

## 3. 🔴 Méthode de cryptage de `id_rh`

- **Constat** : 6 tickets exigent un `id_rh` « crypté » (634, 644, 645, 646, 656,
  661) mais **aucun algorithme** n'est spécifié et **aucun utilitaire** n'existe
  dans le code. Colonnes cibles `VARCHAR(40)`.
- **Questions** : chiffrement réversible (AES + clé applicative) ou hash
  irréversible ? Où est gérée/segregée la clé ? Format garantissant ≤ 40 caractères ?
  Faut-il pouvoir **déchiffrer** pour affichage/audit ?
- **Tickets** : 634, 644, 645, 646, 656, 661.

## 4. 🔴 Source des jours fériés nationaux

- **Constat** : DSR-613 et DSR-645 déduisent les fériés ; aucune source n'existe.
  Fériés mobiles (Pâques, Ascension, Pentecôte) ⇒ pas une simple liste statique.
- **Questions** : table `trppu_jours_feries` (maîtrisée, offline) **ou** lib Python
  (`workalendar` / `jours-feries-france`) ? Périmètre national seul (pour l'instant) ?
- **Tickets** : 613, 645.

## 5. 🟠 Contrats d'API non spécifiés (paths, verbes, payloads)

- **Constat** : les tickets décrivent des « services yb04 » sans chemin HTTP, verbe
  ni format JSON. Les fiches proposent des contrats par défaut (préfixe
  `/trppu-api/scenarios/{id}/...`).
- **Question** : valider la convention proposée, ou s'aligner sur un contrat
  d'interface IHM↔yb04 préexistant (Swagger/contrat de POD) ?
- **Tickets** : tous.

## 6. 🟠 `nb_jours_semaine` par défaut : 5 (code) vs 6 (tickets)

- **Constat** : `app/routes/trppu_scenario/schemas.py` fixe le défaut **5** ; les
  tickets DSR-634/656 disent « par défaut **6** ».
- **Question** : aligner le défaut à 6 ? Impacte aussi le calcul `nb_jour` (DSR-645).
- **Tickets** : 634, 645, 656.

## 7. 🟠 Bornes « today » du réalisé / prévisionnel

- **Constat** : `recompute_realise_prev()` pose `prev_fin` dès que
  `periode_fin >= today` (renseigné le jour-même), alors que DSR-634/656 disent
  « si la période s'arrête **à la date du jour ou avant** → `NULL` ». Idem
  `periode_realise_debut` quand `periode_debut == today`.
- **Question** : trancher l'inclusion/exclusion de la journée courante, puis
  encoder les cas limites en tests.
- **Tickets** : 634, 656 (lecture 655).

## 8. 🟠 « Valeurs par défaut » DSR-651 / DSR-652

- **DSR-651** : renvoyer **uniquement** les variations ≠ 0 (l'IHM applique 0 %) ou
  **hydrater** toute la liste des produits actifs à 0 % (source `trppu_produit`) ?
- **DSR-652** : structure de réponse plate (lignes brutes) vs **regroupée** par
  type (proposée dans la fiche) ?
- **Tickets** : 651, 652.

## 9. 🟠 Rétention PIC — sélection de version & table legacy

- **`id_pic_version` du scénario** : si plusieurs lignes `trppu_pic_version` pour un
  `id_scenario` (activation/désactivation), laquelle prendre (DSR-660) ?
- **Clé naturelle du coefficient** (DSR-661) : (produit, densité) seul (texte cas
  1.1) ou (produit, **jour**, densité) (acceptance #1) ? → on retient le jour inclus.
- **`coef` absent** de la liste des paramètres DSR-661 alors qu'il est requis : confirmer.
- **`co_regate`** non transmis à DSR-661 : dérivé de `trppu_scenario` — OK ?
- **Table `trppu_scenario_pic_coeffs`** (override legacy au schéma) : abandonnée au
  profit de `trppu_pic_version`+`trppu_pic_coefficients` ? À nettoyer ?
- **Niveaux `DEX` / `SITE`** : non traités par 660/661 — quand interviennent-ils ?
- **Tickets** : 660, 661.

## 10. 🟠 `id_pic_version` par défaut (création)

- **Constat** : DSR-634 dit « id_pic_version par défaut = 0 (national) » ; le code
  résout `est_par_defaut=1` (fallback 1) ; DSR-660 lit le défaut sur `id_pic_version=1`.
- **Question** : harmoniser la convention de la version « par défaut » (0 ? 1 ?
  flag `est_par_defaut` ?).
- **Tickets** : 634, 660.

## 11. 🟡 `id session IHM` (logs Kibana)

- **Constat** : DSR-650/651/652/653/660/661 exigent que **tous les logs portent
  l'id de session IHM**. Sa provenance n'est pas définie.
- **Question** : header HTTP (`X-Session-Id`) ? paramètre ? généré côté gateway ?
- **Tickets** : 650, 651, 652, 653, 654, 660, 661.

## 12. 🟡 Unicité / upsert par table

- **Question** : clés fonctionnelles à contraindre (UNIQUE) ?
  - comptages : (`id_scenario`, `co_produit`) — un seul comptage/produit ?
  - variations : (`id_scenario`, `co_produit`).
  - neutralisations : PEAK/SAISON = **1 seule ligne** par scénario (DSR-652) ⇒ contrainte ?
  - TMH : (`id_scenario`, `co_produit`).
- **Impact** : choix INSERT pur vs `INSERT ... ON DUPLICATE KEY UPDATE`.
- **Tickets** : 644, 645, 646, 649, 659.

## 13. 🟡 TMH — calcul des moyennes & ligne absente

- Les moyennes (`moyenne_journaliere`, `moyenne_hebdo`) sont-elles calculées par
  l'IHM (reçues telles quelles) ou (re)calculées serveur (DSR-649/659/634) ?
- Si la ligne TMH n'existe pas : `404` ou **upsert** (DSR-649/659) ?
- **Chevauchement DSR-649 / DSR-659** (cf. amélioration dédiée).
- **Tickets** : 649, 659.

---

## Récapitulatif par criticité

| # | Sujet | Criticité | Tickets |
| - | ----- | --------- | ------- |
| 1 | SAISON vs LOCAL | 🔴 | 645, 652 |
| 2 | Colonnes id_rh/dt_creation manquantes | 🔴 | 644, 645, 646 |
| 3 | Cryptage id_rh | 🔴 | 634, 644, 645, 646, 656, 661 |
| 4 | Source jours fériés | 🔴 | 613, 645 |
| 5 | Contrats d'API | 🟠 | tous |
| 6 | nb_jours_semaine défaut 5/6 | 🟠 | 634, 645, 656 |
| 7 | Bornes today réalisé/prév | 🟠 | 634, 656 |
| 8 | Défauts 651/652 | 🟠 | 651, 652 |
| 9 | PIC version/clé/legacy | 🟠 | 660, 661 |
| 10 | id_pic_version par défaut | 🟠 | 634, 660 |
| 11 | id session IHM | 🟡 | 650-653, 660, 661 |
| 12 | Unicité / upsert | 🟡 | 644, 645, 646, 649, 659 |
| 13 | TMH moyennes / ligne absente | 🟡 | 649, 659 |
