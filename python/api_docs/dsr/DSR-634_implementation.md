# DSR-634 — Sauvegarde en base des informations d'un scénario à sa création

> Document d'analyse et de spécification de l'implémentation à réaliser.
> Couvre **DSR-634** (scénario) et ses deux tickets liés **DSR-647** (site) et
> **DSR-648** (trafics TMH).

---

## 1. Contexte et objectif

À la création d'un scénario TRPPU, une fois que :

1. les données de trafic ont été **récupérées depuis Databricks**, puis
2. le **tableau « TMH »** de l'IHM a été mis à jour (volumes, moyennes),

il faut **enregistrer automatiquement en base** toutes les informations du
scénario « en cours ». Tous les paramètres non saisis prennent leur valeur
**par défaut** (aucune période neutralisée, aucune variation prévisionnelle —
ces paramètres ne sont écrits qu'en cas de modification ultérieure).

Trois tables sont concernées, **un service dédié** doit exister pour chacune
(service applicatif côté API `ys04` / `yb04`) :

| Table            | Ticket   | Rôle                                                        |
| ---------------- | -------- | ----------------------------------------------------------- |
| `trppu_scenario` | DSR-634  | 1 ligne par scénario : métadonnées + bornes de périodes     |
| `trppu_tmh`      | DSR-648  | 1 ligne par produit du tableau TMH : trafics & moyennes     |
| `trppu_site`     | DSR-647  | 1 ligne par site (créée uniquement si le site est inconnu)  |

---

## 2. État actuel de l'implémentation (`app/routes/trppu_scenario/`)

Le sous-package existant expose déjà un CRUD + une machine à états :

| Fichier        | Contenu                                                                                   |
| -------------- | ----------------------------------------------------------------------------------------- |
| `routes.py`    | Endpoints : `POST` (création), `GET` (liste/détail), `PATCH` (périodes, nb_jours, libellé, statut, est_fige), `DELETE` (archivage), `POST /duplicate`, `POST /mise-en-prod` |
| `helpers.py`   | `SELECT_SCENARIO_SQL`, `default_periode()`, `recompute_realise_prev()`, `resolve_default_pic_version()`, `ensure_site_exists()`, `increment_version()`, `fetch_scenario_or_404()` |
| `schemas.py`   | `ScenarioCreate`, `ScenarioOut`, `PeriodeUpdate`, `NbJoursUpdate`, etc.                    |
| `statuts.py`   | Transitions de statut + effets de bord (`dt_validation`, `dt_mise_en_prod`, `est_fige`)   |

### Ce que `POST /trppu-api/scenarios` fait aujourd'hui

```text
1. Résout id_pic_version (payload ou défaut est_par_defaut=1, fallback 1).
2. Applique la période par défaut (today-1an / today+1an) si absente.
3. Calcule realise/prev via recompute_realise_prev().
4. Dans une transaction :
   a. ensure_site_exists()  → INSERT trppu_site si le site n'existe pas  ✅ (DSR-647)
   b. INSERT trppu_scenario (sous-ensemble de colonnes)
5. Relit et renvoie le scénario (201).
```

Colonnes actuellement insérées dans `trppu_scenario` :
`co_regate, lb_scenario, co_roc, statut='EN COURS', dt_creation=NOW(),
periode_debut, periode_fin, periode_realise_debut, periode_realise_fin,
periode_prev_debut, periode_prev_fin, nb_jours_semaine, id_pic_version,
version_scenario=1, est_fige=0`.

> **Conclusion :** la **structure** (route + transaction + insertion site) est
> déjà là et réutilisable. Il manque surtout des **colonnes**, le **service TMH**
> (totalement absent), le **calcul des nombres de jours**, le **cryptage id_rh**
> et la **chaîne d'orchestration** trafics → TMH → sauvegarde.

---

## 3. Analyse des écarts (gap analysis)

### 3.1 Table `trppu_scenario` — colonnes manquantes à l'INSERT

| Colonne                  | Attendu par DSR-634                                                          | Présent aujourd'hui |
| ------------------------ | --------------------------------------------------------------------------- | ------------------- |
| `dt_mise_en_oeuvre`      | Date de mise en œuvre saisie par l'utilisateur (défaut = date du jour)      | ❌ non inséré        |
| `dt_real_prev`           | Date séparant réalisé / prévisionnel = **date du jour** à la création       | ❌ non inséré        |
| `nb_jours_ouvres`        | Nb de jours ouvrés sur la période                                           | ❌ non inséré        |
| `nb_jours_ouvrables`     | Nb de jours ouvrables sur la période                                        | ❌ non inséré        |
| `nb_jours_scenario`      | Nb de jours retenus pour le calcul (ouvrés/ouvrables − jours neutralisés)   | ❌ non inséré        |
| `id_rh_creation`         | id RH du créateur, **crypté**                                               | ❌ non inséré        |
| `id_rh_maj`              | id RH de la dernière MAJ, crypté (= id_rh_creation à la création)           | ❌ non inséré        |
| `nb_jours_semaine`       | **Défaut 6** (5 = jours ouvrés / 6 = jours ouvrables)                        | ⚠️ défaut codé à 5  |
| `co_roc`, `lb_scenario`, `statut`, `dt_creation`, `periode_*`, `id_pic_version`, `version_scenario`, `est_fige` | OK | ✅                   |

> ⚠️ **Incohérence de valeur par défaut** : le ticket indique « nb_jours_semaine
> par défaut **6** » (ligne 18) tandis que `schemas.py` fixe le défaut à **5**.
> À aligner (voir §11, question ouverte).

`SELECT_SCENARIO_SQL` et le modèle `ScenarioOut` devront aussi être étendus pour
exposer les nouvelles colonnes.

### 3.2 Table `trppu_tmh` — entièrement à implémenter (DSR-648)

- **Aucun** code n'écrit dans `trppu_tmh` aujourd'hui.
- `app/routes/trafics.py` ne fait que **récupérer les trafics bruts** Databricks ;
  il n'y a **aucun calcul** de `volume_realise`, `volume_previsionnel`,
  `moyenne_journaliere`, `moyenne_hebdo`, ni de TMJ.
- Il faut donc : un **schéma Pydantic**, un **service d'insertion** (1 ligne par
  produit), et la **logique de calcul** des volumes/moyennes/TMH/TMJ.

### 3.3 Table `trppu_site` — déjà couvert (DSR-647)

`ensure_site_exists()` réalise déjà l'INSERT conditionnel attendu
(`co_regate, lb_regate, type_site, co_roc`, `dt_maj` via défaut SQL). ✅

> Point à confirmer : DSR-647 nomme le paramètre `lb_type_entite_regate_court`
> alors que l'implémentation utilise `lb_regate`. C'est le **même champ**
> (libellé court du site) — il faut juste s'assurer que la donnée transmise par
> l'IHM/Databricks alimente bien `trppu_site.lb_regate`.

### 3.4 Briques transverses absentes

| Brique                       | État actuel                                                                 |
| ---------------------------- | --------------------------------------------------------------------------- |
| Cryptage `id_rh`             | ❌ inexistant — aucune fonction de chiffrement dans le code                  |
| Calcul jours ouvrés/ouvrables| ⚠️ partiel — `calcl_nbr_jours` exclut **dimanche** (= ouvrables) mais ne calcule pas la variante ouvrés (Mon–Ven) ni `nb_jours_scenario` |
| Calcul TMH / TMJ             | ❌ inexistant                                                                |
| Orchestration trafics→TMH→save| ❌ inexistante — la création actuelle ignore totalement les trafics         |

---

## 4. Architecture cible

L'esprit du ticket (« un service par table ») se traduit par une couche
**services** réutilisable, appelée par la route de création. Découpage proposé,
dans `app/routes/trppu_scenario/` (et helpers transverses dans `app/`) :

```
app/
├─ services/                      # nouveau — logique métier réutilisable
│  ├─ scenario_service.py         # INSERT trppu_scenario (DSR-634)
│  ├─ tmh_service.py              # INSERT trppu_tmh, 1 ligne / produit (DSR-648)
│  ├─ site_service.py             # ensure_site_exists déplacé ici (DSR-647)
│  └─ jours_service.py            # nb_jours_ouvres / ouvrables / scenario
├─ security/
│  └─ crypto.py                   # encrypt_id_rh() / decrypt_id_rh()
└─ routes/trppu_scenario/
   ├─ routes.py                   # orchestration de la création
   ├─ helpers.py                  # recompute_realise_prev (révisé), SQL
   └─ schemas.py                  # ScenarioCreate étendu + TmhItem + ScenarioOut
```

> Le découpage exact (sous-package `services/` vs enrichissement de `helpers.py`)
> reste au choix de l'équipe ; l'essentiel est d'avoir **une fonction de service
> distincte et testable par table**, conformément au ticket.

---

## 5. Spécification détaillée par table

### 5.1 `trppu_scenario` (cœur DSR-634)

INSERT cible (transaction unique avec les autres tables) :

| Colonne                | Valeur à la création                                                              |
| ---------------------- | --------------------------------------------------------------------------------- |
| `id_scenario`          | AUTO_INCREMENT MySQL                                                               |
| `co_roc`               | reçu                                                                               |
| `co_regate`            | reçu                                                                               |
| `lb_scenario`          | reçu                                                                               |
| `statut`               | `'EN COURS'`                                                                       |
| `dt_creation`          | `NOW()` (date + heure)                                                             |
| `dt_mise_en_oeuvre`    | saisie utilisateur ; **défaut = date du jour**                                    |
| `dt_real_prev`         | **date du jour** (date séparant réalisé / prévisionnel)                           |
| `periode_debut`        | reçu                                                                               |
| `periode_fin`          | reçu                                                                               |
| `periode_realise_debut`| calculé (cf §6.1)                                                                  |
| `periode_realise_fin`  | calculé (cf §6.1)                                                                  |
| `periode_prev_debut`   | calculé (cf §6.1)                                                                  |
| `periode_prev_fin`     | calculé (cf §6.1)                                                                  |
| `nb_jours_semaine`     | 5 (jours ouvrés) ou 6 (jours ouvrables) — **défaut 6**                             |
| `nb_jours_ouvres`      | calculé sur la période (cf §6.2)                                                   |
| `nb_jours_ouvrables`   | calculé sur la période (cf §6.2)                                                   |
| `nb_jours_scenario`    | ouvrés ou ouvrables (selon `nb_jours_semaine`) − jours neutralisés (= 0 à la création) |
| `id_pic_version`       | version PIC par défaut liée au scénario (cf §11 — ambiguïté « 0 / national »)      |
| `version_scenario`     | `1`                                                                               |
| `est_fige`             | `0`                                                                               |
| `id_rh_creation`       | `encrypt_id_rh(id_rh)`                                                             |
| `id_rh_maj`            | `encrypt_id_rh(id_rh)` (identique à la création)                                  |
| `dt_maj`               | défaut SQL `CURRENT_TIMESTAMP`                                                     |

### 5.2 `trppu_tmh` (DSR-648)

**Une ligne par produit** présent dans le tableau TMH de l'IHM :

| Colonne                | Valeur                                                          |
| ---------------------- | -------------------------------------------------------------- |
| `id_tmh`               | AUTO_INCREMENT                                                 |
| `id_scenario`          | id du scénario qui vient d'être inséré                         |
| `co_produit`           | code produit                                                   |
| `volume_realise`       | trafic réalisé (constaté) du produit                          |
| `volume_previsionnel`  | trafic prévisionnel du produit                                |
| `moyenne_journaliere`  | moyenne du trafic sur une journée (TMJ)                       |
| `moyenne_hebdo`        | moyenne du trafic sur une semaine (TMH)                       |
| `dt_calcul`            | date du calcul / MAJ des trafics (défaut SQL `CURRENT_TIMESTAMP`) |
| `bl_exclu`             | flag d'exclusion du produit du calcul des trafics             |

> Le service reçoit en entrée la liste produits {`co_produit`, `volume_realise`,
> `volume_previsionnel`, `moyenne_journaliere`, `moyenne_hebdo`, `exclusion`} —
> soit telle que calculée par l'IHM, soit calculée côté serveur (cf §6.3).

### 5.3 `trppu_site` (DSR-647) — déjà en place

INSERT conditionnel si `co_regate` absent :
`co_regate, lb_regate (libellé court), type_site, co_roc, dt_maj=now`.
Pas de MAJ si le site existe déjà.

---

## 6. Règles de calcul

### 6.1 Bornes réalisé / prévisionnel

`recompute_realise_prev()` existe déjà mais doit être **revue** pour coller
exactement aux règles du ticket, en particulier le **traitement de la journée
du jour** (bornes incluses/exclues) :

| Champ                   | Règle DSR-634                                                                 |
| ----------------------- | ---------------------------------------------------------------------------- |
| `periode_realise_debut` | période passée → `periode_debut` ; période entièrement future → `NULL`        |
| `periode_realise_fin`   | fin < today → `periode_fin` ; fin ≥ today → `today` ; période future → `NULL` |
| `periode_prev_debut`    | période future incluant today → `today` ; entièrement future → `periode_debut` ; fin < today → `NULL` |
| `periode_prev_fin`      | période couvrant le futur → `periode_fin` ; fin ≤ today → `NULL`              |

> ⚠️ **Divergence à arbitrer sur la borne « today »** : l'implémentation actuelle
> pose `prev_fin = periode_fin` dès que `periode_fin >= today` (donc renseigné le
> jour-même), alors que le ticket dit « si la période s'arrête **à la date du
> jour ou avant** → `prev_fin = NULL` ». De même pour `periode_realise_debut`
> quand `periode_debut == today`. Ces cas limites doivent être tranchés avec le
> PO puis encodés dans les tests.

### 6.2 Nombres de jours

- `nb_jours_ouvrables` : jours **lundi → samedi** (dimanche exclu) sur la période
  → c'est ce que calcule déjà `calcl_nbr_jours` (`isoweekday != 7`).
- `nb_jours_ouvres` : jours **lundi → vendredi** (samedi et dimanche exclus)
  → **à ajouter** (variante non couverte aujourd'hui).
- `nb_jours_scenario` : `nb_jours_ouvres` **ou** `nb_jours_ouvrables` selon
  `nb_jours_semaine`, **moins les jours neutralisés**. À la création, aucune
  neutralisation ⇒ `nb_jours_scenario = nb_jours_ouvres|ouvrables`.

→ Factoriser ces calculs dans `jours_service.py` (réutilise la logique de
`calcl_nbr_jours.py`).

### 6.3 TMH / TMJ (par produit)

À partir des trafics Databricks agrégés par produit sur la période :

- `volume_realise`     = somme des volumes sur la portion **réalisée**.
- `volume_previsionnel`= somme des volumes sur la portion **prévisionnelle**.
- `moyenne_journaliere` (**TMJ**) = volume / `nb_jours_scenario`
  (non affiché dans l'IHM mais nécessaire au calcul du TMH).
- `moyenne_hebdo` (**TMH**) = TMJ × `nb_jours_semaine`.
- `bl_exclu` : produit exclu du calcul (flag IHM).

> La formule exacte (notamment quelle base de jours utiliser pour réalisé vs
> prévisionnel) doit être confirmée avec le PO / l'algorithme IHM existant.

---

## 7. Cryptage de l'`id_rh`

- Colonnes cibles : `id_rh_creation`, `id_rh_maj` → `VARCHAR(40)`.
- Créer `app/security/crypto.py` avec `encrypt_id_rh(clear: str) -> str` (et la
  fonction de déchiffrement symétrique pour l'affichage éventuel).
- La **méthode** (clé symétrique applicative, hash, ou format imposé par le SI)
  et la **longueur garantie ≤ 40** doivent être validées (voir §11).

---

## 8. Orchestration de la création

Séquence cible (le déclencheur exact — endpoint unique côté serveur ou appel IHM
après calcul TMH — est à confirmer, cf §11) :

```text
POST /trppu-api/scenarios
  ├─ 1. Valider le body (co_regate, co_roc, lb_scenario, dates, nb_jours_semaine,
  │       id_rh, liste produits TMH…)
  ├─ 2. Résoudre id_pic_version par défaut
  ├─ 3. Appliquer périodes par défaut si absentes
  ├─ 4. Calculer bornes réalisé/prév  (jours_service / recompute_realise_prev)
  ├─ 5. Calculer nb_jours_ouvres / ouvrables / scenario
  ├─ 6. Crypter id_rh
  └─ 7. TRANSACTION unique :
         a. site_service.ensure_site_exists()      → trppu_site   (DSR-647)
         b. scenario_service.insert()              → trppu_scenario (DSR-634)
         c. tmh_service.insert_batch(id_scenario)  → trppu_tmh × N produits (DSR-648)
  → 201 + scénario complet
```

Tout doit être **atomique** : un échec sur TMH annule l'insertion du scénario.

---

## 9. Schémas Pydantic à faire évoluer

- **`ScenarioCreate`** : ajouter `co_roc` (déjà présent), `dt_mise_en_oeuvre`
  (optionnel, défaut today), `id_rh` (obligatoire), `nb_jours_semaine` (défaut
  **6** à confirmer), et la **liste des produits TMH** (`list[TmhItem]`).
- **`TmhItem`** (nouveau) : `co_produit`, `volume_realise`, `volume_previsionnel`,
  `moyenne_journaliere`, `moyenne_hebdo`, `exclusion: bool`.
- **`ScenarioOut`** : ajouter `dt_mise_en_oeuvre`, `dt_real_prev`,
  `nb_jours_ouvres`, `nb_jours_ouvrables`, `nb_jours_scenario`
  (les `id_rh_*` ne sont **pas** exposés en sortie). Étendre `SELECT_SCENARIO_SQL`
  en conséquence.

---

## 10. Mapping critères d'acceptance

| Critère DSR-634                                                        | Couverture                                  |
| --------------------------------------------------------------------- | ------------------------------------------- |
| `trppu_scenario` renseigné et conforme à l'IHM                        | §5.1 + §3.1 (colonnes ajoutées)             |
| `trppu_pic_version` lié à l'id du scénario, pic_version par défaut    | §11 (ambiguïté à lever) + `resolve_default_pic_version` |
| Infos `trppu_pic_version` cohérentes avec le scénario IHM             | dépend de l'arbitrage §11                   |
| `trppu_tmh` correct et en phase avec l'IHM                            | §5.2 + §6.3 (DSR-648)                        |
| Site présent dans `trppu_site`                                        | §5.3 (DSR-647, déjà OK)                      |

---

## 11. Points d'attention & questions ouvertes

1. **`id_pic_version` par défaut** — le corps du ticket dit « id_pic_version par
   défaut = **0** (national) lié au scénario », tandis que le critère d'acceptance
   parle d'**ajouter une ligne dans `trppu_pic_version`** liée au scénario.
   L'implémentation actuelle se contente de **réutiliser** une version existante
   (`est_par_defaut=1`, fallback 1). À clarifier : faut-il **créer** une
   `trppu_pic_version` (niveau NATIONAL) par scénario, ou seulement la **référencer** ?
2. **`nb_jours_semaine` par défaut** : 5 (code actuel) vs 6 (ticket). → Aligner.
3. **Bornes « today »** réalisé/prév : trancher les cas limites (§6.1) avec le PO.
4. **Cryptage `id_rh`** : algorithme, clé, et garantie de longueur ≤ 40 (§7).
5. **Déclencheur de la sauvegarde** : endpoint serveur unique qui orchestre
   `trafics → TMH → save`, ou bien l'IHM POST-e les lignes TMH déjà calculées ?
   (impacte le contrat de `POST /trppu-api/scenarios`, §8–§9).
6. **Source de `volume_realise` / `volume_previsionnel`** et formule exacte TMH/TMJ
   (§6.3) à valider contre l'algorithme IHM existant.
7. **`lb_type_entite_regate_court`** (DSR-647) ↔ `trppu_site.lb_regate` : confirmer
   le mapping de nommage.

---

## 12. Synthèse de l'effort

| Lot                                                | Existant | Reste à faire                          |
| -------------------------------------------------- | -------- | -------------------------------------- |
| Insertion site (DSR-647)                           | ✅       | Vérifier mapping libellé               |
| Colonnes manquantes `trppu_scenario` (DSR-634)     | ⚠️       | dt_mise_en_oeuvre, dt_real_prev, nb_jours_*, id_rh_* |
| Calcul nb_jours ouvrés + scenario                  | ⚠️       | Variante ouvrés + déduction neutralisations |
| Cryptage id_rh                                      | ❌       | Module `security/crypto.py`            |
| Service + calcul TMH (DSR-648)                     | ❌       | Schéma + service + calcul TMJ/TMH      |
| Orchestration trafics → TMH → save                 | ❌       | Contrat d'API + transaction atomique   |
| Extension `ScenarioOut` / `SELECT_SCENARIO_SQL`    | ⚠️       | Exposer les nouvelles colonnes         |
