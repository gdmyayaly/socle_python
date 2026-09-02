# Mode d'emploi — purge de la base `dsr_mercure_aa`

> **Principe :** on **conserve 7 tables** (référentiels + données de trafic calculées),
> on **vide les 18 autres** (données métier : scénarios et tout ce qui en dépend).
> **Rédigé le :** 2026-08-19, à partir du schéma `db/db_new.sql` (25 tables) et de la
> volumétrie `db/count.json` (relevé du 13/08).
>
> ⚠️ **Procédure destructive et non transactionnelle** — `TRUNCATE` est un DDL, il ne se
> `ROLLBACK` pas. La sauvegarde du §3 n'est pas optionnelle.

---

## 1. Périmètre

### 1.1 Tables CONSERVÉES — on n'y touche pas (7)

| Table | Lignes (13/08) | Rôle |
|---|---:|---|
| `trppu_cles_repartition` | 22 395 341 | clés de répartition (batch) |
| `trppu_agrebal_pdi` | 9 505 | correspondance Agrébal ↔ PDI |
| `trppu_produit` | 8 | référentiel produits |
| `trppu_pic_version` | 6 | versions de paramétrage PIC |
| `trppu_cles_repartition_calcule` | 0 | clés recalculées |
| `trppu_trafic_site` | 0 | trafic par site |
| `trppu_version_cle` | 0 | versions de clés |

> **Nom corrigé :** `trppu_version_cles` n'existe pas, la table s'appelle
> `trppu_version_cle` (singulier). Et `trppu_trafic_site` s'appelait `trppu_site_trafic`
> avant le dump du 17/08 — même table, renommée.

### 1.2 Tables VIDÉES (18)

| Table | Lignes | Table | Lignes |
|---|---:|---|---:|
| `trppu_tmh` | 245 | `trppu_scenario_comptages_manuels` | 2 |
| `trppu_scenario` | 88 | `demande_dsr` | 2 |
| `trppu_site` | 77 | `trppu_scenario_exclusions` | 0 |
| `trppu_pic_coefficients` | 127 | `trppu_scenario_pic_coeffs` | 0 |
| `trppu_pic_coefficients_ko` | 42 | `trppu_trafic_agrebal` | 0 |
| `trppu_scenario_variations_prev` | 20 | `trppu_trafic_pdi` | 0 |
| `trppu_neutralisations` | 11 | `trppu_api_log` | 0 |
| `trafic_staging` | 0 | `trppu_recalcul_log` | 0 |
| `trppu_referentiel` | 0 | `trppu_suivi_batch` | 0 |

**Deux entrées à confirmer avant de lancer** (cf. §6) : `trppu_referentiel` et `demande_dsr`
ne sont pas des données de scénario.

---

## 2. Carte des dépendances

Relevé exhaustif des `FOREIGN KEY` du schéma. Le sens de la purge est favorable :

**Aucune des 7 tables conservées ne porte de FK sortante.** Rien de ce qu'on garde ne
dépend de ce qu'on supprime — il n'y a donc aucun risque de laisser un référentiel orphelin.

Les seules FK traversant la frontière vont dans le bon sens (enfant supprimé → parent
conservé), et sont donc satisfaites par construction :

```
trppu_tmh.co_produit                      -> trppu_produit       RESTRICT
trppu_scenario_variations_prev.co_produit -> trppu_produit       RESTRICT
trppu_pic_coefficients.co_produit         -> trppu_produit       RESTRICT
trppu_pic_coefficients.id_pic_version     -> trppu_pic_version   CASCADE
```

**Une seule table du lot est parente d'une FK : `trppu_scenario`**, avec 9 enfants.

```
trppu_scenario
  ├── trppu_tmh                        CASCADE
  ├── trppu_neutralisations            CASCADE
  ├── trppu_scenario_variations_prev   CASCADE
  ├── trppu_scenario_comptages_manuels RESTRICT
  ├── trppu_scenario_exclusions        RESTRICT
  ├── trppu_scenario_pic_coeffs        RESTRICT
  ├── trppu_trafic_agrebal             RESTRICT
  ├── trppu_api_log                    RESTRICT
  └── trppu_recalcul_log               RESTRICT
```

D'où la seule contrainte d'ordre de toute la procédure :

1. **`TRUNCATE` est refusé sur `trppu_scenario`** — erreur MySQL 1701 : une table parente
   d'une FK ne peut pas être tronquée, même quand tous ses enfants sont vides. Il faut
   `DELETE`, puis remettre l'AUTO_INCREMENT à la main.
2. `trppu_scenario` passe **en dernier**, après ses 9 enfants.

Les 17 autres tables n'ont aucune FK entrante : `TRUNCATE` direct, dans n'importe quel
ordre. (Être *enfant* d'une FK n'empêche jamais un `TRUNCATE` ; seul le rôle de *parent* le
bloque.)

---

## 3. Sauvegarde (obligatoire)

```bash
# Adapter l'hôte et l'utilisateur aux variables de python/.env
mysqldump -h "$SGBD_SERVER_WRITE" -u "$SGBD_APP_USER_WRITE" -p \
  --single-transaction --quick --routines --triggers \
  dsr_mercure_aa > backup_dsr_mercure_aa_$(date +%Y%m%d_%H%M).sql
```

Les 22,4 M lignes de `trppu_cles_repartition` rendent ce dump lourd et long — et cette table
fait partie de celles qu'on **conserve**, donc elle ne sera pas touchée. Un dump limité aux
seules tables purgées suffit largement et va beaucoup plus vite :

```bash
mysqldump ... dsr_mercure_aa \
  --ignore-table=dsr_mercure_aa.trppu_cles_repartition \
  --ignore-table=dsr_mercure_aa.trppu_agrebal_pdi > backup_avant_purge.sql
```

---

## 4. Script de purge

Le `SELECT DATABASE()` initial est un garde-fou : vérifier qu'il affiche bien la base
attendue avant d'aller plus loin.

```sql
-- ============================================================
-- Garde-fou : ne JAMAIS exécuter ce script sur la production.
SELECT DATABASE(), @@hostname;
-- ============================================================

-- --- Étape 1 : les 9 enfants de trppu_scenario --------------
TRUNCATE TABLE trppu_tmh;
TRUNCATE TABLE trppu_neutralisations;
TRUNCATE TABLE trppu_scenario_variations_prev;
TRUNCATE TABLE trppu_scenario_comptages_manuels;
TRUNCATE TABLE trppu_scenario_exclusions;
TRUNCATE TABLE trppu_scenario_pic_coeffs;
TRUNCATE TABLE trppu_trafic_agrebal;
TRUNCATE TABLE trppu_trafic_pdi;
TRUNCATE TABLE trppu_api_log;
TRUNCATE TABLE trppu_recalcul_log;

-- --- Étape 2 : les autres tables sans FK entrante -----------
TRUNCATE TABLE trppu_site;
TRUNCATE TABLE trppu_pic_coefficients;
TRUNCATE TABLE trppu_pic_coefficients_ko;
TRUNCATE TABLE trppu_suivi_batch;
TRUNCATE TABLE trafic_staging;
TRUNCATE TABLE trppu_referentiel;     -- cf. §6, à confirmer
TRUNCATE TABLE demande_dsr;           -- cf. §6, à confirmer

-- --- Étape 3 : trppu_scenario, en dernier -------------------
-- TRUNCATE impossible (table parente de 9 FK) -> DELETE + reset du compteur.
DELETE FROM trppu_scenario;
ALTER TABLE trppu_scenario AUTO_INCREMENT = 1;
```

`TRUNCATE` remet l'AUTO_INCREMENT à 1 tout seul : seul `trppu_scenario` a besoin de l'`ALTER`.

> **Variante `SET FOREIGN_KEY_CHECKS = 0`** — elle permettrait de `TRUNCATE trppu_scenario`
> directement. Inutile ici : l'ordre ci-dessus fait le même travail sans désactiver les
> garde-fous. N'y recourir que si un blocage inattendu l'impose, et toujours en rétablissant
> `SET FOREIGN_KEY_CHECKS = 1;` avant de sortir de la session.

---

## 5. Après la purge — état de l'API

Bonne nouvelle : **aucun ré-amorçage n'est nécessaire**, parce que les deux référentiels
critiques sont dans les tables conservées.

| Point | État après purge |
|---|---|
| `resolve_default_pic_version` (`trppu_scenario/helpers.py:61`) | ✅ OK — `trppu_pic_version` conservée, la ligne `est_par_defaut = 1` survit. `POST /scenarios` fonctionne. |
| `ensure_produits_exist` (`trppu_produit/helpers.py:53`) | ✅ OK — les 8 produits et leurs libellés métier survivent. |
| `ensure_site_exists` (`trppu_scenario/helpers.py:316`) | ⚠️ `trppu_site` est vidée : les sites seront recréés à la volée au premier `POST /scenarios`, avec les valeurs du payload. Les 77 libellés actuels sont perdus. |
| Coefficients PIC | ⚠️ Les 6 versions PIC survivent **mais sans aucun coefficient** (`trppu_pic_coefficients` vidée). `GET /pic-versions` répond, `GET /pic-coefficients` renvoie une liste vide. À recharger par l'upload Excel si les tests en ont besoin. |

Si tu veux garder les 77 sites, il suffit de retirer `TRUNCATE TABLE trppu_site;` de
l'étape 2 : cette table n'est parente d'aucune FK, elle est totalement indépendante du reste
de la purge.

---

## 6. Deux tables à confirmer avant de lancer

- **`trppu_referentiel`** (0 ligne) est le pendant de `trppu_version_cle`, que tu as choisi
  de conserver : `trppu_scenario.id_referentiel` et `id_version_cle` pointent vers l'une et
  l'autre (écart n°9 du rapport `RAPPORT-ECARTS-db_new-2026-08-17.md`). Vider l'une en
  gardant l'autre est incohérent. Comme elle est vide, l'enjeu est nul aujourd'hui — mais
  la ranger côté « conservées » serait plus logique.
- **`demande_dsr`** (2 lignes) n'appartient pas au modèle TRPPU et n'est jamais lue par
  l'API. Sans doute une table de travail sans rapport avec la purge.

Les deux lignes correspondantes sont isolées dans le script (étape 2, commentaire
« à confirmer ») pour être commentées d'un trait si besoin.

---

## 7. Contrôles post-purge

```sql
-- Les 18 purgées à 0, les 7 conservées inchangées.
SELECT TABLE_NAME, TABLE_ROWS
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'dsr_mercure_aa'
ORDER BY TABLE_ROWS DESC, TABLE_NAME;
```

Attendu : seules `trppu_cles_repartition` (22,4 M), `trppu_agrebal_pdi` (9 505),
`trppu_produit` (8) et `trppu_pic_version` (6) restent non vides.

```sql
-- Intégrité des référentiels conservés : les 3 requêtes doivent renvoyer 0.
SELECT COUNT(*) FROM trppu_tmh t
  LEFT JOIN trppu_produit p USING (co_produit) WHERE p.co_produit IS NULL;
SELECT COUNT(*) FROM trppu_pic_coefficients c
  LEFT JOIN trppu_pic_version v USING (id_pic_version) WHERE v.id_pic_version IS NULL;
SELECT COUNT(*) FROM trppu_scenario_variations_prev v
  LEFT JOIN trppu_produit p USING (co_produit) WHERE p.co_produit IS NULL;
```

---

## 8. Rejouer les tests

**Point important : `python -m pytest tests/` ne valide pas la purge.** Les 130 tests de la
suite n'ouvrent aucune connexion MySQL — ils s'appuient sur des doublures (`FakeDb`) et une
base SQLite en mémoire. Ils passeront à l'identique, base pleine ou base vide.

La validation réelle est fonctionnelle :

```bash
# 1. Suite unitaire — doit rester à 130/130, indépendamment de la base
cd python && python -m pytest tests/ -q

# 2. API démarrée sur la base purgée
python -m uvicorn app.main:app --reload

# 3. Fumée : les référentiels conservés répondent, les tables vidées aussi
curl http://localhost:8000/trppu-api/produits        # 8 produits (conservés)
curl http://localhost:8000/trppu-api/pic-versions    # 6 versions (conservées)
curl http://localhost:8000/trppu-api/scenarios       # liste vide, 200 (et non 500)
curl http://localhost:8000/trppu-api/sites           # liste vide, 200
```

Puis la collection Postman (`postman/trppu_collection.json`, 79 requêtes, régénérée le
19/08) pour un passage complet. **Commencer par `POST /trppu-api/scenarios`** : c'est
l'appel qui exerce `resolve_default_pic_version`, `ensure_produits_exist` et
`ensure_site_exists` d'un coup — donc toute la frontière entre conservé et purgé.

---

## 9. Retour arrière

```bash
mysql -h "$SGBD_SERVER_WRITE" -u "$SGBD_APP_USER_WRITE" -p \
  dsr_mercure_aa < backup_avant_purge.sql
```

`TRUNCATE` n'étant pas annulable, c'est le seul retour arrière possible — d'où le §3.
