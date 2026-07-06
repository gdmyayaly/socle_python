# Résolution — DSR-679 (Trafics Databricks : nouvelle structure gold + date pivot)

## 1. Statut
**Livré, sous réserve de confirmations données.** Nouvel endpoint YS04 qui adapte la
récupération pivot (DSR-666) à la **nouvelle structure des tables gold TRPPU** : les tables
trafics ne portent plus le référentiel ni `lb_type_objet` ; l'objet est un **code comptage
CTR** joint à la dimension `g_trppu_obj_mapping`, et les requêtes exploitent les **colonnes de
partition** pour l'optimisation. L'endpoint DSR-666 (`get_trafics_pivot`) est **conservé
inchangé** ; DSR-679 est ajouté en endpoint séparé.

## 2. Fichiers créés / modifiés
- **Créés — package autonome `app/routes/trppu_trafics/`** (destiné à remplacer l'ancien
  `trafics.py` une fois validé) :
  - `helpers.py` — validation, découpage pivot, `build_query` (agrégation par objet +
    partitions), `accumulate_trafics` (dynamique).
  - `routes.py` — endpoint production `GET /trppu-api/trafics/get_trafics_pivot`.
  - `debug.py` — routes de debug/test `GET /trppu-api/trafics/test/*` (schema, schema_raw,
    objets, echantillons, pivot_dry_run).
  - `schemas.py`, `__init__.py`.
  - `api_docs/dsr/news/DSR-679.md`, `api_docs/dsr/resolutions/DSR-679_resolution.md`.
- **Modifiés**
  - `app/routes/trafics.py` — réduit à `GET /get_trafics` (DSR-613) ; le pivot a migré dans
    le package. L'ancien `get_trafics_pivot` (DSR-666) est retiré (remplacé, même URL).
  - `app/main.py` — enregistre `trppu_trafics.router` (à la place de `trafics_test`).
- **Ancien conservé mais superséded** : `app/routes/trafics_helpers.py` (bloc DSR-679 mort,
  non importé) — le package est autonome et n'en dépend pas.

## 3. Endpoint livré

`GET /trppu-api/trafics/get_trafics_pivot`

### Entrées (query, format AAAAMMJJ ou AAAA-MM-JJ)
| Paramètre | Obligatoire | Description |
| --------- | ----------- | ----------- |
| `co_regate` | Oui | code régate du site |
| `date_debut` | Oui | début de période |
| `date_fin` | Oui | fin de période |
| `date_pivot` | Oui | date pivot (jour de l'appel ou date de mise en œuvre) |
| `is_day` | Non | force la table jour (court-circuite le découpage auto) |

### Règle pivot (identique DSR-666)
- dates **< pivot** → trafic **réel** (`trafic_constate`), prévisionnel = 0
- dates **>= pivot** → trafic **prévisionnel** (`trafic_prevu`), réel = 0
- découpe au jour près **avant** requête → aucune granularité mois/semaine à cheval.
- période passée → que du réel ; future → que du prévisionnel ; mixte → les deux.

### Nouvelle structure exploitée (confirmée par `SELECT *` réel)
Tables réelles : `g_trppu_trafics_{jour,semaine,mois}_3`. Elles portent **directement** l'objet
TRPPU dans `co_type_objet` (OO/OS/PR/PP/CO/IP) + `trafic_constate` / `trafic_prevu` + `co_regate`
+ `co_annee_comptage` / `co_mois_comptage` / `co_semaine_comptage`.
- **Aucune jointure, aucun mapping en dur** : l'objet est déjà dans la table → agrégation
  directe `SUM(<valeur>) GROUP BY co_type_objet`. Le service **restitue tel quel** le résultat
  du SQL (liste d'objets **dynamique**, non figée).
- **Prédicats de partition** ajoutés au `WHERE` pour le pruning :
  - jour : `co_annee_comptage IN (...)` **et** `co_mois_comptage IN (...)`
  - semaine / mois : `co_annee_comptage IN (...)`
- Les codes année/mois/semaine sont **recalculés à partir des dates** (`fmt_date`) ; la table
  `s_commun_calendrier_jour` reste une alternative disponible mais non requise.

### Sortie — `200 OK`
Une ligne par objet **présent dans le résultat** (restitution dynamique) avec la somme sur la
période et le site :
```json
{ "co_produit": "OO", "trafic_brut": 3500, "trafic_previsionnel": 2435 }
```
Plus `date_debut/fin/pivot`, `count`, `nb_jours` (DSR-613), et `queries` si `DEBUG_SHOW_QUERY`.

### Erreurs (`400`, message rappelant les paramètres)
Paramètre manquant (dont `date_pivot`), `date_debut > date_fin`, ou période > 2 ans.

## 4. Colonnes (confirmées) — surchargeables par env
Noms réels confirmés via `SELECT *` (bloc DSR-679 de `trafics_helpers.py`, surchargeables) :

| Rôle | Colonne réelle | Env de surcharge |
| ---- | -------------- | ---------------- |
| objet TRPPU (agrégation) | `co_type_objet` | `TRAFIC679_COL_OBJET` |
| valeur constatée | `trafic_constate` | `TRAFIC679_COL_CONSTATE` |
| valeur prévisionnelle | `trafic_prevu` | `TRAFIC679_COL_PREVISIONNEL` |
| partition année | `co_annee_comptage` | `TRAFIC679_COL_ANNEE` |
| partition mois (jour) | `co_mois_comptage` | `TRAFIC679_COL_MOIS` |
| tables trafics | `g_trppu_trafics_{jour,semaine,mois}_3` | `TABLES_PERIODE` (helper) |

Points restant à valider :
1. **Sémantique réel/prév par ligne vs pré-découpage Databricks** : le service suppose que
   `trafic_constate` ET `trafic_prevu` sont exploitables par ligne quelle que soit la date
   (le pivot choisit la bonne valeur). Si Databricks pré-découpe déjà, le cas « période passée
   + pivot = mise en œuvre » peut donner une zone prévisionnelle à 0. **À valider équipe data.**
2. **Codes objets renvoyés** : `co_type_objet` = codes 2 car. (OO/OS/PR/PP/CO/IP). Résolution
   vers `co_produit` (`char(2)` de `trppu_produit`) à confirmer côté consommateur.
3. **Objets absents** : restitution dynamique → un objet sans trafic sur la période n'apparaît
   pas (pas d'hydratation à 0). Si l'IHM exige les 6 objets systématiquement, prévoir une
   hydratation côté consommateur ou réactiver une liste fixe.

## 5. Comment tester

### Sans Databricks — route de test (banc d'essai)
```
GET /trppu-api/trafics/test/echantillons        -> échantillons des tables `_3` (jour/semaine/mois)
GET /trppu-api/trafics/test/pivot_dry_run?co_regate=400300&date_debut=20250301&date_fin=20260331&date_pivot=20251001   -> réel + prév
GET /trppu-api/trafics/test/pivot_dry_run?co_regate=400300&date_debut=20261001&date_fin=20270331&date_pivot=20261001   -> que du prévisionnel
GET /trppu-api/trafics/test/pivot_dry_run?co_regate=400300   -> 400 (params manquants)
```

### Debug Databricks — identification des colonnes
```
GET /trppu-api/trafics/test/schema?limit=1&co_regate=400300   -> colonnes réelles des 6 tables
GET /trppu-api/trafics/test/objets?co_regate=400300&table=mois -> distinct co_type_objet du site
GET /trppu-api/trafics/test/schema_raw?table=g_trppu_obj_mapping&limit=20 -> SELECT * libre
```
Le dry-run rejoue **exactement** la règle pivot (constaté avant, prévu à partir du pivot) et
l'agrégation par objet en mémoire, sur des cadences journalières codées en dur (sommes
vérifiables à la main). La fusion N→1 (OOM + OOC → OO) et l'objet sans trafic (IP → 0) sont
couverts.

### Avec Databricks réel
Activer `DEBUG_SHOW_QUERY=true`, appeler `get_trafics_pivot_dsr679` sur 400300, vérifier dans
`queries` la jointure `g_trppu_obj_mapping`, le `GROUP BY` objet et les prédicats de partition,
puis comparer la somme par objet à une requête manuelle en base (3 cas passé/mixte/futur).

## 6. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| Manque période / régate / une date / date_pivot → 400 rappelant les params | `validate_params_pivot` |
| Période > 2 ans → 400 | contrôle `MAX_DATE_RANGE_DAYS` (730 j) |
| Période passée → réel + prév | `split_by_pivot` + `build_query_679` |
| Période mixte → réel + prév | idem |
| Période future → que du prévisionnel | `split_by_pivot` (reel=None) |
| 1 ligne par objet = somme des trafics | `GROUP BY` SQL + hydratation 6 objets |
| Vérif base trafics & objets | `queries` tracées (DEBUG_SHOW_QUERY) + route de test |
