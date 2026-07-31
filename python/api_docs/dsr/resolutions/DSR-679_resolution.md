# Résolution — DSR-679 (Trafics Databricks : nouvelle structure gold + date pivot)

## 1. Statut
**Livré, aligné sur les schémas réels relevés en base.** Nouvel endpoint YS04 qui adapte la
récupération pivot (DSR-666) à la **nouvelle structure des tables gold TRPPU**. Les `DESCRIBE`
ont invalidé les hypothèses tirées du texte du ticket : les tables trafics sont
**auto-suffisantes** (elles portent déjà `co_type_objet`, pas de code comptage CTR), aucune
dimension n'est jointe, et un **filtre sur le niveau de regroupement** est obligatoire. Les
requêtes exploitent les **colonnes de partition** pour l'optimisation.

### Écarts relevés entre le texte du ticket et la base
| Hypothèse (texte du ticket) | Réalité (`DESCRIBE`) | Conséquence |
| --- | --- | --- |
| trafics portent `co_comptage` à joindre à `g_trppu_obj_mapping` | colonne absente ; `co_type_objet` déjà présent | mapping **non joint** (déjà appliqué en amont) |
| une ligne = un site | `co_niveau_regroupement_operationnel` : SITE / ETABLISSEMENT / DEXC / DEPARTEMENT / PIC / PFC / NATIONAL / SIEGE | filtre **obligatoire**, sinon `SUM` gonflés |
| `g_trppu_entite` a un `co_regate` | une seule colonne STRUCT `s_mdp_entite` (34 sous-champs) | dimension **non jointe** (et inutile à l'agrégat) |
| `s_commun_calendrier_jour` : une ligne par jour | partitionnée `(co_annee, no_mois, zone)` | **non jointe** : produit cartésien garanti |

## 2. Fichiers créés / modifiés
- **Créés — package autonome `app/routes/trppu_trafics/`** (destiné à remplacer l'ancien
  `trafics.py` une fois validé) :
  - `helpers.py` — validation, découpage pivot, `build_query` (mono-table : agrégation par
    objet + filtre de niveau + partitions), `accumulate_trafics` (dynamique).
  - `routes.py` — endpoint production `GET /trppu-api/trafics/get_trafics_pivot`.
  - `debug.py` — routes de debug/test `GET /trppu-api/trafics/test/*` (jour_check, config,
    queries_preview, schema, schema_raw, objets, echantillons, pivot_dry_run).
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

### Structure exploitée — requête mono-table
Tables trafics : `g_trppu_trafics_{jour,semaine,mois}` (schéma gold), **sans aucune jointure**.
Elles portent déjà tout ce dont la requête a besoin : `co_regate`, `co_type_objet`,
`co_niveau_regroupement_operationnel`, la date de la maille, les partitions et
`trafic_constate` / `trafic_prevu`.

SQL générée (zone réelle, maille mois) :
```sql
SELECT t.co_type_objet AS co_objet_trppu, SUM(t.trafic_constate) AS somme
FROM ppd_dd_kairos_int.03_gold.g_trppu_trafics_mois t
WHERE t.co_regate = '400300'
  AND t.co_niveau_regroupement_operationnel = 'SITE'
  AND (t.co_mois_comptage BETWEEN '2025-03' AND '2025-09')
  AND t.co_annee_comptage IN (2025)
GROUP BY t.co_type_objet
```

- **Filtre de niveau de regroupement** (`= 'SITE'`) : la même régate apparaît à plusieurs
  niveaux (ETABLISSEMENT, PIC, NATIONAL…). Sans ce filtre, les `SUM` cumulent silencieusement
  plusieurs niveaux — c'est le principal risque de résultat faux sur cette structure.
- **Prédicats de partition** ajoutés au `WHERE` :
  - jour : `t.co_annee_comptage IN (...)` **et** `t.co_mois_comptage IN (...)`
  - semaine / mois : `t.co_annee_comptage IN (...)`
- Les codes année/mois/semaine sont **recalculés à partir des dates** (`fmt_date`) — le ticket
  autorise explicitement le recalcul comme alternative au calendrier ; le calendrier silver
  est de toute façon inutilisable ici (plusieurs lignes par jour, partition `zone`).
- Le service **restitue tel quel** le résultat du SQL (liste d'objets **dynamique**, non figée).
- Tables, colonnes et valeur du niveau restent surchargeables par env (`TRAFIC679_*`) : un
  renommage côté data se corrige sans relivraison.

### Sortie — `200 OK`
Une ligne par objet **présent dans le résultat** (restitution dynamique) avec la somme sur la
période et le site :
```json
{ "co_produit": "OO", "trafic_brut": 3500, "trafic_previsionnel": 2435 }
```
Plus `date_debut/fin/pivot`, `count`, `nb_jours` (DSR-613), et `queries` si `DEBUG_SHOW_QUERY`.

### Erreurs (`400`, message rappelant les paramètres)
Paramètre manquant (dont `date_pivot`), `date_debut > date_fin`, ou période > 2 ans.

## 4. Tables & colonnes — toutes surchargeables par env
| Rôle | Valeur par défaut | Env de surcharge |
| ---- | ----------------- | ---------------- |
| tables trafics | `g_trppu_trafics_{jour,semaine,mois}` | `TRAFIC679_TABLE_{JOUR,SEMAINE,MOIS}` |
| code régate | `co_regate` | `TRAFIC679_COL_REGATE` |
| objet TRPPU (agrégation) | `co_type_objet` | `TRAFIC679_COL_OBJET` |
| niveau de regroupement | `co_niveau_regroupement_operationnel` | `TRAFIC679_COL_NIVEAU` |
| valeur du niveau filtré | `SITE` | `TRAFIC679_NIVEAU_REGROUPEMENT` |
| valeur constatée | `trafic_constate` | `TRAFIC679_COL_CONSTATE` |
| valeur prévisionnelle | `trafic_prevu` | `TRAFIC679_COL_PREVISIONNEL` |
| partition année | `co_annee_comptage` (smallint) | `TRAFIC679_COL_ANNEE` |
| partition mois (jour) | `co_mois_comptage` (string `AAAA-MM`) | `TRAFIC679_COL_MOIS` |

Colonne de date par maille : `da_comptage` (jour), `co_semaine_comptage` `AAAA-NS` (semaine),
`co_mois_comptage` `AAAA-MM` (mois) — formats zéro-paddés, donc le `BETWEEN` sur chaîne est
lexicographiquement correct, y compris à cheval sur un changement d'année.

### Points tranchés par les schémas relevés
1. **Colonnes (maille mois) — confirmées** : `co_regate`, `co_type_objet`, `co_mois_comptage`
   (string), `co_annee_comptage` (smallint, seule colonne de partition), `trafic_constate` /
   `trafic_prevu` (bigint), plus `co_niveau_regroupement_operationnel` et `co_type_regate`.
2 et 3. **Cardinalité et couverture du mapping — sans objet** : il n'y a plus de jointure.
4. **Pré-découpage Databricks — confirmé** : `trafic_prevu` est `null` sur le passé
   (2025-01→03) et renseigné sur 2025-10. La règle pivot reste correcte : `SUM()` ignore les
   `NULL` et `accumulate_trafics` traite `None` comme 0.

### Points restant à valider en base
1. **Mailles jour et semaine** : seule la maille mois a été relevée. `GET /test/jour_check`
   valide la maille jour en un appel (colonnes, niveaux, sommes des 2 zones) ;
   `GET /test/schema` couvre les 3 tables.
2. **Ampleur du cumul de niveaux** : le témoin avec/sans filtre (`/test/queries_preview` →
   `requetes_de_controle`) chiffre le sur-comptage que le filtre `SITE` corrige.
3. **Objets absents** : restitution dynamique → un objet sans trafic sur la période n'apparaît
   pas (pas d'hydratation à 0) — choix confirmé avec le métier. Si l'IHM exige tous les objets
   systématiquement, l'hydratation est à faire côté consommateur.
4. **Valeurs d'objets** : les tables trafics exposent `OO / OS / IP / CO / PQ / EQ / PPI` ;
   noter que `PPI` apparaît côté trafics alors que `g_trppu_obj_mapping` porte `EQ`
   (« E-PAQ / PPI ») — les deux référentiels ne coïncident pas exactement, raison de plus pour
   ne pas joindre le mapping.

## 5. Comment tester

### SQL à rejouer en base (aucune exécution Databricks côté API)
```
GET /trppu-api/trafics/test/config           -> tables, colonnes et jointures actives
GET /trppu-api/trafics/test/queries_preview  -> SQL des 3 cas du ticket + requêtes de contrôle
GET /trppu-api/trafics/test/queries_preview?co_regate=400300&date_debut=20260210&date_fin=20260715&date_pivot=20260404
                                             -> cas libre (touche les 3 mailles jour/semaine/mois)
```
Chaque SQL est rendue **paramètres substitués** (nombres non quotés) : copier/coller direct
dans Databricks, puis comparer aux sommes renvoyées par l'endpoint.

### Sans Databricks — route de test (banc d'essai)
```
GET /trppu-api/trafics/test/echantillons        -> échantillons trafics + dimensions
GET /trppu-api/trafics/test/pivot_dry_run?co_regate=400300&date_debut=20250301&date_fin=20260331&date_pivot=20251001   -> réel + prév
GET /trppu-api/trafics/test/pivot_dry_run?co_regate=400300&date_debut=20261001&date_fin=20270331&date_pivot=20261001   -> que du prévisionnel
GET /trppu-api/trafics/test/pivot_dry_run?co_regate=400300   -> 400 (params manquants)
```

### Debug Databricks — identification des colonnes
```
GET /trppu-api/trafics/test/jour_check?co_regate=400300   -> sonde maille jour (exécutée)
GET /trppu-api/trafics/test/jour_check?execute=false      -> même chose, SQL seul
GET /trppu-api/trafics/test/schema?limit=1&co_regate=400300   -> colonnes réelles des 3 tables
GET /trppu-api/trafics/test/objets?co_regate=400300&table=mois -> objets et niveaux du site
GET /trppu-api/trafics/test/schema_raw?table=g_trppu_obj_mapping&limit=20 -> SELECT * libre
```
`jour_check` enchaîne 3 requêtes sur `g_trppu_trafics_jour` : la sonde `GROUP BY niveau, objet`
**sans** filtre de niveau (si elle passe, les colonnes sont confirmées ; si elle renvoie
plusieurs niveaux, le filtre est indispensable), puis les 2 requêtes de production
(zone réelle / zone prévisionnelle). Les erreurs Databricks sont renvoyées telles quelles.

Le dry-run rejoue **exactement** la règle pivot (constaté avant, prévu à partir du pivot), le
filtre de niveau et l'agrégation par objet en mémoire, sur des cadences journalières codées en
dur (sommes vérifiables à la main : cadence × nb jours). L'objet sans trafic (IP) est couvert
— il n'apparaît pas dans le résultat, conformément à la restitution dynamique.

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
| 1 ligne par objet = somme des trafics | `GROUP BY co_type_objet` (restitution dynamique) |
| Vérif base trafics & objets | `queries` tracées (DEBUG_SHOW_QUERY) + `/test/jour_check` |
