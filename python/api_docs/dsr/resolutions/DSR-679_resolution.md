# Résolution — DSR-679 (Trafics Databricks : nouvelle structure gold + date pivot)

## 1. Statut
**Livré, aligné sur les schémas réels relevés en base.** Nouvel endpoint YS04 qui adapte la
récupération pivot (DSR-666) à la **nouvelle structure des tables gold TRPPU**. Les `DESCRIBE`
ont invalidé une partie des hypothèses tirées du texte du ticket : les tables trafics portent
déjà `co_type_objet` (pas de code comptage CTR), et un **filtre sur le niveau de regroupement**
est obligatoire. La dimension `g_trppu_obj_mapping` est **jointe** à la requête agrégée — elle
porte le regroupement restitué et le libellé — mais **pré-regroupée**, sans quoi ses multiples
lignes par objet dupliqueraient les lignes de trafic. Les requêtes exploitent les **colonnes de
partition** pour l'optimisation.

### Écarts relevés entre le texte du ticket et la base
| Hypothèse (texte du ticket) | Réalité (`DESCRIBE`) | Conséquence |
| --- | --- | --- |
| trafics portent `co_comptage` à joindre à `g_trppu_obj_mapping` | `DESCRIBE g_trppu_trafics_jour_3` : **aucune colonne de code comptage** — les 10 colonnes sont `co_niveau_regroupement_operationnel`, `co_regate`, `co_type_regate`, `co_type_objet`, `da_comptage`, `co_annee_comptage`, `co_mois_comptage`, `co_semaine_comptage`, `trafic_constate`, `trafic_prevu` | le mapping CTR→objet est déjà appliqué en amont : la jointure se fait sur `co_type_objet` (clé surchargeable par `TRAFIC679_MAPPING_COL_CLE`), pas sur un code comptage |
| une ligne = un site | `co_niveau_regroupement_operationnel` : SITE / ETABLISSEMENT / DEXC / DEPARTEMENT / PIC / PFC / NATIONAL / SIEGE | filtre **obligatoire**, sinon `SUM` gonflés |
| `g_trppu_entite` a un `co_regate` | une seule colonne STRUCT `s_mdp_entite` (34 sous-champs) | dimension **non jointe** (et inutile à l'agrégat) |
| `s_commun_calendrier_jour` : une ligne par jour | partitionnée `(co_annee, no_mois, zone)` | **non jointe** : produit cartésien garanti |

## 2. Fichiers créés / modifiés
- **Créés — package autonome `app/routes/trppu_trafics/`** (destiné à remplacer l'ancien
  `trafics.py` une fois validé) :
  - `helpers.py` — validation, découpage pivot, `build_mapping_subquery` (mapping pré-regroupé),
    `build_query` (agrégation par objet du mapping + filtre de niveau + partitions),
    `accumulate_trafics` (dynamique, porte le libellé).
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
  > Obsolète depuis l'uniformisation : `trafics.py`, `trafics_helpers.py` et `schemas.py` ont
  > été supprimés, cf. § 7 en fin de document.

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

### Structure exploitée — trafics joints à la dimension objets
Tables trafics : `g_trppu_trafics_{jour,semaine,mois}_3` (schéma gold), jointes à
`g_trppu_obj_mapping`. Les tables trafics portent `co_regate`, `co_type_objet`,
`co_niveau_regroupement_operationnel`, la date de la maille, les partitions et
`trafic_constate` / `trafic_prevu` ; le mapping porte le **regroupement restitué** et le
**libellé** de l'objet.

SQL générée (zone réelle, maille mois) :
```sql
SELECT COALESCE(m.co_objet_trppu, t.co_type_objet) AS co_objet_trppu,
       m.lb_objet_trppu                            AS lb_objet_trppu,
       SUM(t.trafic_constate)                      AS somme
FROM ppd_dd_kairos_int.03_gold.g_trppu_trafics_mois_3 t
LEFT JOIN (
    SELECT co_type_objet      AS cle_jointure,
           MAX(co_type_objet) AS co_objet_trppu,
           MAX(lb_type_objet) AS lb_objet_trppu
    FROM ppd_dd_kairos_int.03_gold.g_trppu_obj_mapping
    GROUP BY co_type_objet
) m ON m.cle_jointure = t.co_type_objet
WHERE t.co_regate = '400300'
  AND t.co_niveau_regroupement_operationnel = 'SITE'
  AND (t.co_mois_comptage BETWEEN '2025-03' AND '2025-09')
  AND t.co_annee_comptage IN (2025)
GROUP BY 1, 2
```

- **Mapping pré-regroupé** : il porte une ligne par code comptage CTR, donc plusieurs lignes par
  objet. Joint tel quel, il dupliquerait les lignes de trafic et gonflerait les `SUM`
  silencieusement. Le `GROUP BY` de la sous-requête garantit une ligne par code (`MAX` départage
  un éventuel double libellé) — c'est la condition de justesse de la jointure.
- **`LEFT JOIN` + `COALESCE`** : les objets présents dans les trafics mais absents du mapping
  (`PR`, `PPI` — cf. §5 du contrôle données gold) restent restitués, avec `lb_produit` à `null`
  et listés dans `objets_sans_libelle`. Un `INNER JOIN` les ferait disparaître, un `COALESCE`
  manquant les fondrait dans un unique groupe `NULL`.
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
{ "co_produit": "OO", "lb_produit": "OBJETS ORDINAIRES", "trafic_brut": 3500, "trafic_previsionnel": 2435 }
```
Plus `date_debut/fin/pivot`, `count`, `objets_sans_libelle` (objets absents du mapping),
`nb_jours` (DSR-613), et `queries` si `DEBUG_SHOW_QUERY`.

### Erreurs (`400`, message rappelant les paramètres)
Paramètre manquant (dont `date_pivot`), `date_debut > date_fin`, ou période > 2 ans.

## 4. Tables & colonnes — toutes surchargeables par env
| Rôle | Valeur par défaut | Env de surcharge |
| ---- | ----------------- | ---------------- |
| tables trafics | `g_trppu_trafics_{jour,semaine,mois}_3` | `TRAFIC679_TABLE_{JOUR,SEMAINE,MOIS}` |
| code régate | `co_regate` | `TRAFIC679_COL_REGATE` |
| objet TRPPU (agrégation) | `co_type_objet` | `TRAFIC679_COL_OBJET` |
| niveau de regroupement | `co_niveau_regroupement_operationnel` | `TRAFIC679_COL_NIVEAU` |
| valeur du niveau filtré | `SITE` | `TRAFIC679_NIVEAU_REGROUPEMENT` |
| valeur constatée | `trafic_constate` | `TRAFIC679_COL_CONSTATE` |
| valeur prévisionnelle | `trafic_prevu` | `TRAFIC679_COL_PREVISIONNEL` |
| partition année | `co_annee_comptage` (smallint) | `TRAFIC679_COL_ANNEE` |
| partition mois (jour) | `co_mois_comptage` (string `AAAA-MM`) | `TRAFIC679_COL_MOIS` |
| dimension objets | `g_trppu_obj_mapping` | `TRAFIC679_OBJ_MAPPING_TABLE` |
| clé de jointure du mapping | `co_type_objet` | `TRAFIC679_MAPPING_COL_CLE` |
| objet du mapping (regroupement) | `co_type_objet` | `TRAFIC679_MAPPING_COL_OBJET` |
| libellé du mapping | `lb_type_objet` | `TRAFIC679_MAPPING_COL_LIBELLE` |

Colonne de date par maille : `da_comptage` (jour), `co_semaine_comptage` `AAAA-NS` (semaine),
`co_mois_comptage` `AAAA-MM` (mois) — formats zéro-paddés, donc le `BETWEEN` sur chaîne est
lexicographiquement correct, y compris à cheval sur un changement d'année.

### Points tranchés en base
1. **Colonnes — confirmées sur les mailles mois et jour** (`DESCRIBE` + `/test/jour_check` sur
   400300) : `co_regate`, `co_type_objet`, `co_niveau_regroupement_operationnel`,
   `da_comptage` (jour) / `co_mois_comptage` (string `AAAA-MM`), `co_annee_comptage`
   (smallint, colonne de partition), `trafic_constate` / `trafic_prevu` (bigint).
2 et 3. **Cardinalité du mapping — neutralisée par le pré-regroupement** : `g_trppu_obj_mapping`
   compte plusieurs lignes par objet (une par code comptage CTR). La jointure porte donc sur une
   sous-requête `GROUP BY co_type_objet` : une ligne par code, aucune duplication des lignes de
   trafic, `SUM` inchangées. La couverture reste partielle (point 6).
4. **Pas de pré-découpage Databricks** — `trafic_constate` **et** `trafic_prevu` sont
   renseignées quelle que soit la date. Sur 400300 / 01-03-2025 → 31-03-2026 (pivot
   01-10-2025), la sonde donne pour `OO` un constaté total de 1 232 587 alors que la zone
   réelle (avant pivot) n'en somme que 733 179 : du constaté existe donc après le pivot, et
   symétriquement du prévisionnel avant. **C'est bien le pivot qui choisit la colonne** —
   comportement attendu par le ticket. (Les `trafic_prevu` à `null` observés sur la maille
   mois sont des trous de données, pas un découpage systématique.)
5. **Pas de cumul de niveaux sur le site test** : la sonde ne renvoie que `SITE` pour 400300,
   et exactement 396 lignes par objet sur 396 jours — une ligne par jour et par objet, aucune
   duplication. Le filtre reste nécessaire pour les régates têtes de PIC/établissement.
6. **Valeurs d'objets** : `OO / OS / PR / PPI / CO / IP` — exactement les 6 du ticket. `PR` et
   `PPI` ne figurent **pas** dans `g_trppu_obj_mapping`, qui déclare de son côté `PQ` (« PRESSE
   QUOTIDIENNE ») et `EQ` (« E-PAQ / PPI »). Deux référentiels distincts sans correspondance
   déclarée — question posée à l'équipe data (rapport métier §5).
7. **Types et partitions (maille jour) confirmés** : `da_comptage` est une **string**
   `AAAA-MM-JJ` (le `BETWEEN` sur chaîne est donc correct, aucun cast nécessaire) ; partitions
   `co_annee_comptage` **et** `co_mois_comptage`, soit exactement les deux prédicats générés.
   `trafic_constate` peut être `null` au même titre que `trafic_prevu` — `SUM()` les ignore.

### Cohérence des 3 tables — contrôle croisé (`GET /test/pivot_test?paire=toutes`)
> Rapport à destination du métier / de l'équipe data, avec les requêtes Databricks prêtes à
> exécuter : **`api_docs/dsr/DSR-679_controle_donnees_gold.md`**.

Chaque maille confrontée à la table jour sur sa plage effective, site 400300 :

| Paire | Constaté | Prévisionnel |
| ----- | -------- | ------------ |
| jour vs semaine (plage 03-03-2025 → 29-03-2026, pivot 29-09-2025, alignée lun→dim) | identique, **sauf PPI −48** | **identique** |
| jour vs mois (plage 01-03-2025 → 31-03-2026, pivot 01-10-2025, mois entiers) | identique, **sauf PPI −48** | **diverge sur les 6 objets** |

**La table semaine est un agrégat fidèle de la table jour** — tous les écarts nuls sur les deux
zones. Le routage vers la maille semaine est donc validé.

**Anomalie 1 — le prévisionnel mensuel n'est pas la somme des prévisionnels journaliers.**
Sur 10-2025 → 03-2026 (mois entiers, donc aucun effet de bord) :

| Objet | jour | mois | delta | % |
| ----- | ---: | ---: | ----: | -: |
| OO | 45 477 | 73 530 | **+28 053** | **+61,7 %** |
| PR | 88 251 | 83 937 | −4 314 | −4,9 % |
| IP | 136 968 | 135 008 | −1 960 | −1,4 % |
| CO | 67 937 | 67 278 | −659 | −1,0 % |
| OS | 24 350 | 24 219 | −131 | −0,5 % |
| PPI | 9 212 | 9 191 | −21 | −0,2 % |
| **Total** | **372 195** | **393 163** | **+20 968** | **+5,6 %** |

Les écarts vont dans les deux sens : ce n'est pas un décalage de bornes (la maille semaine, sur
exactement les mêmes jours, tombe juste). Hypothèse à confirmer avec l'équipe data : le
prévisionnel mensuel est calculé **au niveau mois** (`nb_objet_prevu_recadre_bu`, recadrage BU)
et non comme somme des prévisions journalières.

**Impact endpoint** : `decompose_auto` route les mois entiers vers `g_trppu_trafics_mois_3` ;
une période prévisionnelle longue renvoie donc le chiffre « mois », pas la somme des jours.
Décision à prendre avec le métier : quelle maille fait foi pour le prévisionnel.

**Anomalie 2 — PPI, −48 en constaté** dans la semaine **et** dans le mois (même valeur) :
la table jour porte 48 objets PPI que les deux agrégats ne reprennent pas. Faible volume mais
systématique.

### Points restant à valider en base
1. **Prévisionnel : quelle maille fait foi ?** (anomalie 1 ci-dessus) — question **bloquante
   pour la recette**, à poser à l'équipe data. Si la maille jour fait foi, il faudra router la
   zone prévisionnelle vers `g_trppu_trafics_jour_3` quelle que soit la durée.
2. **Référentiel des codes objets** : `PR`/`PPI` (trafics) contre `PQ`/`EQ` (mapping), sans
   correspondance déclarée. **Bloquant** : conditionne les codes restitués par l'API.
3. **PPI −48 en constaté** (anomalie 2) — localiser le jour concerné, puis remonter à l'équipe
   data. Requête de ciblage mois par mois fournie dans le rapport métier.
4. **Régate multi-niveaux** : rejouer `/test/jour_check` sur une régate tête de PIC ou
   d'établissement pour chiffrer le sur-comptage que le filtre `SITE` corrige.
4. **Objets absents** : restitution dynamique → un objet sans trafic sur la période n'apparaît
   pas (pas d'hydratation à 0) — choix confirmé avec le métier. Si l'IHM exige tous les objets
   systématiquement, l'hydratation est à faire côté consommateur.

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
`jour_check` enchaîne 3 requêtes sur `g_trppu_trafics_jour_3` : la sonde `GROUP BY niveau, objet`
**sans** filtre de niveau (si elle passe, les colonnes sont confirmées ; si elle renvoie
plusieurs niveaux, le filtre est indispensable), puis les 2 requêtes de production
(zone réelle / zone prévisionnelle). Les erreurs Databricks sont renvoyées telles quelles.

Le dry-run rejoue **exactement** la règle pivot (constaté avant, prévu à partir du pivot), le
filtre de niveau et l'agrégation par objet en mémoire, sur des cadences journalières codées en
dur (sommes vérifiables à la main : cadence × nb jours). L'objet sans trafic (IP) est couvert
— il n'apparaît pas dans le résultat, conformément à la restitution dynamique.

Résultat de référence (`/test/jour_check` sur 400300, 01-03-2025 → 31-03-2026, pivot
01-10-2025) — à comparer à la réponse de l'endpoint :

| Objet | trafic_brut (réel) | trafic_previsionnel |
| ----- | -----------------: | ------------------: |
| OO    | 733 179 | 45 477 |
| IP    | 161 369 | 136 968 |
| PR    | 106 190 | 88 251 |
| CO    | 60 769 | 67 937 |
| OS    | 27 966 | 24 350 |
| PPI   | 9 201 | 9 212 |

### Avec Databricks réel
Activer `DEBUG_SHOW_QUERY=true`, appeler `get_trafics_pivot` sur 400300, vérifier dans `queries`
la jointure `g_trppu_obj_mapping` (sous-requête pré-regroupée), le `GROUP BY` sur l'objet du
mapping et les prédicats de partition, puis comparer la somme par objet à une requête manuelle
en base (3 cas passé/mixte/futur).

**Contrôle de non-régression de la jointure** — la même somme doit sortir avec et sans mapping ;
tout écart signale une duplication des lignes de trafic :
```sql
SELECT COALESCE(j.objet, s.objet) AS objet, j.sans_jointure, s.avec_jointure,
       s.avec_jointure - j.sans_jointure AS ecart
FROM (
    SELECT t.co_type_objet AS objet, SUM(t.trafic_constate) AS sans_jointure
    FROM ppd_dd_kairos_int.03_gold.g_trppu_trafics_mois_3 t
    WHERE t.co_regate = '400300' AND t.co_niveau_regroupement_operationnel = 'SITE'
      AND t.co_mois_comptage BETWEEN '2025-03' AND '2025-09'
      AND t.co_annee_comptage IN (2025)
    GROUP BY 1
) j
FULL OUTER JOIN (
    SELECT COALESCE(m.co_objet_trppu, t.co_type_objet) AS objet,
           SUM(t.trafic_constate) AS avec_jointure
    FROM ppd_dd_kairos_int.03_gold.g_trppu_trafics_mois_3 t
    LEFT JOIN (
        SELECT co_type_objet AS cle_jointure, MAX(co_type_objet) AS co_objet_trppu
        FROM ppd_dd_kairos_int.03_gold.g_trppu_obj_mapping
        GROUP BY co_type_objet
    ) m ON m.cle_jointure = t.co_type_objet
    WHERE t.co_regate = '400300' AND t.co_niveau_regroupement_operationnel = 'SITE'
      AND t.co_mois_comptage BETWEEN '2025-03' AND '2025-09'
      AND t.co_annee_comptage IN (2025)
    GROUP BY 1
) s ON j.objet = s.objet
ORDER BY ABS(COALESCE(s.avec_jointure, 0) - COALESCE(j.sans_jointure, 0)) DESC
```

## 6. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| Manque période / régate / une date / date_pivot → 400 rappelant les params | `validate_params_pivot` |
| Période > 2 ans → 400 | contrôle `MAX_DATE_RANGE_DAYS` (730 j) |
| Période passée → réel + prév | `split_by_pivot` + `build_query_679` |
| Période mixte → réel + prév | idem |
| Période future → que du prévisionnel | `split_by_pivot` (reel=None) |
| 1 ligne par objet = somme des trafics | `GROUP BY` sur le `co_type_objet` du mapping (restitution dynamique) |
| Libellé de l'objet | jointure `g_trppu_obj_mapping` pré-regroupée ; `null` + `objets_sans_libelle` si absent |
| Vérif base trafics & objets | `queries` tracées (DEBUG_SHOW_QUERY) + `/test/jour_check` |

---

## 7. Uniformisation du domaine trafics (post-DSR-679)

Le domaine existait en deux exemplaires : l'ancien couple `app/routes/trafics.py` +
`app/routes/trafics_helpers.py` (DSR-613/666) et le package `app/routes/trppu_trafics/`
(DSR-679) qui l'avait supplanté sans que l'ancien soit retiré. Le package est désormais le
**seul** module trafics du projet.

### Supprimé
- `app/routes/trafics.py` et l'endpoint `GET /trppu-api/trafics/get_trafics` (lignes brutes).
  Aucun consommateur runtime : côté front, `TraficService.getTrafics()` n'était appelé que par
  `loadTrafics()`, marqué `DEPRECATED` et jamais invoqué.
  **Action front** : retirer `getTrafics()` et `loadTrafics()` de `trafics.service.ts` /
  `trppu-trafics-calculer.component.ts`.
- `app/routes/trafics_helpers.py` : dupliquait 12 symboles de `trppu_trafics/helpers.py`
  (`parse_date`, `render_sql`, `fmt_date`, `decompose_auto`, `validate_params_pivot`,
  `split_by_pivot`, `build_partition_conditions`, `TABLES_PERIODE`…) et portait ~250 lignes de
  code mort (bloc `*_679` jamais importé, mapping produit en dur `TRAFIC_PRODUIT_MAPPING`).
- `app/routes/trafics.txt` : ancienne version YS04, zéro référence dans le repo. C'était la
  source des sections obsolètes du `README.md` (un `POST` avec paramètre `periode`).
- `app/routes/trppu_trafics/schemas.py` : jamais importé, aucun `response_model` branché.
  Non réintroduit — un `response_model` **supprime silencieusement toute clé non déclarée**, ce
  qui masquerait le bloc `debug` ci-dessous. Le domaine est en lecture pure : 4 query params
  scalaires validés à la main, sortie dynamique.
- `app/main.py` : import et montage de `trafics_routes`.

### Ajouté — `app/routes/trppu_trafics/errors.py`
Point unique du format `{error, message, code}` (`erreur_400`, `erreur_500`) et du bloc debug.
Seul module du package à lire `DEBUG_SHOW_QUERY` : il ne dépend que de `fastapi` + `app.config`,
ce qui évite le cycle avec `helpers.py` (qui lève les 400). Remplace 7 blocs copiés-collés ;
les messages des 400 sont inchangés.

### Correction — la requête fautive était perdue
`routes.py` ajoutait la requête à la liste tracée **après** son exécution : en cas d'erreur
Databricks, le bloc debug ne montrait que les requêtes ayant réussi, jamais celle qui avait
planté. `helpers.executer_requete` produit désormais la trace (SQL rendu + statut) **au moment
de l'exécution**, donc il n'existe plus de chemin où une requête s'exécute sans être tracée.
Couvert par `tests/test_trafics_erreurs.py::test_requete_fautive_tracee_des_le_premier_appel`.

### Contrat du bloc debug (200 et 500, symétrique)
Uniquement si `DEBUG_SHOW_QUERY=true` :

```json
"debug": {
  "queries": [
    {"index": 0, "statut": "ok", "nb_lignes": 6, "sql": "SELECT ... GROUP BY 1, 2"},
    {"index": 1, "statut": "echec", "erreur": "[TABLE_OR_VIEW_NOT_FOUND] ...", "sql": "SELECT ..."}
  ],
  "requete_en_echec": {"index": 1, "statut": "echec", "erreur": "...", "sql": "SELECT ..."},
  "databricks_error": "[TABLE_OR_VIEW_NOT_FOUND] ..."
}
```

Le SQL est rendu (paramètres substitués, nombres non quotés) donc rejouable tel quel. Les lignes
de données ne figurent jamais dans le bloc, seulement leur volume (`nb_lignes`). Avec
`DEBUG_SHOW_QUERY=false`, un 500 se réduit à `{"error": true, "message": ..., "code": 500}`.
Remplace l'ancien `queries: ["sql1", "sql2"]` à plat.

### Mutualisation
`debug.py` ne porte plus ni `_executer` ni `_trafics` : les routes `/test/*` passent par les
mêmes `executer_requete` / `formater_trafics` / `zones_pivot` que la production. Les clés des
routes de debug suivent la trace commune (`statut`, `erreur` au lieu de `error`).

Bilan : ~1150 lignes supprimées, commentaires réduits aux pièges métier non déductibles du code
(pré-regroupement du mapping, filtre `co_niveau_regroupement_operationnel`, découpage au jour
près autour du pivot, pruning porté par l'alias `t`).
