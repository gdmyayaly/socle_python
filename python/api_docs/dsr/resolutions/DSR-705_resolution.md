# Résolution — DSR-705 (Exposer les trafics Agrébal d'un scénario à OPTIPACC)

## 1. Statut
**Terminé.** Nouvel endpoint dans le package `trppu_optipacc`. Lecture seule, sans état,
paginé. Première route du projet à lire `trppu_trafic_agrebal` et `trppu_agrebal_pdi`.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_optipacc/routes.py` — endpoint `trafic_amas` + `_regrouper_amas`.
- `app/routes/trppu_optipacc/schemas.py` — `TraficAmasRequest`, `ProduitVolumes`,
  `AmasOut`, `PaginationOut`, `TraficAmasResponse`.
- `app/routes/trppu_optipacc/helpers.py` — `SELECT_SCENARIO_VISIBLE_SQL`, `COUNT_AMAS_SQL`,
  `SELECT_AMAS_PAGE_SQL`, `select_amas_existants_sql`, `select_trafics_amas_sql`,
  `assert_visible_optipacc`.
- `app/config.py` + `.env.example` — `NB_AMAS_PAR_PAGE` (défaut `200`).
- `tests/test_optipacc_amas.py` — 45 tests.
- `api_docs/api_trppu_optipacc.md` — §9.

## 3. Endpoint livré
`POST /trppu-api/optipacc/trafic-amas`

```json
{ "code_regate": "123456", "scenario_id": 125,
  "amas": ["0bb6f27c-..."], "page": 1 }
```
Réponse `200` : `site`, `scenario`, `pagination`, `amas[]`, `amas_non_trouves[]`
(structure détaillée dans `api_docs/api_trppu_optipacc.md` §9.2).

Codes : `200` · `400` (C2) · `404` (C1, ou aucun amas valide en C4) · `409` (C3) ·
`422` corps invalide · `500` technique.

## 4. Migrations / dépendances
Aucune migration. **Dépendance fonctionnelle bloquante** : `trppu_trafic_agrebal` est
alimentée exclusivement par le batch YB05 (`app/traitements/trafic_agrebal.py`), qui pose
aussi `trafic_agrebal_calcule = 1`. Tant qu'il n'a pas tourné sur un scénario, C3 refuse
l'appel en `409`.

Nouvelle variable d'environnement : `NB_AMAS_PAR_PAGE` (facultative, défaut `200`).

## 5. Le point structurant : la forme des données en base

Le JSON attendu par le ticket ne correspond pas à la forme stockée. Deux écarts ont dicté
l'implémentation.

**(a) `nom_amas` n'existe pas dans `trppu_trafic_agrebal`.** Le libellé vient de
`trppu_agrebal_pdi.agrebal_nom`. La jointure passe par `(agrebal_id, agrebal_code_regate)`,
qui est la clé unique `uq_agrpdi_courant` — et **non** par `agrebal_uuid`, non indexé dans
cette table (un join dessus serait un balayage complet). `LEFT JOIN` volontaire :
`agrebal_nom` est nullable et la ligne référentiel peut avoir disparu ; un amas sans
libellé conserve alors ses trafics avec `"nom_amas": null`.

**(b) La densité est un discriminant de ligne, pas trois colonnes.** En base :
`couleur_pic enum('DENSE','FAIBLE1','FAIBLE2')` + une colonne `volume`. Le JSON veut
`fort` / `faible1` / `faible2` sur la même ligne produit. La requête pivote donc :

```sql
SUM(CASE WHEN t.couleur_pic = 'DENSE'   THEN t.volume END) AS fort,
SUM(CASE WHEN t.couleur_pic = 'FAIBLE1' THEN t.volume END) AS faible1,
SUM(CASE WHEN t.couleur_pic = 'FAIBLE2' THEN t.volume END) AS faible2
```

Le `GROUP BY (agrebal_uuid, jour_semaine, co_produit)` n'agrège **rien de métier** : il
replie les trois lignes de densité, plus d'éventuels doublons techniques (la table n'a
aucune clé unique). RG-API-006 est respectée — aucune clé de répartition, aucun coefficient
PIC n'est appliqué à la restitution. Un test le vérifie en interdisant `coef`,
`trppu_scenario_pic_coeffs` et `cle_repartition` dans la requête.

## 6. Hypothèses & écarts

- **Chemin `/trafic-amas`** et non `/trafic_amas` : kebab-case comme le reste de l'API et
  comme les deux routes OPTIPACC déjà livrées. Simple renommage de segment côté OPTIPACC.

- **Corps d'erreur `{"detail": ...}`** et non `{"erreur": ...}` — cf. DSR-707, même
  raison. Le message de C3 est repris littéralement : `"Scenario non disponible"`.

- **C3 accepte `EN PRODUCTION` en plus de `VALIDE`.** Écart assumé et nécessaire : DSR-707
  fait justement passer `EN PRODUCTION` le scénario qu'OPTIPACC vient de retenir. Une
  lecture stricte du ticket rendrait ses propres trafics illisibles juste après la mise en
  production. Cohérent avec `STATUTS_EXPLOITABLES`, déjà en place pour DSR-689.

- **Garde dédiée `assert_visible_optipacc`**, distincte de `assert_exploitable`.
  DSR-705 C3 contrôle 5 conditions (statut, `est_fige = 1`, `calcul_trafic_en_cours = 0`,
  `trafic_pdi_calcule = 1`, `trafic_agrebal_calcule = 1`) là où DSR-689 n'en contrôle que
  2. Durcir `assert_exploitable` aurait changé le contrat d'un service déjà livré : les deux
  gardes coexistent, chacune adossée à son ticket.

- **`page` déclaré dans le corps** bien qu'absent de la section « Paramètres » du ticket :
  il est utilisé par les critères d'acceptation 6 et 7. Défaut `1`.

- **UUID inconnus → champ `amas_non_trouves`.** Le ticket demande « on trace dans le retour
  json et dans les logs », sans nommer le champ (son exemple de réponse est du JSON
  invalide). Les deux traces sont faites : le champ, et un `logger.warning`.

- **Volumes restitués en `int`.** La colonne est `decimal(12,4)` et `SUM()` renvoie un
  `Decimal`, mais les valeurs sont entières par construction : YB05 somme les colonnes
  `smallint unsigned` de `trppu_trafic_pdi`. Cohérent avec `volumeBrut` de DSR-689. Une
  densité absente pour un couple (jour, produit) vaut `0`.

- **Liste `amas` bornée à 1000 éléments**, pour borner la clause `IN (...)` générée. Une
  liste vide (`"amas": []`) est traitée comme une demande explicite de rien → `404`, et
  aucune requête ne part en base (`IN ()` est une erreur de syntaxe MySQL).

## 7. Pagination (RG-API-007 / RG-API-008)

Première route du projet à exposer une **enveloppe** de pagination : le reste de l'API
utilise `limit` / `offset` sans métadonnées. Le contrat du ticket prime ici.

Deux requêtes en mode « tous les amas » : un `COUNT(DISTINCT agrebal_uuid)` pour
`nb_amas_total`, puis la page d'UUID. Le tri sur `agrebal_uuid` précède le `LIMIT`
(RG-API-008) : un même Agrébal ne change jamais de page entre deux appels. Un test vérifie
que `ORDER BY` apparaît bien avant `LIMIT` dans la constante SQL.

Comportements à connaître :

| Situation | Résultat |
|-----------|----------|
| Filtre `amas` fourni | pas de pagination (Cas 8). Ni `COUNT`, ni `LIMIT`. Le bloc `pagination` reste présent et décrit l'unique page (`page: 1`, `nb_pages: 1`, `page_suivante: null`) |
| Page au-delà de la dernière | `200` avec `"amas": []`, pas une erreur |
| Scénario sans aucun amas | `200`, `nb_amas_total: 0`, `nb_pages: 1` |

Le bloc `pagination` est **toujours** présent, y compris en mode filtre : un champ
conditionnel obligerait OPTIPACC à gérer deux formes de réponse.

## 8. Ordre de restitution

`ORDER BY agrebal_uuid, FIELD(jour_semaine, 'LUNDI', …, 'SAMEDI'), co_produit` — les jours
suivent la semaine, pas l'alphabet. `FIELD()` étant propre à MySQL, il est isolé dans la
constante `ORDER_BY_TRAFICS_AMAS` pour que les tests puissent rejouer le SELECT sur SQLite.
Le regroupement Python préserve cet ordre (les `dict` conservent l'ordre d'insertion), la
sortie est donc déterministe.

## 9. Tests
`tests/test_optipacc_amas.py` — 45 tests :
- **pivot des densités rejoué sur SQLite en mémoire** (constante SQL réelle) : les trois
  densités sur une ligne, densité absente à `NULL`, isolation scénario/site, repli des
  doublons techniques, `LEFT JOIN` conservant un amas sans référentiel ;
- regroupement Python (amas → jour → produit, `Decimal` → `int`, jours en minuscules) ;
- gardes C1/C2/C3 avec la matrice statut × flags, et acceptation de `EN PRODUCTION` ;
- pagination : cas 6 (742 amas / 4 pages), cas 7 (dernière page), offset calculé, page
  hors bornes, cas 8 (filtre désactivant la pagination) ;
- C4 : UUID inconnus tracés, `404` si aucun valide, dédoublonnage de la demande ;
- RG-API-003 : aucune requête autre que des `SELECT` ;
- non-régression schéma : colonnes citées présentes dans `db/db_new.sql`, ENUM
  `couleur_pic` et `jour_semaine` alignés sur les littéraux codés en dur.

Suite complète : **283 tests** au vert.

## 10. Points ouverts

1. **Valeur de `NB_AMAS_PAR_PAGE`.** Le ticket cite « 200 ou 500 selon la configuration
   retenue ». Le défaut est `200` ; à arbitrer avec OPTIPACC selon les volumes réels et le
   temps de réponse observé.
2. **Index.** `trppu_trafic_agrebal` a `idx_scen_site_agrebal (id_scenario, co_regate,
   id_agrebal)` mais aucun index sur `agrebal_uuid`, sur lequel portent le `DISTINCT`, le
   tri et la clause `IN`. À surveiller sur les sites à fort volume : un index
   `(id_scenario, co_regate, agrebal_uuid)` serait le candidat naturel si les temps de
   réponse se dégradent.
