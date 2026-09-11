# Résolution — DSR-707 (Déclarer un scénario en production depuis OPTIPACC)

## 1. Statut
**Terminé.** Nouvel endpoint dans le package `trppu_optipacc`, seul service OPTIPACC en
écriture. La route de mise en production existante côté IHM a été durcie pour partager les
mêmes garde-fous métier.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_optipacc/routes.py` — endpoint `scenario_mise_en_production`.
- `app/routes/trppu_optipacc/schemas.py` — `MiseEnProductionRequest` / `MiseEnProductionResponse`.
- `app/routes/trppu_optipacc/helpers.py` — `SELECT_SCENARIO_MISE_EN_PROD_SQL`,
  `UPDATE_MISE_EN_PROD_SQL`, `assert_mise_en_prod_possible` (C2, C3).
- `app/routes/trppu_scenario/helpers.py` — gardes **partagées** `assert_trafics_calcules`
  (C4) et `assert_aucun_scenario_en_production` (C5), + `Calcul_trafic_en_cours` ajouté à
  `SELECT_SCENARIO_SQL`.
- `app/routes/trppu_scenario/routes.py` — la route IHM `mise_en_prod` applique C4 et C5.
- `app/routes/trppu_scenario/statuts.py` — `_internal_route_for` cite les deux routes.
- `tests/test_optipacc_mise_en_prod.py` — 28 tests.
- `api_docs/api_trppu_optipacc.md` — §10.

## 3. Endpoint livré
`POST /trppu-api/optipacc/scenario/mise-en-production`

```json
{ "code_regate": "123456", "scenario_id": 125, "date_mise_en_oeuvre": "2027-04-01" }
```
Réponse `200` :
```json
{ "scenario_id": 125, "code_regate": "123456",
  "statut": "EN PRODUCTION", "date_mise_en_oeuvre": "2027-04-01" }
```

Contrôles : C1 `404` · C2 `400` · C3/C4/C5 `409` · `422` corps invalide · `500` technique.

UPDATE appliqué dans une transaction, suivi de `increment_version` :
```sql
UPDATE trppu_scenario SET statut = 'EN PRODUCTION', est_fige = 1,
  dt_mise_en_oeuvre = %s, dt_mise_en_prod = %s,
  dt_validation = COALESCE(dt_validation, NOW())
WHERE id_scenario = %s
```

## 4. Migrations / dépendances
Aucune migration. **Dépendance fonctionnelle** : C4 exige `trafic_pdi_calcule = 1`,
`trafic_agrebal_calcule = 1` et `Calcul_trafic_en_cours = 0` — trois colonnes écrites
exclusivement par le batch YB05. Tant qu'il n'est pas déployé, aucun scénario ne peut être
mis en production par ce service.

## 5. Hypothèses & écarts

- **Un seul champ date.** Le ticket se contredit : l'exemple de body montre
  `date_mise_en_prod`, la section « Paramètres » et le critère d'acceptation 1 décrivent
  `date_mise_en_oeuvre`, et le « Traitement » écrit les deux colonnes. Le Cas 1 tranche —
  une seule date envoyée donne `DT_MISE_EN_OEUVRE = DT_MISE_EN_PROD = 2027-04-01` — ce que
  confirme RG-API-PROD-006 (« la date transmise constitue la référence officielle de mise en
  œuvre **et** de mise en production »). Un `date_mise_en_prod` envoyé par erreur produit un
  `422` explicite plutôt qu'un silence (`extra="forbid"`).

- **Corps d'erreur `{"detail": ...}`** et non `{"erreur": ...}`. Le projet entier, y compris
  les services OPTIPACC déjà livrés (DSR-689/690), utilise le format FastAPI standard.
  Les **messages** du ticket sont repris mot pour mot, seule la clé JSON diffère.

- **C2 répond `400`**, alors que `assert_exploitable` (DSR-689) répond `404` sur le même
  contrôle. Les deux tickets divergent, les deux contrats sont conservés tels quels.

- **`DT_MAJ = NOW()` non écrit explicitement** : la colonne est
  `ON UPDATE CURRENT_TIMESTAMP` en base, l'écrire serait redondant. Un test de
  non-régression schéma le vérifie.

- **`dt_validation` comblée si NULL**, effet de bord repris de
  `apply_transition_side_effects` : un scénario en production sans date de validation est
  incohérent. (Le `chk_scen_prod` que cite la docstring de `statuts.py` n'existe pas dans
  `db/db_new.sql` — l'effet est donc applicatif, pas contraint par la base.)

- **`version_scenario` incrémenté**, comme toute écriture sur un scénario.

## 6. Unicité du scénario en production (C5 / RG-API-PROD-005)

Point de vigilance principal du ticket. **Aucune contrainte d'unicité n'existe en base**
sur `(co_regate, statut = 'EN PRODUCTION')`. Le contrôle est donc applicatif et repose sur
un verrou, exécuté **dans** la transaction d'écriture juste avant l'UPDATE :

```sql
SELECT id_scenario FROM trppu_scenario
WHERE co_regate = %s AND statut = 'EN PRODUCTION' AND id_scenario <> %s
LIMIT 1 FOR UPDATE
```

Sans `FOR UPDATE`, deux appels concurrents sur le même site passeraient le contrôle tous
les deux. L'exclusion `id_scenario <> %s` évite qu'un scénario se bloque lui-même — ce cas
est de toute façon déjà refusé par C3.

> **Piste d'amélioration** : une colonne générée + index unique en base rendrait la règle
> structurelle plutôt qu'applicative. Hors périmètre de ce ticket (migration nécessaire).

## 7. Durcissement de la route IHM

`POST /trppu-api/scenarios/{id_scenario}/mise-en-prod` existait avant ce ticket et ne
contrôlait **ni** les flags de calcul **ni** l'unicité par site. Elle aurait constitué un
contournement direct de RG-API-PROD-005. C4 et C5 lui sont donc appliqués via les mêmes
helpers partagés. Comportement inchangé par ailleurs : la date reste `NOW()` et
`dt_mise_en_oeuvre` n'est pas touchée.

| | Route IHM | Route OPTIPACC |
|-|-----------|----------------|
| Date de mise en prod | `NOW()` | fournie par l'appelant |
| `dt_mise_en_oeuvre` | inchangée | renseignée |
| Contrôle du site (C2) | non | oui |
| C4 / C5 | oui (ajoutés) | oui |

**Impact de régression** : une mise en production depuis l'IHM sur un scénario non calculé,
ou sur un site ayant déjà un scénario en production, renvoie désormais `409` au lieu de
réussir. C'est l'effet recherché, mais il change le comportement d'une route déjà
consommée par le front.

## 8. Effets sur les traitements futurs (Cas 5 du ticket)

Aucun code nouveau : les règles existent déjà et se déclenchent via `est_fige = 1`.

| Tentative | Mécanisme | Résultat |
|-----------|-----------|----------|
| Modification | `assert_editable` → `assert_not_fige` | `409` |
| Suppression | mêmes gardes | `409` |
| Recalcul | le batch YB05 ne sélectionne pas les scénarios figés | non recalculé |
| Retour arrière | `PATCH /{id}/est-fige` permet de défiger | **volontairement conservé** (voir ci-dessous) |

> **Point ouvert.** RG-API-PROD-002 parle d'un figement « définitif », mais
> `PATCH /trppu-api/scenarios/{id}/est-fige` permet toujours de défiger un scénario en
> production. Cette porte de sortie est conservée telle quelle : la fermer sortirait du
> périmètre du ticket et supprimerait le seul recours en cas d'erreur de manipulation. À
> arbitrer avec le métier si le caractère définitif doit être strict.

## 9. Tests
`tests/test_optipacc_mise_en_prod.py` — 28 tests : contrat d'entrée, C1 à C5 (dont la
matrice des 4 statuts refusés et des flags de calcul), ordre contrôle/écriture dans la
transaction, propagation du `409` sans transformation en `500`, audit, et non-régression
schéma (colonnes existantes, alias de `Calcul_trafic_en_cours`, `dt_maj` automatique).

Suite complète : **283 tests** au vert.
