# Résolution — DSR-669 (Activer/désactiver les changements d'un scénario via le figement)

## 1. Statut
**Terminé.** Service YS04 qui met à jour le champ `est_fige` de `trppu_scenario` à
partir d'un statut reçu de l'IHM : `validé`/`simulation` → figé (1), `en cours` →
défigé (0), tout autre statut → erreur « inconnu » sans action. Met à jour
**uniquement** `est_fige`, pas le `statut` du scénario.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_scenario/statuts.py` : mapping `FIGE_PAR_STATUT` + `resolve_fige_from_statut()`
  (normalisation casse/accents, 422 si inconnu).
- `app/routes/trppu_scenario/schemas.py` : schéma `FigementParStatutRequest` (`statut: str`).
- `app/routes/trppu_scenario/routes.py` : endpoint `PATCH /{id_scenario}/figement`.

## 3. Endpoint livré

`PATCH /trppu-api/scenarios/{id_scenario}/figement`

### Entrées
| Emplacement | Paramètre | Type | Obligatoire | Description |
| ----------- | --------- | ---- | ----------- | ----------- |
| Path | `id_scenario` | int | Oui | Id du scénario à figer/défiger |
| Body | `statut` | str | Oui | Statut IHM (`validé`, `simulation`, `en cours`) |

Body JSON :
```json
{ "statut": "validé" }
```

### Mapping statut → est_fige
| Statut reçu (insensible casse/accents) | est_fige |
| -------------------------------------- | -------- |
| `validé` / `simulation` | 1 (figé) |
| `en cours` | 0 (défigé) |
| tout autre (`en production`, `archive`, libellé libre…) | 422, aucune action |

### Sortie — `200 OK` → `ScenarioOut`
Scénario complet rafraîchi (avec `est_fige` à jour).

### Codes d'erreur
| Code | Cas |
| ---- | --- |
| `404` | Scénario inexistant |
| `409` | Scénario archivé (terminal, non modifiable) |
| `422` | Statut inconnu (paramètre non reconnu) |
| `500` | Erreur de mise à jour base |

## 4. Migrations / dépendances
Aucune. Schéma DB conforme (`db_10_09_2026.sql`) : `trppu_scenario.est_fige` smallint
DEFAULT 0.

## 5. Hypothèses & écarts
- Le service met à jour **uniquement `est_fige`** (le ticket ne demande pas de changer
  `statut`). Le changement de statut reste géré par `PATCH /{id}/statut`.
- Les libellés du ticket (`validé`, `simulation`, `en cours`) ne sont pas l'enum DB
  (`EN COURS`, `VALIDE`, `EN PRODUCTION`, `ARCHIVE`) ; en particulier `simulation`
  n'existe pas en base. Le mapping est donc fait sur le **libellé reçu**, insensible à
  la casse et aux accents.
- `EN PRODUCTION` / `ARCHIVE` reçus → traités comme « inconnu » (422), le ticket ne les
  couvrant pas.
- Cohérence avec l'existant : `assert_not_archive` (un scénario archivé n'est pas
  figeable/défigeable) et `increment_version` (comme `PATCH /est-fige`).

## 6. Comment tester
```
PATCH /trppu-api/scenarios/1/figement   body {"statut":"validé"}      -> est_fige=1
PATCH /trppu-api/scenarios/1/figement   body {"statut":"en cours"}    -> est_fige=0
PATCH /trppu-api/scenarios/1/figement   body {"statut":"toto"}        -> 422
```
Vérifier dans Kibana les logs `Début figement par statut (id_scenario=..., statut=...)`
et `Figement par statut terminé (..., est_fige=...)`, puis en base
`SELECT est_fige FROM trppu_scenario WHERE id_scenario = 1`.

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| Statut `validé` → service appelé, `est_fige`=1 en base | mapping → 1 + UPDATE |
| Statut `validé`/`simulation` puis `en cours` → `est_fige`=0 | mapping → 0 + UPDATE |
| Statut inconnu → erreur « inconnu », aucune action | 422 avant tout UPDATE |
| Trace Kibana avec id_scenario | `logger.info` début + fin |
| Conformité base | UPDATE direct sur `trppu_scenario.est_fige` |

## 8. ➡️ Commentaire Jira (à coller)

> **URL d'appel**
> `PATCH /trppu-api/scenarios/{id_scenario}/figement`
> Exemple : `PATCH /trppu-api/scenarios/1/figement`
>
> **Données d'entrée**
> - `id_scenario` | id du scénario à figer/défiger.
> - `statut` (corps JSON) | statut IHM : `validé`, `simulation` ou `en cours`
>   (insensible à la casse et aux accents).
> ```json
> { "statut": "validé" }
> ```
>
> **Règle de figement**
> - `validé` ou `simulation` => est_fige = 1 (modifications empêchées)
> - `en cours` => est_fige = 0 (modifications possibles)
> - tout autre statut => erreur 422 « statut inconnu », aucune action réalisée
>
> **Données de sortie**
> le scénario complet à jour (champ `est_fige` reflétant le nouveau figement).
> - Scénario inexistant => 404.
> - Scénario archivé => 409.
> - Statut inconnu => 422.
> - Erreur base => 500.
>
> **Traçabilité / Kibana**
> chaque appel logue `id_scenario` et `statut` à l'entrée, puis `est_fige` résultant
> et la durée à la sortie.
>
> **À valider PO**
> 1. Le service met à jour uniquement `est_fige`, pas le `statut` du scénario (le
>    changement de statut passe par le service dédié). Est-ce bien le comportement
>    attendu, ou faut-il aussi positionner le statut ?
> 2. DSR-634 indique qu'un scénario « ne sera figé qu'une fois validé ET les trafics
>    au PDI calculés ». Ici on fige dès `validé`/`simulation` sans contrôler le calcul
>    PDI : faut-il conserver cette simplification ou ajouter la condition PDI ?
> 3. Le libellé `simulation` n'existe pas dans l'enum de statut en base : on le traite
>    comme un simple déclencheur de figement. À confirmer.
