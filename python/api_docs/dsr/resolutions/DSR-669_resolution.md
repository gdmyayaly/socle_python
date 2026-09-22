# Résolution — DSR-669 (Activer/désactiver les changements d'un scénario via le figement)

## 1. Statut

**Retiré** (anomalie signalée). La route `PATCH /trppu-api/scenarios/{id_scenario}/figement`
et son mapping statut IHM → `est_fige` n'existent plus.

### Pourquoi

Le service livré initialement figeait un scénario (`est_fige = 1`) dès réception des
libellés `validé` ou `simulation`. Deux problèmes :

1. **Le flag ne doit être à `1` qu'en production.** Un scénario validé mais non mis en
   production passait `est_fige = 1` et devenait non modifiable — `assert_editable`
   renvoyant 409 sur l'entête **et** sur toutes les sous-ressources. C'est l'anomalie
   remontée.
2. **La route ne lisait ni ne modifiait le `statut` en base**, elle se fiait au seul
   libellé envoyé par l'IHM. Même corrigée en « seul `en production` fige », elle aurait
   permis de poser `est_fige = 1` sur un scénario réellement `EN COURS` ou `VALIDE`.

Or la mise en production passe **exclusivement par OPTIPACC**
(`POST /trppu-api/optipacc/scenario/mise-en-production`, DSR-707), qui pose déjà
`statut = 'EN PRODUCTION'` et `est_fige = 1` dans la même transaction. L'IHM n'a donc
aucune raison de piloter le figement : la route est sans objet et le retrait supprime
la cause de l'anomalie.

## 2. Fichiers modifiés (retrait)

- `app/routes/trppu_scenario/routes.py` : suppression de l'endpoint
  `PATCH /{id_scenario}/figement` (`update_figement_par_statut`).
- `app/routes/trppu_scenario/statuts.py` : suppression de `FIGE_PAR_STATUT`,
  `resolve_fige_from_statut()` et `_normalize_statut()`.
- `app/routes/trppu_scenario/schemas.py` : suppression de `FigementParStatutRequest`.
- Docs alignées : `specifications_routes_io.md`, `cartographie_complete_systeme.md`,
  `cartographie_donnees_persistees.md`, `rapport_complet_fonctionnalites.md`.

## 3. Qui écrit `est_fige` après le retrait

| Écrivain | Valeur | Condition |
| -------- | ------ | --------- |
| `POST /trppu-api/optipacc/scenario/mise-en-production` (DSR-707) | `1` | avec `statut = 'EN PRODUCTION'`, contrôles C1→C5 |
| `apply_transition_side_effects` (`POST /scenarios/{id}/mise-en-prod`) | `1` | uniquement sur la cible `EN PRODUCTION` |
| `POST /scenarios` et `POST /scenarios/{id}/duplicate` | `0` | à la création |
| `PATCH /scenarios/{id}/est-fige` | payload | forçage manuel, seul moyen de défiger après mise en production |

Aucun autre chemin ne peut poser `est_fige = 1`, et aucun ne le fait hors
`EN PRODUCTION`.

## 4. Migrations / rattrapage

Aucune migration de schéma. En revanche les scénarios figés à tort par l'ancien mapping
restent en base :

```sql
-- contrôle
SELECT id_scenario, co_regate, statut, est_fige
FROM trppu_scenario
WHERE est_fige = 1 AND statut <> 'EN PRODUCTION';

-- rattrapage
UPDATE trppu_scenario SET est_fige = 0
WHERE est_fige = 1 AND statut <> 'EN PRODUCTION';
```

## 5. Impacts appelants

- Front `trppu/` : aucun composant n'appelait cette route (`scenario.service.ts` ne la
  déclare même pas). Le front ne fait que **lire** `est_fige`
  (`trppu/services/tmh-recalcul.service.ts`) pour bloquer les ajustements TMH.
- Collection Postman : `postman/trppu_collection.json` contient encore l'entrée
  `PATCH .../figement` ; elle est générée depuis l'OpenAPI, à régénérer via
  `python scripts/gen_postman_collection.py`.
- Tout appelant résiduel recevra un `405 Method Not Allowed` (le préfixe
  `/trppu-api/scenarios/{id}` existe toujours).

## 6. ➡️ Commentaire Jira (à coller)

> **Retrait du service de figement**
>
> Le service `PATCH /trppu-api/scenarios/{id_scenario}/figement` est supprimé.
>
> **Motif** : il figeait le scénario dès le statut `validé`/`simulation`, ce qui rendait
> le scénario non modifiable (erreur 409) alors qu'il n'était pas en production —
> anomalie remontée. Le service se fiait au libellé envoyé par l'IHM sans vérifier le
> statut réel du scénario en base, donc même recalibré il permettait de figer hors
> production.
>
> **Règle retenue** : `est_fige = 1` uniquement en production, et la mise en production
> se fait exclusivement via OPTIPACC
> (`POST /trppu-api/optipacc/scenario/mise-en-production`, DSR-707) qui pose déjà le
> statut et le figement dans la même transaction. Le figement n'est donc plus piloté par
> l'IHM.
>
> **Défigement** : `PATCH /trppu-api/scenarios/{id_scenario}/est-fige` reste disponible
> (seul moyen de défiger un scénario après sa mise en production).
>
> **À prévoir** : rattrapage en base des scénarios figés à tort
> (`UPDATE trppu_scenario SET est_fige = 0 WHERE est_fige = 1 AND statut <> 'EN PRODUCTION'`).
