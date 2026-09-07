# Convention de log — YB05

La grammaire, les niveaux et l'outillage sont **communs au module YS04** et décrits
dans **`python/api_docs/CONVENTION-LOGS.md`**, qui fait référence. Ce document ne
liste que ce qui diffère parce que YB05 est un **batch console** et non une API HTTP.

## Rappel de la grammaire

```
Début  <action> (<cle>=<valeur>, …)
Fin    <action> (<cle>=<valeur>, …, duration_ms=<f>)
Rejet  <action> (<cle>=<valeur>, …, verdict=…, motif=…)
Erreur <action> (<cle>=<valeur>, …)          ← toujours via logger.exception
```

`Fin`, jamais `terminé`/`terminée` : l'accord de genre variait selon l'action et
cassait le regroupement dans Kibana.

## Les cinq écarts avec YS04

### 1. `verdict=` remplace `http=`

Il n'y a pas de code HTTP dans un batch. Un `Rejet` porte donc `verdict=` — la valeur
du `Rapport.statut` (`NON_ELIGIBLE`, `ECHEC`) — et `motif=`.

```
Rejet traitement scénario (verdict=NON_ELIGIBLE, etape=éligibilité, motifs=[…])
Rejet calcul trafics PDI (verdict=ECHEC, motif=verrou déjà pris par un autre calcul)
```

### 2. `id_scenario` remplace `id_session_ihm`

Même mécanisme — un ContextVar posé une fois, relu par `JsonFormatter`, clé toujours
présente (`null` hors contexte), lecture protégée pour que `format()` ne lève jamais —
mais une autre clé, puisque YB05 n'a pas de session IHM. `app_tm` distingue déjà les
deux modules dans Kibana.

Le contexte est posé à deux endroits seulement :

- `orchestrateur._worker` — au dépilement d'un scénario, reset en `finally` ;
- `main._executer_traitement` — pour les trois commandes mono-scénario.

Toutes les lignes émises dessous le portent, **y compris celles de `app.db.mysql`**,
qui n'ont aucun moyen de connaître le scénario. C'est ce qui rend une trace lisible en
mode `ALL`, où `NB_WORKER` scénarios s'entrelacent.

### 3. Le niveau par défaut est INFO, `-v` donne DEBUG

`main.py` configurait auparavant WARNING par défaut « pour ne pas polluer la sortie
console ». Or les deux flux sont séparés : le rapport part sur **stdout**, les logs
JSON sur **stderr**. INFO ne gêne donc pas l'exploitant (`2>/dev/null` rend toujours un
rapport nu), et sans lui le batch ne laisserait aucune trace sous ordonnanceur.

### 4. `log_utils` est un portage réduit

`yb05/app/log_utils.py` reprend de son homologue YS04 : `ctx()` (rendu paresseux),
`safe_preview()`, et le ContextVar.

Ne sont **pas** portés `params_loggables`, `CHAMPS_SENSIBLES` et `diff_champs` : YB05
ne manipule aucun `id_rh` — la contrainte de non-journalisation qui les motive côté API
ne s'applique pas ici — et les états avant/après sont déjà portés par `Rapport.etats`.

### 5. Les verdicts doivent être journalisés explicitement

Les traitements **ne lèvent pas** : ils rendent un `Rapport` dont `reussi` vaut `False`.
Un scénario en échec ne produit donc aucune exception, et sans une ligne dédiée il ne
laisse **aucune trace** — le verdict ne vivrait que dans le `Bilan`, en mémoire.

C'est pourquoi `orchestrateur._traiter` journalise chaque issue : `SUCCES` en INFO,
`NON_ELIGIBLE` et `ECHEC` en WARNING avec leurs motifs.

## Ce qui ne se logue pas

- Le SQL complet d'un script : `statement_preview` le borne à 120 caractères, les
  scripts de données pouvant contenir des informations personnelles.
- Rien ne part sur **stdout** : c'est le flux du rapport exploitant, lu par
  l'ordonnanceur. Les `print()` de `app/main.py` sont ce rapport, pas des logs.
- `traitements/rapport.py` et `traitements/erreurs.py` restent **sans logger** :
  brancher les logs sur `Rapport.ko()` serait tentant — c'est l'entonnoir unique des
  échecs métier — mais leur docstring en fait des modules purs, sans log ni base. Les
  verdicts se loguent à la frontière des traitements et de l'orchestrateur.

## Traçabilité en base

Indépendamment des logs, chaque calcul écrit une ligne dans **`trppu_recalcul_log`**
(`scenario.journaliser`), avec sa `raison` et son commentaire. Sur un échec, l'écriture
se fait **hors** de la transaction annulée — une trace d'incident qui disparaît avec le
rollback ne sert à rien.

`journaliser` est **best-effort** : appelée depuis les chemins d'échec juste après
`liberer_verrou`, si elle levait, son exception remplacerait l'erreur métier d'origine
et l'exploitant lirait un incident base à la place de la cause réelle.

`liberer_verrou`, à l'inverse, reste **propageante** : un verrou non libéré laisse le
scénario bloqué pour tous les autres processus, cela doit rester un échec visible.

## Tests

- `tests/test_log_convention.py` — rendu de `ctx()`, paresse, et le champ `id_scenario`
  (toujours présent, `null` hors contexte, isolé entre tâches asyncio).
- `tests/test_journalisation.py` — `journaliser` best-effort, `liberer_verrou`
  propageante, verrou non obtenu tracé.
- `tests/test_traitements_orchestrateur.py` — un verdict journalisé par scénario.
