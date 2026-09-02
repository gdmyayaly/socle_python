# Convention de log — API trppu (YS04)

Objectif : qu'une ligne de log se lise comme une phrase métier explicite, et qu'un
incident soit **reconstituable** depuis les seuls logs.

```
Fin création scénario (id_scenario=52, co_regate=012345, rows_affected=1, duration_ms=84.2)
```

---

## 1. Grammaire

Une seule forme par phase. Le bloc de contexte est toujours produit par `ctx()`.

```
Début  <action> (<cle>=<valeur>, …)
Fin    <action> (<cle>=<valeur>, …, duration_ms=<f>)
Rejet  <action> (<cle>=<valeur>, …, http=<code>, motif=<texte>)
Erreur <action> (<cle>=<valeur>, …)          ← toujours via logger.exception
```

- **`Fin`, jamais `terminé`/`terminée`.** L'accord de genre variait selon l'action
  (`Listing … terminé` / `Récupération … terminée`) et rendait les messages non
  regroupables dans Kibana.
- **`<action>`** : un libellé français figé (`création scénario`, `MAJ TMH ciblée`,
  `suppression neutralisation`, `upload Excel produits`), identique du `Début` au
  `Fin` d'un même endpoint.
- **Ordre des clés** : identifiants d'abord (`id_scenario`, `co_regate`,
  `co_produit`), puis volumétrie (`rows_affected`, `inseres`, `modifies`, `count`),
  puis `duration_ms` **en dernier**.
- **`duration_ms=%.1f` partout.** Les champs `execution_time_s` des *réponses* HTTP
  gardent leurs secondes : c'est un contrat d'API, pas un log.
- **Pas de flèches.** `>>>` / `<<<` restent réservés au middleware HTTP de
  `app/main.py`.
- **`id_session_ihm` ne se met jamais dans le message** : `JsonFormatter` le pose
  déjà comme champ racine du JSON, sur *toutes* les lignes de la requête.

## 2. Niveaux

| Niveau | Usage |
|---|---|
| `INFO` | Début/Fin d'action métier, identifiants générés, volumétrie |
| `DEBUG` | Étapes de calcul intermédiaires (bornes, résolutions, vérifications OK) |
| `WARNING` | **Rejets métier** — tout 4xx levé, avec `http=` et `motif=` |
| `logger.exception` | Échecs techniques, avec stacktrace |

`logger.error` est réservé au cas où l'exception a déjà été absorbée ailleurs et
que la stacktrace a été journalisée à son point de capture (un seul site
aujourd'hui : `trppu_trafics/routes.py`, commenté sur place).

Un 4xx levé sans WARNING est un trou de traçabilité : l'IHM reçoit une erreur dont
le serveur ne garde aucune trace. C'est vrai aussi dans la couche `helpers` —
`fetch_scenario_or_404`, `assert_not_fige`, `assert_transition_allowed` tracent
leur propre refus, parce que l'endpoint appelant, lui, ne le voit pas passer.

## 3. Outillage — `app/log_utils.py`

**`ctx(**champs)`** — produit le bloc `(cle=valeur, …)`.

```python
logger.info("Fin création scénario %s", ctx(id_scenario=52, duration_ms=84.2))
```

Ordre d'appel préservé, `None` omis, flottants à 1 décimale, valeurs longues
tronquées. Le rendu est **paresseux** (`__str__` différé) : un `logger.debug` ne
coûte rien quand DEBUG est désactivé. Toujours passer `ctx(...)` en **argument**
de `%s`, jamais concaténé dans le message — sinon la paresse est perdue.

**`params_loggables(payload)`** — `model_dump(mode="json")` privé des champs
sensibles (`CHAMPS_SENSIBLES` : `id_rh`, `id_rh_creation`, `id_rh_maj`, `cle`).
Tout payload journalisé passe par là.

> ⚠️ `encrypt_id_rh` est un **passthrough** quand `ID_RH_CRYPTO_KEY` est vide
> (`app/security/crypto.py`). Une variable nommée `id_rh_token` n'est donc pas
> garantie chiffrée : elle ne se logue pas davantage que l'id_rh en clair.

**`diff_champs(avant, apres)`** — `{champ: [avant, après]}` réduit aux champs
modifiés. Seules les clés présentes dans `apres` sont comparées, pour qu'un UPDATE
partiel ne fasse pas apparaître les colonnes qu'il ne touche pas.

**`safe_preview(obj, max_len=…)`** — repli pour du texte libre non structuré
(message d'erreur de parsing Excel, `exc.errors()` du handler 422).

## 4. Reconstitution des données

| Opération | Ce qui doit être journalisé |
|---|---|
| **CREATE** | les paramètres d'entrée au `Début`, l'**id généré** et `rows_affected` à la `Fin` |
| **UPDATE** | l'état avant est déjà relu par l'endpoint → `delta=` via `diff_champs` |
| **DELETE** | une ligne `État avant suppression …` **avant** l'écriture, puis la volumétrie |

`execute()` retourne le `rowcount` : le capturer, ne pas le jeter.
`delete_scenario_cascade` retourne `{table: nb_lignes}` pour restituer l'ampleur
d'une suppression en cascade.

## 5. Persistance en base — `trppu_api_log`

`app/services/api_log.enregistrer_appel()` écrit une ligne par **écriture**
(POST/PUT/PATCH/DELETE) dans `trppu_api_log`, en complément des logs Kibana.

- **Hors de la transaction métier**, une fois son issue connue : une trace annulée
  par le rollback serait inutile précisément quand on en a besoin.
- **Best-effort** : un échec d'écriture est redescendu en WARNING et n'interrompt
  jamais la requête.
- `api_name` reprend le vocabulaire d'actions du module d'audit
  (`CREATION_SCENARIO`, `MAJ_SCENARIO`, `ECRITURE_TMH`…), défini une fois dans
  `app/services/api_log.py`.
- `caller` = `id_session_ihm` (les routes ne sont pas authentifiées).

> **Contrainte FK.** `trppu_api_log.id_scenario` et `trppu_recalcul_log.id_scenario`
> référencent `trppu_scenario` **sans `ON DELETE`** (donc RESTRICT). Dès que ces
> tables sont alimentées, supprimer un scénario échouerait en MySQL 1451.
> `detach_logs_scenario` (appelé par `delete_scenario_cascade`) détache
> `trppu_api_log` (`id_scenario = NULL`) et supprime les lignes
> `trppu_recalcul_log` (colonne NOT NULL). L'`id_scenario` est recopié dans la
> colonne `params` pour que la trace survive au détachement.
> Cf. écart n°10 de `db/RAPPORT-ECARTS-db_new-2026-08-17.md`.

## 6. Ce qui ne se logue jamais

- `id_rh` en clair — et donc pas non plus un `id_rh_token` (cf. §3).
- La clé de déchiffrement de l'endpoint d'audit.
- Un jeu de résultats complet en INFO : volumétrie en INFO, contenu en DEBUG et
  borné par `ctx` (cf. `trppu_trafics/routes.py`).
- Le SQL brut en INFO (`app/db/databricks.py` : passé en DEBUG — en INFO il expose
  la structure des tables à chaque requête).

## 7. Format JSON produit

`JsonFormatter` (`app/json_formatter.py`) émet un jeu de clés **fixe** ; le
contexte vit dans `app_message`.

```json
{
  "app_datetime": "2026-09-02T10:12:31.482Z",
  "app_ccx": "dsr", "app_env": "sdev", "app_ptf": "build",
  "app_tm": "ys04", "app_version": "1.0.0",
  "severity_label": "INFO",
  "app_message": "Fin création scénario (id_scenario=52, co_regate=012345, duration_ms=84.2)",
  "id_session_ihm": "a1b2c3d4-…",
  "name": "app.routes.trppu_scenario.routes",
  "filename": "routes.py", "lineno": 331
}
```

> `logger.info(..., extra={...})` est **sans effet** : le formatteur construit un
> dict fermé et ne lit pas `record.__dict__`. C'est pourquoi la convention porte
> sur le texte de `app_message` et non sur des champs structurés.

## 8. Tests

- `tests/test_log_convention.py` — rendu de `ctx`, paresse, expurgation de
  `params_loggables`, `diff_champs`.
- `tests/test_api_log.py` — bornage des colonnes, absence d'`id_rh`, et le fait
  qu'un échec d'écriture de l'audit ne remonte pas.
- `tests/test_delete_scenario_cascade.py` — détachement des tables de logs avant
  le parent (garde-fou contre le MySQL 1451) et volumétrie retournée.
- `tests/test_log_session.py` — `id_session_ihm` présent sur chaque ligne.
