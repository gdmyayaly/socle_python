# Convention de log — YB04

Les logs sont émis au format JSON par `app/json_formatter.py` et construits avec
`app/log_utils.py`. `tests/test_log_convention.py` verrouille l'essentiel de ce
document : une modification qui s'en écarte fait échouer les tests.

## Grammaire des messages

```
Début      <action> (<cle>=<valeur>, …)
Avancement <action> (<cle>=<valeur>, …, duration_ms=<f>)
Fin        <action> (<cle>=<valeur>, …, duration_ms=<f>)
Rejet      <action> (<cle>=<valeur>, …, motif=…)
Erreur     <action> (<cle>=<valeur>, …)      ← toujours via logger.exception
```

`Fin`, jamais `terminé`/`terminée` : l'accord de genre varie selon l'action et casse le
regroupement dans Kibana.

`Avancement` est réservé aux traitements longs (cf. « Traitements longs » plus bas). Il
n'apparaît jamais seul : un `Début` l'ouvre, un `Fin` ou un `Rejet` le clôt.

Le bloc de contexte se construit avec `ctx()` et se passe **en argument** de `%s` :

```python
log.info("Fin chargement %s", ctx(lignes=1240, duration_ms=8421.0))
```

Jamais en f-string ni concaténé au message : le rendu de `ctx()` est paresseux, il n'a
lieu que si la ligne est réellement émise. Un `logger.debug` invisible sans `-v` ne doit
rien coûter.

Dans le bloc : identifiants d'abord, `duration_ms` en dernier ; les valeurs `None` sont
omises, les valeurs longues tronquées à `CTX_VALEUR_MAX_LEN` (300 caractères).

## Identifiant de corrélation

`id_traitement` est posé une fois par unité de travail via `set_id_traitement()`, relu
par `JsonFormatter` et ajouté à **chaque** ligne — y compris celles émises par les
couches basses (`app.db.mysql`), qui n'ont aucun moyen de connaître l'unité en cours.

La clé est **toujours présente**, à `null` hors traitement, pour que le mapping Kibana
reste stable. Sa lecture est protégée : `format()` ne doit jamais lever.

Le ContextVar est isolé par tâche asyncio : si le module traite plusieurs unités en
parallèle, une tâche ne voit jamais l'identifiant d'une autre. Poser le contexte en
début de traitement, le libérer avec `reset_id_traitement(token)` dans un `finally`.

## Niveaux

INFO par défaut, DEBUG avec `-v`. Le batch tourne sous ordonnanceur sans `-v` : sans
INFO il ne laisserait aucune trace de ce qu'il a fait.

## Traitements longs

Un traitement qui dure plus de quelques minutes journalise sa progression **en INFO**, avec
le verbe `Avancement`. Sans ces lignes, un chargement d'une heure ne laisse rien entre son
`Début` et son `Fin`, et l'exploitant ne peut pas distinguer un traitement lent d'un
traitement bloqué.

```python
log.info("Avancement chargement clés de répartition %s",
         ctx(id_referentiel=1, lignes=400000, lots=80,
             debit_lignes_s=12500.0, duration_ms=32000.0))
```

Deux règles :

- **Cadencer sur le volume, pas sur le temps** — une ligne toutes les N unités traitées
  (`CHARGEMENT_LOG_TOUTES_LES`, 100 000 par défaut), de sorte que le nombre de lignes de log
  reste proportionnel au travail et non à la durée.
- **Ne pas annoncer de pourcentage si le total est inconnu.** Un fichier lu en streaming
  n'est jamais compté d'avance : on journalise un volume et un débit. Un pourcentage
  inventé est pire que pas de pourcentage.

## Ce qui ne se logue pas

- Le SQL complet d'un script : `statement_preview` le borne à 120 caractères, les
  scripts de données pouvant contenir des informations personnelles.
- Rien ne part sur **stdout** : c'est le flux du rapport exploitant, lu par
  l'ordonnanceur. Les `print()` de `app/main.py` sont ce rapport, pas des logs. Les
  logs JSON partent sur **stderr** — `2>/dev/null` rend toujours un rapport nu.
- Les modules purs (calcul, structures de données) restent sans logger : les verdicts
  se loguent à la frontière, là où le traitement est orchestré.

## Un verdict d'échec se logue explicitement

Si un traitement ne lève pas mais rend un statut d'échec, l'échec ne produit aucune
exception : sans ligne dédiée il ne laisse **aucune trace**. Journaliser chaque issue —
succès en INFO, rejet et échec en WARNING avec leur motif.

## Sortie fichier

Un fichier par jour, `logs/{YYYY-MM-DD}.log`, activé uniquement si `APP_ENV=local` ou si
`LOGS_DIR` est renseigné. Le nom est calculé **une fois au démarrage** : ce n'est pas un
handler rotatif, un processus qui tourne plus de 24 h continue d'écrire dans le fichier
de son jour de lancement.
