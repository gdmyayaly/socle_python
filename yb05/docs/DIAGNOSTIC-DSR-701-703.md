# Diagnostic des tickets DSR-701, DSR-702, DSR-703 — et DSR-704

Confrontation des tickets (`docs/agreball/`) au schéma réellement déployé
(`python/db/db_new.sql`, ré-extrait le 17/08/2026) et au code livré dans `app/traitements/`.

> Le nom du fichier s'est arrêté à DSR-703, mais le document couvre aussi **DSR-704**, livré
> depuis (constats 34 et 36 à 40). Il n'est pas renommé : les commentaires Jira le référencent
> déjà sous ce nom.

Même principe que pour la chaîne des clés de répartition (`DIAGNOSTIC-DSR-696-699.md`) : le
ticket exprime l'intention métier, la base fait foi sur les noms, et tout écart est tracé ici.
La numérotation des constats poursuit celle de ce document (1 à 20).

DSR-700 n'a pas de section propre : il n'a pas de traitement, son exigence — un scénario mémorise
le référentiel et la version de clés utilisés, et les conserve — est satisfaite par l'étape 3 de
DSR-702.

## Synthèse

| # | Ticket | Constat | Statut |
| - | ------ | ------- | ------ |
| 21 | 701 r11 | La règle cherche les Agrébals dans la table des trafics **calculés**, que la règle 6 exige vide | Corrigé |
| 22 | 701 r10 | « Référentiel actif » n'est pas exprimable en base | Convention retenue |
| 23 | 701 r10 | Le ticket exige que deux requêtes rendent le même identifiant, sans dire quoi faire sinon | Tranché — bloquant |
| 24 | 702 | La correspondance produit → clé est donnée en familles métier, absentes de la base | Configurable |
| 25 | 702 | Le TMH « du produit » n'est pas unique : plusieurs lignes, deux mécanismes d'exclusion | Somme des lignes retenues |
| 26 | 702 | `dense` / `faible1` / `faible2` sont des `smallint unsigned`, le calcul produit des décimales | Arrondi + échec au-delà |
| 27 | 702 | La densité des coefficients n'est reliée à aucune colonne de la cible | Correspondance retenue — **à confirmer** |
| 28 | 702 | `dt_effet` / `dt_fin` des coefficients ne discriminent rien | Ignorées |
| 29 | 702 | Aucun ticket ne dit quels jours calculer | Semaine du scénario ∩ jours coefficientés |
| 30 | 702 | Le verrou posé dans la transaction du calcul serait invisible des autres processus | Commité seul |
| 31 | 703 | `id_agrebal` : `bigint` en source, `int` en cible | Contrôlé avant insertion |
| 32 | 703 | Le ticket compte une ligne de journal par traitement, DSR-704 une par scénario | Tranché — une par traitement |
| 33 | 701/703 | Coquilles de noms dans les tickets | Noms réels du schéma |
| 34 | 704 | Le worker réserve le scénario avant un contrôle qui exige qu'il ne soit pas réservé | Ordre inversé |
| 35 | 702 | Le CA-06 interdit de modifier `TRAFIC_AGREBAL_CALCULE`, que l'étape 4 du même ticket remet à 0 | L'étape 4 l'emporte |
| 36 | 704 | DSR-703 s'arrête sans libérer le verrou : dans la chaîne, le scénario resterait bloqué | Filet de sécurité dans l'orchestrateur |
| 37 | 704 | Un scénario à moitié calculé n'est jamais repris par un `ALL` suivant | Signalé dans le bilan |
| 38 | 704 | Le code de retour n'est pas spécifié | 1 si au moins un échec |
| 39 | 704 | `NB_WORKER` pouvait dépasser la taille des pools MySQL | Pools dimensionnés sur `NB_WORKER` |
| 40 | 704 | Le cas « aucun scénario éligible » n'est pas prévu | Bilan à zéro, code de retour 0 |

---

## DSR-701 — contrôle d'éligibilité

### 21. La règle 11 interroge la mauvaise table

> Règle 11 — « présence d'au moins un Agrébal pour `CO_REGATE` du scénario dans la table
> `trppu_trafic_agrebal` ».

`trppu_trafic_agrebal` contient les **trafics calculés** par DSR-703, pas la structure des
Agrébals. Or la règle 6 du même ticket exige `TRAFIC_AGREBAL_CALCULE = 0`, et l'étape 4 de
DSR-702 vide cette table avant chaque recalcul. Appliquée littéralement, la règle 11 ne pourrait
donc **jamais** passer pour un premier calcul : elle exigerait la présence de données que les
règles voisines interdisent.

> **Corrigé** : les Agrébals du site sont lus dans `trppu_agrebal_pdi`
> (`agrebal_code_regate = co_regate`, `agrebal_deleteddAt IS NULL`), qui est bien le référentiel
> de structure. La règle 12 s'appuie sur la même lecture, en comptant les PDI portés par
> `agrebal_pdiList`.

### 22. « Référentiel actif » n'est pas exprimable en base

Déjà relevé pour DSR-698 (`INCOHERENCES.md` §6) : `trppu_referentiel` ne porte ni colonne
`actif`, ni date de fin de validité, et son `co_regate` est nullable et sans index.

> **Convention retenue**, celle du ticket : le référentiel actif d'un site est son plus grand
> `id_referentiel`. Conséquence à connaître — un référentiel national (`co_regate IS NULL`) n'est
> jamais retourné, et la règle 10 le déclarerait absent.

### 23. Le ticket ne dit pas quoi faire quand les deux requêtes divergent

La règle 10 propose deux requêtes — le dernier référentiel du site, et celui porté par la version
de clés active — et précise que la seconde « doit renvoyer le même `id_referentiel` ». Elle ne
prévoit aucun message pour le cas où elles diffèrent.

> **Tranché : bloquant.** Un écart signifie que la version de clés active repose sur un
> référentiel dépassé ; calculer produirait des trafics à partir de clés périmées, ce qui est
> exactement ce que DSR-700 cherche à rendre traçable. Le motif nomme les deux identifiants.

---

## DSR-702 — calcul des trafics PDI

### 24. La correspondance produit → clé n'existe pas en base

Le ticket donne le tableau `Colis → cle_colis`, `OO → cle_oo`, `3S → cle_3s`,
`IP → cle_potentielip`. Mais `trppu_produit` est alimentée dynamiquement depuis Databricks
(`co_type_objet` : `OO`, `OS`, `PR`, `PPI`, `CO`, `IP` — cf. DSR-679), avec création automatique
des codes manquants par YS04. **Rien en base ne dit à quelle famille appartient un code**, et
« Colis » ou « 3S » ne sont pas des codes produits.

> **Corrigé, sans deviner** : la correspondance est une donnée de configuration,
> `CLES_PAR_PRODUIT` (défaut `CO:colis,OO:oo,IP:potentielip,OS:3s,PR:3s,PPI:3s`). Le métier la
> corrige sans livraison. Un produit absent de la liste **fait échouer le calcul** avec un
> message qui le nomme : mieux vaut un batch en échec qu'un trafic faux, qui se propagerait à
> tous les scénarios sans plus jamais se voir.

### 25. Le TMH « du produit » n'est pas unique

Le ticket lit « le TMH du scénario, pour chaque produit ». Or `trppu_tmh` autorise **plusieurs
lignes par produit** depuis la migration du 24/06/2026 (`uq_tmh` porte sur `id_tmh`), chaque ligne
portant un indicateur `bl_exclu` — et `trppu_scenario_exclusions` liste en plus des produits
exclus du scénario.

> **Retenu** : pour chaque produit, la somme de `moyenne_hebdo` sur les lignes `bl_exclu = 0`, en
> écartant les produits présents dans `trppu_scenario_exclusions`. Un produit dont le TMH résultant
> est nul ne produit aucune ligne de trafic.

### 26. Les colonnes de trafic n'acceptent que des entiers

`trppu_trafic_pdi.dense`, `faible1` et `faible2` sont des `smallint unsigned` : entiers, et
plafonnés à 65 535. Or `TMH × coefficient × clé` est un produit de décimaux
(`decimal(12,2) × decimal(7,4) × decimal(24,18)`). Le ticket ne mentionne ni arrondi ni plafond.

> **Retenu** : arrondi à l'entier le plus proche (`ROUND_HALF_UP`), et **échec explicite** au-delà
> de 65 535 comme sur une valeur négative. En mode strict MySQL refuserait la ligne, sinon il la
> tronquerait en silence : dans les deux cas, mieux vaut que le batch dise lequel des trafics
> déborde.

### 27. La densité n'est reliée à rien — **à confirmer**

`trppu_pic_coefficients.densite` vaut 0, 1 ou 2 (`chk_pic_densite`) ; la cible porte trois
colonnes `dense`, `faible1`, `faible2`. Aucun ticket, aucune contrainte ne relie les deux.

> **Retenu** : `0 → dense`, `1 → faible1`, `2 → faible2`, d'après l'ordre des colonnes de la table
> cible et l'ordre naturel des densités. C'est une hypothèse : une inversion produirait des
> trafics plausibles mais faux, sans qu'aucun contrôle ne le voie. **À faire confirmer par le
> métier.**

### 28. Les dates des coefficients ne discriminent rien

`trppu_pic_coefficients` porte `dt_effet` et `dt_fin`, mais sa clé unique
`(id_pic_version, co_produit, jour_semaine, densite)` garantit déjà **une seule ligne** par
combinaison : filtrer sur les dates ne changerait rien, sauf à écarter une ligne pourtant unique.

> **Retenu** : aucun filtre de date. Documenté dans le code, à l'endroit du chargement.

### 29. Quels jours calculer ?

Aucun ticket ne le dit. La cible porte un `jour_semaine` de `LUNDI` à `SAMEDI`, le scénario un
`nb_jours_semaine` valant 5 ou 6, et les coefficients existent par jour.

> **Retenu** : la semaine du scénario (5 ou 6 jours), **intersectée** avec les jours réellement
> coefficientés par la version PIC. Un jour qu'aucun produit ne coefficiente n'est pas calculé —
> c'est un choix d'exploitation. En revanche, un produit qui n'a pas de coefficient pour un jour
> que d'autres couvrent est un **oubli de paramétrage** : le calcul échoue en le nommant. Les deux
> cas sont différents et traités différemment.

### 30. Le verrou de l'étape 2 doit être commité seul

L'étape 2 pose `CALCUL_TRAFIC_EN_COURS = 1` « afin d'empêcher tout lancement concurrent ». Placé
dans la même transaction que le calcul, il n'aurait aucun effet : les autres processus ne
verraient rien avant le commit final, c'est-à-dire après le calcul.

> **Corrigé** : le verrou est un `UPDATE … WHERE calcul_trafic_en_cours = 0` exécuté et commité
> seul, avant tout le reste. Zéro ligne affectée signifie qu'un autre processus détient le
> scénario : le traitement s'arrête sans rien toucher. La traçabilité DSR-700 est commitée de la
> même façon, pour satisfaire le CA-03 (« mémorisés avant le premier calcul »).

### 35. Le CA-06 contredit l'étape 4

> CA-06 — « Le traitement ne doit jamais modifier : `STATUT`, `EST_FIGE`,
> `TRAFIC_AGREBAL_CALCULE`. »
> Étape 4 — « Puis remettre les flags des trafics du scénario à 0 :
> `UPDATE trppu_scenario SET trafic_pdi_calcule = 0, trafic_agrebal_calcule = 0` ».

Le même ticket interdit et prescrit la même écriture.

> **L'étape 4 l'emporte** : elle est cohérente avec le reste du ticket, qui pose que « TRAFIC_PDI
> et TRAFIC_AGREBAL sont indissociables » et fait supprimer les trafics Agrébal du scénario. Un
> flag `TRAFIC_AGREBAL_CALCULE` resté à 1 alors que la table vient d'être vidée serait un
> mensonge. Le CA-06 garde tout son sens sur `STATUT` et `EST_FIGE`, que le traitement ne touche
> jamais.

---

## DSR-703 — calcul des trafics Agrébal

### 31. `id_agrebal` rétrécit entre la source et la cible

`trppu_trafic_pdi.id_agrebal` est un `bigint`, `trppu_trafic_agrebal.id_agrebal` un `int`
(cf. `INCOHERENCES.md` §11, qui relève trois noms et trois types pour le même concept).

> **Contrôlé** : le traitement refuse d'agréger si un `id_agrebal` dépasse la capacité de la
> cible, plutôt que de laisser MySQL tronquer ou refuser au milieu de l'insertion.

### 32. Une ligne de journal par traitement, ou par scénario ?

DSR-702 et DSR-703 demandent chacun d'écrire dans `trppu_recalcul_log`. DSR-704 énonce l'inverse :
« un seul enregistrement de journalisation est créé par scénario traité », à la fin du calcul
Agrébal.

> **Tranché avec DSR-704** : une ligne par traitement, avec des commentaires distincts
> (« … des trafics du scénario » / « … des trafics Agrébal »). Le mode `ALL` n'écrit rien de
> son côté — en faire un troisième auteur du journal contredirait son CA-09. Le CA-12 est
> satisfait sur le fond : une ligne existe bien, avec le bon motif.
>
> Effet de bord à connaître : DSR-703 relit « la dernière raison » et y trouve donc la ligne
> écrite par DSR-702 juste avant. C'est le comportement voulu — le motif d'un même calcul se
> propage d'une étape à l'autre — mais il faut le savoir pour lire le journal.

### 33. Coquilles des tickets

Reprises ici pour que la relecture Jira ne s'y perde pas : `id_senario`,
`trppu_trafic_agreal`, `CALUL_TRAFIC_EN_COURS`, `TRAFIC_AGEBAL_CALCULE`, `TRAFIC_PDI_CALCLE`,
`ID_REFEENTIEL`, `ID_VERSION_CE`, `CALCU_TRAFIC_PDI`, `B05`. Le code utilise les noms réels du
schéma.

---

## DSR-704 — mode `ALL`

### 34. Le worker réserve avant un contrôle qui interdit la réservation

DSR-704 fait réserver le scénario par le worker (`calcul_trafic_en_cours = 1`) **puis** appeler
`controleEligibiliteScenario()`, dont la règle 4 exige `CALCUL_TRAFIC_EN_COURS = 0`. En l'état,
aucun scénario ne serait jamais éligible en mode `ALL`.

> **Ordre inversé** : contrôler d'abord (lecture seule, CA-05 de DSR-701 respecté), réserver
> ensuite. L'atomicité de l'`UPDATE … WHERE calcul_trafic_en_cours = 0` suffit à garantir qu'un
> seul worker obtient le scénario ; le perdant passe au suivant.
>
> La réservation n'est d'ailleurs pas réimplémentée : c'est exactement le verrou que pose déjà
> `calcul_trafic_pdi`. L'orchestrateur n'a rien à ajouter — ce qui est précisément ce que
> demande son CA-09.

### 36. Un scénario peut rester verrouillé indéfiniment

DSR-703 s'arrête sur ses contrôles préalables — scénario introuvable, `TRAFIC_PDI_CALCULE = 0`,
`trppu_trafic_pdi` vide — **sans libérer le verrou**. C'est correct en lancement isolé : il ne
le détient pas, et le relâcher couperait le calcul d'un autre processus.

Mais dans la chaîne du mode `ALL`, DSR-702 vient de le poser. Un échec précoce de l'étape
Agrébal laisserait donc `CALCUL_TRAFIC_EN_COURS = 1` — ce que DSR-704 interdit explicitement :
« le scénario ne doit jamais rester bloqué ».

> **Filet de sécurité dans l'orchestrateur**, seul à savoir qu'il détient le verrou : si l'étape
> PDI a réussi et que l'étape Agrébal n'aboutit pas, il libère. DSR-703 n'est pas modifié, son
> comportement isolé étant le bon. Verrouillé par un test.

### 37. Les scénarios à moitié calculés ne sont jamais repris

Les critères de recherche exigent `TRAFIC_PDI_CALCULE = 0` **et** `TRAFIC_AGREBAL_CALCULE = 0`.
Un scénario dont l'étape PDI a réussi mais dont l'Agrébal a échoué (`1 / 0`, verrou libéré) ne
répond donc plus aux critères : aucun `YB05 ALL` ultérieur ne le reprendra. Il reste en plan
jusqu'à ce que quelqu'un s'en aperçoive.

> **Critères du ticket appliqués à la lettre**, mais ces scénarios sont comptés par une seconde
> requête et **nommés dans le bilan**, avec la commande à jouer. Aucun comportement inventé, et
> l'exploitant les voit le jour même.

### 38. Le code de retour n'est pas spécifié

Le ticket exige qu'une erreur sur un scénario n'arrête pas le batch, mais ne dit rien du code de
retour du processus — or c'est lui que lit l'ordonnanceur.

> **Retenu** : `1` dès qu'un scénario est en échec, `0` sinon, après avoir vidé toute la file.
> Un scénario **non éligible** ne compte pas comme un échec : le ticket distingue les deux dans
> son bilan, et le rendu les distingue aussi — `[--]` et non `[KO]`.

### 39. `NB_WORKER` pouvait dépasser la taille des pools

`Database` ouvrait au maximum 10 connexions (`max_connections`, en dur). Au-delà de
`NB_WORKER = 10`, les workers supplémentaires se seraient attendus sur `pool.acquire()` : le
parallélisme annoncé n'aurait pas existé, sans que rien ne le signale.

> **Corrigé** : les deux pools sont dimensionnés sur `max(10, NB_WORKER)`
> (`app/db/mysql.py`). Chaque worker ne détient au plus qu'une connexion par pool à un instant
> donné, l'égalité suffit donc.

### 40. Le cas « aucun scénario éligible » n'est pas prévu

> **Retenu** : ce n'est pas une erreur. Bilan à zéro, code de retour `0` — un batch qui ne
> trouve rien a simplement fini son travail.

---

## Ce qui reste à décider

| Constat | Question | Pour qui |
| ------- | -------- | -------- |
| 24 | La correspondance produit → famille de clé est-elle la bonne ? (`CO:colis, OO:oo, IP:potentielip, OS/PR/PPI:3s`) | Métier / data |
| 27 | `densite` 0/1/2 correspond-elle bien à `dense` / `faible1` / `faible2` ? | Métier |
| 22 | « Référentiel actif » : convention « dernier id du site », ou colonne dédiée ? | DSR-701 / DSR-697 |
