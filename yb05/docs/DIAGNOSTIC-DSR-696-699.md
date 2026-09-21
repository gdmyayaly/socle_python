# Diagnostic des tickets DSR-696 à DSR-699

Confrontation ligne à ligne des quatre tickets (`docs/DSR-696.md`, `docs/DSR-697.md`,
`docs/DSR-698.md`, `docs/DSR-699.md`) au schéma réellement déployé (`python/db/db_new.sql`,
ré-extrait le 17/08/2026) et aux scripts livrés dans `db/`.

**Principe retenu** : le ticket exprime l'intention métier, la base fait foi sur les noms.
Quand les deux divergent, on tranche par l'intention — et l'écart est tracé ici. Une fois sur
deux, c'est le ticket qui a raison et c'est la base qu'il faut corriger : voir le constat 10.

Vingt-six constats. Trois restent ouverts — les constats 5 et 15, et la question de fond
soulevée par le 9 ; les autres sont traités dans les scripts.

DSR-697 est arrivé après les trois autres, alors que ses scripts étaient déjà écrits : ses
constats sont numérotés à la suite (21 à 26) et sa section vient en dernier, pour ne pas
décaler les renvois existants. Dans la chaîne, c'est pourtant lui qui vient **en premier**.

## Synthèse

| # | Ticket | Constat | Statut |
| - | ------ | ------- | ------ |
| 1 | 696 | Table renommée `trppu_site_trafic` → `trppu_trafic_site` | Traité |
| 2 | 696 | La structure annoncée ne contient aucune colonne de site | Traité |
| 3 | 696 | `id_site_trafic` confondu avec un identifiant de site | Traité |
| 4 | 696 | L'`INSERT` d'exemple garde `id_site`, colonne inexistante | Traité |
| 5 | 696 | `date_fin_validite` n'est jamais alimentée | **Ouvert** — renvoyé à DSR-697 |
| 6 | 696 | CA1 contredit CA4 | Traité — lecture explicitée |
| 7 | 696 | L'exemple nomme les sites `SITE_A`, la colonne porte un code régate | Cosmétique |
| 8 | 696 | `TRPPU_CLE_REPARTITION_CALCULE` au singulier | Hors périmètre |
| 9 | 696 | Débordement décimal sur les totaux | **Survenu, corrigé** — `db/fix_error.sql` |
| 10 | 698 | `date_creation` attendue par le ticket, absente du schéma | Traité — colonne rétablie |
| 11 | 698 | `date_fin_validite` ignorée par le ticket | Traité — posée à la désactivation |
| 12 | 698 | `libelle` ignorée par le ticket | Traité — paramètre à NULL |
| 13 | 698 | La désactivation de la version précédente n'est pas demandée | Traité — à confirmer métier |
| 14 | 698 | Index `(co_regate, actif)` désormais fourni par la base | Traité — bloc retiré |
| 15 | 698 | « Référentiel actif » inexprimable en base | **Ouvert** — bloque DSR-701 |
| 16 | 699 | `TRPPU_SITE_TRAFIC`, ancien nom de table | Traité |
| 17 | 699 | Division par zéro sur un site à trafic nul | Traité — échec explicite |
| 18 | 699 | Division entière : la clé potentiel IP perdrait 14 décimales | Traité — `CAST` |
| 19 | 699 | CA1 suppose que chaque site a une version active | Traité — garde-fous |
| 20 | 699 | « Alerte dans les logs » impossible en SQL pur | Traité — verdict rendu à l'appelant |
| 21 | 697 | Les contrôles écrivent `SELECT COUNT FROM …`, sans parenthèses | Traité |
| 22 | 697 | La déduplication RG4 ne suffit pas à garantir la RG5 | Traité — contrôle amont |
| 23 | 697 | `id_referentiel` codé en dur dans les deux instructions | Traité — paramètre de session |
| 24 | 697 | Le chemin du fichier n'est paramétrable par aucun moyen | Traité — documenté |
| 25 | 697 | Rien n'impose le mode strict : `LOAD DATA` tronque en silence | Traité — `sql_mode` durci |
| 26 | 697 | Ordre des colonnes, et anciens noms de tables dans les CA | Cosmétique |

---

## DSR-696

### 1. La table a été renommée, l'amendement du ticket est incomplet

Le ticket a été amendé à la main : `TRPPU_SITE_TRAFIC` devient `TRPPU_TRAFIC_SITE` aux lignes
44, 137, 146, 179 et 214 — mais les deux noms y cohabitent sur la même ligne, et la ligne 39
(« Table cible ») a été oubliée. Le dump confirme le renommage : la table s'appelle
`trppu_trafic_site`.

> **Traité** : `trppu_trafic_site` dans les trois scripts, le README et les tests. Un test de
> non-régression (`test_aucun_script_ne_vise_l_ancien_nom_de_table`) interdit le retour de
> l'ancien nom dans le SQL exécutable.
>
> Le nom de l'index `uq_site_trafic` créé par la migration est délibérément **conservé** : là
> où la migration a déjà été jouée avant le renommage, l'index a suivi sa table sous ce nom,
> et le garde-fou de rejouabilité doit pouvoir le reconnaître.

### 2. La structure annoncée n'a pas de colonne de site

La structure du ticket (l. 44-56) liste `id_site_trafic, id_referentiel, trafic_*,
date_debut_validite, date_fin_validite`. **`co_regate_site` en est absente** : une table
d'agrégats « par site » sans colonne de site.

> **Traité** : structure réelle utilisée par le script —
> `(id_site_trafic PK, id_referentiel, co_regate_site, trafic_colis_total, trafic_oo_total,
> trafic_3s_total, potentielip_total, date_debut_validite, date_fin_validite, date_creation)`.

### 3. `id_site_trafic` n'est pas un identifiant de site

L'amendement a remplacé `id_site` par `id_site_trafic` dans la structure. C'est un
contresens : `id_site_trafic` est la **PK `AUTO_INCREMENT`** de la table, pas la référence du
site. L'alimenter explicitement casserait la séquence.

> **Traité** : la colonne n'est jamais citée dans l'`INSERT`, comme `date_creation`. Test :
> `test_dsr696_n_insere_que_des_colonnes_existantes`.

### 4. L'`INSERT` d'exemple reste sur `id_site`

L'amendement n'a pas touché l'`INSERT` de la ligne 148, qui liste toujours `id_site` — colonne
qui n'a jamais existé. Recopié tel quel : `ERROR 1054 (Unknown column 'id_site')`.

Le ticket porte pourtant en lui-même de quoi trancher : son `SELECT` (l. 158) place
`co_regate_site` en première position, face à `id_site` en première position de l'`INSERT`. Le
mapping positionnel est sans ambiguïté — `id_site` désignait bien le site.

> **Traité** : noms réels dans le script.
> `test_aucun_script_ne_reprend_la_colonne_fantome_du_ticket` verrouille le point.

### 5. `date_fin_validite` n'est jamais alimentée — **ouvert**

Le ticket écrit `NULL` en dur dans l'`INSERT`, et historise par `DELETE`/`INSERT` sur le
référentiel. Aucune instruction, nulle part, ne pose de fin de validité. Conséquence : **toutes
les lignes de la table, tous référentiels confondus, portent `NULL`**. La colonne existe mais
ne discrimine rien, et un consommateur ne peut pas identifier le jeu d'agrégats courant par
`WHERE date_fin_validite IS NULL` — il doit filtrer sur `id_referentiel`.

> **Décision** : on reste fidèle au ticket. La clôture des jeux antérieurs n'est pas ajoutée
> d'office — elle relève de DSR-697, qui définira la notion de jeu courant. Le comportement et
> sa conséquence sont commentés dans `db/DSR-696_site_trafic.sql`, étape 2.

### 6. CA1 contredit CA4

> CA1 — « L'ensemble des sites présents dans TRPPU_CLES_REPARTITION est présent dans
> TRPPU_TRAFIC_SITE. »
> CA4 — « Les sites sans PDI actif ne sont pas chargés. »

Un site dont tous les PDI sont clôturés est « présent dans TRPPU_CLES_REPARTITION » au sens
littéral, et ne doit pourtant pas être chargé. Les deux critères ne sont conciliables que si
CA1 s'entend **des sites ayant au moins un PDI actif**.

> **Traité** : lecture explicitée en commentaire, et le contrôle CA1 du script applique le
> même filtre RG1 (`date_fin_validite IS NULL`) que le calcul.

### 7. L'exemple nomme les sites `SITE_A` / `SITE_B`

`co_regate_site` porte un code régate (`char(6)` en source, `varchar(10)` en cible). Sans
incidence, mais l'exemple ne peut pas être rejoué tel quel.

### 8. `TRPPU_CLE_REPARTITION_CALCULE` au singulier

Le schéma de flux du ticket (l. 216) écrit `TRPPU_CLE_REPARTITION_CALCULE` ; la table réelle
est `trppu_cles_repartition_calcule`. Hors périmètre de DSR-696, à traiter en DSR-697.

### 9. Débordement décimal — **survenu en recette, corrigé**

| | Source (`trppu_cles_repartition`) | Cible (`trppu_trafic_site`) |
| --- | --- | --- |
| trafics colis / oo / 3s | `decimal(25,19)` | `decimal(24,18)` |
| potentiel IP | `smallint` nullable | `bigint NOT NULL` |

`decimal(24,18)` n'autorise que six chiffres avant la virgule, soit
`999999.999999999999999999` au maximum — or on y écrit la **somme** de valeurs qui peuvent
elles-mêmes atteindre ce plafond. Le ticket est muet sur le sujet.

Contrôle à jouer **avant** le premier chargement réel :

```sql
SELECT MAX(t) FROM (
  SELECT SUM(trafic_colis) t FROM trppu_cles_repartition
   WHERE id_referentiel = 1 AND date_fin_validite IS NULL GROUP BY co_regate_site) x;
```

Si le maximum approche `999999`, les colonnes sont trop étroites.

**C'est arrivé.** L'`INSERT` de DSR-696 a échoué en `ERROR 1264 (Out of range value for column
'trafic_oo_total')` au premier chargement réel.

> **Traité** : `db/fix_error.sql` porte les trois totaux à `decimal(35,19)` — seize chiffres
> avant la virgule, de quoi contenir la somme de la table entière (2,24 × 10¹³, quatorze
> chiffres), et dix-neuf décimales, l'échelle de la source. Ce second point n'est pas
> cosmétique : à dix-huit décimales, MySQL arrondit la somme, et le contrôle CA1+CA3 de
> DSR-696 — qui compare le total stocké à la somme recalculée — afficherait des `ecart_*` non
> nuls de l'ordre de 10⁻¹⁹. Le correctif est rejouable, gardé par la définition courante des
> colonnes et non par leur nom.
>
> Le script est à jouer avant de relancer DSR-696, qui recalcule tout le référentiel : son
> `DELETE` ayant été validé avant l'échec de l'`INSERT`, les agrégats sont perdus.

**La question de fond reste ouverte**, et le correctif ne la tranche pas : des colonnes à
dix-neuf décimales laissent penser que la source porte déjà des ratios plutôt que des volumes.
Le second constat de `fix_error.sql` affiche, pour les dix plus gros sites, le nombre de
chiffres entiers réellement atteint — de quoi poser la question à l'équipe data sur pièces.

---

## DSR-698

### 10. `date_creation` : c'est le ticket qui a raison

Le ticket liste `date_creation` parmi les colonnes de la ligne créée, avec l'exemple
« 01/09/2026 ». Le schéma ré-extrait le 17/08/2026 l'a **supprimée**, au profit du couple
`date_debut_validite` (`DEFAULT CURRENT_TIMESTAMP`) / `date_fin_validite`.

Les deux informations ne se confondent pas : la période de validité dit depuis quand la version
est utilisable, la date de création dit quand la ligne a été écrite. Elles coïncident lors d'une
création active, rien ne l'impose ensuite.

> **Traité** : `DSR-696-699_migration.sql` bloc 3 rétablit
> `date_creation datetime NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER actif`. Le `DEFAULT`
> l'alimente seul — aucun script ne l'écrit, et le contrôle final de DSR-698 restitue les trois
> dates. Test : `test_dsr698_restitue_les_trois_dates`.
>
> L'`ALTER` est idempotent (garde-fou sur `information_schema.COLUMNS`) et sans risque tant que
> la table est vide, ce qui est le cas. Sur une table peuplée, les lignes existantes
> prendraient l'horodatage de l'`ALTER`, pas leur vraie date de création.

### 11. `date_fin_validite` ignorée par le ticket

Le ticket ne mentionne pas la colonne — il décrit encore le schéma d'avant. Laisser une version
désactivée (`actif = 'N'`) avec une fin de validité vide ferait se contredire les deux colonnes.

> **Traité** : l'`UPDATE` de désactivation pose `actif = 'N', date_fin_validite = NOW()`.
> Test : `test_dsr698_clot_la_version_desactivee`.

### 12. `libelle` ignorée par le ticket

`varchar(100)` nullable, présente en base, absente du ticket.

> **Traité** : exposée en paramètre `@libelle`, à `NULL` par défaut — à alimenter si le métier
> veut y voir autre chose que le commentaire.

### 13. La désactivation de la version précédente n'est pas demandée — **à confirmer**

Le ticket ne la mentionne nulle part. Elle est pourtant impliquée par la colonne `actif` et par
la règle « une nouvelle version par changement de périmètre » : sans elle, la lecture
d'éligibilité de DSR-701 règle 9 (`WHERE co_regate = ? AND actif = 'O'`) renverrait plusieurs
lignes et deviendrait ambiguë.

> **Traité, sous réserve** : la version active précédente du site est désactivée. Si le métier
> veut autoriser plusieurs versions actives simultanées, il suffit de retirer l'`UPDATE`.

### 14. L'index `(co_regate, actif)` est désormais fourni par la base

La migration créait `idx_vc_site_actif (co_regate, actif)`. Le schéma ré-extrait porte le même
index sous le nom `idx_regate_actif`. Le garde-fou de la migration testant le **nom** et non les
colonnes, il ne l'aurait pas vu et aurait posé un second index redondant.

> **Traité** : bloc retiré de la migration. Le contrôle final du fichier liste malgré tout
> `idx_regate_actif`, pour vérifier d'un coup d'œil que DSR-698 dispose de son index d'accès.

### 15. « Référentiel actif » n'est pas exprimable en base — **ouvert**

DSR-701 règle 10 exige de trouver le référentiel actif d'un site. `trppu_referentiel` ne porte
que `id_referentiel`, `co_regate`, `date_reference`, `commentaire` :

- **aucune colonne `actif`** — « actif » se réduit à « le plus grand id du site », ce qui
  interdit de désactiver un référentiel sans en créer un autre ;
- **`co_regate` est nullable et sans index** — d'éventuels référentiels nationaux
  (`co_regate IS NULL`) ne seraient jamais retournés ;
- aucune date de fin de validité, alors que toutes les tables filles en portent une.

À arbitrer avant DSR-697 / DSR-701 : soit on aligne la table, soit on documente que « actif =
dernier id du site » est la définition officielle.

---

## DSR-699

C'est **le ticket le mieux formulé des trois** : sa liste de colonnes correspond exactement à
`trppu_cles_repartition_calcule` — dix colonnes, mêmes noms, même ordre. Ses pièges ne sont pas
dans les noms mais dans le calcul.

### 16. `TRPPU_SITE_TRAFIC` (ligne 24)

Le ticket désigne la table des agrégats sous son ancien nom, contrairement à DSR-696 qui a été
amendé (constat 1).

> **Traité** : `trppu_trafic_site` dans le script.

### 17. Division par zéro — décision : échec explicite

Un site dont un total de trafic vaut zéro produit une division par zéro. En MySQL, celle-ci
vaut `NULL`, refusé par les quatre colonnes cibles `NOT NULL` — **mais uniquement si le serveur
est en mode strict**. Sur un serveur laxiste, la même ligne passerait avec un simple
avertissement et une clé fausse.

Le cas est réaliste, et d'abord sur `potentielip` : un site dont aucun PDI ne porte de potentiel
IP a un total à zéro, sans que rien ne soit anormal par ailleurs.

> **Décision : échouer, plutôt que masquer.** Mieux vaut une `ERROR 1365` qu'un jeu de clés
> silencieusement faux, sur lequel tout le calcul de trafic des scénarios s'appuiera ensuite.
> Trois conséquences dans le script :
>
> - il durcit son propre `sql_mode` de session (`STRICT_ALL_TABLES`,
>   `ERROR_FOR_DIVISION_BY_ZERO`), pour que l'échec ne dépende pas de la configuration du
>   serveur ;
> - un garde-fou liste les sites à dénominateur nul **avant** l'`INSERT`, de sorte que
>   l'erreur, si elle survient, soit déjà expliquée ;
> - aucun `NULLIF` ne protège les dénominateurs, et le seul `COALESCE` porte sur `potentielip`,
>   numérateur nullable. `test_dsr699_ne_masque_aucun_denominateur_nul` verrouille ce point.
>
> Ce qu'il faut faire d'un site qui remonte est une **question métier** : que vaut « sa part »
> d'un trafic dont il n'a rien ? Le script ne tranche pas à la place du métier.

### 18. La clé potentiel IP perdrait quatorze décimales

`potentielip` est un `smallint`, `potentielip_total` un `bigint` : la division est **entière**.
Or pour MySQL, l'échelle du résultat d'une division est celle du premier opérande augmentée de
`div_precision_increment` — **4 par défaut**. La clé serait donc calculée à 10⁻⁴ près, puis
stockée dans un `decimal(24,18)` qui laisserait croire à dix-huit décimales significatives.

Le contrôle CA3 ne verrait rien : une somme de clés arrondies au dix-millième vaut toujours 1 à
la tolérance près. L'erreur ne se manifesterait que plus loin, dans la répartition des trafics.

> **Traité** : `CAST(COALESCE(c.potentielip, 0) AS DECIMAL(24,18)) / s.potentielip_total`. Les
> trois autres clés partent d'un `decimal(25,19)` et ne sont pas concernées.
> `test_dsr699_cast_la_cle_potentiel_ip` verrouille le point, et `db/README.md` donne le
> contrôle à jouer sur données réelles.

### 19. Le CA1 suppose que chaque site a une version active

> CA1 — « Une ligne de clés est créée pour chaque PDI actif du référentiel. »

Le calcul joint `trppu_version_cle` pour satisfaire le CA2 (toute clé rattachée à une version).
Un site sans agrégat DSR-696, ou sans version active DSR-698, est donc écarté par la jointure —
**silencieusement**. Ses PDI n'ont pas de clés, et rien dans le résultat ne le dit.

> **Traité** : le premier garde-fou du script compte `nb_sites_sans_agregat` et
> `nb_sites_sans_version` avant d'écrire — les deux doivent valoir 0 — et le contrôle CA1 liste
> après coup les PDI actifs restés sans clé.

### 20. « Alerte dans les logs » — hors de portée du SQL

Le ticket demande, quand une somme de clés sort de `[0,9999 ; 1,0001]`, « une alerte dans les
logs indiquant le site et la clé et la somme obtenue ». Un script SQL ne sait pas journaliser.

> **Traité** : le contrôle CA3 rend, par site, les quatre sommes et un verdict `OK` /
> `ANOMALIE`, les anomalies triées en tête. C'est l'appelant — socle ou exploitant — qui
> journalise. Documenté dans `db/README.md`.

---

## DSR-697

Le ticket de tête de chaîne : le chargement du CSV métier dans `trppu_cles_repartition`, table
que les trois autres se contentent de lire. Sur les **noms**, il est juste — sa structure cible
contient bien les dix-neuf colonnes de la table, et son `LOAD DATA` est exécutable presque tel
quel. Les écarts portent sur ce qu'il ne dit pas, et sur des contrôles qui ne compilent pas.

### 21. Les contrôles ne compilent pas

Les contrôles 1 et 3 s'écrivent `SELECT COUNT FROM trppu_cles_repartition WHERE …`. Sans
parenthèses, `COUNT` n'est pas la fonction d'agrégat mais un **nom de colonne** — qui n'existe
pas : `ERROR 1054 (Unknown column 'COUNT')`. Le contrôle 2, lui, écrit correctement `COUNT(*)`.

> **Traité** : `COUNT(*)` dans les six contrôles du script. Un test de non-régression
> (`test_aucun_script_ne_reprend_le_count_sans_parentheses`) interdit la forme fautive dans
> tous les scripts de `db/`.

### 22. La déduplication demandée ne garantit pas l'unicité exigée

La RG4 définit le doublon comme une ligne strictement identique — même PDI, même site, mêmes
trafics, même potentiel IP — et le `SELECT DISTINCT *` de l'étape de préparation l'élimine. La
RG5, elle, exige l'unicité de `(id_pdi, id_referentiel)`, que la base porte sous
`uk_pdi_ref`.

Les deux règles ne se recouvrent pas : **deux lignes d'un même PDI aux trafics différents
survivent au `DISTINCT`** et violent la RG5. Le ticket ne prévoit rien pour ce cas, et son
contrôle 2 ne le détecte qu'après coup — alors que le chargement aura déjà échoué.

> **Traité** : le chargement échoue franchement (ni `IGNORE`, ni `REPLACE` sur le `LOAD
> DATA` : `ERROR 1062`), et `db/README.md` ajoute à la préparation du fichier un contrôle
> d'unicité des PDI à jouer **avant** le dépôt. Quelques secondes de DuckDB contre un
> chargement complet perdu.

### 23. `id_referentiel` est codé en dur

Le ticket fixe `id_referentiel = 1` dans le `DELETE` comme dans le `SET` du `LOAD DATA`, et
l'illustre par « Référentiel n°1 ». Recopié tel quel pour un deuxième référentiel, le script
purgerait le premier puis chargerait par-dessus.

> **Traité** : `@id_referentiel`, en tête de fichier, comme dans les trois autres scripts. Le
> garde-fou affiche avant toute écriture le nombre de lignes que la purge va supprimer, et
> vérifie que le référentiel est déclaré dans `trppu_referentiel` — aucune clé étrangère ne
> l'impose, rien n'empêcherait de charger 22 M de lignes sous un identifiant inexistant.

### 24. Le chemin du fichier, lui, ne peut pas l'être

`LOAD DATA` n'accepte qu'un **littéral** comme chemin : ni variable de session, ni
concaténation. Le contournement employé par `DSR-696-699_migration.sql` — construire
l'instruction dans une chaîne et la jouer par `PREPARE`/`EXECUTE` — ne s'applique pas non
plus, `LOAD DATA` ne figurant pas parmi les instructions préparables.

> **Traité** : le chemin reste un littéral dans le corps du script, signalé en majuscules dans
> l'en-tête et dans `db/README.md`. C'est le seul paramètre de la chaîne qui ne se règle pas
> en tête de fichier, et il doit être changé **en même temps** que `@id_referentiel` : charger
> `…_ref1.csv` sous `@id_referentiel := 2` ne produit aucune erreur, seulement un référentiel
> faux.

### 25. Rien n'impose le mode strict

Hors mode strict, `LOAD DATA` ne rejette presque rien : une valeur non numérique devient 0,
une chaîne trop longue est tronquée, une date invalide devient `'0000-00-00'`. Le chargement
se termine « avec succès », sur un avertissement. Pour une photographie du référentiel dont
tout le calcul des clés dépend, c'est le pire des comportements : un trafic ramené à zéro ne
se voit plus nulle part ensuite.

Deux pièges concrets s'y rattachent. Un CSV produit sous Windows se termine par `\r\n` : avec
le `LINES TERMINATED BY '\n'` du ticket, le `\r` reste collé au dernier champ — `potentielip`,
dont la conversion numérique échoue alors, ou pas, selon le mode. Et les quatre `NULLIF` de la
RG3 sont ce qui distingue « champ vide » de « zéro » sur `nb_pre` et `potentielip` : sans eux,
un potentiel IP inconnu deviendrait un potentiel IP nul, et DSR-699 en ferait une clé.

> **Traité** : `STRICT_ALL_TABLES`, `NO_ZERO_DATE` et `NO_ZERO_IN_DATE` ajoutés au `sql_mode`
> **de la session** du script, comme le fait DSR-699 pour la division par zéro. Les quatre
> `NULLIF` sont verrouillés par `test_dsr697_convertit_les_quatre_champs_vides_en_null`, et le
> cas `\r\n` est documenté dans le script comme dans les erreurs typiques du README.

### 26. Ordre des colonnes, et anciens noms de tables

La structure cible du ticket liste les dix-neuf colonnes de la table — aucune ne manque,
aucune n'est inventée, ce qui le distingue nettement de DSR-696. Seule la place de
`potentielip` diffère : le ticket la met après `trafic_3s`, la base après `nb_pre`. Sans
conséquence, la liste du `LOAD DATA` décrivant l'ordre du **fichier** et non celui de la table.

Le CA7 et le diagramme final, en revanche, reprennent `TRPPU_SITE_TRAFIC` (ancien nom, cf.
constat 1) et `TRPPU_CLE_REPARTITION` au singulier (cf. constat 8).

> **Cosmétique** : les noms réels sont utilisés dans le script et dans `db/README.md`. Rien à
> corriger dans le SQL.

---

## Ce qui reste à décider

| Constat | Question | Pour qui |
| ------- | -------- | -------- |
| 5 | La table d'agrégats doit-elle clôturer les jeux des référentiels antérieurs, ou l'appartenance à un référentiel suffit-elle ? | Métier — DSR-697 ne tranche pas |
| 9 | Ces `decimal(x,19)` portent-ils des volumes ou déjà des ratios ? (le débordement, lui, est corrigé) | Équipe data |
| 13 | Un site peut-il avoir plusieurs versions de clés actives simultanément ? | Métier |
| 15 | « Référentiel actif » : colonne dédiée, ou convention « dernier id du site » ? | DSR-701 |
| 17 | Que vaut la clé d'un site dont le total d'une famille de trafic est nul ? | Métier |
