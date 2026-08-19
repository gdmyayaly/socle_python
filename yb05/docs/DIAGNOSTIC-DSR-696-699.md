# Diagnostic des tickets DSR-696, DSR-698 et DSR-699

Confrontation ligne à ligne des trois tickets (`docs/DSR-696.md`, `docs/DSR-698.md`,
`docs/DSR-699.md`) au schéma réellement déployé (`python/db/db_new.sql`, ré-extrait le
17/08/2026) et aux scripts livrés dans `db/`.

**Principe retenu** : le ticket exprime l'intention métier, la base fait foi sur les noms.
Quand les deux divergent, on tranche par l'intention — et l'écart est tracé ici. Une fois sur
deux, c'est le ticket qui a raison et c'est la base qu'il faut corriger : voir le constat 10.

Vingt constats. Seize sont traités dans les scripts, quatre restent ouverts.

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
| 9 | 696 | Débordement décimal possible sur les totaux | **Ouvert** — contrôle à jouer |
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

### 9. Débordement décimal — **ouvert**

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

Si le maximum approche `999999`, élargir les trois colonnes `trafic_*_total` avant de charger.

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

## Ce qui reste à décider

| Constat | Question | Pour qui |
| ------- | -------- | -------- |
| 5 | La table d'agrégats doit-elle clôturer les jeux des référentiels antérieurs, ou l'appartenance à un référentiel suffit-elle ? | DSR-697 |
| 9 | Les totaux peuvent-ils dépasser six chiffres entiers ? Et ces `decimal(x,19)` portent-ils des volumes ou déjà des ratios ? | Équipe data |
| 13 | Un site peut-il avoir plusieurs versions de clés actives simultanément ? | Métier |
| 15 | « Référentiel actif » : colonne dédiée, ou convention « dernier id du site » ? | DSR-701 |
| 17 | Que vaut la clé d'un site dont le total d'une famille de trafic est nul ? | Métier |
