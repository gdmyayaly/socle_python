# API OPTIPACC — Services TRPPU exposés à l'application OPTIPACC

> Module : `app/routes/trppu_optipacc/`
> Préfixe HTTP : `/trppu-api/optipacc`
> Tag Swagger : **OPTIPACC**
> Tickets : **DSR-690** (liste des scénarios) · **DSR-689** (volumes bruts) ·
> **DSR-705** (trafics Agrébal) · **DSR-707** (mise en production)

TRPPU met à disposition d'OPTIPACC quatre services sans état. Trois sont en **lecture
seule** et permettent de récupérer directement le résultat des calculs TRPPU : OPTIPACC n'a
rien à recalculer ni à réagréger, TRPPU reste seul responsable du calcul. Le quatrième
(DSR-707) est le seul en écriture : il acte la mise en production d'un scénario.

Les services sont regroupés sous le segment `/optipacc` pour être identifiables sans
ambiguïté par les applications tierces et pouvoir évoluer indépendamment des routes qui
servent l'IHM TRPPU.

**Parcours type côté OPTIPACC**

1. L'utilisateur saisit un site (code Regate) → appel de **`site-liste-scenarios`** pour
   alimenter la liste déroulante des scénarios sélectionnables.
2. L'utilisateur choisit un scénario → appel de **`scenario-trafic-brut`** pour récupérer
   les volumes par produit et construire les charges de travail.
3. Pour construire les organisations de distribution → appel de **`trafic-amas`**, qui
   descend au niveau Agrébal × jour × produit × densité.
4. Une fois les simulations terminées, le scénario retenu est acté par
   **`scenario/mise-en-production`** : TRPPU historise la date de mise en œuvre et fige
   définitivement le scénario.

---

## 1. Vue d'ensemble

| Méthode | Chemin | Service (Jira) | Description |
|---------|--------|----------------|-------------|
| `GET` | `/trppu-api/optipacc/site-liste-scenarios?codeRegate=` | `S_SiteListeScenarios` (DSR-690) | Scénarios exploitables d'un site |
| `POST` | `/trppu-api/optipacc/scenario-trafic-brut` | `S_ScenarioTraficBrut` (DSR-689) | Volume brut final par produit |
| `POST` | `/trppu-api/optipacc/trafic-amas` | DSR-705 | Trafics Agrébal calculés, paginés |
| `POST` | `/trppu-api/optipacc/scenario/mise-en-production` | DSR-707 | Déclare un scénario en production |

`site-liste-scenarios` est une simple lecture paramétrée par un seul champ : il est exposé
en `GET`, avec `codeRegate` en paramètre de requête. Les trois autres sont en `POST` avec
un corps JSON.

Tous les services acceptent un paramètre de requête optionnel `?id_session_ihm=` utilisé
uniquement pour la traçabilité (regroupement des lignes de log dans Kibana).

> **Nommage des corps JSON.** DSR-689/690 sont en camelCase (`codeRegate`, `scenarioId`),
> DSR-705/707 en snake_case (`code_regate`, `scenario_id`). Chaque contrat reproduit son
> ticket : l'écart est dans les spécifications, pas dans l'implémentation.

Documentation interactive : **`/docs`** (Swagger UI), tag « OPTIPACC ».

---

## 2. `GET /trppu-api/optipacc/site-liste-scenarios` — DSR-690

Retourne les scénarios d'un site que TRPPU considère comme **prêts à être exploités** par
OPTIPACC.

### 2.1 Entrée

```
GET /trppu-api/optipacc/site-liste-scenarios?codeRegate=123456
```

| Paramètre de requête | Type | Obligatoire | Règle |
|----------------------|------|-------------|-------|
| `codeRegate` | string | oui | exactement 6 caractères alphanumériques |

### 2.2 Sortie

```json
{
  "codeRegate": "123456",
  "scenarios": [
    { "id_scenario": 125, "lb_scenario": "Scénario Septembre 2026" },
    { "id_scenario": 128, "lb_scenario": "Scénario Vieillissement" }
  ]
}
```

### 2.3 Quels scénarios sont retournés ?

Un scénario n'apparaît dans la liste que si **les deux conditions** sont réunies :

| Condition | Signification métier |
|-----------|----------------------|
| statut = **`VALIDE`** | le scénario a été validé dans TRPPU |
| **trafics Agrébal calculés** | le traitement de calcul des trafics est allé au bout avec succès |

Sont donc **exclus** : les scénarios « EN COURS », « SIMULATION », « EN PRODUCTION »,
« ARCHIVE », ainsi que tout scénario validé dont le calcul Agrébal n'a pas encore tourné
ou a échoué.

> **Le cas le plus fréquent d'une liste vide** : le scénario existe et est bien validé,
> mais le traitement de calcul des trafics Agrébal n'a pas encore été exécuté. Il ne
> deviendra visible pour OPTIPACC qu'à la fin de ce traitement.

### 2.4 Aucun scénario éligible

Ce n'est pas une erreur technique : le service répond **200** avec une liste vide et un
message explicatif à afficher à l'utilisateur.

```json
{
  "codeRegate": "654321",
  "scenarios": [],
  "message": "Aucun scénario trouvé pour le site 654321."
}
```

Un code Regate inconnu produit la même réponse (ce service ne contrôle pas l'existence du
site).

---

## 3. `POST /trppu-api/optipacc/scenario-trafic-brut` — DSR-689

Retourne, pour un site et un scénario, le **volume brut final de chaque produit**.

### 3.1 Entrée

```json
{ "codeRegate": "123456", "scenarioId": 789 }
```

| Champ | Type | Obligatoire | Règle |
|-------|------|-------------|-------|
| `codeRegate` | string | oui | 6 caractères alphanumériques |
| `scenarioId` | entier | oui | ≥ 1 ; le scénario doit appartenir à ce site |

Aucun autre champ n'est accepté : le corps est validé en `extra="forbid"`, un champ inconnu
donne un `422`.

### 3.2 Sortie

```json
{
  "codeRegate": "123456",
  "scenarioId": 789,
  "produits": [
    { "codeProduit": "CO", "volumeBrut": 791000 },
    { "codeProduit": "EP", "volumeBrut": 45000 },
    { "codeProduit": "IP", "volumeBrut": 5900000 },
    { "codeProduit": "OS", "volumeBrut": 1250000 },
    { "codeProduit": "PQ", "volumeBrut": 250000 }
  ]
}
```

Les produits sont triés par code produit. La restitution est **au niveau Produit
uniquement** : aucun sous-produit n'est retourné.

### 3.3 Ce que contient — et ne contient pas — le volume brut

**Le volume brut est la somme, pour chaque produit :**

```
   trafic constaté (réalisé)
 + trafic prévisionnel recalculé
 + trafic(s) manuel(s) éventuel(s)
```

Toutes les interventions de l'utilisateur dans TRPPU sont donc reflétées : ajustements du
prévisionnel, ajouts de trafics manuels, corrections de trafic, scénarios recalculés.
OPTIPACC récupère toujours **la dernière valeur consolidée disponible**.

**Ne sont volontairement pas restitués** (règle RG6 du ticket) : le trafic constaté seul,
le prévisionnel seul, les trafics manuels séparément, les TMH, et tout détail de calcul.
Seule la valeur finale exploitable est transmise.

**Ne sont pas comptés dans le volume brut :**

| Élément | Raison |
|---------|--------|
| Les **comptages manuels** (comptages physiques datés saisis dans TRPPU) | Ce sont des données de comptage distinctes, qui ne sont pas reportées dans les trafics du scénario. Les additionner créerait un double comptage. **À confirmer par le métier.** |
| Les **produits exclus** du scénario | Voir §3.4 |

### 3.4 Produits exclus

Un produit que l'utilisateur a marqué comme **exclu** dans le tableau des trafics du
scénario n'est **jamais** restitué à OPTIPACC : l'exclusion est une décision utilisateur
que le service respecte sans dérogation possible. Il n'existe pas d'option pour les
inclure.

### 3.5 Quels scénarios sont interrogeables ?

| Condition | Détail |
|-----------|--------|
| Le site existe | sinon 404 |
| Le scénario existe **et appartient à ce site** | sinon 404 |
| statut ∈ { **`VALIDE`**, **`EN PRODUCTION`** } | sinon 409 |
| trafics Agrébal calculés | sinon 409 |

> **Différence assumée avec DSR-690** : le service de liste ne propose que les scénarios
> `VALIDE`, alors que ce service accepte aussi `EN PRODUCTION`. Objectif : un projet
> OPTIPACC déjà créé sur un scénario continue de fonctionner après la mise en production
> de ce scénario, même s'il n'est plus proposé dans la liste de sélection.

---

## 4. Gestion des erreurs

| Cas | Code HTTP | Corps |
|-----|-----------|-------|
| Aucun scénario éligible (DSR-690) | `200` | liste vide + `message` (cf. §2.4) |
| Site inconnu (DSR-689) | `404` | `{"detail": "Site 123456 introuvable."}` |
| Scénario inexistant ou rattaché à un autre site | `404` | `{"detail": "Scénario 789 introuvable pour le site 123456."}` |
| Scénario non exploitable | `409` | `{"detail": "Le scénario 789 n'est pas disponible pour OPTIPACC (statut=EN COURS, trafic_agrebal_calcule=0)."}` |
| Requête invalide (champ manquant, code Regate mal formé) | `422` | détail des champs en erreur |
| Erreur technique | `500` | `{"detail": "Une erreur est survenue lors de la récupération des trafics."}` |

Le message du 409 précise **pourquoi** le scénario est refusé (statut et état du calcul
Agrébal), ce qui permet à l'utilisateur OPTIPACC de savoir s'il doit attendre le calcul ou
faire valider le scénario dans TRPPU.

Un scénario sans aucun trafic renvoie `200` avec `"produits": []`.

---

## 5. Exemples d'appel

```bash
# Liste des scénarios exploitables du site 123456
curl "http://localhost:8080/trppu-api/optipacc/site-liste-scenarios?codeRegate=123456"

# Volumes bruts du scénario 789
curl -X POST http://localhost:8080/trppu-api/optipacc/scenario-trafic-brut \
  -H "Content-Type: application/json" \
  -d '{"codeRegate":"123456","scenarioId":789}'
```

---

## 6. Performance et exploitation

- Les deux services sont **sans état** et compatibles avec un appel unitaire à
  l'ouverture ou à la création d'un projet OPTIPACC.
- Lecture seule : ils n'écrivent rien et ne modifient aucun scénario.
- Requêtes en un seul aller-retour base, sur les index existants
  (`co_regate` + `statut` pour la liste, `id_scenario` pour les volumes).
- Chaque appel est tracé dans les logs applicatifs (code Regate, identifiant de scénario,
  nombre de lignes, durée) et exploitable dans Kibana.

---

## 7. Points ouverts à valider avec le métier

1. **Nomenclature des codes produits.** Les tickets citent `OS`, `IP`, `CO`, `EP`, `PQ` et
   renvoient à « la nomenclature commune validée entre TRPPU et OPTIPACC ». Cette
   nomenclature n'est aujourd'hui **définie nulle part dans TRPPU** : les codes produits y
   sont créés dynamiquement à partir du référentiel des objets. Le service restitue donc
   les codes produits **tels qu'ils existent dans le scénario**, sans transcodage. Si un
   mapping TRPPU → OPTIPACC s'avère nécessaire, il fera l'objet d'un ticket dédié.
2. **Comptages manuels** exclus du volume brut (cf. §3.3) — à confirmer.
3. **Fraîcheur du prévisionnel recalculé.** Le prévisionnel recalculé est produit par
   l'IHM TRPPU au moment de la saisie. Si un taux de variation est modifié ensuite sans
   que l'écran des trafics soit revalidé, le volume restitué reflète la dernière valeur
   enregistrée, pas le taux courant.
4. **Visibilité conditionnée au calcul Agrébal.** Tant que le traitement de calcul des
   trafics Agrébal n'est pas déployé et exécuté, aucun scénario ne remonte à OPTIPACC
   (liste vide côté DSR-690, 409 côté DSR-689). C'est le pré-requis n°1 de la mise en
   service.

---

## 8. Structure des fichiers

```
app/routes/trppu_optipacc/
├── __init__.py
├── routes.py     # les 4 endpoints
├── schemas.py    # contrats d'entrée/sortie (Pydantic v2)
└── helpers.py    # requêtes SQL + gardes de visibilité
tests/
├── test_optipacc.py              # DSR-689, DSR-690
├── test_optipacc_amas.py         # DSR-705
└── test_optipacc_mise_en_prod.py # DSR-707
```

Les gardes partagées avec la route IHM de mise en production (C4, C5) vivent dans
`app/routes/trppu_scenario/helpers.py` : `assert_trafics_calcules` et
`assert_aucun_scenario_en_production`.

---

## 9. `POST /trppu-api/optipacc/trafic-amas` — DSR-705

Restitue les **trafics Agrébal** (les « amas ») calculés par le batch YB05 pour un
scénario. C'est le niveau de restitution nominal attendu par OPTIPACC pour construire les
organisations de distribution.

Service **purement consultatif** : aucun recalcul n'est déclenché (RG-API-003), et les
volumes sont ceux stockés dans `trppu_trafic_agrebal`, sans clé de répartition ni
coefficient PIC appliqué à la restitution (RG-API-006).

### 9.1 Entrée

```json
{
  "code_regate": "123456",
  "scenario_id": 125,
  "amas": ["0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234"],
  "page": 1
}
```

| Champ | Type | Obligatoire | Règle |
|-------|------|-------------|-------|
| `code_regate` | string | oui | exactement 6 caractères alphanumériques |
| `scenario_id` | int | oui | ≥ 1 |
| `amas` | array\<string\> | non | `agrebal_uuid` à restituer, 1000 maximum. Absent → **tous** les amas du scénario (RG-API-005) |
| `page` | int | non | défaut `1`. **Ignoré** lorsque `amas` est fourni |

> Le corps est en **snake_case**, comme l'écrit le ticket — contrairement à DSR-689/690 qui
> sont en camelCase. Les deux conventions coexistent volontairement : chaque contrat
> reproduit son ticket.

### 9.2 Sortie

```json
{
  "site": "123456",
  "scenario": 125,
  "pagination": {
    "page": 1,
    "taille_page": 200,
    "nb_amas_total": 742,
    "nb_pages": 4,
    "page_suivante": 2
  },
  "amas": [
    {
      "agrebal_uuid": "0bb6f27c-e4ec-42fa-a61b-f0fe6e4a1234",
      "nom_amas": "PLUVENCE_2449",
      "jours": {
        "lundi": [
          {"produit": "OO", "fort": 12, "faible1": 2, "faible2": 1},
          {"produit": "PPI", "fort": 8, "faible1": 1, "faible2": 1}
        ],
        "mardi": [
          {"produit": "OO", "fort": 10, "faible1": 3, "faible2": 2}
        ]
      }
    }
  ],
  "amas_non_trouves": []
}
```

`nom_amas` provient de `trppu_agrebal_pdi.agrebal_nom` : colonne nullable, jointe en
`LEFT JOIN`. Un amas dont la ligne référentiel a disparu conserve ses trafics avec
`"nom_amas": null`.

**Mapping des densités** (en base, la densité est un discriminant de ligne `couleur_pic` ;
la réponse la pivote sur trois clés) :

| Base | JSON |
|------|------|
| `DENSE` | `fort` |
| `FAIBLE1` | `faible1` |
| `FAIBLE2` | `faible2` |

Une densité absente pour un couple (jour, produit) vaut `0` dans la réponse.

**Mapping des jours** : `LUNDI` → `lundi`, … `SAMEDI` → `samedi`.

### 9.3 Pagination

La taille de page est pilotée par la variable d'environnement **`NB_AMAS_PAR_PAGE`**
(défaut `200`, cf. `app/config.py`).

| Champ | Signification |
|-------|---------------|
| `page` | page actuellement restituée |
| `taille_page` | nombre maximum d'Agrébals par page |
| `nb_amas_total` | nombre total d'Agrébals disponibles pour le scénario |
| `nb_pages` | nombre total de pages |
| `page_suivante` | page suivante, ou `null` s'il n'y en a plus |

Les Agrébals sont triés par `agrebal_uuid` **avant** découpage (RG-API-008) : un même
Agrébal ne change donc jamais de page entre deux appels.

Deux comportements à connaître :

- **Filtre `amas` fourni** → la pagination n'est pas appliquée (le périmètre est déjà borné
  par l'appelant, Cas 8). Le bloc `pagination` reste présent et décrit l'unique page
  renvoyée (`page: 1`, `nb_pages: 1`, `page_suivante: null`).
- **Page au-delà de la dernière** → `200` avec `"amas": []`, ce n'est pas une erreur.

### 9.4 Contrôles

| Contrôle | Échec |
|----------|-------|
| C1 — le scénario existe | `404` |
| C2 — le scénario appartient au site demandé | `400` |
| C3 — scénario visible OPTIPACC (5 conditions ci-dessous) | `409` `{"detail": "Scenario non disponible"}` |
| C4 — au moins un des `amas` demandés existe | `404` |

Les 5 conditions de visibilité (C3) : `statut ∈ {VALIDE, EN PRODUCTION}`, `est_fige = 1`,
`calcul_trafic_en_cours = 0`, `trafic_pdi_calcule = 1`, `trafic_agrebal_calcule = 1`.

> **Écart assumé avec le ticket** : DSR-705 n'autorise que `VALIDE`. On accepte aussi
> `EN PRODUCTION`, car DSR-707 fait justement passer EN PRODUCTION le scénario
> qu'OPTIPACC vient de retenir — une lecture stricte rendrait ses propres trafics
> illisibles juste après la mise en production. Cohérent avec DSR-689 (cf. §3).

Les `agrebal_uuid` inconnus sont **ignorés** : ils sont restitués dans `amas_non_trouves`
et tracés en WARNING dans les logs. Si aucun UUID valide n'est trouvé, la réponse est un
`404`.

---

## 10. `POST /trppu-api/optipacc/scenario/mise-en-production` — DSR-707

OPTIPACC déclare qu'un scénario devient la référence opérationnelle du site. C'est le seul
service OPTIPACC **en écriture**.

### 10.1 Entrée

```json
{
  "code_regate": "123456",
  "scenario_id": 125,
  "date_mise_en_oeuvre": "2027-04-01"
}
```

| Champ | Type | Obligatoire | Règle |
|-------|------|-------------|-------|
| `code_regate` | string | oui | exactement 6 caractères alphanumériques |
| `scenario_id` | int | oui | ≥ 1 |
| `date_mise_en_oeuvre` | date | oui | format `YYYY-MM-DD` |

> **Un seul champ date.** Le ticket se contredit (l'exemple de body montre
> `date_mise_en_prod`, la section « Paramètres » et le critère d'acceptation 1 décrivent
> `date_mise_en_oeuvre`). Le Cas 1 tranche : une seule date alimente les deux colonnes, ce
> que confirme RG-API-PROD-006. Envoyer `date_mise_en_prod` produit un `422` (champs
> inconnus refusés) plutôt qu'un silence.

### 10.2 Sortie

```json
{
  "scenario_id": 125,
  "code_regate": "123456",
  "statut": "EN PRODUCTION",
  "date_mise_en_oeuvre": "2027-04-01"
}
```

### 10.3 Contrôles

| Contrôle | Échec | Message |
|----------|-------|---------|
| C1 — le scénario existe | `404` | `Scénario 125 introuvable.` |
| C2 — il appartient au site demandé | `400` | `Le scénario 125 n'appartient pas au site 123456` |
| C3 — statut `VALIDE` (seul autorisé) | `409` | `Les paramètres du scénario 125 ne permettent pas la mise en production du scénario` |
| C4 — trafics complètement calculés | `409` | `Les trafics du scénario 125 ne sont pas complètement calculés` |
| C5 — aucun autre scénario en production sur le site | `409` | `Un scénario est déjà en production pour ce site` |

C3 refuse `EN COURS`, `SIMULATION`, `ARCHIVE` et `EN PRODUCTION` — ce dernier couvrant le
cas d'un scénario déjà mis en production.

C4 exige `trafic_pdi_calcule = 1`, `trafic_agrebal_calcule = 1` et
`calcul_trafic_en_cours = 0`.

C5 est vérifié **dans la transaction d'écriture**, avec `SELECT … FOR UPDATE` : aucune
contrainte d'unicité n'existe en base, ce verrou est la seule protection contre deux appels
concurrents qui produiraient deux scénarios en production sur le même site
(RG-API-PROD-005).

### 10.4 Traitement

Après validation, `trppu_scenario` est mis à jour dans une transaction :

| Colonne | Valeur |
|---------|--------|
| `statut` | `EN PRODUCTION` |
| `est_fige` | `1` |
| `dt_mise_en_oeuvre` | date transmise |
| `dt_mise_en_prod` | date transmise |
| `dt_validation` | `NOW()` si elle était `NULL` |
| `dt_maj` | automatique (`ON UPDATE CURRENT_TIMESTAMP`) |
| `version_scenario` | incrémenté |

L'appel est tracé dans `trppu_api_log` (action `TRANSITION_STATUT`, `origine: OPTIPACC`).

Conséquences (RG-API-PROD-002 à 004) : le scénario étant figé, toute modification est
refusée par `assert_editable`, il ne peut plus être supprimé, et le batch YB05 ne le
recalcule plus.

### 10.5 Relation avec la route IHM

`POST /trppu-api/scenarios/{id_scenario}/mise-en-prod` reste disponible pour l'IHM TRPPU.
Elle applique désormais **les mêmes contrôles C4 et C5** (helpers partagés dans
`app/routes/trppu_scenario/helpers.py`) — sans quoi l'unicité d'un scénario en production
par site serait contournable depuis l'IHM. Deux différences subsistent :

| | Route IHM | Route OPTIPACC |
|-|-----------|----------------|
| Date de mise en prod | `NOW()` | fournie par l'appelant |
| `dt_mise_en_oeuvre` | inchangée | renseignée |
| Contrôle du site | non | oui (C2) |

---

## 11. Écarts assumés par rapport aux tickets DSR-705 / DSR-707

| Sujet | Ticket | Implémentation | Raison |
|-------|--------|----------------|--------|
| Chemin | `/trafic_amas` | `/trafic-amas` | kebab-case, comme tout le reste de l'API |
| Corps d'erreur | `{"erreur": "..."}` | `{"detail": "..."}` | convention FastAPI du projet ; les messages sont repris à l'identique |
| Statut lisible (DSR-705 C3) | `VALIDE` | `VALIDE` + `EN PRODUCTION` | sinon DSR-707 rendrait les trafics illisibles juste après la mise en production |
| Champ date (DSR-707) | deux noms contradictoires | `date_mise_en_oeuvre` unique | Cas 1 + RG-API-PROD-006 |
| UUID inconnus (DSR-705 C4) | « tracer dans le retour json » | champ `amas_non_trouves` | le ticket impose la trace, pas le nom du champ |
