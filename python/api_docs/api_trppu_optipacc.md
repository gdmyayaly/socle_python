# API OPTIPACC — Services TRPPU exposés à l'application OPTIPACC

> Module : `app/routes/trppu_optipacc/`
> Préfixe HTTP : `/trppu-api/optipacc`
> Tag Swagger : **OPTIPACC**
> Tickets : **DSR-690** (liste des scénarios) · **DSR-689** (volumes bruts)

TRPPU met à disposition d'OPTIPACC deux services de **lecture seule**, sans état, qui
permettent de récupérer directement le résultat des calculs TRPPU. OPTIPACC n'a rien à
recalculer ni à réagréger : TRPPU reste seul responsable du calcul, OPTIPACC ne consomme
que la valeur finale.

Les deux services sont regroupés sous le segment `/optipacc` pour être identifiables sans
ambiguïté par les applications tierces et pouvoir évoluer indépendamment des routes qui
servent l'IHM TRPPU.

**Parcours type côté OPTIPACC**

1. L'utilisateur saisit un site (code Regate) → appel de **`site-liste-scenarios`** pour
   alimenter la liste déroulante des scénarios sélectionnables.
2. L'utilisateur choisit un scénario → appel de **`scenario-trafic-brut`** pour récupérer
   les volumes par produit et construire les charges de travail.

---

## 1. Vue d'ensemble

| Méthode | Chemin | Service (Jira) | Description |
|---------|--------|----------------|-------------|
| `POST` | `/trppu-api/optipacc/site-liste-scenarios` | `S_SiteListeScenarios` (DSR-690) | Scénarios exploitables d'un site |
| `POST` | `/trppu-api/optipacc/scenario-trafic-brut` | `S_ScenarioTraficBrut` (DSR-689) | Volume brut final par produit |

Les deux services acceptent un paramètre de requête optionnel `?id_session_ihm=` utilisé
uniquement pour la traçabilité (regroupement des lignes de log dans Kibana).

Documentation interactive : **`/docs`** (Swagger UI), tag « OPTIPACC ».

---

## 2. `POST /trppu-api/optipacc/site-liste-scenarios` — DSR-690

Retourne les scénarios d'un site que TRPPU considère comme **prêts à être exploités** par
OPTIPACC.

### 2.1 Entrée

```json
{ "codeRegate": "123456" }
```

| Champ | Type | Obligatoire | Règle |
|-------|------|-------------|-------|
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
| `inclureExclus` | booléen | non (défaut `false`) | voir §3.4 |

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
scénario n'est pas restitué à OPTIPACC — c'est le comportement par défaut, cohérent avec
le fait que l'exclusion est une décision utilisateur à respecter.

Pour obtenir malgré tout la totalité des produits (contrôle, rapprochement, audit) :

```json
{ "codeRegate": "123456", "scenarioId": 789, "inclureExclus": true }
```

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
curl -X POST http://localhost:8080/trppu-api/optipacc/site-liste-scenarios \
  -H "Content-Type: application/json" \
  -d '{"codeRegate":"123456"}'

# Volumes bruts du scénario 789
curl -X POST http://localhost:8080/trppu-api/optipacc/scenario-trafic-brut \
  -H "Content-Type: application/json" \
  -d '{"codeRegate":"123456","scenarioId":789}'

# Variante : inclure les produits exclus
curl -X POST http://localhost:8080/trppu-api/optipacc/scenario-trafic-brut \
  -H "Content-Type: application/json" \
  -d '{"codeRegate":"123456","scenarioId":789,"inclureExclus":true}'
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
├── routes.py     # les 2 endpoints
├── schemas.py    # contrats d'entrée/sortie (Pydantic v2)
└── helpers.py    # requêtes SQL + contrôle d'exploitabilité
tests/
└── test_optipacc.py
```
