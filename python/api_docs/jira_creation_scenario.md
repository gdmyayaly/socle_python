# Ticket Jira — Création d'un scénario via l'API

## Type
Story

## Module concerné
`trppu_scenario`

## Endpoint
`POST /trppu-api/scenarios`

## Pré-requis
Avoir au moins un site dans `trppu_site` et une version PIC dans `trppu_pic_version`.

---

## Contexte

Un scénario TRPPU regroupe un site, un code ROC, une version PIC et une
période. Cette période est ensuite découpée en deux parties :
- la partie **réalisée** (ce qui est dans le passé ou aujourd'hui)
- la partie **prévision** (ce qui est dans le futur)

Le scénario suit un workflow : EN COURS, puis VALIDE, puis EN PRODUCTION,
et enfin ARCHIVE. La création ouvre ce workflow en posant le
scénario au statut EN COURS.

---

## Ce qu'il faut faire

Permettre à un utilisateur de créer un nouveau scénario en envoyant un body
JSON simple. Le serveur complète automatiquement les champs manquants et
calcule lui-même les bornes réalisé/prévision.

### Exemple de body

```json
{
  "co_regate": "012345",
  "lb_scenario": "Scénario test",
  "co_roc": "012345",
  "nb_jours_semaine": 6,
  "id_pic_version": 1,
  "periode_debut": "2026-01-01",
  "periode_fin": "2026-12-31"
}
```

### Champs obligatoires
- `co_regate` (6 caractères)
- `lb_scenario` (entre 1 et 50 caractères)
- `co_roc` (6 caractères)

### Champs optionnels
- `nb_jours_semaine` : 5 ou 6, par défaut 5
- `id_pic_version` : si non fourni, le serveur prend la version par défaut
- `periode_debut` / `periode_fin` : par défaut today-1an / today+1an

### Champs interdits dans le body
`periode_realise_debut`, `periode_realise_fin`, `periode_prev_debut`,
`periode_prev_fin`. Le serveur les calcule lui-même, donc ils sont rejetés
si on essaie de les envoyer.

---

## Comportement attendu

1. Le serveur valide le body.
2. Si `id_pic_version` n'est pas fourni, il prend la première PIC version avec
   le flag par défaut (sinon `id_pic_version = 1`, sinon erreur 422).
3. Si une période n'est pas fournie, il applique le défaut today-1an /
   today+1an.
4. Il calcule les bornes réalisé/prévision selon ces règles :
   - réalisé = de `periode_debut` jusqu'au minimum entre aujourd'hui et `periode_fin`
   - prévision = du maximum entre aujourd'hui et `periode_debut` jusqu'à `periode_fin`
   - si la période est entièrement passée, la prévision est null
   - si la période est entièrement future, le réalisé est null
5. Il insère le scénario en base avec :
   - statut = `EN COURS`
   - version = 1
   - est_fige = false
   - dt_creation = maintenant
   - dt_validation = null
   - dt_mise_en_prod = null
6. Il renvoie 201 avec le scénario complet (incluant l'`id_scenario` généré).

---

## Tests d'acceptance

### Cas qui doivent fonctionner

**Création minimale**
On envoie seulement co_regate, lb_scenario, co_roc.
On doit recevoir un 201 avec un scénario en EN COURS, version 1, non figé,
nb_jours_semaine = 5, période today-1an / today+1an.

**Création avec période qui englobe aujourd'hui**
On envoie une période qui contient aujourd'hui.
La partie réalisée doit aller du début jusqu'à aujourd'hui.
La partie prévision doit aller d'aujourd'hui jusqu'à la fin.

**Création avec période entièrement passée**
On envoie une période qui se termine avant aujourd'hui.
Le réalisé doit couvrir toute la période, la prévision doit être null.

**Création avec période entièrement future**
On envoie une période qui commence après aujourd'hui.
Le réalisé doit être null, la prévision doit couvrir toute la période.

**Création avec nb_jours_semaine = 6**
On doit recevoir un 201 avec nb_jours_semaine = 6.

### Cas qui doivent échouer

**nb_jours_semaine invalide** (par exemple 4) → 422

**periode_fin avant periode_debut** → 422 avec message clair

**co_regate qui n'existe pas** dans trppu_site → 500, avec une stack trace
dans les logs montrant l'erreur de clé étrangère MySQL.

**id_pic_version qui n'existe pas** → 500, même type d'erreur.

**Aucune PIC version par défaut disponible** et pas d'id_pic_version fourni
→ 422 avec un message qui explique le problème.

**Champ réalisé ou prévision dans le body** → 422 (le serveur refuse les
champs en trop).

**lb_scenario trop long** (plus de 50 caractères) → 422.

**co_regate au mauvais format** (moins de 6 caractères ou caractères non
alphanumériques) → 422.

---

## Vérifications dans les logs

Pour chaque création, on doit voir dans les logs JSON :
- un log d'entrée `create_scenario start` avec les paramètres reçus
- un log intermédiaire qui annonce le calcul des bornes réalisé/prévision
- un log de sortie `create_scenario OK` avec l'id généré et la durée

En cas d'erreur, on doit voir un log de niveau ERROR avec la stack trace
complète dans le champ `exc_info`.

Aucun log ne doit contenir les caractères de flèche.

---

## Critère de fin (Done)

- Le code Python compile sans warning.
- L'app démarre et la route apparaît dans `/docs` (Swagger).
- Les cas qui doivent fonctionner ci-dessus passent en test manuel via Swagger.
- Les cas qui doivent échouer renvoient bien le bon code HTTP.
- Le PO valide trois cas représentatifs : aujourd'hui dans la période,
  période entièrement passée, période entièrement future.

---

## Hors-scope (autres tickets)

- La modification de la période d'un scénario existant (PATCH /periodes).
- Le changement de statut (PATCH /statut, POST /mise-en-prod).
- L'authentification et le tracking utilisateur (id_rh_creation).
- La suppression physique d'un scénario (on archive seulement).
