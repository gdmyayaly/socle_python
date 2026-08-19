# Résolution — DSR-690 (Liste des scénarios exploitables d'un site pour OPTIPACC)

## 1. Statut
**Terminé.** Service `S_SiteListeScenarios` livré dans un nouveau package `trppu_optipacc`
regroupant les services consommés par OPTIPACC. Lecture seule, sans état.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_optipacc/{__init__,routes,schemas,helpers}.py` — nouveau package (mutualisé avec DSR-689).
- `app/main.py` — enregistrement du routeur OPTIPACC.
- `tests/test_optipacc.py` — tests mutualisés 689/690.
- `api_docs/api_trppu_optipacc.md` — documentation d'usage destinée au métier / à OPTIPACC.

## 3. Endpoint livré
`POST /trppu-api/optipacc/site-liste-scenarios`
```json
{ "codeRegate": "123456" }
```
Réponse :
```json
{ "codeRegate": "123456",
  "scenarios": [ { "id_scenario": 125, "lb_scenario": "Scénario Septembre 2026" } ] }
```
Codes : `200`, `422` body invalide, `500` erreur technique.

Filtre appliqué (`SELECT_SCENARIOS_EXPLOITABLES_SQL`) :
```sql
SELECT id_scenario, lb_scenario FROM trppu_scenario
WHERE co_regate = %s AND statut = 'VALIDE' AND trafic_agrebal_calcule = 1
ORDER BY id_scenario
```

## 4. Migrations / dépendances
Aucune migration. **Dépendance fonctionnelle bloquante** : `trafic_agrebal_calcule` n'est
écrit par aucun traitement de ce dépôt — c'est le batch Agrébal (DSR-702/703) qui doit le
passer à 1 en fin de calcul. Tant qu'il n'est pas déployé, le service renvoie
systématiquement une liste vide.

## 5. Hypothèses & écarts
- **« Aucun scénario » = `200` + liste vide + `message`**, pas une erreur HTTP : une liste
  vide est un résultat, et OPTIPACC doit pouvoir afficher le message de CA4 sans traiter un
  code d'erreur. `message` est absent de la réponse quand des scénarios existent
  (`response_model_exclude_none`), le cas nominal reste donc strictement le contrat.
- **Existence du site non contrôlée** : le ticket ne prévoit pas ce cas d'erreur, un code
  Regate inconnu donne la même réponse « aucun scénario ».
- `POST` retenu (et non `GET`) pour reprendre littéralement le contrat d'entrée du ticket.
- Sortie en `snake_case` (`id_scenario`, `lb_scenario`) conforme au ticket, alors que
  l'entrée est en `camelCase` (`codeRegate`) — incohérence du contrat source, reproduite
  telle quelle pour ne pas créer d'écart d'intégration.
- Le `Literal` de statut du module scénario n'est **pas** réutilisé : la requête filtre
  directement en SQL sur `statut = 'VALIDE'`, aucun risque de 500 à la sérialisation.
  (Ce `Literal` ignorait `SIMULATION`, pourtant présent en base — écart n°2 du rapport
  `db/RAPPORT-ECARTS-db_new-2026-08-17.md`, corrigé depuis. Le service n'en dépendait pas.)

## 6. Comment tester
```sql
UPDATE trppu_scenario SET statut = 'VALIDE', trafic_agrebal_calcule = 1 WHERE id_scenario = 12;
```
```bash
curl -X POST http://localhost:8080/trppu-api/optipacc/site-liste-scenarios \
  -H "Content-Type: application/json" -d '{"codeRegate":"123456"}'
```

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| CA1 — seuls les scénarios `VALIDE` | `statut = 'VALIDE'` dans le SQL |
| CA2 — trafics Agrébal non calculés exclus | `trafic_agrebal_calcule = 1` |
| CA3 — identifiant + libellé | `id_scenario`, `lb_scenario` |
| CA4 — message si aucun scénario | `200` + `message` « Aucun scénario trouvé pour le site … » |

## 8. ➡️ Commentaire Jira (à coller)

> **✅ Service de liste des scénarios exploitables — livré.**
>
> **Endpoint :** `POST /trppu-api/optipacc/site-liste-scenarios`
> Les services destinés à OPTIPACC sont regroupés sous le segment `/optipacc` pour être
> identifiables sans ambiguïté par les applications tierces.
>
> **Entrée :** `{ "codeRegate": "123456" }` — `codeRegate` obligatoire, exactement
> 6 caractères alphanumériques ; tout autre champ dans le body est refusé.
> **Sortie :** `{ "codeRegate": "123456", "scenarios": [ { "id_scenario": 125, "lb_scenario": "Scénario Septembre 2026" } ] }`
> **Codes :** `200` OK · `422` requête invalide · `500` erreur technique.
>
> **Règles appliquées :** seuls les scénarios du site demandé, au statut `VALIDE`
> **et** dont `trafic_agrebal_calcule = 1`. Tout autre statut (« EN COURS »,
> « SIMULATION », « EN PRODUCTION », « ARCHIVE ») ou un calcul Agrébal non terminé exclut
> le scénario (RG2 à RG5).
>
> **Cas « aucun scénario » (CA4) :** renvoyé en `200` avec `"scenarios": []` et
> `"message": "Aucun scénario trouvé pour le site <codeRegate>."` — une liste vide est un
> résultat, pas une erreur HTTP, et OPTIPACC peut afficher le message directement. Le champ
> `message` est absent quand des scénarios existent. Un code Regate inconnu produit la même
> réponse (le ticket ne prévoit pas de cas d'erreur « site inconnu » ici, contrairement à
> DSR-689).
>
> **⚠️ Pré-requis de mise en service :** la colonne `trppu_scenario.trafic_agrebal_calcule`
> n'est alimentée par aucun traitement de l'API — c'est le batch de calcul des trafics
> Agrébal (DSR-702/703) qui doit la passer à 1 en fin d'exécution. **Tant que ce batch n'est
> pas déployé et exécuté, le service renvoie une liste vide**, ce qui est le comportement
> attendu mais doit être connu en recette (forcer le flag en base pour tester).
>
> **Documentation :** `api_docs/api_trppu_optipacc.md` (fiche d'usage partageable au métier
> et à l'équipe OPTIPACC).
>
> **Tests :** `tests/test_optipacc.py` — 30 tests (mutualisés DSR-689/690). Suite complète OK
> (130/130).
