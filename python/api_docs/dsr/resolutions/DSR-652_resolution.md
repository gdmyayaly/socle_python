# Résolution — DSR-652 (Lecture des périodes neutralisées)

## 1. Statut
**Terminé.** Endpoint de lecture des neutralisations **regroupées par type**
(feries / peak / saison) avec valeurs par défaut si type absent.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_neutralisations/{helpers,routes}.py` (module mutualisé avec DSR-645).
  `group_neutralisations` construit la réponse structurée.

## 3. Endpoint livré
`GET /trppu-api/scenarios/{id_scenario}/neutralisations` → `NeutralisationsOut`
```json
{
  "feries": { "actif": true, "nb_jours_total": 2,
              "jours": [ {"dt":"2026-11-11","nb_jour":1}, {"dt":"2026-12-25","nb_jour":1} ] },
  "peak":   { "actif": true,  "dt_debut": "2026-11-10", "dt_fin": "2026-12-19", "nb_jour": 28 },
  "saison": { "actif": false, "dt_debut": null, "dt_fin": null, "nb_jour": 0 }
}
```

## 4. Migrations / dépendances
Dépend de l'enum `SAISON` (migration `002`, portée par DSR-645).

## 5. Hypothèses & écarts
- Réponse **regroupée par type** (structure directement exploitable par l'IHM)
  plutôt que liste plate (#8).
- Type absent → bloc par défaut (`actif:false`, listes vides).
- `id_session_ihm` accepté en query (#11).

## 6. Comment tester
```
GET /trppu-api/scenarios/{id}/neutralisations
```

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| Service appelé pour le scénario édité | GET |
| Paramètres renseignés selon le type | regroupement feries/peak/saison |
| Type absent → défaut | `actif:false` / listes vides |
| Log Kibana avec id_scenario | logs |
| Conformité base | SELECT direct |

## 8. ➡️ Commentaire Jira
> Endpoint `GET /trppu-api/scenarios/{id}/neutralisations` livré : renvoie les
> neutralisations regroupées en 3 blocs (`feries` avec total + liste de jours, `peak`,
> `saison`), chacun avec un indicateur `actif` (false = non neutralisé par défaut).
> Prêt à alimenter directement les widgets IHM.
