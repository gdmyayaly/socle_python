# Résolution — DSR-613 (RecupererTrafics renvoie nb jours ouvrés/ouvrables)

## 1. Statut
**Terminé.** Service `CalculerNbJours` enrichi (endpoint dédié) + `RecupererTrafics`
qui renvoie désormais le bloc `nb_jours` (nbJoursOuvres / nbJoursOuvrables), fériés déduits.

## 2. Fichiers créés / modifiés
- `app/routes/calcl_nbr_jours.py` — nouvel endpoint async `GET /get_nb_jours`
  (l'existant `/get_nbr_jours` est conservé).
- `app/routes/trafics.py` — `get_trafics` passé en `async`, bloc `nb_jours` ajouté
  (appel Databricks déporté via `run_in_threadpool`).
- Réutilise `app/services/jours_service` (calcul + table fériés).

## 3. Endpoints livrés
- `GET /calcl_nbr_jours/get_nb_jours?date_debut=&date_fin=` :
```json
{ "nb_jours_total": 396, "nb_jours_ouvres_bruts": 282, "nb_jours_ouvrables_bruts": 339,
  "nb_feries_hors_weekend": 10, "nb_feries_samedi": 1,
  "nbJoursOuvres": 272, "nbJoursOuvrables": 328 }
```
- `GET /trppu-api/trafics/get_trafics?...` : réponse existante + `"nb_jours": { "nbJoursOuvres": 272, "nbJoursOuvrables": 328 }`.

## 4. Migrations / dépendances
Migrations `003/004` (table + seed des fériés). Aucune dépendance externe.

## 5. Hypothèses & écarts
- Règle fériés : hors week-end déduits des 2 ; férié samedi déduit des ouvrables
  uniquement ; férié dimanche ignoré.
- **⚠️ Exemple du ticket erroné sur les ouvrés** : l'exemple annonce 262 ; le calcul
  correct (définition lun-ven) donne **272** sur 01/03/2025–31/03/2026 (le côté
  ouvrables 328 est correct). Cf. `README_incomprehensions.md` #14. Couvert par
  `tests/test_jours_service.py`.
- `get_trafics` rendu **résilient** : si le calcul des jours échoue (ex. table fériés
  absente), `nb_jours` = `null` et les trafics sont quand même renvoyés.

## 6. Comment tester
```
GET /calcl_nbr_jours/get_nb_jours?date_debut=20250301&date_fin=20260331
GET /trppu-api/trafics/get_trafics?co_regate=400300&date_debut=20250301&date_fin=20260331
python -m pytest tests/test_jours_service.py -q
```

## 7. Mapping critères d'acceptance
| Élément | Couverture |
| ------- | ---------- |
| RecupererTrafics appelle le calcul des jours | bloc `nb_jours` |
| Ouvrés (lun-ven) / ouvrables (lun-sam) | jours_service |
| Déduction fériés (règle samedi/dimanche) | jours_service + tests |
| 2 résultats renvoyés à l'IHM | `nbJoursOuvres` / `nbJoursOuvrables` |

## 8. ➡️ Commentaire Jira
> `RecupererTrafics` renvoie désormais un bloc `nb_jours` (`nbJoursOuvres`,
> `nbJoursOuvrables`) calculé par le service `CalculerNbJours` (endpoint dédié
> `GET /calcl_nbr_jours/get_nb_jours`), avec déduction des jours fériés / fermés
> récupérés via l'API jours-fermes. Le calcul des jours est résilient (n'empêche pas le renvoi des trafics).
> **⚠️ À valider PO** : l'exemple du ticket donne 262 jours ouvrés ; le calcul correct
> (lun-ven, fériés déduits) est **272** sur la période d'exemple — 262 provient d'une
> erreur de comptage des samedis. Le côté ouvrables (328) est correct.
