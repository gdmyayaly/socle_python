# Résolution — DSR-660 (Lecture du paramétrage de rétention PIC)

## 1. Statut
**Terminé.** Lecture fusionnée : coefficients par défaut (national, `id_pic_version=1`)
surchargés par ceux du scénario. Chaque ligne porte son **`id_pic_version`** (distinction
défaut/modifié exigée par le ticket) + un marqueur `modifie` (confort IHM).

## 2. Fichiers créés / modifiés
- `app/routes/trppu_scenario_pic/{__init__,helpers,schemas,routes}.py`
- `app/main.py` — enregistrement du routeur.

## 3. Endpoint livré
`GET /trppu-api/scenarios/{id_scenario}/pic-coefficients` → `PicScenarioOut`
```json
{
  "id_pic_version_defaut": 1,
  "id_pic_version_scenario": 57,
  "niveau_scenario": "SCENARIO",
  "coefficients": [
    { "id_pic_version": 57, "co_produit": "OO", "jour_semaine": "LUNDI", "densite": 0, "coef": 0.8500, "modifie": true },
    { "id_pic_version": 1, "co_produit": "OO", "jour_semaine": "LUNDI", "densite": 1, "coef": 0.7000, "modifie": false }
  ]
}
```
`id_pic_version_scenario = null` si le scénario n'a jamais été surchargé.

## 4. Migrations / dépendances
Aucune (tables existantes).

## 5. Hypothèses & écarts
- Défaut = `id_pic_version = 1` (national), conforme au ticket.
- Surcharge = version `niveau='SCENARIO'` et `id_scenario` correspondants, **active**
  (`dt_desactivation` nulle/future), la plus récente si plusieurs (#9).
- Merge sur clé `(co_produit, jour_semaine, densite)` ; chaque ligne porte son
  `id_pic_version` (1 = défaut, version scénario = modifié) + `modifie=true` sur les
  surcharges (couleur IHM).
- Niveaux `DEX`/`SITE` non traités (#9). Table legacy `trppu_scenario_pic_coeffs` non utilisée (#9).
- `id_pic_version` tracé dans les logs + `id_session_ihm` (exigence ticket).

## 6. Comment tester
```
GET /trppu-api/scenarios/{id}/pic-coefficients
```
Sans surcharge : tous `modifie=false`. Après un PUT (DSR-661) : la ligne concernée passe `modifie=true`.

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| 3 densités × 6 jours par produit | merge sur la clé |
| Scénario non modifié → tout national | overrides vides |
| Scénario modifié → national + surcharges | `modifie` |
| Distinction des modifiés via id_pic_version | `id_pic_version` par ligne (+ `modifie`) |
| Logs id_pic_version + id session | logs |

## 8. ➡️ Commentaire Jira (à coller)
> **URL d'appel**
> `GET /trppu-api/scenarios/{id_scenario}/pic-coefficients` (option `?id_session_ihm=`).
>
> **Données d'entrée** : `id_scenario` (path), `id_session_ihm` (query, traçabilité).
>
> **Données de sortie** : les coefficients PIC du scénario = défaut national
> (`id_pic_version = 1`) **fusionné** avec les surcharges propres au scénario sur la clé
> (co_produit, jour_semaine, densite). Chaque ligne porte :
> - `id_pic_version` | 1 = valeur par défaut ; autre = valeur **modifiée** pour le scénario
>   (c'est sur ce champ que l'IHM distingue les lignes modifiées) ;
> - `co_produit`, `jour_semaine`, `densite` (0=dense,1=faible1,2=faible2), `coef`, `modifie`.
> En tête : `id_pic_version_defaut`, `id_pic_version_scenario` (null si jamais surchargé),
> `niveau_scenario`.
>
> **Traçabilité** : `id_pic_version` utilisé et `id_session_ihm` tracés dans les logs.
>
> **À valider PO** : sélection de la version scénario si plusieurs, traitement des niveaux
> DEX/SITE, sort de la table legacy `trppu_scenario_pic_coeffs`.
