# Résolution — DSR-645 (Écriture des périodes neutralisées)

## 1. Statut
**Terminé.** Ajout / suppression des neutralisations (FERIE / PEAK / SAISON) avec
**calcul serveur du `nb_jour`** (fériés + week-end selon la semaine de travail).

## 2. Fichiers créés / modifiés
- `app/routes/trppu_neutralisations/{__init__,helpers,schemas,routes}.py`
- `app/main.py` — enregistrement du routeur.
- Migration `002` (enum `SAISON`, `dt_creation`, `id_rh`).
- Calcul réutilise `app/services/jours_service.compute_nb_jour_neutralise_db`.

## 3. Endpoints livrés
| Méthode | Chemin | Rôle |
| ------- | ------ | ---- |
| POST | `/trppu-api/scenarios/{id}/neutralisations` | Ajout (nb_jour calculé). FERIE multiple ; PEAK/SAISON ligne unique (remplacée) |
| DELETE | `/.../neutralisations?type=FERIE&dt=YYYY-MM-DD` | Supprime un jour férié |
| DELETE | `/.../neutralisations?type=PEAK` (ou SAISON) | Désactive la période |

Body POST : `{ "type": "PEAK", "dt_debut": "2026-11-10", "dt_fin": "2026-12-19", "id_rh": "A123456" }`.

## 4. Migrations / dépendances
Migration `002` (enum `SAISON` au lieu de `LOCAL`, `dt_creation`, `id_rh`),
migrations `003/004` (table fériés), var d'env `ID_RH_CRYPTO_KEY`.

## 5. Hypothèses & écarts
- **Calcul `nb_jour`** (cf. SOCLE) : FERIE=1 ; PEAK/SAISON = jours ouvrés (semaine 5)
  ou ouvrables (semaine 6) de la période, fériés déduits. La semaine de travail vient
  de `trppu_scenario.nb_jours_semaine`.
- **⚠️ Exemple PEAK du ticket erroné** (10 samedis + 11 dimanches sur 40 j impossible) :
  le calcul correct est 28 (5j) / 34 (6j). L'exemple SAISON (10/12) est, lui, reproduit.
  Cf. `README_incomprehensions.md` #14.
- PEAK/SAISON : 1 ligne unique par scénario (remplacée si déjà présente).
- Enum `SAISON` requis (migration `002`) — vérifier l'absence de données `LOCAL` (#1).

## 6. Comment tester
```
POST .../neutralisations {type:FERIE,dt_debut=dt_fin} ; {type:PEAK,...} ; {type:SAISON,...}
DELETE .../neutralisations?type=FERIE&dt=... ; ?type=PEAK ; ?type=SAISON
GET .../neutralisations
```

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| FERIE : 1 ligne/jour, dt_debut=dt_fin, nb_jour=1 | POST FERIE |
| PEAK/SAISON : 1 ligne, dates IHM, nb_jour calculé | POST + jours_service |
| dt_creation = date du jour | défaut SQL / NOW() |
| id_rh crypté | Fernet |
| Suppression par type | DELETE |

## 8. ➡️ Commentaire Jira
> Service neutralisations livré : `POST` (FERIE/PEAK/SAISON, **nb_jour calculé serveur**
> en déduisant fériés + week-end selon la semaine de travail du scénario) et `DELETE`
> (par jour pour FERIE, par type pour PEAK/SAISON). `id_rh` chiffré, `dt_creation`
> positionnée. **Pré-requis** : migration `002` (enum `SAISON`, colonnes) + `003/004`
> (fériés) + `ID_RH_CRYPTO_KEY`.
> **⚠️ À valider PO** : l'exemple PEAK du ticket (18/28) est arithmétiquement impossible
> (10 samedis + 11 dimanches sur 40 jours) ; le service calcule les valeurs correctes
> (28/34). L'exemple SAISON (10/12) est correct.
