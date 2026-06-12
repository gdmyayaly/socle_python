# Résolution — DSR-649 (MAJ ciblée d'un trafic initial modifié)

## 1. Statut
**Terminé.** Endpoint de MAJ ciblée d'un produit du tableau TMH (volume réalisé +
moyennes). Cas particulier de DSR-659.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_tmh/routes.py` — handler `update_tmh_volume`.
- `app/routes/trppu_tmh/schemas.py` — `TmhVolumeUpdate`.

## 3. Endpoint livré
`PATCH /trppu-api/scenarios/{id_scenario}/tmh/{co_produit}`
```json
{ "volume_realise": 120000, "moyenne_journaliere": 4000.00, "moyenne_hebdo": 24000.00 }
```
Met à jour `volume_realise`, `moyenne_journaliere`, `moyenne_hebdo`, `dt_calcul=NOW()` et
**force `bl_manuel = 1`** : une modification ciblée d'un trafic initial est par nature une
saisie manuelle (cf. DSR-665/648). `bl_manuel` n'est pas un paramètre reçu (positionné serveur).
**Ne touche pas** `volume_previsionnel` ni `bl_exclu` (différence avec DSR-659).
Codes : `200` (renvoie la ligne), `404` ligne TMH introuvable, `409` figé.

## 4. Migrations / dépendances
Aucune.

## 5. Hypothèses & écarts
- La ligne TMH doit déjà exister (créée à DSR-634) ; sinon `404` (pas d'upsert ici,
  contrairement à DSR-659 — comportement à confirmer, `README_incomprehensions.md` #13).

## 6. Comment tester
```
PATCH /trppu-api/scenarios/12/tmh/OO   body = { "volume_realise": 120000, ... }
```

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| trppu_tmh correct pour les produits modifiés, en phase IHM | UPDATE ciblé + relecture |

## 8. ➡️ Commentaire Jira (à coller)
> **URL d'appel**
> `PATCH /trppu-api/scenarios/{id_scenario}/tmh/{co_produit}`
>
> **Données d'entrée**
> - `id_scenario` (path) | scénario concerné.
> - `co_produit` (path) | produit dont le trafic a changé.
> - `volume_realise` | nouveau volume de trafic.
> - `moyenne_journaliere` | moyenne du trafic sur une journée.
> - `moyenne_hebdo` | moyenne du trafic sur une semaine.
>
> **Mise à jour en base (trppu_tmh)**
> `volume_realise`, `moyenne_journaliere`, `moyenne_hebdo`, `dt_calcul` = date du jour,
> et `bl_manuel` = 1 (la ligne est marquée « saisie manuelle », puisqu'il s'agit d'une
> modification manuelle d'un trafic initial). `volume_previsionnel` et `bl_exclu` ne sont
> pas modifiés.
>
> **Données de sortie**
> la ligne TMH à jour du produit. 404 si la ligne n'existe pas ; 409 si le scénario est
> figé/archivé.
>
> **À noter** : ce besoin est un sous-ensemble de DSR-659 (même module/table).
