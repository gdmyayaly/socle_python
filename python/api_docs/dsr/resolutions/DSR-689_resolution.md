# Résolution — DSR-689 (Volumes bruts par produit d'un scénario, pour OPTIPACC)

## 1. Statut
**Terminé.** Service `S_ScenarioTraficBrut` livré dans le package `trppu_optipacc`
(mutualisé avec DSR-690). Lecture seule, sans état.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_optipacc/{__init__,routes,schemas,helpers}.py` — nouveau package.
- `app/routes/trppu_site/helpers.py` — ajout de `fetch_site_or_404` (n'existait pas ; le
  404 site était inliné dans chaque route). Miroir de `fetch_scenario_or_404`.
- `app/main.py` — enregistrement du routeur OPTIPACC.
- `tests/test_optipacc.py`, `api_docs/api_trppu_optipacc.md`.

## 3. Endpoint livré
`POST /trppu-api/optipacc/scenario-trafic-brut`
```json
{ "codeRegate": "123456", "scenarioId": 789 }
```
Réponse :
```json
{ "codeRegate": "123456", "scenarioId": 789,
  "produits": [ { "codeProduit": "OS", "volumeBrut": 1250000 } ] }
```
Codes : `200`, `404` site inconnu / scénario inexistant ou d'un autre site,
`409` scénario non exploitable, `422` body invalide, `500` erreur technique.

## 4. Calcul du volume brut (RG4)

```sql
SELECT co_produit,
       SUM(COALESCE(volume_realise, 0)
         + COALESCE(volume_previsionnel_recalcule, volume_previsionnel, 0)) AS volume_brut
FROM trppu_tmh
WHERE id_scenario = %s AND bl_exclu = 0
GROUP BY co_produit ORDER BY co_produit
```

Justification de chaque terme :

| Terme | Justification |
| ----- | ------------- |
| `volume_realise` | le **constaté** (cf. DSR-648) |
| `COALESCE(volume_previsionnel_recalcule, volume_previsionnel, 0)` | le **prévisionnel recalculé**, avec le même repli que `insert_tmh_row` / `update_tmh_row` : la colonne est nullable et reste NULL sur les lignes antérieures à son introduction |
| `SUM(...) GROUP BY co_produit` | absorbe les **trafics manuels** : depuis la migration du 24/06/2026 un même `co_produit` peut avoir plusieurs lignes dans `trppu_tmh` (`uq_tmh` inclut `id_tmh`), un ajout manuel est une ligne supplémentaire |
| pas de test sur `bl_manuel` | c'est un flag de **provenance**, pas un opérateur : une correction DSR-649 écrase les colonnes de sa propre ligne tout en posant `bl_manuel = 1`. Filtrer dessus produirait des faux positifs et un double comptage |
| `bl_exclu = 0` | reprend le seul précédent du projet (`SELECT_VARIATIONS_SQL`) |

> **Mise à jour** — la colonne `trppu_tmh.volume_brut` (présente en base mais jamais alimentée)
> porte désormais cette même valeur **ligne à ligne**, écrite par `compute_volume_brut`
> (`app/routes/trppu_tmh/helpers.py`) à chaque INSERT/UPDATE du module TMH et restituée par
> `GET /scenarios/{id}/tmh`. Le service OPTIPACC conserve volontairement la somme calculée
> ci-dessus : la colonne reste NULL sur les lignes écrites avant cette mise en service. Les deux
> formes sont identiques par construction (mêmes COALESCE, même repli), et
> `tests/test_tmh_volume_brut.py` les confronte pour empêcher toute divergence.

## 5. Migrations / dépendances
Aucune migration. Même dépendance que DSR-690 : `trafic_agrebal_calcule` n'est écrit par
aucun traitement de ce dépôt (batch Agrébal DSR-702/703) → en l'état, le service répond
`409` tant que le flag n'est pas posé.

## 6. Hypothèses & écarts
- **Statuts exploitables : `VALIDE` et `EN PRODUCTION`** (+ Agrébal calculé). Choix assumé :
  un projet OPTIPACC déjà créé doit continuer de fonctionner après la mise en production du
  scénario, alors que DSR-690 ne liste que les `VALIDE`. Asymétrie documentée.
- **Produits exclus (`bl_exclu = 1`) non restitués par défaut**, avec une extension
  optionnelle du contrat : `"inclureExclus": true` (champ facultatif, le payload exact du
  ticket fonctionne inchangé).
- **Comptages manuels hors périmètre** : RG4 vise explicitement « la table TRPPU_TMH », et
  `trppu_scenario_comptages_manuels` n'est jamais reporté dans `trppu_tmh` par le code —
  l'additionner créerait un double comptage. **À confirmer par le PO.**
- **Nomenclature produits (CA6) non implémentée** : `OS / IP / CO / EP / PQ` n'existe nulle
  part dans TRPPU, où les codes produits sont créés dynamiquement depuis le référentiel des
  objets. Le service restitue les `co_produit` du scénario **sans transcodage**. Un mapping
  TRPPU ↔ OPTIPACC nécessiterait un ticket dédié.
- `codeProduit` borné à 3 caractères (longueur réelle de la colonne) et non 2 comme le
  `CO_PRODUIT_PATTERN` du module scénario, qui rejetterait un produit tel que `PPI`.
- Scénario existant mais rattaché à un autre site → `404` (du point de vue de l'appelant il
  est introuvable pour ce site) plutôt que `403`.
- `SUM()` MySQL renvoyant un `Decimal`, la conversion en entier est faite côté API.

## 7. Comment tester
```sql
UPDATE trppu_scenario SET statut = 'VALIDE', trafic_agrebal_calcule = 1 WHERE id_scenario = 12;
```
```bash
curl -X POST http://localhost:8080/trppu-api/optipacc/scenario-trafic-brut \
  -H "Content-Type: application/json" -d '{"codeRegate":"123456","scenarioId":12}'
```
Contrôle de cohérence : comparer avec `GET /trppu-api/scenarios/12/tmh` et avec la requête
SQL du §4 exécutée directement en base.

## 8. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| CA1 — produits + volumes bruts d'un site/scénario existants | endpoint POST |
| CA2 — volumes conformes aux calculs TRPPU | somme directe sur `trppu_tmh`, aucun recalcul |
| CA3 — trafics manuels intégrés | `SUM ... GROUP BY co_produit` (lignes multiples) |
| CA4 — ajustements de prévision intégrés | `volume_previsionnel_recalcule` prioritaire |
| CA5 — aucun détail de calcul restitué | sortie limitée à `codeProduit` + `volumeBrut` |
| CA6 — codifications communes | ⚠️ restitution sans transcodage, cf. §6 |
| CA7 — consommable unitairement | service stateless, 1 requête base |

## 9. ➡️ Commentaire Jira (à coller)

> **✅ Service de restitution des volumes bruts — livré.**
>
> **Endpoint :** `POST /trppu-api/optipacc/scenario-trafic-brut`
> (les services destinés à OPTIPACC sont regroupés sous le segment `/optipacc`).
>
> **Entrée :** `{ "codeRegate": "123456", "scenarioId": 789 }`
> **Sortie :** `{ "codeRegate": "123456", "scenarioId": 789, "produits": [ { "codeProduit": "OS", "volumeBrut": 1250000 } ] }`
> **Codes :** `200` OK · `404` site inconnu / scénario inexistant ou rattaché à un autre
> site · `409` scénario non exploitable · `422` requête invalide · `500` erreur technique.
>
> **Composition du volume brut (RG4)** — somme par produit sur `trppu_tmh` :
> `volume_realise` (constaté) `+ COALESCE(volume_previsionnel_recalcule, volume_previsionnel)`
> (prévisionnel recalculé), **sommée sur toutes les lignes du produit** : un trafic manuel
> est une ligne TMH supplémentaire (un même `co_produit` peut avoir plusieurs lignes depuis
> la migration du 24/06/2026), il s'additionne donc naturellement. Le flag `bl_manuel` n'est
> volontairement **pas** utilisé comme filtre : c'est un indicateur de provenance, et une
> correction manuelle (DSR-649) le pose aussi sur une ligne calculée — filtrer dessus
> provoquerait pertes ou doubles comptages. Toutes les interventions utilisateur (RG5) sont
> donc reflétées, et aucun détail de calcul n'est renvoyé (RG6/CA5).
>
> **Scénarios interrogeables :** statut `VALIDE` **ou** `EN PRODUCTION`, **et**
> `trafic_agrebal_calcule = 1`. À noter, écart volontaire avec DSR-690 qui ne *liste* que
> les `VALIDE` : un projet OPTIPACC déjà créé doit continuer à fonctionner après la mise en
> production du scénario. Le message du `409` précise le statut et l'état du calcul Agrébal.
>
> **Produits exclus :** les produits marqués exclus dans le TMH ne sont pas restitués (c'est
> une décision utilisateur). Extension optionnelle pour les cas de contrôle :
> `{"codeRegate": "...", "scenarioId": ..., "inclureExclus": true}` — champ facultatif, le
> payload du contrat fonctionne inchangé.
>
> **⚠️ Deux points à arbitrer côté métier :**
> 1. **Comptages manuels** (`trppu_scenario_comptages_manuels`) **non inclus** : RG4 vise
>    explicitement la table TRPPU_TMH, et ces comptages n'y sont jamais reportés — les
>    additionner créerait un double comptage. À confirmer.
> 2. **Nomenclature produits (CA6)** : les codes `OS / IP / CO / EP / PQ` ne sont définis
>    nulle part dans TRPPU, où les codes produits proviennent dynamiquement du référentiel
>    des objets. Le service restitue les codes **tels quels, sans transcodage**. Si un
>    mapping TRPPU ↔ OPTIPACC est attendu, merci de fournir la table de correspondance : ce
>    sera un ticket dédié.
>
> **Pré-requis de mise en service :** comme pour DSR-690, `trafic_agrebal_calcule` est posé
> par le batch Agrébal (DSR-702/703). Tant qu'il n'a pas tourné, le service répond `409`.
>
> **Documentation :** `api_docs/api_trppu_optipacc.md`.
> **Tests :** `tests/test_optipacc.py` (dont l'arithmétique du volume brut vérifiée sur une
> base en mémoire : lignes multiples par produit, repli du prévisionnel, produits exclus) et
> `tests/test_tmh_volume_brut.py` (persistance de la colonne) — suite complète OK (124/124).
