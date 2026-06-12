# Résolution — DSR-666 (Trafics Databricks avec date pivot)

## 1. Statut
**Livré, sous réserve de confirmations données.** Nouveau service YS04 qui récupère les
trafics Databricks d'un site sur une période, ventile **réel vs prévisionnel** selon une
**date pivot**, et renvoie la **somme par objet** (une ligne par produit). Le mapping des
produits (`lb_type_objet` → code produit) et les colonnes sources sont **variabilisés**
(surchargeables par variables d'environnement) car le ticket et le JSON Databricks réel
divergent (cf. §5).

## 2. Fichiers créés / modifiés
- `app/config.py` : bloc DSR-666 variabilisé (`TRAFIC_PRODUITS`, `TRAFIC_PRODUIT_MAPPING`,
  `TRAFIC_COL_OBJET`, `TRAFIC_COL_CONSTATE`, `TRAFIC_COL_PREVISIONNEL`).
- `app/routes/trafics_helpers.py` : `validate_params_pivot`, `split_by_pivot`,
  `map_produit`, `empty_trafics_accumulator`, `accumulate_trafics`.
- `app/routes/trafics.py` : `build_period_queries` (extrait, mutualisé) + endpoint
  `GET /trppu-api/trafics/get_trafics_pivot`.
- L'endpoint existant `GET /get_trafics` est **conservé inchangé** (consommé par
  DSR-613/634/648) — DSR-666 est ajouté en **endpoint séparé** pour ne pas casser le contrat.

## 3. Endpoint livré

`GET /trppu-api/trafics/get_trafics_pivot`

### Entrées (query, format AAAAMMJJ ou AAAA-MM-JJ)
| Paramètre | Obligatoire | Description |
| --------- | ----------- | ----------- |
| `co_regate` | Oui | code régate du site |
| `date_debut` | Oui | début de période |
| `date_fin` | Oui | fin de période |
| `date_pivot` | Oui | date pivot (jour de l'appel ou date de mise en œuvre) |

### Règle pivot
- dates **< pivot** → trafic **réel** (constaté/brut), prévisionnel = 0
- dates **>= pivot** → trafic **prévisionnel**, réel = 0
- découpe au jour près **avant** requête → aucune granularité mois/semaine à cheval.
- période passée → que du réel ; future → que du prévisionnel ; mixte → les deux.

### Sortie — `200 OK`
```json
{
  "co_regate": "400300",
  "date_debut": "2025-03-01", "date_fin": "2026-03-31", "date_pivot": "2025-10-01",
  "execution_time_s": 1.2, "count": 6,
  "trafics": [
    { "co_produit": "OO", "trafic_brut": 3500, "trafic_previsionnel": 2435 },
    { "co_produit": "OS", "trafic_brut": 1230, "trafic_previsionnel": 456 },
    { "co_produit": "PRESSE", "trafic_brut": 4658, "trafic_previsionnel": 2563 },
    { "co_produit": "PPI", "trafic_brut": 1532, "trafic_previsionnel": 956 },
    { "co_produit": "COLIS", "trafic_brut": 458, "trafic_previsionnel": 526 },
    { "co_produit": "IP", "trafic_brut": 0, "trafic_previsionnel": 0 }
  ],
  "nb_jours": { "nbJoursOuvres": 485, "nbJoursOuvrables": 584 }
}
```
Les 6 objets sont **toujours présents** (hydratés à 0).

### Erreurs (`400`, message rappelant les paramètres)
Paramètre manquant (dont `date_pivot`), `date_debut > date_fin`, ou période > 2 ans.

## 4. Conformité base de données
- `co_produit` = `char(2)` (table `trppu_produit`) : `PRESSE`/`COLIS`/`PPI` **dépassent
  2 caractères** → ils ne peuvent pas être des `co_produit` bruts. Le service renvoie
  des **codes objets** (mapping variabilisé) ; la résolution finale vers `co_produit`
  (≤ 2 car.) reste à arbitrer côté consommateur. **À confirmer PO.**
- Source Databricks (gold) : tables `g_trppu_trafics_jour|semaine|mois`, filtre
  `co_regate`, dates sur `da_comptage` / `co_semaine_comptage` (AAAA-NS) /
  `co_mois_comptage` (AAAA-MM) — conforme au ticket.

## 5. Hypothèses & écarts (⚠️ à lever avant recette)
1. **Noms de colonnes trafics** : le ticket cite `nb_objet_retenu` (brut) et
   `nb_objet_prevu_recadre_bu` (prévisionnel) ; le JSON Databricks réel expose
   `trafic_constate` / `trafic_prevu`. → défaut = noms du ticket, **surchargeables**
   via `TRAFIC_COL_CONSTATE` / `TRAFIC_COL_PREVISIONNEL`. **À confirmer.**
2. **Mapping `lb_type_objet` → produit** : seul `OS` (« COURRIER - OBJETS SIGNALES (OS) »)
   est connu via l'exemple. Les 5 autres libellés sont des valeurs par défaut à
   confirmer, surchargeables via `TRAFIC_PRODUIT_MAPPING` (JSON). **À confirmer.**
   - **Fusion N→1 supportée** : plusieurs `lb_type_objet` peuvent pointer vers un même
     code produit ; leurs trafics sont alors sommés sur ce produit (regroupement). Il
     suffit d'ajouter les libellés concernés avec la même valeur cible dans le mapping.
3. **Sémantique réel/prév vs pré-découpage Databricks** : le service suppose que
   `nb_objet_retenu` ET `nb_objet_prevu_recadre_bu` sont renseignés **par ligne quelle
   que soit la date**, pour que le pivot choisisse la bonne valeur. Si Databricks
   pré-découpe déjà constaté/prévu sur la date du jour réelle, alors pour une période
   **entièrement passée avec pivot = date de mise en œuvre** (cas 2), la zone prévisionnelle
   [pivot, fin] pourrait être à 0. **À valider avec l'équipe data.**

## 6. Comment tester
```
GET /trppu-api/trafics/get_trafics_pivot?co_regate=400300            -> 400 (params manquants)
GET /...?co_regate=400300&date_debut=20250301&date_fin=20260331&date_pivot=20251001  -> réel+prév
GET /...&date_debut=20261001&date_fin=20270331&date_pivot=20261001   -> que du prévisionnel
GET /...  (période > 2 ans)                                          -> 400 (dépasse 2 ans)
```
Vérifier en base Databricks la somme par objet pour les 3 cas (réel/prév).

## 7. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| Manque période / régate / une date / date_pivot → 400 rappelant les params | `validate_params_pivot` |
| Période > 2 ans → 400 | contrôle `MAX_DATE_RANGE_DAYS` |
| Période passée → réel + prév | `split_by_pivot` zone réelle + prév |
| Période mixte → réel + prév | idem |
| Période future → que du prévisionnel | `split_by_pivot` (reel=None) |
| 1 ligne par objet = somme des trafics | agrégation + hydratation 6 produits |
| Vérif base trafics & objets | requêtes Databricks tracées (DEBUG_SHOW_QUERY) |

## 8. ➡️ Commentaire Jira (à coller)

> **URL d'appel**
> `GET /trppu-api/trafics/get_trafics_pivot`
> Exemple : `GET /trppu-api/trafics/get_trafics_pivot?co_regate=400300&date_debut=20250301&date_fin=20260331&date_pivot=20251001`
>
> **Données d'entrée** (format AAAAMMJJ)
> - `co_regate` | code régate du site.
> - `date_debut` | début de la période.
> - `date_fin` | fin de la période.
> - `date_pivot` | date du jour de l'appel ou date de mise en œuvre.
>
> **Règle de récupération**
> - dates avant le pivot => trafic réel (constaté), prévisionnel = 0
> - dates à partir du pivot => trafic prévisionnel, réel = 0
> - période passée => que du réel ; future => que du prévisionnel ; mixte => les deux
>
> **Données de sortie**
> une ligne par objet (les 6 : OO, OS, PRESSE, PPI, COLIS, IP, toujours présents, à 0 si
> pas de trafic) avec la somme des trafics sur la période et le site :
> ```json
> { "co_produit": "OO", "trafic_brut": 3500, "trafic_previsionnel": 2435 }
> ```
> - paramètre manquant (dont date_pivot) => 400 rappelant les paramètres
> - période > 2 ans => 400
>
> **Traçabilité / Kibana**
> chaque appel logue co_regate, les bornes et la date pivot ; les requêtes Databricks
> sont traçables (mode debug).
>
> **À valider PO / data**
> 1. Noms réels des colonnes trafics : `nb_objet_retenu`/`nb_objet_prevu_recadre_bu` (ticket)
>    ou `trafic_constate`/`trafic_prevu` (donnée Databricks observée) ? (variabilisé,
>    surchargeable sans livraison).
> 2. Mapping `lb_type_objet` → produit : seul « (OS) » est connu, confirmer les 5 autres
>    libellés (variabilisé via env). La fusion de plusieurs libellés vers un même produit
>    (regroupement, trafics sommés) est supportée — préciser les éventuels regroupements.
> 3. `co_produit` est `char(2)` en base : PRESSE/COLIS/PPI > 2 caractères — quelle est la
>    correspondance finale vers les codes produits stockés ?
> 4. Les deux trafics (réel et prévisionnel) sont-ils renseignés par ligne quelle que soit
>    la date, ou Databricks pré-découpe-t-il déjà sur la date du jour ? (impacte le cas
>    « période passée + pivot = mise en œuvre »).
