# Résolution — DSR-645 (Écriture des neutralisations d'un scénario)

## 1. Statut
**Réaligné sur db_10_09_2026.** Le code s'appuyait sur une colonne **`type`**
(enum FERIE/PEAK/SAISON) **supprimée en base** → toutes les requêtes échouaient à
l'exécution (`Unknown column 'type'`). Migré vers **`motif` (texte libre)** + clé unique
`(id_scenario, dt_debut, dt_fin)`, conformément au ticket et au schéma réel.

## 2. Constat initial (non conforme)
- `trppu_neutralisations` (db_10_09) : colonnes `id_neutralisation, id_scenario, dt_debut,
  dt_fin, nb_jour, motif, dt_creation, id_rh` ; **pas de `type`** ; unique `(id_scenario,
  dt_debut, dt_fin)` ; CHECK `nb_jour > 0` et `dt_debut <= dt_fin`.
- Code (avant) : INSERT/SELECT/DELETE sur `type`, regroupement lecture par `type`,
  audit lisant `type`. → cassé runtime.

## 3. Décision (arbitrage retenu)
**`motif` = texte vraiment libre** saisi par l'utilisateur (conforme à la lettre du
ticket). Conséquences :
- Plus de discriminateur FERIE/PEAK/SAISON : chaque neutralisation est **une période**
  unique `(dt_debut, dt_fin)`.
- `nb_jour` déterminé par la **structure** (et non le libellé) :
  - **jour unique** (`dt_debut == dt_fin`) → `nb_jour = 1` ;
  - **période** → jours ouvrés/ouvrables hors fériés et week-ends selon
    `nb_jours_semaine` (`compute_nb_jour_neutralise_db`, inchangé).
- La lecture (DSR-652) devient une **liste à plat** (le regroupement feries/peak/saison
  est abandonné).

## 4. Fichiers modifiés
- `app/routes/trppu_neutralisations/schemas.py` : `NeutralisationCreate` (motif au lieu de
  type), `NeutralisationOut`, `NeutralisationItem` (lecture à plat). `NeutralisationsOut`
  / blocs groupés supprimés.
- `app/routes/trppu_neutralisations/helpers.py` : `SELECT` sur `motif` ; suppression de
  `group_neutralisations`.
- `app/routes/trppu_neutralisations/routes.py` : POST (motif, nb_jour structurel, 409 si
  période déjà présente), DELETE (par `dt_debut` + `dt_fin`), GET (liste à plat).
- `app/routes/trppu_scenario/routes.py` : agrégateur `GET /{id}/edition` (DSR-654) →
  neutralisations renvoyées à plat.
- `app/routes/trppu_audit/helpers.py` : SELECT/details `type` → `motif`.

## 5. Endpoints livrés
- `POST /trppu-api/scenarios/{id_scenario}/neutralisations`
  Body : `{ "dt_debut", "dt_fin", "motif", "id_rh" }` → `201` `{id, dt_debut, dt_fin,
  nb_jour, motif, action:"created"}`. `409` si période déjà neutralisée ; `422` si
  `nb_jour = 0` ; `409` si scénario figé/archivé.
- `DELETE /trppu-api/scenarios/{id_scenario}/neutralisations?dt_debut=&dt_fin=` → `204`
  (`404` si aucune ligne).
- `GET /trppu-api/scenarios/{id_scenario}/neutralisations` → `list[NeutralisationItem]`.

## 6. Conformité base de données (db_10_09_2026)
- INSERT sur `(id_scenario, dt_debut, dt_fin, nb_jour, motif, id_rh)` ; `dt_creation`
  via DEFAULT CURRENT_TIMESTAMP. ✅
- `nb_jour > 0` garanti (jour unique = 1 ; période rejetée en 422 si 0). ✅
- Unicité `(id_scenario, dt_debut, dt_fin)` respectée (pré-check + 409). ✅
- `id_rh` crypté (Fernet). ✅

## 7. Impact croisé (vérifié)
- **DSR-652 (lecture)** : la réponse passe de regroupée (feries/peak/saison) à **liste à
  plat** → **contrat IHM modifié** ; le doc `DSR-652_resolution.md` (déjà obsolète) est
  à réécrire. **À acter PO/IHM.**
- **DSR-654 (`/edition`)** : bloc `neutralisations` désormais à plat (aligné).
- **Audit** : trace `motif` au lieu de `type`.
- `import app.main` OK ; **18/18 tests** passants.

## 8. Mapping critères d'acceptance
| Critère | Couverture |
| ------- | ---------- |
| 2-3 jours FERIE non consécutifs → 1 ligne/jour, dt_debut=dt_fin, nb_jour=1, motif=FERIE | POST jour unique (nb_jour=1), motif libre="FERIE" |
| Péak ~40 j → 1 ligne, nb_jour calculé selon 5j/6j, motif=PEAK PERIODE | POST période + `compute_nb_jour_neutralise_db` |
| dt_creation = date du jour ; id_rh crypté | DEFAULT + `encrypt_id_rh` |
| Suppression d'un jour férié | DELETE (dt_debut=dt_fin=jour) |
| Suppression de la période PEAK | DELETE (dt_debut, dt_fin) |

⚠️ **Caveat conservé** : l'exemple PEAK du ticket (40 j → 18 en 5j / 28 en 6j) est
arithmétiquement incohérent (10 samedis + 11 dimanches sur 40 j impossible). Le calcul
`compute_nb_jour_neutralise_db` (inchangé) produit les valeurs cohérentes ; à revalider PO.

## 9. ➡️ Commentaire Jira (à coller)

> **Réalignement base** : la table `trppu_neutralisations` n'a plus de colonne `type` ;
> la catégorie est portée par `motif` (texte libre). Le service a été migré en
> conséquence.
>
> **Ajout** — `POST /trppu-api/scenarios/{id_scenario}/neutralisations`
> Entrée : `dt_debut`, `dt_fin` (= dt_debut pour un jour unique), `motif` (texte libre),
> `id_rh` (crypté en base).
> `nb_jour` calculé serveur : 1 pour un jour unique ; pour une période, nombre de jours
> ouvrés/ouvrables hors fériés et week-ends selon la semaine du scénario (5 ou 6 jours).
> Sortie : la neutralisation créée. 409 si la période est déjà neutralisée.
>
> **Suppression** — `DELETE /trppu-api/scenarios/{id_scenario}/neutralisations?dt_debut=&dt_fin=`
> supprime la ligne (scénario, dt_debut, dt_fin) ; 404 si elle n'existe pas.
>
> **Lecture** — `GET /trppu-api/scenarios/{id_scenario}/neutralisations`
> renvoie la liste à plat des neutralisations (id, dt_debut, dt_fin, nb_jour, motif).
>
> **À acter PO/IHM** : `motif` étant du texte libre, la lecture n'est plus regroupée par
> FERIE/PEAK/SAISON mais renvoyée à plat — l'IHM doit s'aligner. L'exemple PEAK du ticket
> (18/28) reste à revalider (arithmétique incohérente).
