# TRPPU / DSR — Ordre d'implémentation

Séquencement recommandé pour implémenter les tickets DSR, déduit du **graphe de
dépendances** (cf. `README.md` §3), des **arbitrages bloquants**
(`README_incomprehensions.md`) et des **briques transverses**
(`README_ameliorations.md`, items `NEW-*` / `IMP-*`).

Tailles indicatives : **S** (petit), **M** (moyen), **L** (gros).

---

## 1. Principes directeurs

1. **Socle d'abord** : rien d'écriture propre n'est possible tant que la migration
   schéma, le cryptage `id_rh` et la source de jours fériés ne sont pas là.
2. **Tranches verticales** : livrer chaque fonctionnalité par paire
   **écriture + lecture** (ex. comptages 644 + 653) pour des incréments testables
   de bout en bout.
3. **Arbitrages avant code** : les 4 points 🔴 de `README_incomprehensions.md`
   (SAISON/LOCAL, colonnes manquantes, crypto, fériés) doivent être tranchés
   **avant** d'attaquer les phases qui en dépendent.
4. **Mutualiser** : un seul module par table (`trppu_tmh`, `trppu_neutralisations`,
   `trppu_variations`, `trppu_comptages`) sert à la fois l'écriture et la lecture.

---

## 2. Vue d'ensemble (phases)

| Phase | Objet | Tickets / items | Prérequis | Taille |
| :---: | ----- | --------------- | --------- | :----: |
| **0** | Arbitrages PO | (décisions) | — | S |
| **1** | Socle transverse | NEW-1, NEW-2, NEW-3 | Phase 0 | L |
| **2** | Création + harmonisation scénario | IMP-2, **DSR-634** (complément), **DSR-655** | 1 | M |
| **3** | Module TMH | NEW-4/IMP-1, **DSR-650**, **DSR-659**, **DSR-649** | 2 | M |
| **4** | Écriture + lecture des paramètres | **644+653**, **646+651**, **645+652** | 1 (+ 3 pour cohérence) | L |
| **5** | Rétention PIC | **DSR-660**, **DSR-661** | 1 | M |
| **6** | MAJ scénario + orchestration | **DSR-656**, **DSR-654** (+IMP-4) | 2,3,4,5 | M |
| **7** | Trafics enrichis + finitions | **DSR-613**, IMP-3, IMP-5, IMP-6 | 1 | S–M |

> **DSR-613** ne dépend que du socle (NEW-3) : il peut être traité dès la phase 1
> terminée, en parallèle des phases 2–5.

---

## 3. Détail par phase

### Phase 0 — Arbitrages PO (préalable, bloquant)
Trancher au minimum les items 🔴 de `README_incomprehensions.md` :
- `LOCAL` → `SAISON` (et données existantes ?) ;
- ajout des colonnes `id_rh` / `dt_creation` ;
- méthode de cryptage `id_rh` (réversible ? clé ? ≤ 40) ;
- source des jours fériés (table vs lib).
> Sans ces décisions, les phases 1, 3-bis et 4 partent sur des hypothèses.

### Phase 1 — Socle transverse  ⛏️ *fondation*
- **NEW-1** : migration schéma (`ALTER TABLE` : enum SAISON, colonnes `id_rh`/`dt_creation`).
- **NEW-2** : `app/security/crypto.py` (`encrypt_id_rh` / `decrypt_id_rh`), clé en config.
- **NEW-3** : table/lib jours fériés + `app/services/jours_service.py`
  (`compute_nb_jours`, `compute_nb_jour_neutralise`).
- **Débloque** : 613, 634, 644, 645, 646, 656, 661.
- **Livrable testable** : tests unitaires crypto (round-trip) et jours
  (non-régression sur l'exemple DSR-613 : 262 / 328).

### Phase 2 — Création & harmonisation scénario
- **IMP-2** : étendre `INSERT` création, `SELECT_SCENARIO_SQL`, `ScenarioOut`,
  `UPDATE` pour `nb_jours_ouvres/ouvrables/scenario`, `dt_mise_en_oeuvre`,
  `dt_real_prev`, `id_rh_creation/maj`.
- **DSR-634** : compléter la création (utilise NEW-2 + NEW-3).
- **DSR-655** : lecture des périodes/jours (dépend de l'alimentation faite en IMP-2,
  sinon renvoie des `NULL`).
- **Livrable testable** : créer un scénario → relire ses périodes/jours conformes.

### Phase 3 — Module TMH
- **NEW-4 / IMP-1** : module `app/routes/trppu_tmh/` unifié ; `DSR-659` (upsert
  batch) comme service de référence, **DSR-649** = cas particulier (UPDATE ciblé).
- **DSR-650** : lecture TMH (avec `bl_exclu` pour le grisage IHM).
- **Prérequis** : les lignes TMH sont créées en phase 2 (DSR-634).
- **Livrable testable** : créer → lire → modifier un TMH, cohérence IHM/base.

### Phase 4 — Écriture + lecture des paramètres (tranches verticales)
À livrer par paires, chacune indépendante des autres :
- **DSR-644 + DSR-653** : comptages manuels (écriture / lecture).
- **DSR-646 + DSR-651** : variations prévisionnelles (écriture / lecture).
- **DSR-645 + DSR-652** : neutralisations (écriture avec calcul `nb_jour` via NEW-3 / lecture regroupée par type).
- **Prérequis** : NEW-1 (colonnes/enum), NEW-2 (crypto), NEW-3 (pour 645).
- **Livrable testable par paire** : ajouter/modifier/supprimer puis relire.

### Phase 5 — Rétention PIC
- **DSR-660** : lecture avec merge défaut (`id_pic_version=1`) ↔ surcharge scénario.
- **DSR-661** : écriture (create-version-then-coef), utilise NEW-2.
- **Prérequis** : arbitrage item 9 (clé naturelle, version, table legacy).
- **Livrable testable** : modifier un coef → relire le merge avec `modifie=true`.

### Phase 6 — Mise à jour & orchestration
- **DSR-656** : MAJ scénario EN COURS (périodes recalculées + appel **DSR-659**
  dans la même transaction). Réutilise `recompute_realise_prev` (révisé).
- **DSR-654** : édition (orchestration des lectures 655/650/653/651/652/660) ;
  option **IMP-4** = endpoint agrégateur `GET /scenarios/{id}/edition`.
- **Prérequis** : phases 2 à 5 livrées.
- **Livrable testable** : éditer un scénario complet → tous les blocs conformes.

### Phase 7 — Trafics enrichis & finitions
- **DSR-613** : enrichir `RecupererTrafics` / `CalculerNbJours` (dès NEW-3 prêt).
- **IMP-3** : verrou de statut homogène (`assert_not_fige` / EN COURS) sur toutes les écritures.
- **IMP-5** : traçabilité (`trppu_api_log`, `trppu_recalcul_log`).
- **IMP-6** : convention de nommage des champs de sortie (alias Pydantic).

---

## 4. Chemin critique

```
Phase 0 ─► Phase 1 (NEW-1/2/3) ─► Phase 2 (IMP-2 + 634 + 655)
                                      └─► Phase 3 (TMH: 659/649/650)
                                                  └─► Phase 6 (656 ─► 659, puis 654)
```
La **chaîne la plus longue** est `0 → 1 → 2 → 3 → 6`. Les phases **4, 5 et 7**
ne sont pas sur ce chemin et peuvent avancer en parallèle dès la phase 1 terminée.

---

## 5. Parallélisation possible (après Phase 1)

| Voie | Contenu | Indépendante de |
| ---- | ------- | --------------- |
| A | Phase 2 → 3 → 6 (cœur scénario/TMH/édition) | — |
| B | Phase 4 : 644+653, 646+651, 645+652 | A (sauf agrégation finale 654) |
| C | Phase 5 : 660 + 661 (PIC) | A, B |
| D | DSR-613 (Phase 7) | A, B, C |

> Seul **DSR-654** (phase 6) reconverge toutes les voies : il a besoin des lectures
> des voies A/B/C.

---

## 6. Checklist d'avancement

- [ ] **Phase 0** — arbitrages 🔴 validés par le PO
- [ ] **Phase 1** — NEW-1 (migration) · NEW-2 (crypto) · NEW-3 (jours/fériés)
- [ ] **Phase 2** — IMP-2 · DSR-634 complété · DSR-655
- [ ] **Phase 3** — module TMH (DSR-659/649) · DSR-650
- [ ] **Phase 4** — DSR-644+653 · DSR-646+651 · DSR-645+652
- [ ] **Phase 5** — DSR-660 · DSR-661
- [ ] **Phase 6** — DSR-656 · DSR-654 (+ IMP-4)
- [ ] **Phase 7** — DSR-613 · IMP-3 · IMP-5 · IMP-6

---

*Voir aussi : `README.md` (synthèse & graphe), `README_incomprehensions.md`
(arbitrages), `README_ameliorations.md` (détail NEW-* / IMP-*).*
