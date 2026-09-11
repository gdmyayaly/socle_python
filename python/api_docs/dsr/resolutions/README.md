# Résolutions des tickets DSR — Index

Chaque fichier décrit l'implémentation livrée pour un ticket et contient un bloc
**« ➡️ Commentaire Jira »** prêt à coller. Voir aussi `SOCLE_resolution.md` pour les
briques transverses (cryptage, jours fériés, migrations).

| Ticket | Sujet | Sens | Statut | Fiche |
| ------ | ----- | ---- | ------ | ----- |
| SOCLE | crypto + jours fériés + migrations | infra | ✅ | [SOCLE](SOCLE_resolution.md) |
| DSR-613 | nb jours ouvrés/ouvrables (RecupererTrafics) | calcul | ✅ | [613](DSR-613_resolution.md) |
| DSR-634 | création scénario (scénario+site+TMH) | écriture | ✅ | [634](DSR-634_resolution.md) |
| DSR-644 | écriture comptages manuels | écriture | ✅ | [644](DSR-644_resolution.md) |
| DSR-645 | écriture neutralisations (+ calcul nb_jour) | écriture | ✅ | [645](DSR-645_resolution.md) |
| DSR-646 | écriture variations prévisionnelles | écriture | ✅ | [646](DSR-646_resolution.md) |
| DSR-649 | MAJ ciblée TMH | écriture | ✅ | [649](DSR-649_resolution.md) |
| DSR-650 | lecture TMH | lecture | ✅ | [650](DSR-650_resolution.md) |
| DSR-651 | lecture variations | lecture | ✅ | [651](DSR-651_resolution.md) |
| DSR-652 | lecture neutralisations | lecture | ✅ | [652](DSR-652_resolution.md) |
| DSR-653 | lecture comptages | lecture | ✅ | [653](DSR-653_resolution.md) |
| DSR-654 | édition (agrégateur) | lecture | ✅ | [654](DSR-654_resolution.md) |
| DSR-655 | lecture périodes scénario | lecture | ✅ | [655](DSR-655_resolution.md) |
| DSR-656 | MAJ scénario EN COURS | écriture | ✅ | [656](DSR-656_resolution.md) |
| DSR-659 | MAJ TMH recalculé (batch) | écriture | ✅ | [659](DSR-659_resolution.md) |
| DSR-660 | lecture rétention PIC (merge) | lecture | ✅ | [660](DSR-660_resolution.md) |
| DSR-661 | écriture coefficient PIC | écriture | ✅ | [661](DSR-661_resolution.md) |
| DSR-689 | volumes bruts par produit (OPTIPACC) | lecture | ✅ | [689](DSR-689_resolution.md) |
| DSR-690 | liste des scénarios exploitables (OPTIPACC) | lecture | ✅ | [690](DSR-690_resolution.md) |
| DSR-705 | trafics Agrébal d'un scénario (OPTIPACC) | lecture | ✅ | [705](DSR-705_resolution.md) |
| DSR-707 | mise en production d'un scénario (OPTIPACC) | écriture | ✅ | [707](DSR-707_resolution.md) |

## Pré-requis d'exploitation (rappel)
1. Variable d'environnement **`ID_RH_CRYPTO_KEY`** (cryptage id_rh).
2. Migrations `db_migrations/001`→`004` appliquées (ordre dans `db_migrations/README.md`).
3. Dépendance `cryptography` (présente).
4. **Services OPTIPACC (DSR-689/690/705/707)** : `trppu_scenario.trafic_agrebal_calcule`
   doit être posé à 1 par le batch Agrébal (DSR-702/703, hors de ce dépôt). Tant qu'il vaut
   0, la liste des scénarios est vide, la restitution des volumes et des trafics Agrébal
   répond 409, et la mise en production est refusée (409). Même remarque pour
   `trafic_pdi_calcule` et `Calcul_trafic_en_cours`, contrôlés par DSR-705 et DSR-707.
5. **`NB_AMAS_PAR_PAGE`** (facultative, défaut 200) : taille de page de
   `/trppu-api/optipacc/trafic-amas` (DSR-705).

## Points à valider avec le PO
Centralisés dans `../README_incomprehensions.md` (notamment : SAISON↔LOCAL, exemples
chiffrés erronés #14, id_pic_version par défaut, id_session_ihm, bornes « today »).

## Tests
`python -m pytest tests/ -q` (crypto, calcul des jours, bornes réalisé/prév).
