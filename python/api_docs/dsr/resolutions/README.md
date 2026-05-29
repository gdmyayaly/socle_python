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

## Pré-requis d'exploitation (rappel)
1. Variable d'environnement **`ID_RH_CRYPTO_KEY`** (cryptage id_rh).
2. Migrations `db_migrations/001`→`004` appliquées (ordre dans `db_migrations/README.md`).
3. Dépendance `cryptography` (présente).

## Points à valider avec le PO
Centralisés dans `../README_incomprehensions.md` (notamment : SAISON↔LOCAL, exemples
chiffrés erronés #14, id_pic_version par défaut, id_session_ihm, bornes « today »).

## Tests
`python -m pytest tests/ -q` (crypto, calcul des jours, bornes réalisé/prév).
