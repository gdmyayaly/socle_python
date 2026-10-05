# Résolution — DSR-737 (Agrébals et PDI d'un scénario ou d'un site)

## 1. Statut
**Terminé.** Nouvel endpoint de consultation dans le package `trppu_audit`. Lecture seule
(RG-006), sans pagination.

## 2. Fichiers créés / modifiés
- `app/routes/trppu_audit/routes.py` — endpoint `agrebals_pdi` + `_rejeter`. Le tag
  Swagger du package passe de « Audit id_rh » à « Audit ».
- `app/routes/trppu_audit/schemas.py` — `AgrebalsPdiRequest`, `AgrebalPdisOut`,
  `AgrebalsPdiOut` (camelCase du ticket).
- `app/routes/trppu_audit/helpers.py` — SQL, `extraire_pdi_ids`, `regrouper_par_agrebal`,
  `agrebals_du_site`.
- `tests/test_audit_agrebals_pdi.py` — 22 tests.
- `postman/trppu_collection.json` — régénérée.

## 3. Endpoint livré
`POST /trppu-api/audit/agrebals-pdi` — `{ "idScenario": 12345 }` **ou**
`{ "codeRegate": "372920" }`, jamais les deux (RG-001, sinon `422`).

```json
{
  "typeRecherche": "SITE", "idScenario": null,
  "codeRegate": "372920", "libelleSite": "SORIGNY PDC1",
  "nbAgrebals": 2, "nbPdis": 5,
  "agrebals": [
    { "agrebalUuid": "AGREBAL-001", "pdis": [100005554, 100011320, 10004389] },
    { "agrebalUuid": "AGREBAL-002", "pdis": [100075421, 100087456] }
  ],
  "message": null
}
```

Codes : `200` · `404` « Scénario introuvable. » / « Site introuvable. » (CA-04, CA-05) ·
`422` corps invalide · `500` technique.

## 4. Sources de données (choix)
- **Par scénario (RG-003, CA-03)** — `trppu_trafic_pdi`, où YB05 (DSR-702) écrit l'Agrébal
  et le PDI de chaque ligne calculée : c'est la photo exacte du calcul, juste même si
  l'Agrébal a changé ou disparu depuis. `trppu_agrebal_pdi` ne garde **aucun historique**
  (une ligne par Agrébal et par site, `uq_agrpdi_courant`) et ne permettrait pas de
  reconstituer un calcul passé.
- **Scénario pas encore calculé** — aucune ligne dans `trppu_trafic_pdi` : `200`, liste
  vide et `message` « Scénario non calculé… ». *Choix à confirmer avec le PO* (l'alternative
  serait de rendre les Agrébals actuels du site).
- **Par site (RG-002)** — `trppu_agrebal_pdi`, `agrebal_deleteddAt IS NULL`. La table n'a pas
  de `DATE_FIN_VALIDITE` : la suppression logique est le « mécanisme équivalent » que le
  ticket autorise. PDI lus dans `agrebal_pdiList` (même lecture que YB05).

## 5. Journalisation
`Début` / `Fin` / `Rejet` / `Erreur audit agrébals PDI` avec `type_recherche`, `id_scenario`
ou `co_regate`, `nb_agrebals`, `nb_pdis`, `duration_ms` ; champ racine `co_regate` posé dès
que le site est connu.
