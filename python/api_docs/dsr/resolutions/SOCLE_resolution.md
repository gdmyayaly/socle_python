# Résolution — SOCLE transverse (NEW-1 / NEW-2 / NEW-3)

## 1. Statut
**Terminé.** Briques transverses requises par plusieurs tickets : cryptage `id_rh`
(Fernet réversible), service de calcul des jours (+ table de jours fériés) et
migrations de schéma. 12 tests unitaires verts.

## 2. Fichiers créés / modifiés
- **Créés**
  - `app/security/__init__.py`, `app/security/crypto.py` — `encrypt_id_rh` / `decrypt_id_rh` (Fernet).
  - `app/services/__init__.py`, `app/services/jours_service.py` — calcul jours ouvrés/ouvrables/neutralisés + fériés FR + accès table.
  - `scripts/gen_jours_feries_sql.py` — génère le seed des fériés.
  - `db_migrations/001_widen_id_rh_columns.sql`
  - `db_migrations/002_add_param_columns.sql`
  - `db_migrations/003_create_trppu_jours_feries.sql`
  - `db_migrations/004_seed_trppu_jours_feries.sql` (généré, 176 lignes 2020-2035)
  - `db_migrations/README.md`
  - `tests/test_crypto.py`, `tests/test_jours_service.py`, `tests/test_recompute_realise_prev.py`
- **Modifiés**
  - `app/config.py` — ajout `ID_RH_CRYPTO_KEY`.
  - `requirements.txt` — ajout `cryptography` et `pytest` (dev).

## 3. Migrations / dépendances
- Migrations `001`→`004` à appliquer dans l'ordre (cf. `db_migrations/README.md`).
- Variable d'env requise : **`ID_RH_CRYPTO_KEY`** (secret de cryptage ; n'importe quel
  secret accepté, dérivé en clé Fernet, ou clé Fernet native de 44 car.).
- `cryptography` déjà présent dans l'environnement (v46).

## 4. Comment tester
```bash
python -m pytest tests/ -q          # 12 tests : crypto + jours + recompute
python scripts/gen_jours_feries_sql.py 2020 2035   # régénère le seed
python -c "import app.main"         # import sans DB (connexion lazy)
```

## 5. Hypothèses & écarts
- **Cryptage réversible (Fernet)** retenu (choix validé) → colonnes `id_rh*`
  élargies en `VARCHAR(255)` (un token Fernet dépasse 40 car.).
- **Jours fériés = table `trppu_jours_feries`** (choix validé), seedée par calcul
  Computus (fériés nationaux FR ; Alsace-Moselle non inclus).
- **Erreurs arithmétiques dans les exemples des tickets** (à confirmer PO) :
  - DSR-613 : l'exemple annonce 272 ouvrés bruts / 262 nets ; le compte correct est
    **282 / 272** (le nb de samedis 339-272=67 est impossible sur 396 j ; il y en a 57).
    Le côté ouvrables (339 → 328) du ticket est correct.
  - DSR-645 PEAK : l'exemple « 10 samedis + 11 dimanches » sur 40 j est impossible ;
    le calcul correct donne **28 (5j) / 34 (6j)** (ticket : 18 / 28). L'exemple
    **SAISON** (10 / 12) est, lui, cohérent et reproduit exactement.
  → Le code applique la **définition** (ouvrés = lun-ven, ouvrables = lun-sam,
    fériés déduits selon la règle), pas les valeurs erronées des exemples.

## 6. Mapping (items socle)
| Item | Couverture |
| ---- | ---------- |
| NEW-1 migration colonnes/enum | `db_migrations/001` & `002` |
| NEW-2 crypto id_rh | `app/security/crypto.py` + tests |
| NEW-3 jours fériés + service | `app/services/jours_service.py` + table + seed + tests |

## 7. ➡️ Commentaire Jira (à coller)

> **Socle technique livré** (pré-requis aux US DSR-634/644/645/646/656/661/613).
>
> - **Cryptage id_rh** : module réversible (Fernet) `app/security/crypto.py`, clé via
>   variable d'env `ID_RH_CRYPTO_KEY`. Les colonnes `id_rh*` sont élargies en
>   VARCHAR(255) (migration `001`) car un token chiffré dépasse 40 caractères.
> - **Jours fériés** : table `trppu_jours_feries` (migration `003`) + seed national
>   2020-2035 (migration `004`, généré par `scripts/gen_jours_feries_sql.py`).
> - **Migrations** `001`→`004` à appliquer en recette (voir `db_migrations/README.md`) ;
>   la `002` ajoute `id_rh`/`dt_creation` et remplace l'enum `LOCAL` par `SAISON`
>   sur `trppu_neutralisations` (⚠️ vérifier l'absence de données `LOCAL` existantes).
> - **Tests** : 12 tests unitaires (crypto, calcul des jours, bornes réalisé/prév).
>
> **⚠️ Points à valider (PO)** : deux exemples chiffrés des tickets sont
> arithmétiquement erronés et n'ont pas été reproduits tels quels — le code applique
> la définition métier :
> - DSR-613 : nbJoursOuvres correct = **272** (l'exemple affiche 262) ; nbJoursOuvrables
>   = **328** (conforme).
> - DSR-645 PEAK : nb_jour correct = **28** (5j) / **34** (6j) (l'exemple affiche 18/28,
>   basé sur 10 samedis + 11 dimanches sur 40 jours, ce qui est impossible). L'exemple
>   SAISON (10/12) est correct.
