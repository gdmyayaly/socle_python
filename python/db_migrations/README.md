# Migrations base TRPPU (chantier DSR)

Scripts SQL **additifs** à appliquer sur la base `trppu` pour supporter les
tickets DSR. Ils ne modifient **pas** `db_analyse/schema_trppu.sql` (fichier
**généré** depuis les dumps JSON par `scripts/gen_schema_sql.py`).

## Ordre d'application

| # | Fichier | Objet | Tickets |
|---|---------|-------|---------|
| 001 | `001_widen_id_rh_columns.sql` | `id_rh*` → VARCHAR(255) (token Fernet) | 634, 661 |
| 002 | `002_add_param_columns.sql` | colonnes `id_rh`/`dt_creation` + enum `SAISON` | 644, 645, 646 |
| 003 | `003_create_trppu_jours_feries.sql` | table des jours fériés | 613, 645 |
| 004 | `004_seed_trppu_jours_feries.sql` | seed fériés FR 2020-2035 (généré) | 613, 645 |

```bash
mysql -h <host> -u <user> -p trppu < db_migrations/001_widen_id_rh_columns.sql
mysql -h <host> -u <user> -p trppu < db_migrations/002_add_param_columns.sql
mysql -h <host> -u <user> -p trppu < db_migrations/003_create_trppu_jours_feries.sql
mysql -h <host> -u <user> -p trppu < db_migrations/004_seed_trppu_jours_feries.sql
```

## Régénérer le seed des fériés

```bash
python scripts/gen_jours_feries_sql.py 2020 2035
```

## Points de vigilance

- **002** : `MODIFY ... ENUM('FERIE','PEAK','SAISON')` remplace `LOCAL`. Vérifier
  qu'aucune ligne `type='LOCAL'` n'existe (sinon la convertir au préalable). Voir
  `api_docs/dsr/README_incomprehensions.md` (item SAISON/LOCAL).
- **001** : nécessaire car le cryptage retenu est **réversible (Fernet)**, dont le
  token dépasse 40 caractères.
- Variable d'environnement requise par l'app : `ID_RH_CRYPTO_KEY` (secret de
  cryptage de l'id_rh).
