# Migrations base TRPPU (chantier DSR)

Scripts SQL **additifs** à appliquer sur la base `trppu` pour supporter les
tickets DSR. Ils ne modifient **pas** `db_analyse/schema_trppu.sql` (fichier
**généré** depuis les dumps JSON par `scripts/gen_schema_sql.py`).

## Ordre d'application

| # | Fichier | Objet | Tickets |
|---|---------|-------|---------|
| 001 | `001_widen_id_rh_columns.sql` | `id_rh*` → VARCHAR(255) (token Fernet) | 634, 661 |
| 002 | `002_add_param_columns.sql` | colonnes `id_rh`/`dt_creation` + enum `SAISON` | 644, 645, 646 |

```bash
mysql -h <host> -u <user> -p trppu < db_migrations/001_widen_id_rh_columns.sql
mysql -h <host> -u <user> -p trppu < db_migrations/002_add_param_columns.sql
```

> **Jours fériés / fermés (613, 645)** : ne reposent plus sur une table en base.
> Ils sont récupérés via l'API jours-fermes (cf. `app/services/jours_fermes_client.py`,
> variable `JOURS_FERMES_API_BASE_URL`).

## Points de vigilance

- **002** : `MODIFY ... ENUM('FERIE','PEAK','SAISON')` remplace `LOCAL`. Vérifier
  qu'aucune ligne `type='LOCAL'` n'existe (sinon la convertir au préalable). Voir
  `api_docs/dsr/README_incomprehensions.md` (item SAISON/LOCAL).
- **001** : nécessaire car le cryptage retenu est **réversible (Fernet)**, dont le
  token dépasse 40 caractères.
- Variable d'environnement requise par l'app : `ID_RH_CRYPTO_KEY` (secret de
  cryptage de l'id_rh).
