# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository layout

Monorepo with six independent sub-projects (part of the La Poste / DSR "TRPPU" ecosystem — TRPPU = average traffic per product for a postal site, computed via parameterizable scenarios). Code comments, docs, and Jira tickets are in French.

- **`python/`** — the main, active project: FastAPI backend ("trppu API", module YS04) backed by MySQL and Databricks SQL Warehouse.
- **`yb05/`** — module YB05: a **console batch** (`argparse` + `asyncio` + `aiomysql`, no HTTP server, no port) run as `python -m app.main <subcommand>`. Its technical base — MySQL connection, JSON logging, DB diagnostics (`app/health.py` returns dicts, it is *not* a set of routes), and a `.sql` script runner on the `Database` class — carries the DSR-696→704 traffic-computation logic in `app/traitements/`. Has its own `yb05/README.md`.
- **`yb06/`** — module YB06: **socle only**, destined to host Agrébal impact detection. `yb06/docs/DSR-715.md` (socle — done code-side, infra not covered) and `DSR-717.md` (Agrébal detection — **not developed**, several under-specified points). Read `DSR-715-717_analyse_yb06.txt` at the repo root before touching it.
- **`yb07/`** — module YB07: a console batch that **initialises `trppu_cles_repartition`** — S3 access (`app/services/s3.py`, the only `boto3` use in the repo) and `charger-cles-repartition`, which streams a CSV from S3 into the table (22.4 M rows, purge then batched commits). Commands: `db-info`, `db-check`, `s3-check`, `charger-cles-repartition`. Has its own `yb07/README.md`. *(Was `yb06/`, then `yb04/` — renamed to `yb07/` on 24/09/2026; the former bare-socle `yb07/` was deleted.)*

The batch socle is duplicated across `yb05/`, `yb06/` and `yb07/` rather than shared — a fix to `app/db/mysql.py` or `app/json_formatter.py` has to be carried over by hand. In `yb06/` and `yb07/` the log correlation field is the neutral `id_traitement` (not `id_scenario` as in `yb05/`), and `tests/test_log_convention.py` locks the logging convention documented in `docs/CONVENTION-LOGS.md` — read it before adding a log.

Analysis reports produced for the PMO live at the repo root as plain `.txt` (Teams-friendly: no Markdown, ≤ 72 columns, no accents): `DSR-729_rapport_PMO.txt`, `DSR-715-717_analyse_yb06.txt`.
- **`as03/`** — Angular 14 app skeleton. Has its own `as03/CLAUDE.md`; read it when working there.
- **`trppu/`** — Angular module source extracted from the larger front-end, kept here for reference/study only (no build setup). `trppu/ETUDE-COMPREHENSION.md` explains the front-end module and which backend endpoints it consumes.

## Commands (python/)

Run all commands from the `python/` directory.

```bash
pip install -r requirements.txt
python -m uvicorn app.main:app --reload      # dev server on http://localhost:8000
docker compose up --build                     # containerized run (port 8000)

python -m pytest tests/                       # all tests
python -m pytest tests/test_crypto.py         # one file
python -m pytest tests/test_crypto.py -k name # one test
```

Configuration comes from `python/.env` (loaded by `app/config.py`). Note: `app/config.py` is the source of truth for env var names (`SGBD_SERVER_WRITE`, `SGBD_APP_USER_READ`, `SGBD_APP_PWD_WRITE`, `SGBD_DB_NAME`, `DATABRICKS_*`, `JOURS_FERMES_API_*`…) — the table in `python/README.md` is partially outdated. `SKIP_MYSQL=true` skips MySQL connection.

Swagger UI is at `/docs` and is served from local static assets (`app/static/swagger-ui/`) so it works offline — don't reintroduce CDN URLs.

## Architecture (python/)

**Two data sources:**
- **MySQL** (`app/db/mysql.py`) — application data (scenarios, sites, products, comptages…). Exposes **two async pools**: `db_read` and `db_write` (separate hosts/credentials for read vs write). Async, with connection retry, `fetch_one`/`fetch_all`/`execute`, and `async with db.transaction()` for multi-statement commits.
- **Databricks SQL Warehouse** (`app/db/databricks.py`) — read-only analytical traffic data (`gold.trafics_jours` / `trafics_semaines` / `trafics_mois`), OAuth M2M service principal, synchronous with retry.

**Route packages:** each business domain lives in `app/routes/trppu_<domain>/` (scenario, site, produit, tmh, comptages, variations, neutralisations, pic_version, pic_coefficients, scenario_pic, audit, trafics) with a fixed internal structure:
- `routes.py` — FastAPI endpoints, exports `router` (re-exported by `__init__.py` and mounted in `app/main.py`)
- `schemas.py` — Pydantic v2 request/response models
- `helpers.py` — SQL constants and business logic (e.g. `fetch_scenario_or_404`, `assert_editable`, cascade deletes)

Follow this structure when adding a new domain. Flat modules in `app/routes/` (`health.py`, `databricks.py`, `mysql_debug.py`, `calcl_nbr_jours.py`) are older/utility routes.

**Cross-cutting pieces:**
- `app/services/jours_fermes_client.py` — external "jours fermés" (closed days) API client with TTL cache; its `JoursFermesAPIError` is mapped to a 503 by a global exception handler in `main.py`. `app/services/jours_service.py` computes working-day counts (`compute_nb_jours`), used when scenario periods change.
- `app/security/crypto.py` — reversible Fernet encryption of the user identifier `id_rh` (key `ID_RH_CRYPTO_KEY`; empty key = encryption disabled, stored in clear).
- Scenario lifecycle is a state machine (`EN COURS → VALIDE → EN PRODUCTION`, plus soft-delete archive and `est_fige` freeze) enforced in `trppu_scenario/statuts.py` and `helpers.py` (`assert_editable`, `assert_not_archive`).
- Logging: JSON logs (one file per day in `logs/`) via `app/json_formatter.py`; every HTTP request/response is logged by middleware; 422 validation failures are logged with `id_session_ihm` (a front-end tracing UUID passed as a query param) for Kibana grouping.

**Docs to consult before changing behavior:** `python/api_docs/` holds per-API specs and `python/api_docs/dsr/` holds integration/resolution notes keyed to Jira tickets (DSR-xxx); `python/jira/` holds the ticket descriptions. When a feature references a DSR number, the matching files there give the expected behavior.

**Scripts:** `python/scripts/` holds exactly two standalone tools (not part of the app): `gen_postman_collection.py` (regenerates `postman/trppu_collection.json` from the app's OpenAPI schema) and `controle_trafics_679.py` (gold-data controls, ex-`/trppu-api/trafics/test/*` routes). The Excel template generators for sites/produits/pic were removed.
