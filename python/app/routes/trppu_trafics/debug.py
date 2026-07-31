"""DSR-679 — Routes de debug/test pour la structure gold TRPPU.

- `/test/jour_check` : **sonde de la maille jour** — exécute en base la requête de contrôle
  (niveaux × objets) puis les 2 requêtes de production (zone réelle / prévisionnelle).
  C'est la route à appeler en premier pour valider la structure de `g_trppu_trafics_jour`.
- `/test/config` : configuration effective (tables, colonnes, filtre de niveau).
- `/test/queries_preview` : SQL générée (paramètres substitués) pour les 3 cas du ticket
  ou pour des paramètres libres — à copier/coller tel quel dans Databricks.
- `/test/schema` et `/test/schema_raw` : `SELECT *` sur les vraies tables Databricks
  (identification des colonnes).
- `/test/objets` : objets et niveaux de regroupement réellement présents pour un site.
- `/test/echantillons` : échantillons codés en dur de la structure, pour raisonner sans
  accès Databricks.
- `/test/pivot_dry_run` : rejoue en mémoire la ventilation réel/prévisionnel selon la date
  pivot, pour valider les 3 cas du ticket et les cas d'erreur, sans Databricks.
"""

import logging
from datetime import datetime, timedelta

from fastapi import APIRouter
from starlette.concurrency import run_in_threadpool

from app.config import DATABRICKS_CATALOG, DATABRICKS_SCHEMA
from app.db.databricks import databricks
from app.routes.trppu_trafics.helpers import (
    ALIAS_TRAFIC,
    DATE_COLUMN_PERIODE,
    NIVEAU_REGROUPEMENT,
    OBJ_ALIAS,
    TABLES_PERIODE,
    TRAFIC_COL_ANNEE,
    TRAFIC_COL_CONSTATE,
    TRAFIC_COL_MOIS,
    TRAFIC_COL_NIVEAU,
    TRAFIC_COL_OBJET,
    TRAFIC_COL_PREVISIONNEL,
    TRAFIC_COL_REGATE,
    build_partition_conditions,
    build_period_queries,
    build_query,
    empty_accumulator,
    fmt_date,
    fqtn,
    render_sql,
    split_by_pivot,
    validate_params_pivot,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/trafics/test", tags=["Trafics (test DSR-679)"])

SITE_TEST = "400300"

# --- Cadences journalières par objet/produit TRPPU (constaté / prévu) pour synthétiser les
# lignes de la table jour dans le dry-run. Sommes prévisibles (cadence × nb jours).
# Les tables gold portent directement `co_type_objet` : aucun mapping à rejouer. ---
DAILY_RATES = {
    "OO": {"constate": 10, "prevu": 7},
    "OS": {"constate": 3, "prevu": 1},
    "PQ": {"constate": 8, "prevu": 5},
    "EQ": {"constate": 5, "prevu": 3},
    "CO": {"constate": 3, "prevu": 3},
    "PPI": {"constate": 2, "prevu": 1},
    # Pas de cadence pour IP -> aucun trafic (illustre l'objet absent du résultat,
    # la restitution étant dynamique).
    "IP": None,
}

# Cas du ticket (libellé, date_debut, date_fin, date_pivot).
CAS_TICKET = [
    ("1 - période entièrement passée (pivot = date de mise en oeuvre)",
     "20250301", "20260331", "20251001"),
    ("2 - période à cheval sur la date d'exécution (pivot = date du jour)",
     "20260301", "20270331", "20260701"),
    ("3 - période entièrement future (pivot = début de période)",
     "20261001", "20270331", "20261001"),
]


def _sample_trafics_jour(dt_debut: datetime, dt_fin: datetime) -> list[dict]:
    """Synthétise les lignes de `g_trppu_trafics_jour` (site test) sur [dt_debut, dt_fin]."""
    rows: list[dict] = []
    d = dt_debut
    while d <= dt_fin:
        for objet, rate in DAILY_RATES.items():
            if rate is None:
                continue
            rows.append({
                TRAFIC_COL_NIVEAU: NIVEAU_REGROUPEMENT,
                TRAFIC_COL_REGATE: SITE_TEST,
                TRAFIC_COL_OBJET: objet,
                "da_comptage": d.strftime("%Y-%m-%d"),
                TRAFIC_COL_ANNEE: d.year,
                TRAFIC_COL_MOIS: d.strftime("%Y-%m"),
                "co_semaine_comptage": f"{d.isocalendar()[0]}-{d.isocalendar()[1]:02d}",
                TRAFIC_COL_CONSTATE: rate["constate"],
                TRAFIC_COL_PREVISIONNEL: rate["prevu"],
            })
        d += timedelta(days=1)
    return rows


def _accumulate_range(rows: list[dict], rng, value_col: str, target_key: str, acc) -> None:
    """Somme `value_col` par objet pour les lignes dont la date ∈ rng (comme le GROUP BY SQL)."""
    if rng is None:
        return
    debut, fin = rng
    for row in rows:
        da = datetime.strptime(row["da_comptage"], "%Y-%m-%d")
        if not (debut <= da <= fin):
            continue
        if row.get(TRAFIC_COL_NIVEAU) != NIVEAU_REGROUPEMENT:  # comme le filtre SQL
            continue
        objet = row.get(TRAFIC_COL_OBJET)
        if objet is None:
            continue
        slot = acc.setdefault(objet, {"trafic_brut": 0, "trafic_previsionnel": 0})
        valeur = row.get(value_col)
        if valeur is None:
            continue
        slot[target_key] += valeur


def _config_effective() -> dict:
    """Tables, colonnes et filtres réellement utilisés par la requête (état .env)."""
    return {
        "catalog": DATABRICKS_CATALOG,
        "schema_gold": DATABRICKS_SCHEMA,
        "tables_trafics": {p: fqtn(t) for p, t in TABLES_PERIODE.items()},
        "jointures": (
            "aucune — les tables trafics portent déjà l'objet (co_type_objet) ; "
            "g_trppu_obj_mapping/g_trppu_entite/s_commun_calendrier_jour ne sont pas jointes"
        ),
        "filtre_niveau": {
            "colonne": TRAFIC_COL_NIVEAU,
            "valeur": NIVEAU_REGROUPEMENT,
            "env": "TRAFIC679_NIVEAU_REGROUPEMENT",
            "raison": (
                "les tables gold empilent SITE/ETABLISSEMENT/PIC/NATIONAL... : "
                "sans ce filtre les SUM sont gonflés"
            ),
        },
        "colonnes_trafics": {
            "regate": TRAFIC_COL_REGATE,
            "objet": TRAFIC_COL_OBJET,
            "constate": TRAFIC_COL_CONSTATE,
            "previsionnel": TRAFIC_COL_PREVISIONNEL,
            "partition_annee": TRAFIC_COL_ANNEE,
            "partition_mois": TRAFIC_COL_MOIS,
            "dates_par_maille": DATE_COLUMN_PERIODE,
        },
    }


@router.get("/config")
def config():
    """DEBUG — configuration effective de la requête DSR-679 (tables, colonnes, filtres)."""
    return _config_effective()


def _requetes_de_controle(co_regate: str) -> list[dict]:
    """Requêtes à passer en base pour valider la requête avant recette.

    Le risque principal n'est plus la jointure (il n'y en a plus) mais le **cumul des
    niveaux de regroupement** : une régate présente à plusieurs niveaux double les SUM.
    """
    jour = fqtn(TABLES_PERIODE["jours"])
    mois = fqtn(TABLES_PERIODE["mois"])
    return [
        {
            "controle": (
                f"niveaux de regroupement du site (si > 1 valeur, le filtre "
                f"{TRAFIC_COL_NIVEAU} = '{NIVEAU_REGROUPEMENT}' est indispensable)"
            ),
            "sql": (
                f"SELECT {TRAFIC_COL_NIVEAU}, COUNT(*) AS nb FROM {jour} "
                f"WHERE {TRAFIC_COL_REGATE} = '{co_regate}' "
                f"GROUP BY {TRAFIC_COL_NIVEAU} ORDER BY 1"
            ),
        },
        {
            "controle": "objets réellement présents pour le site (restitution dynamique)",
            "sql": (
                f"SELECT DISTINCT {TRAFIC_COL_OBJET} FROM {mois} "
                f"WHERE {TRAFIC_COL_REGATE} = '{co_regate}' ORDER BY 1"
            ),
        },
        {
            "controle": (
                "témoin avec/sans filtre de niveau sur un mois — un écart prouve le "
                "sur-comptage corrigé par le filtre"
            ),
            "sql": (
                f"SELECT {TRAFIC_COL_OBJET}, "
                f"SUM({TRAFIC_COL_CONSTATE}) AS constate_tous_niveaux, "
                f"SUM(CASE WHEN {TRAFIC_COL_NIVEAU} = '{NIVEAU_REGROUPEMENT}' "
                f"THEN {TRAFIC_COL_CONSTATE} END) AS constate_site "
                f"FROM {mois} WHERE {TRAFIC_COL_REGATE} = '{co_regate}' "
                f"AND {TRAFIC_COL_MOIS} = '2025-03' AND {TRAFIC_COL_ANNEE} IN (2025) "
                f"GROUP BY {TRAFIC_COL_OBJET} ORDER BY 1"
            ),
        },
    ]


def _executer(sql: str, params: dict) -> dict:
    """Exécute une requête de debug et renvoie le SQL rendu + les lignes, ou l'erreur brute.

    L'exception Databricks est renvoyée telle quelle : sur une sonde de structure, un
    message « column not found » est précisément l'information recherchée.
    """
    resultat = {"sql": render_sql(sql, params)}
    try:
        rows = databricks.fetch_all(sql, params)
        resultat["nb_lignes"] = len(rows)
        resultat["lignes"] = rows
    except Exception as e:
        logger.warning("Requête de debug DSR-679 en échec : %s", e)
        resultat["error"] = str(e)
    return resultat


def _sql_sonde_jour(co_regate: str, dt_debut: datetime, dt_fin: datetime) -> tuple[str, dict]:
    """Sonde maille jour : niveaux × objets, **sans** filtre de niveau, sur la période.

    Valide d'un seul appel l'existence des colonnes de `g_trppu_trafics_jour` et révèle si
    le site est présent à plusieurs niveaux de regroupement.
    """
    periode = "jours"
    ranges = [(dt_debut, dt_fin)]
    params: dict = {
        "co_regate": co_regate,
        "dt_start_0": fmt_date(dt_debut, periode),
        "dt_end_0": fmt_date(dt_fin, periode),
    }
    where = [
        f"{ALIAS_TRAFIC}.{TRAFIC_COL_REGATE} = :co_regate",
        f"({ALIAS_TRAFIC}.{DATE_COLUMN_PERIODE[periode]} BETWEEN :dt_start_0 AND :dt_end_0)",
    ]
    where.extend(build_partition_conditions(periode, ranges, params))

    sql = (
        f"SELECT {ALIAS_TRAFIC}.{TRAFIC_COL_NIVEAU} AS niveau, "
        f"{ALIAS_TRAFIC}.{TRAFIC_COL_OBJET} AS {OBJ_ALIAS}, "
        f"COUNT(*) AS nb_lignes, "
        f"SUM({ALIAS_TRAFIC}.{TRAFIC_COL_CONSTATE}) AS constate, "
        f"SUM({ALIAS_TRAFIC}.{TRAFIC_COL_PREVISIONNEL}) AS prevu "
        f"FROM {fqtn(TABLES_PERIODE[periode])} {ALIAS_TRAFIC} "
        f"WHERE {' AND '.join(where)} "
        f"GROUP BY 1, 2 ORDER BY 1, 2"
    )
    return sql, params


@router.get("/jour_check")
async def jour_check(
    co_regate: str = SITE_TEST,
    date_debut: str | None = None,
    date_fin: str | None = None,
    date_pivot: str | None = None,
    execute: bool = True,
):
    """DEBUG — sonde la maille jour : structure, niveaux de regroupement et sommes pivot.

    Trois requêtes sur `g_trppu_trafics_jour` uniquement :
    1. **sonde** — `GROUP BY niveau, objet` sans filtre de niveau : prouve l'existence des
       colonnes et révèle un éventuel cumul de niveaux ;
    2. **zone réelle** — SQL de production (`SUM(trafic_constate)`, dates < pivot) ;
    3. **zone prévisionnelle** — SQL de production (`SUM(trafic_prevu)`, dates >= pivot).

    Sans dates : période [aujourd'hui-30j, aujourd'hui+30j], pivot = aujourd'hui (les deux
    zones sont donc exercées). `execute=false` renvoie le SQL sans toucher Databricks.
    """
    aujourdhui = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    date_debut = date_debut or fmt_date(aujourdhui - timedelta(days=30))
    date_fin = date_fin or fmt_date(aujourdhui + timedelta(days=30))
    date_pivot = date_pivot or fmt_date(aujourdhui)

    dt_debut, dt_fin, dt_pivot = validate_params_pivot(
        co_regate, date_debut, date_fin, date_pivot
    )
    reel_range, prev_range = split_by_pivot(dt_debut, dt_fin, dt_pivot)

    sonde_sql, sonde_params = _sql_sonde_jour(co_regate, dt_debut, dt_fin)
    etapes: list[dict] = [{
        "etape": "1 - sonde (sans filtre de niveau)",
        "attendu": (
            "la requête passe -> colonnes confirmées sur la maille jour ; "
            "plusieurs valeurs de 'niveau' -> le filtre est indispensable"
        ),
        **({"sql": render_sql(sonde_sql, sonde_params)} if not execute
           else await run_in_threadpool(_executer, sonde_sql, sonde_params)),
    }]

    for libelle, rng, value_col in (
        ("2 - zone réelle (trafic constaté, dates < pivot)", reel_range, TRAFIC_COL_CONSTATE),
        ("3 - zone prévisionnelle (trafic prévu, dates >= pivot)", prev_range,
         TRAFIC_COL_PREVISIONNEL),
    ):
        if rng is None:
            etapes.append({"etape": libelle, "plage": None, "note": "zone vide sur cette période"})
            continue
        sql, params = build_query("jours", co_regate, [rng], value_col)
        etapes.append({
            "etape": libelle,
            "plage": [fmt_date(rng[0]), fmt_date(rng[1])],
            "colonne_valeur": value_col,
            **({"sql": render_sql(sql, params)} if not execute
               else await run_in_threadpool(_executer, sql, params)),
        })

    return {
        "note": "Sonde maille jour (g_trppu_trafics_jour uniquement) — DSR-679.",
        "execute": execute,
        "parametres": {
            "co_regate": co_regate,
            "date_debut": fmt_date(dt_debut),
            "date_fin": fmt_date(dt_fin),
            "date_pivot": fmt_date(dt_pivot),
        },
        "config": _config_effective(),
        "etapes": etapes,
    }


@router.get("/queries_preview")
def queries_preview(
    co_regate: str = SITE_TEST,
    date_debut: str | None = None,
    date_fin: str | None = None,
    date_pivot: str | None = None,
    is_day: bool = False,
):
    """DEBUG — SQL générée (paramètres substitués), à exécuter telle quelle dans Databricks.

    Sans dates, renvoie les 3 cas du ticket. Avec `date_debut`/`date_fin`/`date_pivot`,
    renvoie le seul cas demandé. Aucune exécution Databricks : c'est un rendu.
    """
    if date_debut or date_fin or date_pivot:
        cas = [("cas personnalisé", date_debut, date_fin, date_pivot)]
    else:
        cas = CAS_TICKET

    resultats = []
    for libelle, d_debut, d_fin, d_pivot in cas:
        dt_debut, dt_fin, dt_pivot = validate_params_pivot(
            co_regate, d_debut, d_fin, d_pivot
        )
        reel_range, prev_range = split_by_pivot(dt_debut, dt_fin, dt_pivot)

        zones = []
        for nom_zone, rng, value_col in (
            ("reelle (trafic constaté)", reel_range, TRAFIC_COL_CONSTATE),
            ("previsionnelle (trafic prévu)", prev_range, TRAFIC_COL_PREVISIONNEL),
        ):
            if rng is None:
                zones.append({"zone": nom_zone, "plage": None, "requetes": []})
                continue
            requetes = [
                render_sql(sql, params)
                for sql, params in build_period_queries(
                    co_regate, rng[0], rng[1], value_col, force_jours=is_day
                )
            ]
            zones.append({
                "zone": nom_zone,
                "plage": [fmt_date(rng[0]), fmt_date(rng[1])],
                "colonne_valeur": value_col,
                "requetes": requetes,
            })

        resultats.append({
            "cas": libelle,
            "parametres": {
                "co_regate": co_regate,
                "date_debut": fmt_date(dt_debut),
                "date_fin": fmt_date(dt_fin),
                "date_pivot": fmt_date(dt_pivot),
                "is_day": is_day,
            },
            "zones": zones,
            "sql_a_tester": [q for z in zones for q in z["requetes"]],
        })

    return {
        "note": "SQL DSR-679 rendue (sans exécution) — à coller dans Databricks.",
        "config": _config_effective(),
        "requetes_de_controle": _requetes_de_controle(co_regate),
        "cas": resultats,
    }


@router.get("/echantillons")
def echantillons():
    """Échantillons codés en dur des tables gold trafics (DSR-679), sans accès Databricks."""
    ref_debut = datetime(2025, 10, 1)
    ref_fin = datetime(2025, 10, 3)

    # Valeurs relevées en base sur le site test (SELECT * ... WHERE co_regate = '400300').
    trafics_semaine = [
        {TRAFIC_COL_NIVEAU: "SITE", TRAFIC_COL_REGATE: SITE_TEST, "co_type_regate": "072",
         TRAFIC_COL_OBJET: "PQ", "co_semaine_comptage": "2025-06", TRAFIC_COL_ANNEE: 2025,
         TRAFIC_COL_CONSTATE: 4086, TRAFIC_COL_PREVISIONNEL: 3983},
        {TRAFIC_COL_NIVEAU: "SITE", TRAFIC_COL_REGATE: SITE_TEST, "co_type_regate": "072",
         TRAFIC_COL_OBJET: "OS", "co_semaine_comptage": "2025-06", TRAFIC_COL_ANNEE: 2025,
         TRAFIC_COL_CONSTATE: 1064, TRAFIC_COL_PREVISIONNEL: None},
    ]
    trafics_mois = [
        {TRAFIC_COL_NIVEAU: "SITE", TRAFIC_COL_REGATE: SITE_TEST, "co_type_regate": "072",
         TRAFIC_COL_OBJET: "IP", TRAFIC_COL_MOIS: "2025-10", TRAFIC_COL_ANNEE: 2025,
         TRAFIC_COL_CONSTATE: 31764, TRAFIC_COL_PREVISIONNEL: 24456},
        {TRAFIC_COL_NIVEAU: "SITE", TRAFIC_COL_REGATE: SITE_TEST, "co_type_regate": "072",
         TRAFIC_COL_OBJET: "OO", TRAFIC_COL_MOIS: "2025-03", TRAFIC_COL_ANNEE: 2025,
         TRAFIC_COL_CONSTATE: 117591, TRAFIC_COL_PREVISIONNEL: None},
    ]

    return {
        "note": (
            "Données illustratives DSR-679 (site 400300). `trafic_prevu` est null sur le "
            "passé : Databricks pré-découpe déjà réel/prévisionnel."
        ),
        "cadences_journalieres_par_objet": DAILY_RATES,
        "g_trppu_trafics_jour": _sample_trafics_jour(ref_debut, ref_fin),
        "g_trppu_trafics_semaine": trafics_semaine,
        "g_trppu_trafics_mois": trafics_mois,
    }


@router.get("/pivot_dry_run")
def pivot_dry_run(
    co_regate: str | None = None,
    date_debut: str | None = None,
    date_fin: str | None = None,
    date_pivot: str | None = None,
):
    """Rejoue en mémoire la logique pivot DSR-679 sur les échantillons (sans Databricks).

    Même validation (400 rappelant les paramètres, période <= 2 ans) et même règle que
    l'endpoint réel : filtre de niveau, constaté avant le pivot, prévisionnel à partir du
    pivot, agrégé par objet. Valide les 3 cas du ticket.
    """
    dt_debut, dt_fin, dt_pivot = validate_params_pivot(
        co_regate, date_debut, date_fin, date_pivot
    )
    reel_range, prev_range = split_by_pivot(dt_debut, dt_fin, dt_pivot)

    acc = empty_accumulator()
    rows = _sample_trafics_jour(dt_debut, dt_fin) if co_regate == SITE_TEST else []

    _accumulate_range(rows, reel_range, TRAFIC_COL_CONSTATE, "trafic_brut", acc)
    _accumulate_range(rows, prev_range, TRAFIC_COL_PREVISIONNEL, "trafic_previsionnel", acc)

    trafics = [
        {
            "co_produit": objet,
            "trafic_brut": acc[objet]["trafic_brut"],
            "trafic_previsionnel": acc[objet]["trafic_previsionnel"],
        }
        for objet in sorted(acc)
    ]

    return {
        "mode": "dry_run (en mémoire, sans Databricks)",
        "co_regate": co_regate,
        "date_debut": fmt_date(dt_debut),
        "date_fin": fmt_date(dt_fin),
        "date_pivot": fmt_date(dt_pivot),
        "zone_reelle": [fmt_date(reel_range[0]), fmt_date(reel_range[1])] if reel_range else None,
        "zone_previsionnelle": [fmt_date(prev_range[0]), fmt_date(prev_range[1])] if prev_range else None,
        "count": len(trafics),
        "trafics": trafics,
    }


@router.get("/schema")
def schema(limit: int = 3, co_regate: str | None = None):
    """DEBUG — `SELECT *` sur les 3 tables trafics pour identifier les colonnes réelles.

    Renvoie, par table, la liste des colonnes et jusqu'à `limit` lignes d'exemple. Si
    `co_regate` est fourni, les tables sont filtrées sur ce site.
    """
    limit = max(1, min(int(limit), 50))
    resultats: dict[str, dict] = {}

    for periode, table in TABLES_PERIODE.items():
        table_fqtn = fqtn(table)
        sql = f"SELECT * FROM {table_fqtn}"
        if co_regate:
            sql += f" WHERE {TRAFIC_COL_REGATE} = '{co_regate}'"
        sql += f" LIMIT {limit}"
        try:
            rows = databricks.fetch_all(sql)
            resultats[table] = {
                "maille": periode,
                "table": table_fqtn,
                "query": sql,
                "colonnes": list(rows[0].keys()) if rows else [],
                "nb_lignes": len(rows),
                "echantillon": rows,
            }
        except Exception as e:
            logger.warning("SELECT * échoué sur %s : %s", table_fqtn, e)
            resultats[table] = {"table": table_fqtn, "query": sql, "error": str(e)}

    return {
        "note": "SELECT * de debug sur les vraies tables trafics (DSR-679).",
        "catalog": DATABRICKS_CATALOG,
        "schema_gold": DATABRICKS_SCHEMA,
        "tables": resultats,
    }


@router.get("/objets")
def objets(co_regate: str = SITE_TEST, table: str = "mois"):
    """DEBUG — objets et niveaux de regroupement présents pour un site.

    Deux contrôles avant recette : la liste réelle des `co_type_objet` restitués (la sortie
    de l'API est dynamique) et le nombre de lignes par niveau — plus d'un niveau signifie
    que le filtre `co_niveau_regroupement_operationnel` est ce qui évite un sur-comptage.
    """
    tbl = TABLES_PERIODE.get(
        {"jour": "jours", "semaine": "semaines", "mois": "mois"}.get(table, table),
        TABLES_PERIODE["mois"],
    )
    trafics_fqtn = fqtn(tbl)
    sql = (
        f"SELECT {TRAFIC_COL_NIVEAU} AS niveau, {TRAFIC_COL_OBJET} AS {OBJ_ALIAS}, "
        f"COUNT(*) AS nb_lignes "
        f"FROM {trafics_fqtn} WHERE {TRAFIC_COL_REGATE} = :co_regate "
        f"GROUP BY 1, 2 ORDER BY 1, 2"
    )
    params = {"co_regate": co_regate}

    resultat: dict = {"table_trafics": trafics_fqtn, **_executer(sql, params)}
    lignes = resultat.get("lignes")
    if lignes is not None:
        niveaux = sorted({r.get("niveau") for r in lignes if r.get("niveau") is not None})
        resultat["niveaux_presents"] = niveaux
        resultat["filtre_niveau_necessaire"] = len(niveaux) > 1
        resultat["objets_au_niveau_" + NIVEAU_REGROUPEMENT] = sorted(
            r.get(OBJ_ALIAS) for r in lignes
            if r.get("niveau") == NIVEAU_REGROUPEMENT and r.get(OBJ_ALIAS) is not None
        )
    return resultat


@router.get("/schema_raw")
async def schema_raw(table: str, limit: int = 5, schema: str | None = None):
    """DEBUG — `SELECT *` libre sur une table arbitraire (catalogue.schéma.table).

    `table` = nom court (ex. `g_trppu_obj_mapping`) résolu dans `schema` (défaut = gold),
    ou nom pleinement qualifié `catalog.schema.table` (utilisé tel quel s'il contient un `.`).
    Utile pour inspecter les tables non exploitées par la requête (mapping, entité, calendrier).
    """
    limit = max(1, min(int(limit), 100))
    table_fqtn = table if "." in table else fqtn(table, schema)
    sql = f"SELECT * FROM {table_fqtn} LIMIT {limit}"
    try:
        rows = await run_in_threadpool(databricks.fetch_all, sql)
    except Exception as e:
        return {"table": table_fqtn, "query": sql, "error": str(e)}
    return {
        "table": table_fqtn,
        "query": sql,
        "colonnes": list(rows[0].keys()) if rows else [],
        "nb_lignes": len(rows),
        "echantillon": rows,
    }
