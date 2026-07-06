"""DSR-679 — Routes de debug/test pour la nouvelle structure gold TRPPU.

- `/test/schema` et `/test/schema_raw` : `SELECT *` sur les vraies tables Databricks
  (identification des colonnes).
- `/test/objets` : distinct `co_type_objet` d'un site.
- `/test/echantillons` : échantillons codés en dur de la structure réelle (tables `_3`).
- `/test/pivot_dry_run` : rejoue en mémoire la ventilation réel/prévisionnel selon la date
  pivot et l'agrégation par objet, pour valider les 3 cas du ticket et les cas d'erreur,
  sans Databricks.
"""

import logging
import os
from datetime import datetime, timedelta

from fastapi import APIRouter
from starlette.concurrency import run_in_threadpool

from app.config import DATABRICKS_CATALOG, DATABRICKS_SCHEMA
from app.db.databricks import databricks
from app.routes.trppu_trafics.helpers import (
    CALENDRIER_TABLE,
    ENTITE_TABLE,
    OBJ_MAPPING_TABLE,
    TABLES_PERIODE,
    TRAFIC_COL_CONSTATE,
    TRAFIC_COL_OBJET,
    TRAFIC_COL_PREVISIONNEL,
    empty_accumulator,
    fmt_date,
    split_by_pivot,
    validate_params_pivot,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/trafics/test", tags=["Trafics (test DSR-679)"])

SITE_TEST = "400300"

# Schéma silver pour la table calendrier partagée (catalogue identique au gold).
DATABRICKS_SCHEMA_SILVER = os.getenv("DATABRICKS_SCHEMA_SILVER", "02_silver")

# --- Cadences journalières par objet TRPPU (constaté / prévu) pour synthétiser les
# lignes de la table jour dans le dry-run. Sommes prévisibles (cadence × nb jours). ---
DAILY_RATES = {
    "OO": {"constate": 10, "prevu": 7},
    "OS": {"constate": 3, "prevu": 1},
    "PR": {"constate": 8, "prevu": 5},
    "PP": {"constate": 5, "prevu": 3},
    "CO": {"constate": 2, "prevu": 2},
    # Pas de cadence pour IP -> aucun trafic (illustre l'objet sans donnée).
}


def _sample_trafics_jour(dt_debut: datetime, dt_fin: datetime) -> list[dict]:
    """Synthétise les lignes de `g_trppu_trafics_jour_3` (site test) sur [dt_debut, dt_fin]."""
    rows: list[dict] = []
    d = dt_debut
    while d <= dt_fin:
        for objet, rate in DAILY_RATES.items():
            rows.append({
                "co_niveau_regroupement_operationnel": "SITE",
                "co_regate": SITE_TEST,
                "co_type_regate": "072",
                TRAFIC_COL_OBJET: objet,
                "da_comptage": d.strftime("%Y-%m-%d"),
                "co_annee_comptage": d.year,
                "co_mois_comptage": d.strftime("%Y-%m"),
                "co_semaine_comptage": f"{d.isocalendar()[0]}-{d.isocalendar()[1]:02d}",
                TRAFIC_COL_CONSTATE: rate["constate"],
                TRAFIC_COL_PREVISIONNEL: rate["prevu"],
            })
        d += timedelta(days=1)
    return rows


def _accumulate_range(rows: list[dict], rng, value_col: str, target_key: str, acc) -> None:
    """Somme `value_col` par objet pour les lignes dont la date ∈ rng (inclus) — dynamique."""
    if rng is None:
        return
    debut, fin = rng
    for row in rows:
        da = datetime.strptime(row["da_comptage"], "%Y-%m-%d")
        if not (debut <= da <= fin):
            continue
        objet = row.get(TRAFIC_COL_OBJET)
        if objet is None:
            continue
        slot = acc.setdefault(objet, {"trafic_brut": 0, "trafic_previsionnel": 0})
        valeur = row.get(value_col)
        if valeur is None:
            continue
        slot[target_key] += valeur


@router.get("/echantillons")
def echantillons():
    """Échantillons codés en dur de la structure gold réelle (tables `_3`, DSR-679)."""
    ref_debut = datetime(2025, 10, 1)
    ref_fin = datetime(2025, 10, 3)
    trafics_jour = _sample_trafics_jour(ref_debut, ref_fin)

    trafics_semaine = [
        {"co_regate": SITE_TEST, "co_type_regate": "072", TRAFIC_COL_OBJET: "PR",
         "co_annee_comptage": 2025, "co_semaine_comptage": "2025-06",
         TRAFIC_COL_CONSTATE: 4086, TRAFIC_COL_PREVISIONNEL: 3983},
        {"co_regate": SITE_TEST, "co_type_regate": "072", TRAFIC_COL_OBJET: "OS",
         "co_annee_comptage": 2025, "co_semaine_comptage": "2025-06",
         TRAFIC_COL_CONSTATE: 1064, TRAFIC_COL_PREVISIONNEL: None},
    ]
    trafics_mois = [
        {"co_regate": SITE_TEST, "co_type_regate": "072", TRAFIC_COL_OBJET: "IP",
         "co_mois_comptage": "2025-10", "co_annee_comptage": 2025,
         TRAFIC_COL_CONSTATE: 31764, TRAFIC_COL_PREVISIONNEL: 24456},
        {"co_regate": SITE_TEST, "co_type_regate": "072", TRAFIC_COL_OBJET: "OO",
         "co_mois_comptage": "2025-03", "co_annee_comptage": 2025,
         TRAFIC_COL_CONSTATE: 5722, TRAFIC_COL_PREVISIONNEL: None},
    ]

    return {
        "note": "Données illustratives DSR-679 (site 400300, structure réelle `_3`).",
        "cadences_journalieres_par_objet": DAILY_RATES,
        "g_trppu_trafics_jour": trafics_jour,
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
    l'endpoint réel : constaté avant le pivot, prévisionnel à partir du pivot, agrégé par
    objet (`co_type_objet`), restitution dynamique. Valide les 3 cas (passé/mixte/futur).
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


def _tables_a_introspecter() -> list[tuple[str, str, str]]:
    """Liste (label, schéma, table) des tables réelles à inspecter via SELECT *."""
    gold = DATABRICKS_SCHEMA
    return [
        ("g_trppu_trafics_jour", gold, TABLES_PERIODE["jours"]),
        ("g_trppu_trafics_semaine", gold, TABLES_PERIODE["semaines"]),
        ("g_trppu_trafics_mois", gold, TABLES_PERIODE["mois"]),
        ("g_trppu_obj_mapping", gold, OBJ_MAPPING_TABLE),
        ("g_trppu_entite", gold, ENTITE_TABLE),
        ("s_commun_calendrier_jour", DATABRICKS_SCHEMA_SILVER, CALENDRIER_TABLE),
    ]


@router.get("/schema")
def schema(limit: int = 3, co_regate: str | None = None):
    """DEBUG — `SELECT *` sur les vraies tables Databricks pour identifier les colonnes.

    Renvoie, par table, la liste des colonnes et jusqu'à `limit` lignes d'exemple. Si
    `co_regate` est fourni, les 3 tables trafics sont filtrées sur ce site.
    """
    limit = max(1, min(int(limit), 50))
    resultats: dict[str, dict] = {}

    for label, sch, table in _tables_a_introspecter():
        fqtn = f"{DATABRICKS_CATALOG}.{sch}.{table}"
        sql = f"SELECT * FROM {fqtn}"
        if co_regate and label.startswith("g_trppu_trafics_"):
            sql += f" WHERE co_regate = '{co_regate}'"
        sql += f" LIMIT {limit}"
        try:
            rows = databricks.fetch_all(sql)
            colonnes = list(rows[0].keys()) if rows else []
            resultats[label] = {
                "table": fqtn,
                "query": sql,
                "colonnes": colonnes,
                "nb_lignes": len(rows),
                "echantillon": rows,
            }
        except Exception as e:
            logger.warning("SELECT * échoué sur %s : %s", fqtn, e)
            resultats[label] = {"table": fqtn, "query": sql, "error": str(e)}

    return {
        "note": "SELECT * de debug sur les vraies tables Databricks (DSR-679).",
        "catalog": DATABRICKS_CATALOG,
        "schema_gold": DATABRICKS_SCHEMA,
        "schema_silver": DATABRICKS_SCHEMA_SILVER,
        "tables": resultats,
    }


@router.get("/objets")
def objets(co_regate: str = SITE_TEST, table: str = "mois"):
    """DEBUG — liste distincte des `co_type_objet` présents pour un site (par table)."""
    tbl = TABLES_PERIODE.get(
        {"jour": "jours", "semaine": "semaines", "mois": "mois"}.get(table, table),
        TABLES_PERIODE["mois"],
    )
    fqtn = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{tbl}"
    sql = f"SELECT DISTINCT {TRAFIC_COL_OBJET} FROM {fqtn} WHERE co_regate = '{co_regate}'"
    try:
        rows = databricks.fetch_all(sql)
        objets = sorted(r.get(TRAFIC_COL_OBJET) for r in rows if r.get(TRAFIC_COL_OBJET) is not None)
    except Exception as e:
        return {"table": fqtn, "query": sql, "error": str(e)}
    return {"table": fqtn, "query": sql, "distinct_co_type_objet": objets}


@router.get("/schema_raw")
async def schema_raw(table: str, limit: int = 5, schema: str | None = None):
    """DEBUG — `SELECT *` libre sur une table arbitraire (catalogue.schéma.table).

    `table` = nom court (ex. `g_trppu_obj_mapping`) résolu dans `schema` (défaut = gold),
    ou nom pleinement qualifié `catalog.schema.table` (utilisé tel quel s'il contient un `.`).
    """
    limit = max(1, min(int(limit), 100))
    fqtn = table if "." in table else f"{DATABRICKS_CATALOG}.{schema or DATABRICKS_SCHEMA}.{table}"
    sql = f"SELECT * FROM {fqtn} LIMIT {limit}"
    try:
        rows = await run_in_threadpool(databricks.fetch_all, sql)
    except Exception as e:
        return {"table": fqtn, "query": sql, "error": str(e)}
    return {
        "table": fqtn,
        "query": sql,
        "colonnes": list(rows[0].keys()) if rows else [],
        "nb_lignes": len(rows),
        "echantillon": rows,
    }
