"""Route de récupération des trafics depuis Databricks (mode auto)."""

import logging
import time
from datetime import datetime

from fastapi import APIRouter, HTTPException
from starlette.concurrency import run_in_threadpool

from app.config import (
    DATABRICKS_CATALOG,
    DATABRICKS_SCHEMA,
    DEBUG_SHOW_QUERY,
    TRAFIC_COL_CONSTATE,
    TRAFIC_COL_OBJET,
    TRAFIC_COL_PREVISIONNEL,
)
from app.db.databricks import databricks
from app.routes.trafics_helpers import (
    DATE_COLUMN_PERIODE,
    TABLES_PERIODE,
    TRAFIC_OBJET_LABELS,
    TRAFIC_PRODUITS,
    accumulate_trafics,
    decompose_auto,
    empty_trafics_accumulator,
    fmt_date,
    render_sql,
    split_by_pivot,
    validate_params,
    validate_params_pivot,
)
from app.services.jours_service import compute_nb_jours

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/trafics", tags=["Trafics"])


def build_query(
    periode: str,
    co_regate: str,
    ranges: list[tuple[datetime, datetime]],
    limit: int | None = None,
    objet_labels: list[str] | None = None,
) -> tuple[str, dict]:
    """Construit un SELECT * sur la table de la période, couvrant toutes les plages.

    Si `objet_labels` est fourni (et non vide), restreint la requête aux libellés
    via `lb_type_objet IN (...)` (utilisé par le pivot pour ne ramener que les
    objets mappés)."""
    table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TABLES_PERIODE[periode]}"
    date_col = DATE_COLUMN_PERIODE[periode]

    params: dict = {"co_regate": co_regate}
    conditions: list[str] = []
    for i, (dt_start, dt_end) in enumerate(ranges):
        s_key, e_key = f"dt_start_{i}", f"dt_end_{i}"
        params[s_key] = fmt_date(dt_start, periode)
        params[e_key] = fmt_date(dt_end, periode)
        conditions.append(f"{date_col} BETWEEN :{s_key} AND :{e_key}")

    sql = (
        f"SELECT * FROM {table} "
        f"WHERE co_regate = :co_regate "
        f"AND ({' OR '.join(conditions)})"
    )
    if objet_labels:
        objet_keys: list[str] = []
        for j, label in enumerate(objet_labels):
            o_key = f"objet_{j}"
            params[o_key] = label
            objet_keys.append(f":{o_key}")
        sql += f" AND {TRAFIC_COL_OBJET} IN ({', '.join(objet_keys)})"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return sql, params


def build_period_queries(
    co_regate: str,
    dt_debut: datetime,
    dt_fin: datetime,
    limit: int | None = None,
    objet_labels: list[str] | None = None,
) -> list[tuple[str, dict]]:
    """Découpe [dt_debut, dt_fin] en segments mois/semaines/jours et regroupe en
    une requête par table (max 3 requêtes)."""
    segments = decompose_auto(dt_debut, dt_fin)
    grouped: dict[str, list[tuple[datetime, datetime]]] = {}
    for periode, s, e in segments:
        grouped.setdefault(periode, []).append((s, e))
    return [
        build_query(p, co_regate, ranges, limit, objet_labels)
        for p, ranges in grouped.items()
    ]


@router.get("/get_trafics")
async def get_trafics(
    co_regate: str,
    date_debut: str,
    date_fin: str,
    limit: int | None = None,
):
    """Récupère les trafics bruts par code régate sur un intervalle de dates.

    L'intervalle est découpé dynamiquement (mode auto) en segments mois /
    semaines / jours, puis les segments d'une même période sont regroupés en
    une seule requête (max 1 requête par table). `limit` est appliqué par table.
    """
    dt_debut, dt_fin = validate_params(co_regate, date_debut, date_fin)
    queries = build_period_queries(co_regate, dt_debut, dt_fin, limit)

    start = time.perf_counter()
    try:
        results = []
        for sql, params in queries:
            # Databricks est synchrone : déporté hors de la boucle d'événements.
            rows = await run_in_threadpool(databricks.fetch_all, sql, params)
            results.extend(rows)
    except Exception as e:
        logger.error("Erreur requête trafics : %s", e)
        detail = {
            "error": True,
            "message": "Erreur lors de la récupération des trafics.",
            "code": 500,
        }
        if DEBUG_SHOW_QUERY:
            detail["queries"] = [render_sql(sql, params) for sql, params in queries]
            detail["databricks_error"] = str(e)
        raise HTTPException(status_code=500, detail=detail) from e
    duration_s = round(time.perf_counter() - start, 3)

    # DSR-613 : RecupererTrafics renvoie aussi le nb de jours ouvrés / ouvrables.
    # Résilient : un échec du calcul des jours n'invalide pas la réponse trafics.
    nb_jours = None
    try:
        nbj = await compute_nb_jours(dt_debut.date(), dt_fin.date())
        nb_jours = {
            "nbJoursOuvres": nbj.nb_jours_ouvres,
            "nbJoursOuvrables": nbj.nb_jours_ouvrables,
        }
    except Exception:
        logger.warning(
            "Calcul nb_jours indisponible (co_regate=%s) — bloc nb_jours=null.", co_regate
        )

    response = {
        "execution_time_s": duration_s,
        "co_regate": co_regate,
        "date_debut": fmt_date(dt_debut),
        "date_fin": fmt_date(dt_fin),
        "count": len(results),
        "data": results,
        "nb_jours": nb_jours,
    }
    if DEBUG_SHOW_QUERY:
        response["queries"] = [render_sql(sql, params) for sql, params in queries]
    return response


@router.get("/get_trafics_pivot")
async def get_trafics_pivot(
    co_regate: str | None = None,
    date_debut: str | None = None,
    date_fin: str | None = None,
    date_pivot: str | None = None,
):
    """DSR-666 : trafics agrégés par objet, ventilés réel/prévisionnel selon la date pivot.

    - dates < pivot  -> trafic réel (constaté/brut) ; prévisionnel = 0
    - dates >= pivot -> trafic prévisionnel ; réel = 0
    Renvoie une ligne par objet (les 6 produits, hydratés à 0) avec la somme des
    trafics sur la période et le site. Paramètres au format AAAAMMJJ ; période <= 2 ans.
    """
    dt_debut, dt_fin, dt_pivot = validate_params_pivot(
        co_regate, date_debut, date_fin, date_pivot
    )
    reel_range, prev_range = split_by_pivot(dt_debut, dt_fin, dt_pivot)

    # Une requête par zone (réel/prév) ; on somme la bonne colonne par produit.
    plan: list[tuple[tuple[datetime, datetime], str, str]] = []
    if reel_range is not None:
        plan.append((reel_range, TRAFIC_COL_CONSTATE, "trafic_brut"))
    if prev_range is not None:
        plan.append((prev_range, TRAFIC_COL_PREVISIONNEL, "trafic_previsionnel"))

    acc = empty_trafics_accumulator()
    executed: list[tuple[str, dict]] = []

    start = time.perf_counter()
    try:
        for (rg_debut, rg_fin), value_col, target_key in plan:
            for sql, params in build_period_queries(
                co_regate, rg_debut, rg_fin, objet_labels=list(TRAFIC_OBJET_LABELS)
            ):
                rows = await run_in_threadpool(databricks.fetch_all, sql, params)
                accumulate_trafics(rows, value_col, target_key, acc)
                executed.append((sql, params))
    except Exception as e:
        logger.error("Erreur requête trafics pivot : %s", e)
        detail = {
            "error": True,
            "message": "Erreur lors de la récupération des trafics.",
            "code": 500,
        }
        if DEBUG_SHOW_QUERY:
            detail["queries"] = [render_sql(sql, params) for sql, params in executed]
            detail["databricks_error"] = str(e)
        raise HTTPException(status_code=500, detail=detail) from e
    duration_s = round(time.perf_counter() - start, 3)

    trafics = [
        {
            "co_produit": produit,
            "trafic_brut": acc[produit]["trafic_brut"],
            "trafic_previsionnel": acc[produit]["trafic_previsionnel"],
        }
        for produit in TRAFIC_PRODUITS
    ]

    # nb_jours (DSR-613) — résilient : un échec n'invalide pas la réponse trafics.
    nb_jours = None
    try:
        nbj = await compute_nb_jours(dt_debut.date(), dt_fin.date())
        nb_jours = {
            "nbJoursOuvres": nbj.nb_jours_ouvres,
            "nbJoursOuvrables": nbj.nb_jours_ouvrables,
        }
    except Exception:
        logger.warning(
            "Calcul nb_jours indisponible (co_regate=%s) — bloc nb_jours=null.", co_regate
        )

    response = {
        "execution_time_s": duration_s,
        "co_regate": co_regate,
        "date_debut": fmt_date(dt_debut),
        "date_fin": fmt_date(dt_fin),
        "date_pivot": fmt_date(dt_pivot),
        "count": len(trafics),
        "trafics": trafics,
        "nb_jours": nb_jours,
    }
    if DEBUG_SHOW_QUERY:
        response["queries"] = [render_sql(sql, params) for sql, params in executed]
    return response
