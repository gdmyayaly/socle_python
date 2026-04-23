"""Route de récupération des trafics depuis Databricks (mode auto)."""

import logging
import time
from datetime import datetime

from fastapi import APIRouter, HTTPException

from app.config import DATABRICKS_CATALOG, DATABRICKS_SCHEMA, DEBUG_SHOW_QUERY
from app.db.databricks import databricks
from app.routes.trafics_helpers import (
    DATE_COLUMN_PERIODE,
    TABLES_PERIODE,
    decompose_auto,
    fmt_date,
    render_sql,
    validate_params,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trafics", tags=["Trafics"])


def build_query(
    periode: str,
    co_regate: str,
    dt_start: datetime,
    dt_end: datetime,
    limit: int | None = None,
) -> tuple[str, dict]:
    """Construit un SELECT * sur la table correspondant à la période."""
    table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TABLES_PERIODE[periode]}"
    date_col = DATE_COLUMN_PERIODE[periode]

    params: dict = {
        "co_regate": co_regate,
        "dt_start": fmt_date(dt_start, periode),
        "dt_end": fmt_date(dt_end, periode),
    }
    sql = (
        f"SELECT * FROM {table} "
        f"WHERE co_regate = :co_regate "
        f"AND {date_col} BETWEEN :dt_start AND :dt_end"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return sql, params


@router.get("/get_trafics")
def get_trafics(
    co_regate: str,
    date_debut: str,
    date_fin: str,
    limit: int | None = None,
):
    """Récupère les trafics bruts par code régate sur un intervalle de dates.

    L'intervalle est découpé dynamiquement (mode auto) en segments mois /
    semaines / jours pour interroger la table la plus agrégée disponible.
    `limit` est appliqué par segment.
    """
    dt_debut, dt_fin = validate_params(co_regate, date_debut, date_fin)
    segments = decompose_auto(dt_debut, dt_fin)
    queries = [build_query(p, co_regate, s, e, limit) for p, s, e in segments]

    start = time.perf_counter()
    try:
        results = []
        for sql, params in queries:
            results.extend(databricks.fetch_all(sql, params))
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

    response = {
        "execution_time_s": duration_s,
        "co_regate": co_regate,
        "date_debut": fmt_date(dt_debut),
        "date_fin": fmt_date(dt_fin),
        "count": len(results),
        "data": results,
    }
    if DEBUG_SHOW_QUERY:
        response["queries"] = [render_sql(sql, params) for sql, params in queries]
    return response
