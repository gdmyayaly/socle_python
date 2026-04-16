"""Routes de récupération des données de trafics depuis Databricks la base du YS04."""

import logging
import time
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.config import DATABRICKS_CATALOG, DATABRICKS_SCHEMA, DEBUG_SHOW_QUERY
from app.db.databricks import databricks
from app.routes.trafics_helpers import (
    DATE_COLUMN_PERIODE,
    TABLES_PERIODE,
    build_segments,
    fmt_date,
    validate_params,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes spécifiques get_trafics
# ---------------------------------------------------------------------------

TRAFICS_SELECT_COLUMNS = (
    "da_comptage, "
    "co_niveau_regroupement_operationnel, "
    "co_regate, "
    "lb_entite, "
    "co_comptage, "
    "lb_comptage, "
    "co_objet, "
    "lb_objet, "
    "co_type_objet, "
    "lb_type_objet, "
    "SUM(nb_objet_retenu) AS nb_objet_trafic_reel"
)

TRAFICS_GROUP_BY = (
    "da_comptage, "
    "co_niveau_regroupement_operationnel, "
    "co_regate, "
    "lb_entite, "
    "co_comptage, "
    "lb_comptage, "
    "co_objet, "
    "lb_objet, "
    "co_type_objet, "
    "lb_type_objet"
)

TRAFICS_IN_COLUMN = "co_comptage"
TRAFICS_IN_VALUES = [
    "TLOP1", "ULOAD",                    # Colis
    "TRSP1",                              # Objets suivis
    "OR4PM", "OR4PX",                     # Courrier
    "TI_COL_MENAGE", "TI_COL_CEDEX",
    "IMPJ",                               # IP
    "VQQP0",                              # EPACK
]

router = APIRouter(prefix="/trafics", tags=["Trafics"])


class TraficsRequest(BaseModel):
    periode: str | None = Field(None, description="Période : jours, semaines, mois ou auto (défaut: auto)")
    co_regate: str | None = Field(None, description="Code régate de l'entité")
    date_debut: str | None = Field(None, description="Date de début (AAAAMMJJ ou AAAA-MM-JJ)")
    date_fin: str | None = Field(None, description="Date de fin (AAAAMMJJ ou AAAA-MM-JJ)")
    count_only: bool = Field(False, description="Si true, retourne uniquement le count")


# ---------------------------------------------------------------------------
# Query builder
# ---------------------------------------------------------------------------

def build_query(
    periode: str,
    co_regate: str,
    dt_start: datetime,
    dt_end: datetime,
    count_only: bool = False,
) -> tuple[str, dict]:
    """Construit une requête SELECT ou COUNT pour une période donnée.

    Retourne un tuple (sql, params) avec des placeholders nommés
    pour éviter les injections SQL.
    """
    table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TABLES_PERIODE[periode]}"
    date_col = DATE_COLUMN_PERIODE[periode]
    select = "COUNT(*) AS total" if count_only else TRAFICS_SELECT_COLUMNS.replace("da_comptage", date_col)
    params = {
        "co_regate": co_regate,
        "dt_start": fmt_date(dt_start, periode),
        "dt_end": fmt_date(dt_end, periode),
    }
    sql = (
        f"SELECT {select} FROM {table} "
        f"WHERE co_regate = :co_regate "
        f"AND co_niveau_regroupement_operationnel = 'SITE' "
        f"AND {date_col} BETWEEN :dt_start AND :dt_end"
    )
    if TRAFICS_IN_COLUMN and TRAFICS_IN_VALUES:
        in_placeholders = ", ".join(f":in_{i}" for i in range(len(TRAFICS_IN_VALUES)))
        for i, v in enumerate(TRAFICS_IN_VALUES):
            params[f"in_{i}"] = v
        sql += f" AND {TRAFICS_IN_COLUMN} IN ({in_placeholders})"
    if TRAFICS_GROUP_BY and not count_only:
        group_by = TRAFICS_GROUP_BY.replace("da_comptage", date_col)
        sql += f" GROUP BY {group_by}"
    return sql, params


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.post("/get_trafics")
def get_trafics(body: TraficsRequest):
    """Récupère les trafics par code régate et période de dates.

    En mode **auto** (par défaut), l'intervalle est découpé dynamiquement en requêtes
    sur les tables mois, semaines et jours pour optimiser les performances.
    """
    periode = body.periode or "auto"
    co_regate = body.co_regate
    date_debut = body.date_debut
    date_fin = body.date_fin
    count_only = body.count_only

    periode_lower, dt_debut, dt_fin = validate_params(periode, co_regate, date_debut, date_fin)

    segments = build_segments(periode_lower, dt_debut, dt_fin)
    queries = [
        {"periode": seg[0], "query": build_query(seg[0], co_regate, seg[1], seg[2], count_only)}
        for seg in segments
    ]

    # --- Exécution (auto ou période fixe) ---
    start = time.perf_counter()
    try:
        if count_only:
            total = 0
            for q in queries:
                sql, params = q["query"]
                row = databricks.fetch_one(sql, params)
                total += row["total"] if row else 0
        else:
            results = []
            for q in queries:
                sql, params = q["query"]
                results.extend(databricks.fetch_all(sql, params))
    except Exception as e:
        logger.error("Erreur requête (%s) : %s", periode_lower, e)
        detail = {
            "error": True,
            "message": f"Erreur lors de la récupération des trafics ({periode_lower}).",
            "code": 500,
        }
        if DEBUG_SHOW_QUERY:
            detail["queries"] = [q["query"][0] for q in queries]
            detail["databricks_error"] = str(e)
        raise HTTPException(status_code=500, detail=detail) from e
    duration_s = round(time.perf_counter() - start, 3)

    response = {
        "execution_time_s": duration_s,
        "periode": periode_lower,
        "co_regate": co_regate,
        "date_debut": fmt_date(dt_debut),
        "date_fin": fmt_date(dt_fin),
    }
    if count_only:
        response["count"] = total
    else:
        response["count"] = len(results)
        response["data"] = results
    if DEBUG_SHOW_QUERY:
        response["queries"] = [q["query"][0] for q in queries]
    return response
