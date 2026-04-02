import logging
import time
from calendar import monthrange
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.config import DATABRICKS_CATALOG, DATABRICKS_SCHEMA, DEBUG_SHOW_QUERY
from app.db.databricks import databricks

logger = logging.getLogger(__name__)

TABLES_PERIODE = {
    "jours": "g_mdp_trafics_jour_actualise",
    "semaines": "g_mdp_trafics_semaine_actualise",
    "mois": "g_mdp_trafics_mois_actualise",
}

TRAFICS_SELECT_COLUMNS = "*"
# TRAFICS_SELECT_COLUMNS = "co_regate, da_comptage, li_site, nb_entrants, nb_sortants"

TRAFICS_GROUP_BY = ""
# TRAFICS_GROUP_BY = "co_regate, da_comptage"

TRAFICS_IN_COLUMN = ""
# TRAFICS_IN_COLUMN = "li_site"
TRAFICS_IN_VALUES: list[str] = []
# TRAFICS_IN_VALUES = ["Valeur1", "Valeur2", "Valeur3"]

PARAMETRES_RAPPEL = (
    "Paramètres attendus : "
    "periode (jours, semaines, mois, auto, debug), "
    "co_regate (code régate du site), "
    "date_debut (format AAAAMMJJ ou AAAA-MM-JJ), "
    "date_fin (format AAAAMMJJ ou AAAA-MM-JJ)."
)

router = APIRouter(prefix="/trafics", tags=["Trafics"])


class TraficsRequest(BaseModel):
    periode: str | None = Field(None, description="Période : jours, semaines, mois, auto ou debug")
    co_regate: str | None = Field(None, description="Code régate de l'entité")
    date_debut: str | None = Field(None, description="Date de début (AAAAMMJJ ou AAAA-MM-JJ)")
    date_fin: str | None = Field(None, description="Date de fin (AAAAMMJJ ou AAAA-MM-JJ)")
    count_only: bool = Field(False, description="Si true, retourne uniquement le count")


class TraficsPaginatedRequest(BaseModel):
    periode: str | None = Field(None, description="Période : jours, semaines, mois, auto ou debug")
    co_regate: str | None = Field(None, description="Code régate de l'entité")
    date_debut: str | None = Field(None, description="Date de début (AAAAMMJJ ou AAAA-MM-JJ)")
    date_fin: str | None = Field(None, description="Date de fin (AAAAMMJJ ou AAAA-MM-JJ)")
    page: int = Field(1, ge=1, description="Numéro de page (à partir de 1)")
    page_size: int = Field(50, ge=1, le=1000, description="Nombre de résultats par page")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_date(value: str, nom_param: str) -> datetime:
    """Parse une date au format AAAAMMJJ ou AAAA-MM-JJ."""
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise HTTPException(
        status_code=400,
        detail=(
            f"Format de {nom_param} invalide '{value}'. "
            f"Attendu : AAAAMMJJ ou AAAA-MM-JJ. {PARAMETRES_RAPPEL}"
        ),
    )


def fmt_date(dt: datetime) -> str:
    """Formate une date en AAAA-MM-JJ pour les requêtes SQL."""
    return dt.strftime("%Y-%m-%d")


def _decompose_semaines_jours(dt_start: datetime, dt_end: datetime):
    """Découpe un intervalle en semaines complètes (lun-dim) et jours restants."""
    parts: list[tuple[str, datetime, datetime]] = []

    # Premier lundi >= dt_start
    days_to_monday = (7 - dt_start.weekday()) % 7
    first_monday = dt_start + timedelta(days=days_to_monday)

    # Dernier dimanche <= dt_end
    if dt_end.weekday() == 6:
        last_sunday = dt_end
    else:
        last_sunday = dt_end - timedelta(days=dt_end.weekday() + 1)

    if first_monday + timedelta(days=6) <= last_sunday:
        if dt_start < first_monday:
            parts.append(("jours", dt_start, first_monday - timedelta(days=1)))
        parts.append(("semaines", first_monday, last_sunday))
        if last_sunday < dt_end:
            parts.append(("jours", last_sunday + timedelta(days=1), dt_end))
    else:
        parts.append(("jours", dt_start, dt_end))

    return parts


def decompose_auto(dt_debut: datetime, dt_fin: datetime):
    """Découpe un intervalle en requêtes optimales sur mois / semaines / jours."""
    segments: list[tuple[str, datetime, datetime]] = []

    if dt_debut.day == 1:
        mois_start = dt_debut
    else:
        if dt_debut.month == 12:
            mois_start = datetime(dt_debut.year + 1, 1, 1)
        else:
            mois_start = datetime(dt_debut.year, dt_debut.month + 1, 1)

    last_day = monthrange(dt_fin.year, dt_fin.month)[1]
    if dt_fin.day == last_day:
        mois_end = dt_fin
    else:
        mois_end = datetime(dt_fin.year, dt_fin.month, 1) - timedelta(days=1)

    if mois_start <= mois_end:
        if dt_debut < mois_start:
            segments.extend(
                _decompose_semaines_jours(dt_debut, mois_start - timedelta(days=1))
            )
        segments.append(("mois", mois_start, mois_end))
        if mois_end < dt_fin:
            segments.extend(
                _decompose_semaines_jours(mois_end + timedelta(days=1), dt_fin)
            )
    else:
        segments.extend(_decompose_semaines_jours(dt_debut, dt_fin))

    return segments


def build_query(
    periode: str,
    co_regate: str,
    dt_start: datetime,
    dt_end: datetime,
    count_only: bool = False,
) -> str:
    """Construit une requête SELECT ou COUNT pour une période donnée."""
    table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TABLES_PERIODE[periode]}"
    select = "COUNT(*) AS total" if count_only else TRAFICS_SELECT_COLUMNS
    sql = (
        f"SELECT {select} FROM {table} "
        f"WHERE co_regate = '{co_regate}' "
        f"AND da_comptage BETWEEN '{fmt_date(dt_start)}' AND '{fmt_date(dt_end)}'"
    )
    if TRAFICS_IN_COLUMN and TRAFICS_IN_VALUES:
        in_list = ", ".join(f"'{v}'" for v in TRAFICS_IN_VALUES)
        sql += f" AND {TRAFICS_IN_COLUMN} IN ({in_list})"
    if TRAFICS_GROUP_BY and not count_only:
        sql += f" GROUP BY {TRAFICS_GROUP_BY}"
    return sql


def validate_params(periode, co_regate, date_debut, date_fin):
    """Valide les paramètres communs et retourne (periode_lower, dt_debut, dt_fin)."""
    manquants = []
    if not periode:
        manquants.append("periode")
    if not co_regate:
        manquants.append("co_regate")
    if not date_debut:
        manquants.append("date_debut")
    if not date_fin:
        manquants.append("date_fin")
    if manquants:
        msg = f"Paramètre(s) manquant(s) : {', '.join(manquants)}. {PARAMETRES_RAPPEL}"
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    periode_lower = periode.lower()
    periodes_valides = (*TABLES_PERIODE, "auto", "debug")
    if periode_lower not in periodes_valides:
        msg = (
            f"Période invalide '{periode}'. "
            f"Valeurs acceptées : {', '.join(periodes_valides)}. {PARAMETRES_RAPPEL}"
        )
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    dt_debut = parse_date(date_debut, "date_debut")
    dt_fin = parse_date(date_fin, "date_fin")

    if dt_debut > dt_fin:
        msg = f"date_debut doit être antérieure ou égale à date_fin. {PARAMETRES_RAPPEL}"
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    ecart = (dt_fin - dt_debut).days
    if ecart > 730:
        msg = (
            f"L'écart entre les dates ne doit pas dépasser 2 ans (730 jours). "
            f"Écart actuel : {ecart} jours. {PARAMETRES_RAPPEL}"
        )
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    return periode_lower, dt_debut, dt_fin


def build_segments(periode_lower, dt_debut, dt_fin):
    """Retourne la liste des segments selon la période."""
    if periode_lower in ("auto", "debug"):
        return decompose_auto(dt_debut, dt_fin)
    return [(periode_lower, dt_debut, dt_fin)]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/get_trafics")
def get_trafics(body: TraficsRequest):
    """Récupère les trafics par code régate et période de dates.

    En mode **auto** (par défaut), l'intervalle est découpé dynamiquement en requêtes
    sur les tables mois, semaines et jours pour optimiser les performances.

    En mode **debug**, les requêtes sont générées mais pas exécutées (aperçu).
    """
    periode = body.periode
    co_regate = body.co_regate
    date_debut = body.date_debut
    date_fin = body.date_fin
    count_only = body.count_only

    if not periode:
        periode = "auto"
    periode_lower, dt_debut, dt_fin = validate_params(periode, co_regate, date_debut, date_fin)

    segments = build_segments(periode_lower, dt_debut, dt_fin)
    queries = [
        {"periode": seg[0], "query": build_query(seg[0], co_regate, seg[1], seg[2], count_only)}
        for seg in segments
    ]

    # --- Mode debug : aperçu des requêtes sans exécution ---
    if periode_lower == "debug":
        logger.info("Mode debug (preview) : %d requête(s) générées", len(queries))
        return {
            "mode": "preview",
            "periode": "debug",
            "co_regate": co_regate,
            "date_debut": fmt_date(dt_debut),
            "date_fin": fmt_date(dt_fin),
            "nb_queries": len(queries),
            "queries": [
                {"index": i + 1, "periode": q["periode"], "query": q["query"]}
                for i, q in enumerate(queries)
            ],
        }

    # --- Exécution (auto ou période fixe) ---
    start = time.perf_counter()
    try:
        if count_only:
            total = 0
            for q in queries:
                row = databricks.fetch_one(q["query"])
                total += row["total"] if row else 0
        else:
            results = []
            for q in queries:
                results.extend(databricks.fetch_all(q["query"]))
    except Exception as e:
        logger.error("Erreur requête (%s) : %s", periode_lower, e)
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lors de la récupération des trafics ({periode_lower}).",
        )
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
        response["queries"] = [q["query"] for q in queries]
    return response


@router.post("/get_trafics_paginated")
def get_trafics_paginated(body: TraficsPaginatedRequest):
    """Récupère les trafics avec pagination."""
    periode = body.periode
    co_regate = body.co_regate
    date_debut = body.date_debut
    date_fin = body.date_fin
    page = body.page
    page_size = body.page_size

    if not periode:
        periode = "auto"
    periode_lower, dt_debut, dt_fin = validate_params(periode, co_regate, date_debut, date_fin)

    segments = build_segments(periode_lower, dt_debut, dt_fin)
    queries = [
        {"periode": seg[0], "query": build_query(seg[0], co_regate, seg[1], seg[2])}
        for seg in segments
    ]

    # --- Mode debug : aperçu ---
    if periode_lower == "debug":
        paginated_queries = [
            {
                "index": i + 1,
                "periode": q["periode"],
                "query": f"{q['query']} LIMIT {page_size} OFFSET {(page - 1) * page_size}",
            }
            for i, q in enumerate(queries)
        ]
        return {
            "mode": "preview",
            "periode": "debug",
            "co_regate": co_regate,
            "date_debut": fmt_date(dt_debut),
            "date_fin": fmt_date(dt_fin),
            "page": page,
            "page_size": page_size,
            "nb_queries": len(paginated_queries),
            "queries": paginated_queries,
        }

    # --- Exécution avec pagination (auto ou période fixe) ---
    offset = (page - 1) * page_size
    start = time.perf_counter()
    try:
        results = []
        for q in queries:
            query_str = f"{q['query']} LIMIT {page_size} OFFSET {offset}"
            results.extend(databricks.fetch_all(query_str))
    except Exception as e:
        logger.error("Erreur requête paginée (%s) : %s", periode_lower, e)
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lors de la récupération des trafics ({periode_lower}).",
        )
    duration_s = round(time.perf_counter() - start, 3)

    response = {
        "count": len(results),
        "page": page,
        "page_size": page_size,
        "execution_time_s": duration_s,
        "periode": periode_lower,
        "co_regate": co_regate,
        "date_debut": fmt_date(dt_debut),
        "date_fin": fmt_date(dt_fin),
        "data": results,
    }
    if DEBUG_SHOW_QUERY:
        response["queries"] = [f"{q['query']} LIMIT {page_size} OFFSET {offset}" for q in queries]
    return response
