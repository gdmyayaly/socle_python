"""Routes de récupération des trafics TRPPU agrégés par type de produit depuis Databricks."""

import logging
import time
from datetime import datetime

from fastapi import APIRouter, HTTPException

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
# Constantes TRPPU
# ---------------------------------------------------------------------------

TRPPU_SITES_DISTRIBUTEURS = ("PDC1", "PDC2", "PPDC")
TRPPU_SITES_PIC_CTC = ("PIC", "CTC")

TRPPU_WHERE_CO_PROCESS = ["VT", "DT"]
TRPPU_WHERE_CO_COMPTAGE = [
    "TI_COL_MENAGE", "TI_COL_CEDEX",
    "OR4PM", "OR4PX",
    "TRSP1",
    "IMPJ",
    "TLOP1",
    "VQQP0", "2QNP1", "2QQP1",
    "PPLP0", "1POP0", "VPIP0",
]

TRPPU_PERIOD_CONFIG = {
    "jours": {
        "value_col": "trafic_reel",
        "select_prefix": "j.da_comptage",
        "group_by": "j.da_comptage, j.co_regate, j.lb_type_entite_regate_court",
    },
    "semaines": {
        "value_col": "nb_objet_retenu",
        "select_prefix": "j.co_annee_comptage, j.co_semaine_comptage, j.da_lundi_semaine_comptage",
        "group_by": "j.co_annee_comptage, j.co_semaine_comptage, j.da_lundi_semaine_comptage, j.co_regate, j.lb_type_entite_regate_court",
    },
    "mois": {
        "value_col": "nb_objet_retenu",
        "select_prefix": "j.co_annee_comptage, j.co_mois_comptage",
        "group_by": "j.co_annee_comptage, j.co_mois_comptage, j.co_regate, j.lb_type_entite_regate_court",
    },
}

router = APIRouter(prefix="/trafics", tags=["TRPPU Trafics"])


# ---------------------------------------------------------------------------
# Query builder
# ---------------------------------------------------------------------------

def build_trppu_query(
    periode: str,
    co_regate: str,
    dt_start: datetime,
    dt_end: datetime,
) -> tuple[str, dict]:
    """Construit la requête TRPPU avec agrégations CASE/WHEN par type de produit.

    Adapte automatiquement la colonne de valeur (trafic_reel / nb_objet_retenu),
    les colonnes de date et le GROUP BY selon la période (jours/semaines/mois).
    Retourne un tuple (sql, params) avec des placeholders nommés.
    """
    table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TABLES_PERIODE[periode]}"
    date_col = DATE_COLUMN_PERIODE[periode]
    pcfg = TRPPU_PERIOD_CONFIG[periode]
    val = pcfg["value_col"]
    select_prefix = pcfg["select_prefix"]
    group_by = pcfg["group_by"]

    params: dict = {
        "co_regate": co_regate,
        "dt_start": fmt_date(dt_start, periode),
        "dt_end": fmt_date(dt_end, periode),
    }

    # --- Placeholders pour les listes IN du WHERE ---
    proc_placeholders = ", ".join(f":proc_{i}" for i in range(len(TRPPU_WHERE_CO_PROCESS)))
    for i, v in enumerate(TRPPU_WHERE_CO_PROCESS):
        params[f"proc_{i}"] = v

    cpt_placeholders = ", ".join(f":cpt_{i}" for i in range(len(TRPPU_WHERE_CO_COMPTAGE)))
    for i, v in enumerate(TRPPU_WHERE_CO_COMPTAGE):
        params[f"cpt_{i}"] = v

    # --- Littéraux pour les types de site dans les CASE (non injectables) ---
    dist = ", ".join(f"'{s}'" for s in TRPPU_SITES_DISTRIBUTEURS)
    pic_ctc = ", ".join(f"'{s}'" for s in TRPPU_SITES_PIC_CTC)

    select = f"""{select_prefix},
    j.co_regate,
    j.lb_type_entite_regate_court AS type_site,

    SUM(CASE
        WHEN j.lb_type_entite_regate_court IN ({dist})
         AND j.co_comptage IN ('TI_COL_MENAGE', 'TI_COL_CEDEX')
            THEN j.{val}
        WHEN j.lb_type_entite_regate_court IN ({dist})
         AND j.co_comptage IN ('OR4PM', 'OR4PX')
         AND j.co_type_objet = 'R'
            THEN j.{val}
        WHEN j.lb_type_entite_regate_court IN ({pic_ctc})
         AND j.co_process IN ('VT', 'DT')
         AND j.co_type_objet = 'R'
            THEN j.{val}
        ELSE 0
    END) AS trafic_oo,

    SUM(CASE
        WHEN j.lb_type_entite_regate_court IN ({dist})
         AND j.co_comptage IN ('OR4PM', 'OR4PX')
         AND j.co_type_objet = 'P'
            THEN j.{val}
        WHEN j.lb_type_entite_regate_court IN ({pic_ctc})
         AND j.co_process IN ('VT', 'DT')
         AND j.co_type_objet = 'P'
            THEN j.{val}
        ELSE 0
    END) AS presse_mecanisee,

    SUM(CASE
        WHEN j.lb_type_entite_regate_court IN ({dist})
         AND j.co_comptage = 'PPLP0'
            THEN j.{val}
        ELSE 0
    END) AS presse_locale_declarative,

    SUM(CASE
        WHEN j.lb_type_entite_regate_court IN ({dist})
         AND j.co_comptage IN ('1POP0', 'VPIP0')
            THEN j.{val}
        ELSE 0
    END) AS presse_viapost_hors_meca,

    SUM(CASE
        WHEN j.lb_type_entite_regate_court IN ({dist})
         AND j.co_comptage = 'TRSP1'
            THEN j.{val}
        ELSE 0
    END) AS trafic_os,

    SUM(CASE
        WHEN j.lb_type_entite_regate_court IN ({dist})
         AND j.co_comptage = 'IMPJ'
            THEN j.{val}
        ELSE 0
    END) AS trafic_ip,

    SUM(CASE
        WHEN j.lb_type_entite_regate_court IN ({dist})
         AND j.co_comptage = 'TLOP1'
            THEN j.{val}
        ELSE 0
    END) AS trafic_colis,

    SUM(CASE
        WHEN j.lb_type_entite_regate_court IN ({dist})
         AND j.co_comptage IN ('VQQP0', '2QNP1', '2QQP1')
            THEN j.{val}
        ELSE 0
    END) AS trafic_ppi"""

    sql = (
        f"SELECT {select} "
        f"FROM {table} j "
        f"WHERE j.co_regate = :co_regate "
        f"AND j.{date_col} BETWEEN :dt_start AND :dt_end "
        f"AND (j.co_process IN ({proc_placeholders}) "
        f"OR j.co_comptage IN ({cpt_placeholders})) "
        f"GROUP BY {group_by}"
    )

    return sql, params


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get("/get_trppu_trafics")
def get_trppu_trafics(
    co_regate: str,
    date_debut: str,
    date_fin: str,
):
    """Récupère les trafics TRPPU agrégés par type de produit.

    Utilise le mode **auto** : l'intervalle est découpé dynamiquement en requêtes
    sur les tables mois, semaines et jours pour optimiser les performances.
    Retourne pour chaque (date, site) les trafics ventilés : courrier OO,
    presse mécanisée, presse locale, presse Viapost, OS, IP, colis, PPI.
    """
    periode_lower, dt_debut, dt_fin = validate_params("auto", co_regate, date_debut, date_fin)

    segments = build_segments(periode_lower, dt_debut, dt_fin)
    queries = [
        {"periode": seg[0], "query": build_trppu_query(seg[0], co_regate, seg[1], seg[2])}
        for seg in segments
    ]

    start = time.perf_counter()
    try:
        results = []
        for q in queries:
            sql, params = q["query"]
            results.extend(databricks.fetch_all(sql, params))
    except Exception as e:
        logger.error("Erreur requête TRPPU (%s) : %s", periode_lower, e)
        detail = {
            "error": True,
            "message": f"Erreur lors de la récupération des trafics TRPPU ({periode_lower}).",
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
        "count": len(results),
        "data": results,
    }
    if DEBUG_SHOW_QUERY:
        response["queries"] = [q["query"][0] for q in queries]
    return response
