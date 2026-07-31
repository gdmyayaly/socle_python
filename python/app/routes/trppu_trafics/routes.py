"""DSR-679 — Endpoint de récupération des trafics pivot (nouvelle structure gold `_3`).

Remplace la logique DSR-666 : pour un site + période + date pivot, agrège par objet
(`co_type_objet`) le trafic constaté (avant pivot) et prévisionnel (à partir du pivot),
et restitue une ligne par objet présent dans le résultat du SQL.
"""

import json
import logging
import time
from datetime import datetime

from fastapi import APIRouter, HTTPException
from starlette.concurrency import run_in_threadpool

from app.config import DEBUG_SHOW_QUERY
from app.db.databricks import databricks
from app.routes.trppu_trafics.helpers import (
    TRAFIC_COL_CONSTATE,
    TRAFIC_COL_PREVISIONNEL,
    accumulate_trafics,
    build_period_queries,
    empty_accumulator,
    fmt_date,
    render_sql,
    split_by_pivot,
    validate_params_pivot,
)
from app.services.jours_service import compute_nb_jours

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/trafics", tags=["Trafics (DSR-679)"])


@router.get("/get_trafics_pivot")
async def get_trafics_pivot(
    co_regate: str | None = None,
    date_debut: str | None = None,
    date_fin: str | None = None,
    date_pivot: str | None = None,
    is_day: bool = False,
):
    """DSR-679 : trafics agrégés par objet, ventilés réel/prévisionnel selon la date pivot.

    Structure gold : l'objet est porté directement par `co_type_objet` (OO/OS/PQ/EQ/CO/IP/PPI
    — liste dynamique, non figée) ; valeurs `trafic_constate` / `trafic_prevu`. La requête est
    mono-table (aucune jointure de dimension) : `GROUP BY co_type_objet`, filtrée sur le
    niveau de regroupement `SITE` (sans quoi les niveaux ETABLISSEMENT/PIC/NATIONAL cumulent
    et gonflent les sommes), avec pruning sur les partitions (`co_annee_comptage`,
    + `co_mois_comptage` pour le jour). Le résultat est restitué tel quel.

    - dates < pivot  -> trafic réel (constaté) ; prévisionnel = 0
    - dates >= pivot -> trafic prévisionnel ; réel = 0
    Paramètres au format AAAAMMJJ ; période <= 2 ans. `is_day` force la table jour.
    """
    dt_debut, dt_fin, dt_pivot = validate_params_pivot(
        co_regate, date_debut, date_fin, date_pivot
    )
    reel_range, prev_range = split_by_pivot(dt_debut, dt_fin, dt_pivot)

    # Une requête par zone (réel/prév) ; on somme la bonne colonne par objet.
    plan: list[tuple[tuple[datetime, datetime], str, str]] = []
    if reel_range is not None:
        plan.append((reel_range, TRAFIC_COL_CONSTATE, "trafic_brut"))
    if prev_range is not None:
        plan.append((prev_range, TRAFIC_COL_PREVISIONNEL, "trafic_previsionnel"))

    acc = empty_accumulator()
    executed: list[tuple[str, dict]] = []
    raw_rows: list[dict] = []

    start = time.perf_counter()
    try:
        for (rg_debut, rg_fin), value_col, target_key in plan:
            for sql, params in build_period_queries(
                co_regate, rg_debut, rg_fin, value_col, force_jours=is_day
            ):
                rows = await run_in_threadpool(databricks.fetch_all, sql, params)
                raw_rows.extend(rows)
                accumulate_trafics(rows, target_key, acc)
                executed.append((sql, params))
    except Exception as e:
        logger.error("Erreur requête trafics pivot DSR-679 : %s", e)
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

    # Réponse non transformée : lignes agrégées brutes renvoyées par Databricks.
    logger.info(
        "Trafics pivot DSR-679 (co_regate=%s) — réponse non transformée (%d lignes) : %s",
        co_regate,
        len(raw_rows),
        json.dumps(raw_rows, ensure_ascii=False, default=str),
    )

    # Restitution dynamique : uniquement les objets présents dans le résultat SQL.
    trafics = [
        {
            "co_produit": objet,
            "trafic_brut": acc[objet]["trafic_brut"],
            "trafic_previsionnel": acc[objet]["trafic_previsionnel"],
        }
        for objet in sorted(acc)
    ]

    logger.info(
        "Trafics pivot DSR-679 (co_regate=%s) — réponse transformée : %s",
        co_regate,
        json.dumps(trafics, ensure_ascii=False, default=str),
    )

    # nb_jours (DSR-613)
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
        "is_day": is_day,
        "count": len(trafics),
        "trafics": trafics,
        "nb_jours": nb_jours,
    }
    if DEBUG_SHOW_QUERY:
        response["queries"] = [render_sql(sql, params) for sql, params in executed]
    return response
