"""DSR-679 — Endpoint de récupération des trafics pivot (structure gold `_3`)."""

import json
import logging
import time

from fastapi import APIRouter

from app.routes.trppu_trafics.errors import bloc_debug, erreur_500
from app.routes.trppu_trafics.helpers import (
    accumulate_trafics,
    build_period_queries,
    empty_accumulator,
    executer_requete_async,
    fmt_date,
    formater_trafics,
    objets_sans_libelle,
    split_by_pivot,
    validate_params_pivot,
    zones_pivot,
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

    `trafic_constate` et `trafic_prevu` sont renseignés quelle que soit la date : c'est le pivot
    qui choisit. Dates < pivot -> trafic réel (prévisionnel = 0) ; dates >= pivot -> l'inverse.
    La liste des objets restitués est dynamique (elle sort du SQL), pas figée.
    Paramètres au format AAAAMMJJ ; période <= 2 ans. `is_day` force la table jour.
    """
    dt_debut, dt_fin, dt_pivot = validate_params_pivot(
        co_regate, date_debut, date_fin, date_pivot
    )
    reel_range, prev_range = split_by_pivot(dt_debut, dt_fin, dt_pivot)

    acc = empty_accumulator()
    traces: list[dict] = []
    raw_rows: list[dict] = []

    start = time.perf_counter()
    for (rg_debut, rg_fin), value_col, target_key in zones_pivot(reel_range, prev_range):
        for sql, params in build_period_queries(
            co_regate, rg_debut, rg_fin, value_col, force_jours=is_day
        ):
            trace = await executer_requete_async(sql, params)
            traces.append(trace)  # tracé AVANT le test : la requête fautive doit y figurer
            if trace["statut"] == "echec":
                logger.error("Erreur requête trafics pivot : %s", trace["erreur"])
                raise erreur_500(
                    "Erreur lors de la récupération des trafics.", traces, trace["erreur"]
                )
            raw_rows.extend(trace["lignes"])
            accumulate_trafics(trace["lignes"], target_key, acc)
    duration_s = round(time.perf_counter() - start, 3)

    logger.info(
        "Trafics pivot (co_regate=%s) — réponse non transformée (%d lignes) : %s",
        co_regate,
        len(raw_rows),
        json.dumps(raw_rows, ensure_ascii=False, default=str),
    )

    trafics = formater_trafics(acc)
    sans_libelle = objets_sans_libelle(acc)
    if sans_libelle:
        # On expose l'objet avec un libellé nul plutôt que d'inventer une correspondance.
        logger.warning("Objets absents du mapping (libellé nul) : %s", ", ".join(sans_libelle))

    logger.info(
        "Trafics pivot (co_regate=%s) — réponse transformée : %s",
        co_regate,
        json.dumps(trafics, ensure_ascii=False, default=str),
    )

    # Le calcul des jours ouvrés (DSR-613) ne doit pas faire échouer la restitution des trafics.
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
        "objets_sans_libelle": sans_libelle,
        "nb_jours": nb_jours,
    }
    debug = bloc_debug(traces)
    if debug is not None:
        response["debug"] = debug
    return response
