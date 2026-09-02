"""DSR-679 — Endpoint de récupération des trafics pivot (structure gold `_3`)."""

import logging
import time

from fastapi import APIRouter

from app.log_utils import ctx
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
    logger.info(
        "Début lecture trafics pivot %s",
        ctx(
            co_regate=co_regate,
            date_debut=date_debut,
            date_fin=date_fin,
            date_pivot=date_pivot,
            is_day=is_day,
        ),
    )
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
                # Pas de `logger.exception` ici : l'exception a été absorbée par
                # `executer_requete`, qui a déjà journalisé la stacktrace.
                logger.error(
                    "Échec requête trafics pivot %s",
                    ctx(co_regate=co_regate, erreur=trace["erreur"]),
                )
                raise erreur_500(
                    "Erreur lors de la récupération des trafics.", traces, trace["erreur"]
                )
            raw_rows.extend(trace["lignes"])
            accumulate_trafics(trace["lignes"], target_key, acc)
    duration_s = round(time.perf_counter() - start, 3)

    # Le jeu de résultats complet peut peser plusieurs Mo : volumétrie en INFO,
    # contenu en DEBUG et borné par `ctx`.
    logger.info(
        "Trafics pivot lus %s", ctx(co_regate=co_regate, lignes=len(raw_rows))
    )
    logger.debug(
        "Trafics pivot — réponse non transformée %s",
        ctx(co_regate=co_regate, lignes=raw_rows),
    )

    trafics = formater_trafics(acc)
    sans_libelle = objets_sans_libelle(acc)
    if sans_libelle:
        # On expose l'objet avec un libellé nul plutôt que d'inventer une correspondance.
        logger.warning(
            "Objets absents du mapping %s",
            ctx(co_regate=co_regate, nb=len(sans_libelle), objets=sans_libelle),
        )

    logger.debug(
        "Trafics pivot — réponse transformée %s",
        ctx(co_regate=co_regate, trafics=trafics),
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
            "Calcul nb_jours indisponible %s",
            ctx(co_regate=co_regate, consequence="bloc nb_jours=null"),
            exc_info=True,
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

    logger.info(
        "Fin lecture trafics pivot %s",
        ctx(
            co_regate=co_regate,
            count=len(trafics),
            lignes=len(raw_rows),
            objets_sans_libelle=len(sans_libelle),
            duration_ms=round(duration_s * 1000, 1),
        ),
    )
    return response
