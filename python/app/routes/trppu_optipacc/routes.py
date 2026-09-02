"""Services TRPPU consommés par OPTIPACC : liste des scénarios (DSR-690) et
volumes bruts par produit (DSR-689).

Regroupés sous `/trppu-api/optipacc` pour que les applications tierces les
identifient sans ambiguïté et qu'ils puissent évoluer indépendamment des routes
servant l'IHM. Services en lecture seule et sans état.
"""

import logging
import time

from fastapi import APIRouter, HTTPException, Query

from app.db.mysql import db_read
from app.log_utils import ctx
from app.routes.trppu_site.helpers import fetch_site_or_404

from .helpers import (
    SELECT_SCENARIO_GARDE_SQL,
    SELECT_SCENARIOS_EXPLOITABLES_SQL,
    SELECT_VOLUMES_BRUTS_SQL,
    SELECT_VOLUMES_BRUTS_TOUS_SQL,
    assert_exploitable,
)
from .schemas import (
    ProduitVolume,
    ScenarioItem,
    SiteScenariosRequest,
    SiteScenariosResponse,
    TraficBrutRequest,
    TraficBrutResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/optipacc", tags=["OPTIPACC"])


@router.post(
    "/site-liste-scenarios",
    response_model=SiteScenariosResponse,
    # `message` n'apparaît dans la réponse que lorsqu'il est renseigné : le cas
    # nominal reste strictement le contrat DSR-690.
    response_model_exclude_none=True,
)
async def site_liste_scenarios(
    payload: SiteScenariosRequest,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-690 (S_SiteListeScenarios) : scénarios exploitables d'un site.

    Ne sont retournés que les scénarios au statut `VALIDE` **et** dont les trafics
    Agrébal ont été calculés (`trafic_agrebal_calcule = 1`). Tout autre statut
    (« EN COURS », « SIMULATION », …) ou un calcul Agrébal non terminé exclut le
    scénario de la liste.

    L'absence de scénario éligible n'est pas une erreur : la réponse est un 200
    avec une liste vide et un `message` explicatif. L'existence du site n'est pas
    contrôlée — un code Regate inconnu donne la même réponse.
    """
    start = time.perf_counter()
    co_regate = payload.code_regate
    logger.info("Début liste scénarios OPTIPACC %s", ctx(co_regate=co_regate))
    try:
        rows = await db_read.fetch_all(SELECT_SCENARIOS_EXPLOITABLES_SQL, (co_regate,))
    except Exception as e:
        logger.exception(
            "Erreur liste scénarios OPTIPACC %s", ctx(co_regate=co_regate)
        )
        raise HTTPException(
            status_code=500,
            detail="Une erreur est survenue lors de la récupération des scénarios.",
        ) from e

    message = None if rows else f"Aucun scénario trouvé pour le site {co_regate}."
    if message:
        # Pas une erreur (200 + liste vide), mais l'appelant OPTIPACC repart sans
        # scénario : la trace évite d'avoir à rejouer la requête pour comprendre.
        logger.info(
            "Aucun scénario exploitable OPTIPACC %s",
            ctx(co_regate=co_regate, motif="aucun scénario VALIDE avec Agrébal calculé"),
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin liste scénarios OPTIPACC %s",
        ctx(co_regate=co_regate, count=len(rows), duration_ms=duration_ms),
    )
    return SiteScenariosResponse(
        code_regate=co_regate,
        scenarios=[ScenarioItem(**row) for row in rows],
        message=message,
    )


@router.post("/scenario-trafic-brut", response_model=TraficBrutResponse)
async def scenario_trafic_brut(
    payload: TraficBrutRequest,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-689 (S_ScenarioTraficBrut) : volumes bruts finaux par produit.

    Pour chaque produit du scénario, la somme `constaté + prévisionnel recalculé +
    trafics manuels` telle qu'elle est stockée dans `trppu_tmh` (cf. docstring du
    module helpers pour le détail de la formule). Aucun détail de calcul n'est
    restitué : ni constaté, ni prévisionnel, ni TMH, ni trafic manuel (RG6, CA5).

    Les produits marqués exclus dans le TMH sont ignorés, sauf si le body porte
    `inclureExclus: true`.
    """
    start = time.perf_counter()
    co_regate = payload.code_regate
    id_scenario = payload.scenario_id
    logger.info(
        "Début trafic brut OPTIPACC %s",
        ctx(
            co_regate=co_regate,
            id_scenario=id_scenario,
            inclure_exclus=payload.inclure_exclus,
        ),
    )

    await fetch_site_or_404(co_regate)
    scenario = await db_read.fetch_one(SELECT_SCENARIO_GARDE_SQL, (id_scenario,))
    if not scenario:
        logger.warning(
            "Rejet trafic brut OPTIPACC %s",
            ctx(
                co_regate=co_regate,
                id_scenario=id_scenario,
                http=404,
                motif="scénario introuvable",
            ),
        )
        raise HTTPException(
            status_code=404,
            detail=f"Scénario {id_scenario} introuvable pour le site {co_regate}.",
        )
    assert_exploitable(scenario, co_regate)

    sql = (
        SELECT_VOLUMES_BRUTS_TOUS_SQL
        if payload.inclure_exclus
        else SELECT_VOLUMES_BRUTS_SQL
    )
    try:
        rows = await db_read.fetch_all(sql, (id_scenario,))
    except Exception as e:
        logger.exception(
            "Erreur trafic brut OPTIPACC %s",
            ctx(co_regate=co_regate, id_scenario=id_scenario),
        )
        raise HTTPException(
            status_code=500,
            detail="Une erreur est survenue lors de la récupération des trafics.",
        ) from e

    # SUM() renvoie un Decimal même sur des colonnes entières.
    produits = [
        ProduitVolume(
            code_produit=row["co_produit"],
            volume_brut=int(row["volume_brut"] or 0),
        )
        for row in rows
    ]

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin trafic brut OPTIPACC %s",
        ctx(
            co_regate=co_regate,
            id_scenario=id_scenario,
            count=len(produits),
            inclure_exclus=payload.inclure_exclus,
            duration_ms=duration_ms,
        ),
    )
    return TraficBrutResponse(
        code_regate=co_regate,
        scenario_id=id_scenario,
        produits=produits,
    )
