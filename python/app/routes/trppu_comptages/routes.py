"""Comptages manuels d'un scénario : lecture (DSR-653) + écriture (DSR-644)."""

import logging
import time
from datetime import date

from fastapi import APIRouter, HTTPException, Query, Response, status

from app.db.mysql import db_read, db_write
from app.log_utils import ctx, params_loggables
from app.routes.trppu_scenario.helpers import assert_editable, fetch_scenario_or_404
from app.services.api_log import ACTION_ECRITURE_COMPTAGE, enregistrer_appel

from .helpers import SELECT_COMPTAGES_SQL, fetch_comptage
from .schemas import ComptageCreate, ComptageOut, ComptageUpdate

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/scenarios", tags=["Comptages manuels"])


@router.get("/{id_scenario}/comptages", response_model=list[ComptageOut])
async def list_comptages(
    id_scenario: int,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-653 : comptages manuels d'un scénario."""
    start = time.perf_counter()
    logger.info("Début lecture comptages %s", ctx(id_scenario=id_scenario))
    await fetch_scenario_or_404(id_scenario)
    try:
        rows = await db_read.fetch_all(SELECT_COMPTAGES_SQL, (id_scenario,))
    except Exception as e:
        logger.exception("Erreur lecture comptages %s", ctx(id_scenario=id_scenario))
        raise HTTPException(status_code=500, detail="Erreur lecture comptages.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin lecture comptages %s",
        ctx(id_scenario=id_scenario, count=len(rows), duration_ms=duration_ms),
    )
    return rows


@router.post(
    "/{id_scenario}/comptages", response_model=ComptageOut, status_code=status.HTTP_201_CREATED
)
async def add_comptage(id_scenario: int, payload: ComptageCreate):
    """DSR-644 : ajout d'un comptage. 409 si un comptage existe déjà pour ce produit."""
    start = time.perf_counter()
    logged = params_loggables(payload)
    logger.info(
        "Début ajout comptage %s",
        ctx(id_scenario=id_scenario, co_produit=payload.co_produit, params=logged),
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    dt_comptage = payload.dt_comptage or date.today()

    try:
        async with db_write.transaction() as tx:
            if await fetch_comptage(tx, id_scenario, payload.co_produit):
                logger.warning(
                    "Rejet ajout comptage %s",
                    ctx(
                        id_scenario=id_scenario,
                        co_produit=payload.co_produit,
                        http=409,
                        motif="comptage déjà existant pour ce produit",
                    ),
                )
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Un comptage existe déjà pour le produit {payload.co_produit} "
                        f"(scénario {id_scenario}). Utilisez PUT pour le modifier."
                    ),
                )
            rows_inseres = await tx.execute(
                "INSERT INTO trppu_scenario_comptages_manuels "
                "(id_scenario, dt_comptage, co_produit, nb_produit) "
                "VALUES (%s, %s, %s, %s)",
                (id_scenario, dt_comptage, payload.co_produit, payload.nb_produit),
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur ajout comptage %s",
            ctx(id_scenario=id_scenario, co_produit=payload.co_produit, params=logged),
        )
        raise HTTPException(status_code=500, detail="Erreur ajout comptage.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_ECRITURE_COMPTAGE,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={
            "operation": "ajout",
            "co_produit": payload.co_produit,
            "dt_comptage": str(dt_comptage),
            "params": logged,
        },
    )
    logger.info(
        "Fin ajout comptage %s",
        ctx(
            id_scenario=id_scenario,
            co_produit=payload.co_produit,
            nb_produit=payload.nb_produit,
            dt_comptage=dt_comptage,
            rows_affected=rows_inseres,
            duration_ms=duration_ms,
        ),
    )
    return ComptageOut(
        co_produit=payload.co_produit, dt_comptage=dt_comptage, nb_produit=payload.nb_produit
    )


@router.put("/{id_scenario}/comptages/{co_produit}", response_model=ComptageOut)
async def update_comptage(id_scenario: int, co_produit: str, payload: ComptageUpdate):
    """DSR-644 : modification du comptage (id_scenario, co_produit)."""
    start = time.perf_counter()
    logged = params_loggables(payload)
    logger.info(
        "Début MAJ comptage %s",
        ctx(id_scenario=id_scenario, co_produit=co_produit, params=logged),
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    dt_comptage = payload.dt_comptage or date.today()

    try:
        async with db_write.transaction() as tx:
            avant = await fetch_comptage(tx, id_scenario, co_produit)
            if not avant:
                logger.warning(
                    "Rejet MAJ comptage %s",
                    ctx(
                        id_scenario=id_scenario,
                        co_produit=co_produit,
                        http=404,
                        motif="comptage introuvable",
                    ),
                )
                raise HTTPException(
                    status_code=404,
                    detail=f"Comptage introuvable (scénario {id_scenario}, produit {co_produit}).",
                )
            etat_avant = params_loggables(dict(avant))
            rows_maj = await tx.execute(
                "UPDATE trppu_scenario_comptages_manuels "
                "SET dt_comptage = %s, nb_produit = %s "
                "WHERE id_scenario = %s AND co_produit = %s",
                (dt_comptage, payload.nb_produit, id_scenario, co_produit),
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur MAJ comptage %s",
            ctx(id_scenario=id_scenario, co_produit=co_produit, params=logged),
        )
        raise HTTPException(status_code=500, detail="Erreur modification comptage.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_ECRITURE_COMPTAGE,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={
            "operation": "maj",
            "co_produit": co_produit,
            "etat_avant": etat_avant,
            "params": logged,
        },
    )
    logger.info(
        "Fin MAJ comptage %s",
        ctx(
            id_scenario=id_scenario,
            co_produit=co_produit,
            etat_avant=etat_avant,
            nb_produit=payload.nb_produit,
            rows_affected=rows_maj,
            duration_ms=duration_ms,
        ),
    )
    return ComptageOut(co_produit=co_produit, dt_comptage=dt_comptage, nb_produit=payload.nb_produit)


@router.delete("/{id_scenario}/comptages/{co_produit}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_comptage(id_scenario: int, co_produit: str):
    """DSR-644 : suppression du comptage (id_scenario, co_produit)."""
    start = time.perf_counter()
    logger.info(
        "Début suppression comptage %s",
        ctx(id_scenario=id_scenario, co_produit=co_produit),
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    try:
        async with db_write.transaction() as tx:
            # Suppression définitive : état journalisé avant écriture.
            avant = await fetch_comptage(tx, id_scenario, co_produit)
            etat_avant = params_loggables(dict(avant)) if avant else None
            if etat_avant is not None:
                logger.info(
                    "État avant suppression comptage %s",
                    ctx(id_scenario=id_scenario, co_produit=co_produit, etat=etat_avant),
                )
            rc = await tx.execute(
                "DELETE FROM trppu_scenario_comptages_manuels "
                "WHERE id_scenario = %s AND co_produit = %s",
                (id_scenario, co_produit),
            )
    except Exception as e:
        logger.exception(
            "Erreur suppression comptage %s",
            ctx(id_scenario=id_scenario, co_produit=co_produit),
        )
        raise HTTPException(status_code=500, detail="Erreur suppression comptage.") from e

    if not rc:
        logger.warning(
            "Rejet suppression comptage %s",
            ctx(
                id_scenario=id_scenario,
                co_produit=co_produit,
                http=404,
                motif="comptage introuvable",
            ),
        )
        raise HTTPException(
            status_code=404,
            detail=f"Comptage introuvable (scénario {id_scenario}, produit {co_produit}).",
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_ECRITURE_COMPTAGE,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={
            "operation": "suppression",
            "co_produit": co_produit,
            "etat_avant": etat_avant,
        },
    )
    logger.info(
        "Fin suppression comptage %s",
        ctx(
            id_scenario=id_scenario,
            co_produit=co_produit,
            rows_affected=rc,
            duration_ms=duration_ms,
        ),
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
