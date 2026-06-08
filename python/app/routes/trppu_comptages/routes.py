"""Comptages manuels d'un scénario : lecture (DSR-653) + écriture (DSR-644)."""

import logging
import time
from datetime import date

from fastapi import APIRouter, HTTPException, Query, Response, status

from app.db.mysql import db_read, db_write
from app.log_utils import safe_preview
from app.routes.trppu_scenario.helpers import assert_editable, fetch_scenario_or_404

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
    logger.info(
        "Début lecture comptages (id_scenario=%d, id_session_ihm=%s)",
        id_scenario,
        safe_preview(id_session_ihm),
    )
    await fetch_scenario_or_404(id_scenario)
    try:
        rows = await db_read.fetch_all(SELECT_COMPTAGES_SQL, (id_scenario,))
    except Exception as e:
        logger.exception("Erreur lecture comptages (id_scenario=%d)", id_scenario)
        raise HTTPException(status_code=500, detail="Erreur lecture comptages.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Lecture comptages terminée (id_scenario=%d, count=%d, duration_ms=%.1f)",
        id_scenario,
        len(rows),
        duration_ms,
    )
    return rows


@router.post(
    "/{id_scenario}/comptages", response_model=ComptageOut, status_code=status.HTTP_201_CREATED
)
async def add_comptage(id_scenario: int, payload: ComptageCreate):
    """DSR-644 : ajout d'un comptage. 409 si un comptage existe déjà pour ce produit."""
    start = time.perf_counter()
    logger.info(
        "Début ajout comptage (id_scenario=%d, co_produit=%s, nb_produit=%s)",
        id_scenario,
        payload.co_produit,
        payload.nb_produit,
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    dt_comptage = payload.dt_comptage or date.today()

    try:
        async with db_write.transaction() as tx:
            if await fetch_comptage(tx, id_scenario, payload.co_produit):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Un comptage existe déjà pour le produit {payload.co_produit} "
                        f"(scénario {id_scenario}). Utilisez PUT pour le modifier."
                    ),
                )
            await tx.execute(
                "INSERT INTO trppu_scenario_comptages_manuels "
                "(id_scenario, dt_comptage, co_produit, nb_produit) "
                "VALUES (%s, %s, %s, %s)",
                (id_scenario, dt_comptage, payload.co_produit, payload.nb_produit),
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur ajout comptage (id_scenario=%d, co_produit=%s)",
            id_scenario,
            payload.co_produit,
        )
        raise HTTPException(status_code=500, detail="Erreur ajout comptage.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Ajout comptage terminé (id_scenario=%d, co_produit=%s, duration_ms=%.1f)",
        id_scenario,
        payload.co_produit,
        duration_ms,
    )
    return ComptageOut(
        co_produit=payload.co_produit, dt_comptage=dt_comptage, nb_produit=payload.nb_produit
    )


@router.put("/{id_scenario}/comptages/{co_produit}", response_model=ComptageOut)
async def update_comptage(id_scenario: int, co_produit: str, payload: ComptageUpdate):
    """DSR-644 : modification du comptage (id_scenario, co_produit)."""
    start = time.perf_counter()
    logger.info(
        "Début modification comptage (id_scenario=%d, co_produit=%s, nb_produit=%s)",
        id_scenario,
        co_produit,
        payload.nb_produit,
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    dt_comptage = payload.dt_comptage or date.today()

    try:
        async with db_write.transaction() as tx:
            if not await fetch_comptage(tx, id_scenario, co_produit):
                raise HTTPException(
                    status_code=404,
                    detail=f"Comptage introuvable (scénario {id_scenario}, produit {co_produit}).",
                )
            await tx.execute(
                "UPDATE trppu_scenario_comptages_manuels "
                "SET dt_comptage = %s, nb_produit = %s "
                "WHERE id_scenario = %s AND co_produit = %s",
                (dt_comptage, payload.nb_produit, id_scenario, co_produit),
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur modification comptage (id_scenario=%d, co_produit=%s)",
            id_scenario,
            co_produit,
        )
        raise HTTPException(status_code=500, detail="Erreur modification comptage.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Modification comptage terminée (id_scenario=%d, co_produit=%s, duration_ms=%.1f)",
        id_scenario,
        co_produit,
        duration_ms,
    )
    return ComptageOut(co_produit=co_produit, dt_comptage=dt_comptage, nb_produit=payload.nb_produit)


@router.delete("/{id_scenario}/comptages/{co_produit}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_comptage(id_scenario: int, co_produit: str):
    """DSR-644 : suppression du comptage (id_scenario, co_produit)."""
    start = time.perf_counter()
    logger.info(
        "Début suppression comptage (id_scenario=%d, co_produit=%s)", id_scenario, co_produit
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    try:
        async with db_write.transaction() as tx:
            rc = await tx.execute(
                "DELETE FROM trppu_scenario_comptages_manuels "
                "WHERE id_scenario = %s AND co_produit = %s",
                (id_scenario, co_produit),
            )
    except Exception as e:
        logger.exception(
            "Erreur suppression comptage (id_scenario=%d, co_produit=%s)",
            id_scenario,
            co_produit,
        )
        raise HTTPException(status_code=500, detail="Erreur suppression comptage.") from e

    if not rc:
        raise HTTPException(
            status_code=404,
            detail=f"Comptage introuvable (scénario {id_scenario}, produit {co_produit}).",
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Suppression comptage terminée (id_scenario=%d, co_produit=%s, duration_ms=%.1f)",
        id_scenario,
        co_produit,
        duration_ms,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
