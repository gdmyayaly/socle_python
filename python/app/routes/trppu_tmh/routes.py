"""Endpoints TMH d'un scénario : lecture (DSR-650), MAJ batch (DSR-659), MAJ ciblée (DSR-649)."""

import logging
import time

from fastapi import APIRouter, HTTPException, Query

from app.db.mysql import db_read, db_write
from app.log_utils import safe_preview
from app.routes.trppu_scenario.helpers import assert_editable, fetch_scenario_or_404
from app.security.crypto import encrypt_id_rh

from .helpers import SELECT_TMH_ONE_SQL, fetch_tmh, upsert_tmh_rows
from .schemas import TmhBatchResult, TmhBatchUpdate, TmhOut, TmhVolumeUpdate

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/scenarios", tags=["TMH"])


@router.get("/{id_scenario}/tmh", response_model=list[TmhOut])
async def list_tmh(
    id_scenario: int,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-650 : trafics moyen hebdo d'un scénario (1 ligne par produit)."""
    start = time.perf_counter()
    logger.info(
        "Début lecture TMH (id_scenario=%d, id_session_ihm=%s)",
        id_scenario,
        safe_preview(id_session_ihm),
    )
    await fetch_scenario_or_404(id_scenario)
    try:
        rows = await fetch_tmh(db_read, id_scenario)
    except Exception as e:
        logger.exception("Erreur lecture TMH (id_scenario=%d)", id_scenario)
        raise HTTPException(status_code=500, detail="Erreur lecture TMH.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Lecture TMH terminée (id_scenario=%d, count=%d, duration_ms=%.1f)",
        id_scenario,
        len(rows),
        duration_ms,
    )
    return rows


@router.put("/{id_scenario}/tmh", response_model=TmhBatchResult)
async def upsert_tmh(id_scenario: int, payload: TmhBatchUpdate):
    """DSR-659 : MAJ (upsert) des trafics recalculés, 1 entrée par produit."""
    start = time.perf_counter()
    logger.info(
        "Début MAJ TMH batch (id_scenario=%d, nb_produits=%d)",
        id_scenario,
        len(payload.tmh),
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    id_rh_token = encrypt_id_rh(payload.id_rh)
    try:
        async with db_write.transaction() as tx:
            nb_inserted, nb_updated = await upsert_tmh_rows(
                tx, id_scenario, payload.tmh, id_rh=id_rh_token
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erreur MAJ TMH batch (id_scenario=%d)", id_scenario)
        raise HTTPException(status_code=500, detail="Erreur mise à jour TMH.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "MAJ TMH batch terminée (id_scenario=%d, insérés=%d, modifiés=%d, duration_ms=%.1f)",
        id_scenario,
        nb_inserted,
        nb_updated,
        duration_ms,
    )
    return TmhBatchResult(
        id_scenario=id_scenario, nb_inserted=nb_inserted, nb_updated=nb_updated
    )


@router.patch("/{id_scenario}/tmh/{co_produit}", response_model=TmhOut)
async def update_tmh_volume(id_scenario: int, co_produit: str, payload: TmhVolumeUpdate):
    """DSR-649 : MAJ ciblée d'un trafic initial modifié (volume réalisé + moyennes)."""
    start = time.perf_counter()
    logger.info(
        "Début MAJ TMH ciblée (id_scenario=%d, co_produit=%s, payload=%s)",
        id_scenario,
        co_produit,
        safe_preview(payload.model_dump(mode="json")),
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    try:
        async with db_write.transaction() as tx:
            # DSR-649 : modification manuelle d'un trafic initial -> la ligne devient
            # "manuelle" (bl_manuel = 1), cf. DSR-665/648.
            rc = await tx.execute(
                "UPDATE trppu_tmh SET volume_realise = %s, moyenne_journaliere = %s, "
                "moyenne_hebdo = %s, bl_manuel = 1, dt_calcul = NOW() "
                "WHERE id_scenario = %s AND co_produit = %s",
                (
                    payload.volume_realise,
                    payload.moyenne_journaliere,
                    payload.moyenne_hebdo,
                    id_scenario,
                    co_produit,
                ),
            )
            row = await tx.fetch_one(SELECT_TMH_ONE_SQL, (id_scenario, co_produit))
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur MAJ TMH ciblée (id_scenario=%d, co_produit=%s)", id_scenario, co_produit
        )
        raise HTTPException(status_code=500, detail="Erreur mise à jour TMH.") from e

    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"Ligne TMH introuvable (scénario {id_scenario}, produit {co_produit}).",
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "MAJ TMH ciblée terminée (id_scenario=%d, co_produit=%s, rowcount=%s, duration_ms=%.1f)",
        id_scenario,
        co_produit,
        rc,
        duration_ms,
    )
    return row
