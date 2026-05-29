"""Rétention PIC d'un scénario : lecture/merge (DSR-660) + écriture (DSR-661)."""

import logging
import time

from fastapi import APIRouter, HTTPException, Query

from app.db.mysql import db_read, db_write
from app.log_utils import safe_preview
from app.security.crypto import encrypt_id_rh
from app.routes.trppu_scenario.helpers import assert_not_fige, fetch_scenario_or_404

from .helpers import (
    DEFAULT_PIC_VERSION,
    fetch_coeffs_for_version,
    fetch_scenario_pic_version,
    merge_coeffs,
)
from .schemas import PicCoefUpsert, PicCoefUpsertResult, PicScenarioOut

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/scenarios", tags=["Rétention PIC"])


@router.get("/{id_scenario}/pic-coefficients", response_model=PicScenarioOut)
async def get_pic_coefficients(
    id_scenario: int,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-660 : coefficients PIC par défaut (national) surchargés par ceux du scénario."""
    start = time.perf_counter()
    logger.info(
        "Début lecture coefficients PIC (id_scenario=%d, id_session_ihm=%s)",
        id_scenario,
        safe_preview(id_session_ihm),
    )
    await fetch_scenario_or_404(id_scenario)

    try:
        defaults = await fetch_coeffs_for_version(db_read, DEFAULT_PIC_VERSION)
        scen_version = await fetch_scenario_pic_version(db_read, id_scenario)
        overrides: list = []
        id_pic_version_scenario = None
        niveau_scenario = None
        if scen_version:
            id_pic_version_scenario = int(scen_version["id_pic_version"])
            niveau_scenario = scen_version["niveau"]
            overrides = await fetch_coeffs_for_version(db_read, id_pic_version_scenario)
        coefficients = merge_coeffs(defaults, overrides)
    except Exception as e:
        logger.exception("Erreur lecture coefficients PIC (id_scenario=%d)", id_scenario)
        raise HTTPException(status_code=500, detail="Erreur lecture coefficients PIC.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Lecture coefficients PIC terminée (id_scenario=%d, id_pic_version_scenario=%s, "
        "nb_coeffs=%d, nb_surcharges=%d, duration_ms=%.1f)",
        id_scenario,
        id_pic_version_scenario,
        len(coefficients),
        len(overrides),
        duration_ms,
    )
    return PicScenarioOut(
        id_pic_version_defaut=DEFAULT_PIC_VERSION,
        id_pic_version_scenario=id_pic_version_scenario,
        niveau_scenario=niveau_scenario,
        coefficients=coefficients,
    )


@router.put("/{id_scenario}/pic-coefficients", response_model=PicCoefUpsertResult)
async def upsert_pic_coefficient(id_scenario: int, payload: PicCoefUpsert):
    """DSR-661 : enregistre un coefficient PIC modifié pour le scénario.

    - Cas 1 : version scénario existante -> UPDATE du coef, ou INSERT si (produit, jour,
      densité) absent.
    - Cas 2 : aucune version scénario -> INSERT trppu_pic_version (niveau SCENARIO)
      puis INSERT du coef. Le tout en transaction.
    """
    start = time.perf_counter()
    logger.info(
        "Début enregistrement coefficient PIC (id_scenario=%d, co_produit=%s, jour=%s, densite=%s)",
        id_scenario,
        payload.co_produit,
        payload.jour_semaine,
        payload.densite,
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_not_fige(scenario)
    id_rh_token = encrypt_id_rh(payload.id_rh)

    try:
        async with db_write.transaction() as tx:
            version = await tx.fetch_one(
                "SELECT id_pic_version FROM trppu_pic_version "
                "WHERE id_scenario = %s AND niveau = 'SCENARIO' "
                "ORDER BY id_pic_version DESC LIMIT 1",
                (id_scenario,),
            )
            if version:
                id_pic_version = int(version["id_pic_version"])
                existing_coef = await tx.fetch_one(
                    "SELECT id_pic_coef FROM trppu_pic_coefficients "
                    "WHERE id_pic_version = %s AND co_produit = %s "
                    "AND jour_semaine = %s AND densite = %s",
                    (id_pic_version, payload.co_produit, payload.jour_semaine, payload.densite),
                )
                if existing_coef:
                    await tx.execute(
                        "UPDATE trppu_pic_coefficients "
                        "SET coef = %s, dt_maj = NOW(), id_rh = %s WHERE id_pic_coef = %s",
                        (payload.coef, id_rh_token, existing_coef["id_pic_coef"]),
                    )
                    action = "update"
                else:
                    await _insert_coef(tx, id_pic_version, payload, id_rh_token)
                    action = "insert_coef"
            else:
                co_regate = scenario["co_regate"]
                await tx.execute(
                    "INSERT INTO trppu_pic_version "
                    "(lb_pic_version, niveau, co_regate, id_scenario, dt_activation, "
                    " id_rh_creation, id_rh_maj) "
                    "VALUES (%s, 'SCENARIO', %s, %s, NOW(), %s, %s)",
                    (
                        f"{co_regate}_{id_scenario}",
                        co_regate,
                        id_scenario,
                        id_rh_token,
                        id_rh_token,
                    ),
                )
                row = await tx.fetch_one("SELECT LAST_INSERT_ID() AS id")
                id_pic_version = int(row["id"])
                await _insert_coef(tx, id_pic_version, payload, id_rh_token)
                action = "insert_version_and_coef"
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur enregistrement coefficient PIC (id_scenario=%d, co_produit=%s)",
            id_scenario,
            payload.co_produit,
        )
        raise HTTPException(status_code=500, detail="Erreur enregistrement coefficient PIC.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Enregistrement coefficient PIC terminé (id_scenario=%d, id_pic_version=%d, action=%s, duration_ms=%.1f)",
        id_scenario,
        id_pic_version,
        action,
        duration_ms,
    )
    return PicCoefUpsertResult(action=action, id_pic_version=id_pic_version)


async def _insert_coef(tx, id_pic_version: int, payload: PicCoefUpsert, id_rh_token: str) -> None:
    await tx.execute(
        "INSERT INTO trppu_pic_coefficients "
        "(id_pic_version, co_produit, jour_semaine, dt_effet, coef, densite, id_rh) "
        "VALUES (%s, %s, %s, NOW(), %s, %s, %s)",
        (
            id_pic_version,
            payload.co_produit,
            payload.jour_semaine,
            payload.coef,
            payload.densite,
            id_rh_token,
        ),
    )
