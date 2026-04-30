"""CRUD des exclusions de produits attachées à un scénario."""

import logging

from fastapi import APIRouter, Request, status

from app.db.mysql import db_read, db_write
from app.routes.scenarios.helpers import (
    assert_not_fige,
    assert_produit_exists,
    err,
    fetch_scenario,
    get_caller,
    increment_version,
    last_insert_id,
    log_api,
)
from app.routes.scenarios.schemas import ExclusionCreate

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Exclusions"])


async def _fetch_exclusion(tx, id_exclusion: int) -> dict:
    row = await tx.fetch_one(
        "SELECT * FROM trppu_scenario_exclusions WHERE id = %s",
        (id_exclusion,),
    )
    if not row:
        raise err(404, f"Exclusion {id_exclusion} introuvable.")
    return row


# ------------------------------------------------------------------ #
# LIST
# ------------------------------------------------------------------ #
@router.get("/scenarios/{id_scenario}/exclusions")
async def list_exclusions(id_scenario: int):
    rows = await db_read.fetch_all(
        "SELECT * FROM trppu_scenario_exclusions "
        "WHERE id_scenario = %s ORDER BY co_produit",
        (id_scenario,),
    )
    return {"count": len(rows), "data": rows}


# ------------------------------------------------------------------ #
# CREATE
# ------------------------------------------------------------------ #
@router.post(
    "/scenarios/{id_scenario}/exclusions",
    status_code=status.HTTP_201_CREATED,
)
async def create_exclusion(
    id_scenario: int,
    payload: ExclusionCreate,
    request: Request,
):
    caller = get_caller(request)
    async with db_write.transaction() as tx:
        scenario = await fetch_scenario(tx, id_scenario)
        assert_not_fige(scenario)
        await assert_produit_exists(tx, payload.co_produit)

        try:
            await tx.execute(
                "INSERT INTO trppu_scenario_exclusions "
                "(id_scenario, co_produit, motif) VALUES (%s, %s, %s)",
                (id_scenario, payload.co_produit, payload.motif),
            )
        except Exception as e:
            # uq_exclusion (id_scenario, co_produit)
            if "uq_exclusion" in str(e) or "Duplicate" in str(e):
                raise err(
                    409,
                    f"Le produit '{payload.co_produit}' est déjà exclu "
                    f"de ce scénario.",
                )
            raise

        new_id = await last_insert_id(tx)
        version = await increment_version(tx, id_scenario)
        await log_api(
            tx,
            api_name="create_exclusion",
            id_scenario=id_scenario,
            caller=caller,
            params=payload.model_dump(mode="json"),
            co_regate=scenario["co_regate"],
        )

    logger.info("Exclusion %s créée (scénario %s) par %s", new_id, id_scenario, caller)
    return {
        "id": new_id,
        "id_scenario": id_scenario,
        "version_scenario": version,
    }


# ------------------------------------------------------------------ #
# DELETE
# ------------------------------------------------------------------ #
@router.delete("/exclusions/{id_exclusion}")
async def delete_exclusion(id_exclusion: int, request: Request):
    caller = get_caller(request)
    async with db_write.transaction() as tx:
        excl = await _fetch_exclusion(tx, id_exclusion)
        scenario = await fetch_scenario(tx, excl["id_scenario"])
        assert_not_fige(scenario)

        await tx.execute(
            "DELETE FROM trppu_scenario_exclusions WHERE id = %s",
            (id_exclusion,),
        )
        version = await increment_version(tx, scenario["id_scenario"])
        await log_api(
            tx,
            api_name="delete_exclusion",
            id_scenario=scenario["id_scenario"],
            caller=caller,
            params={"id_exclusion": id_exclusion, "co_produit": excl["co_produit"]},
            co_regate=scenario["co_regate"],
        )

    return {
        "id": id_exclusion,
        "id_scenario": scenario["id_scenario"],
        "version_scenario": version,
        "deleted": True,
    }
