"""CRUD scénarios + workflow de statut (verrouillage)."""

import logging

from fastapi import APIRouter, Query, Request, status

from app.db.mysql import db_read, db_write
from app.routes.scenarios.helpers import (
    assert_not_fige,
    assert_pic_version_exists,
    err,
    fetch_scenario,
    get_caller,
    increment_version,
    last_insert_id,
    log_api,
    validate_periode,
)
from app.routes.scenarios.schemas import (
    ScenarioCreate,
    ScenarioUpdate,
    StatutUpdate,
)
from app.routes.scenarios.statuts import (
    apply_transition_side_effects,
    assert_transition_allowed,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Scenarios"])


# ------------------------------------------------------------------ #
# CREATE
# ------------------------------------------------------------------ #
@router.post("/scenarios", status_code=status.HTTP_201_CREATED)
async def create_scenario(payload: ScenarioCreate, request: Request):
    """Crée un scénario en statut EN COURS, version 1."""
    caller = get_caller(request)
    validate_periode(payload.periode_debut, payload.periode_fin)

    async with db_write.transaction() as tx:
        await assert_pic_version_exists(tx, payload.id_pic_version)

        await tx.execute(
            """
            INSERT INTO trppu_scenario
              (co_roc, co_regate, lb_scenario, statut, dt_creation,
               periode_debut, periode_fin,
               periode_realise_debut, periode_realise_fin,
               periode_prev_debut, periode_prev_fin,
               id_pic_version, version_scenario, est_fige)
            VALUES (%s, %s, %s, 'EN COURS', NOW(),
                    %s, %s, %s, %s, %s, %s, %s, 1, FALSE)
            """,
            (
                payload.co_roc,
                payload.co_regate,
                payload.lb_scenario,
                payload.periode_debut,
                payload.periode_fin,
                payload.periode_realise_debut,
                payload.periode_realise_fin,
                payload.periode_prev_debut,
                payload.periode_prev_fin,
                payload.id_pic_version,
            ),
        )
        id_scenario = await last_insert_id(tx)
        await log_api(
            tx,
            api_name="create_scenario",
            id_scenario=id_scenario,
            caller=caller,
            params=payload.model_dump(mode="json"),
            co_regate=payload.co_regate,
        )

    logger.info("Scénario %s créé par %s", id_scenario, caller)
    return {
        "id_scenario": id_scenario,
        "version_scenario": 1,
        "statut": "EN COURS",
    }


# ------------------------------------------------------------------ #
# LIST
# ------------------------------------------------------------------ #
@router.get("/scenarios")
async def list_scenarios(
    co_regate: str | None = Query(None, min_length=6, max_length=6),
    statut: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Liste paginée avec filtres optionnels."""
    conditions: list[str] = []
    params: list = []
    if co_regate:
        conditions.append("co_regate = %s")
        params.append(co_regate)
    if statut:
        conditions.append("statut = %s")
        params.append(statut)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    rows = await db_read.fetch_all(
        f"SELECT * FROM trppu_scenario {where} "
        f"ORDER BY id_scenario DESC LIMIT %s OFFSET %s",
        tuple(params + [limit, offset]),
    )
    total_row = await db_read.fetch_one(
        f"SELECT COUNT(*) AS n FROM trppu_scenario {where}",
        tuple(params) if params else None,
    )
    return {
        "count": len(rows),
        "total": int(total_row["n"]),
        "limit": limit,
        "offset": offset,
        "data": rows,
    }


# ------------------------------------------------------------------ #
# READ
# ------------------------------------------------------------------ #
@router.get("/scenarios/{id_scenario}")
async def get_scenario(id_scenario: int):
    row = await db_read.fetch_one(
        "SELECT * FROM trppu_scenario WHERE id_scenario = %s",
        (id_scenario,),
    )
    if not row:
        raise err(404, f"Scénario {id_scenario} introuvable.")
    return row


# ------------------------------------------------------------------ #
# UPDATE
# ------------------------------------------------------------------ #
@router.patch("/scenarios/{id_scenario}")
async def update_scenario(id_scenario: int, payload: ScenarioUpdate, request: Request):
    """Modifie un scénario non figé. Incrémente version_scenario."""
    caller = get_caller(request)
    changes = payload.model_dump(exclude_unset=True)

    async with db_write.transaction() as tx:
        scenario = await fetch_scenario(tx, id_scenario)
        assert_not_fige(scenario)

        # Si on touche aux dates de période, recontrôler la cohérence.
        new_debut = changes.get("periode_debut", scenario["periode_debut"])
        new_fin = changes.get("periode_fin", scenario["periode_fin"])
        validate_periode(new_debut, new_fin)

        if "id_pic_version" in changes:
            await assert_pic_version_exists(tx, changes["id_pic_version"])

        set_clauses = ", ".join(f"{k} = %s" for k in changes)
        params = tuple(changes.values()) + (id_scenario,)
        await tx.execute(
            f"UPDATE trppu_scenario SET {set_clauses} WHERE id_scenario = %s",
            params,
        )
        version = await increment_version(tx, id_scenario)
        await log_api(
            tx,
            api_name="update_scenario",
            id_scenario=id_scenario,
            caller=caller,
            params=payload.model_dump(mode="json", exclude_unset=True),
            co_regate=scenario["co_regate"],
        )

    logger.info("Scénario %s modifié par %s (v%s)", id_scenario, caller, version)
    return {"id_scenario": id_scenario, "version_scenario": version}


# ------------------------------------------------------------------ #
# DELETE (soft → ARCHIVE)
# ------------------------------------------------------------------ #
@router.delete("/scenarios/{id_scenario}")
async def delete_scenario(id_scenario: int, request: Request):
    """Soft delete : transition de statut vers ARCHIVE.

    Autorisée depuis tous les statuts sauf ARCHIVE (qui est terminal).
    """
    caller = get_caller(request)
    async with db_write.transaction() as tx:
        scenario = await fetch_scenario(tx, id_scenario)
        assert_transition_allowed(scenario["statut"], "ARCHIVE")
        await apply_transition_side_effects(tx, scenario, "ARCHIVE")
        version = await increment_version(tx, id_scenario)
        await log_api(
            tx,
            api_name="archive_scenario",
            id_scenario=id_scenario,
            caller=caller,
            params={"from": scenario["statut"], "to": "ARCHIVE"},
            co_regate=scenario["co_regate"],
        )

    logger.info("Scénario %s archivé par %s", id_scenario, caller)
    return {
        "id_scenario": id_scenario,
        "version_scenario": version,
        "statut": "ARCHIVE",
    }


# ------------------------------------------------------------------ #
# CHANGE STATUS
# ------------------------------------------------------------------ #
@router.patch("/scenarios/{id_scenario}/statut")
async def change_statut(id_scenario: int, payload: StatutUpdate, request: Request):
    """Change le statut. Déclenche les effets de bord (verrouillage, snapshot PIC)."""
    caller = get_caller(request)
    async with db_write.transaction() as tx:
        scenario = await fetch_scenario(tx, id_scenario)
        assert_transition_allowed(scenario["statut"], payload.statut)
        info = await apply_transition_side_effects(tx, scenario, payload.statut)
        version = await increment_version(tx, id_scenario)
        await log_api(
            tx,
            api_name="change_statut",
            id_scenario=id_scenario,
            caller=caller,
            params={
                "from": scenario["statut"],
                "to": payload.statut,
                **info,
            },
            co_regate=scenario["co_regate"],
        )

    logger.info(
        "Scénario %s : %s → %s par %s",
        id_scenario, scenario["statut"], payload.statut, caller,
    )
    response = {
        "id_scenario": id_scenario,
        "version_scenario": version,
        "statut": payload.statut,
    }
    response.update(info)
    return response
