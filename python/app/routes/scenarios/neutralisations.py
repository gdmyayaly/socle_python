"""CRUD des neutralisations (FERIE / PEAK / LOCAL) attachées à un scénario."""

import logging

from fastapi import APIRouter, Request, status

from app.db.mysql import db_read, db_write
from app.routes.scenarios.helpers import (
    assert_not_fige,
    err,
    fetch_scenario,
    get_caller,
    increment_version,
    last_insert_id,
    log_api,
    validate_window_inside_scenario,
)
from app.routes.scenarios.schemas import (
    NeutralisationCreate,
    NeutralisationUpdate,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Neutralisations"])


def _nb_jours(dt_debut, dt_fin) -> int:
    return (dt_fin - dt_debut).days + 1


async def _fetch_neutralisation(tx, id_neutralisation: int) -> dict:
    row = await tx.fetch_one(
        "SELECT * FROM trppu_neutralisations WHERE id = %s",
        (id_neutralisation,),
    )
    if not row:
        raise err(404, f"Neutralisation {id_neutralisation} introuvable.")
    return row


# ------------------------------------------------------------------ #
# LIST
# ------------------------------------------------------------------ #
@router.get("/scenarios/{id_scenario}/neutralisations")
async def list_neutralisations(id_scenario: int):
    rows = await db_read.fetch_all(
        "SELECT * FROM trppu_neutralisations "
        "WHERE id_scenario = %s ORDER BY dt_debut",
        (id_scenario,),
    )
    return {"count": len(rows), "data": rows}


# ------------------------------------------------------------------ #
# CREATE
# ------------------------------------------------------------------ #
@router.post(
    "/scenarios/{id_scenario}/neutralisations",
    status_code=status.HTTP_201_CREATED,
)
async def create_neutralisation(
    id_scenario: int,
    payload: NeutralisationCreate,
    request: Request,
):
    caller = get_caller(request)
    async with db_write.transaction() as tx:
        scenario = await fetch_scenario(tx, id_scenario)
        assert_not_fige(scenario)
        validate_window_inside_scenario(
            payload.dt_debut, payload.dt_fin, scenario, "Neutralisation"
        )

        nb = _nb_jours(payload.dt_debut, payload.dt_fin)
        try:
            await tx.execute(
                "INSERT INTO trppu_neutralisations "
                "(id_scenario, dt_debut, dt_fin, nb_jour, `type`) "
                "VALUES (%s, %s, %s, %s, %s)",
                (
                    id_scenario,
                    payload.dt_debut,
                    payload.dt_fin,
                    nb,
                    payload.type,
                ),
            )
        except Exception as e:
            # uq_neutre (id_scenario, dt_debut, type)
            if "uq_neutre" in str(e) or "Duplicate" in str(e):
                raise err(
                    409,
                    "Neutralisation déjà existante pour ce scénario "
                    f"({payload.dt_debut.isoformat()}, type={payload.type}).",
                )
            raise

        new_id = await last_insert_id(tx)
        version = await increment_version(tx, id_scenario)
        await log_api(
            tx,
            api_name="create_neutralisation",
            id_scenario=id_scenario,
            caller=caller,
            params={**payload.model_dump(mode="json"), "nb_jour": nb},
            co_regate=scenario["co_regate"],
        )

    logger.info("Neutralisation %s créée (scénario %s) par %s", new_id, id_scenario, caller)
    return {
        "id": new_id,
        "id_scenario": id_scenario,
        "version_scenario": version,
        "nb_jour": nb,
    }


# ------------------------------------------------------------------ #
# UPDATE
# ------------------------------------------------------------------ #
@router.patch("/neutralisations/{id_neutralisation}")
async def update_neutralisation(
    id_neutralisation: int,
    payload: NeutralisationUpdate,
    request: Request,
):
    caller = get_caller(request)
    changes = payload.model_dump(exclude_unset=True)

    async with db_write.transaction() as tx:
        neutre = await _fetch_neutralisation(tx, id_neutralisation)
        scenario = await fetch_scenario(tx, neutre["id_scenario"])
        assert_not_fige(scenario)

        new_debut = changes.get("dt_debut", neutre["dt_debut"])
        new_fin = changes.get("dt_fin", neutre["dt_fin"])
        validate_window_inside_scenario(
            new_debut, new_fin, scenario, "Neutralisation"
        )
        nb = _nb_jours(new_debut, new_fin)
        # nb_jour est dérivé : on l'écrase systématiquement
        changes["nb_jour"] = nb

        # `type` est un mot réservé MySQL → backticks
        set_parts = []
        params: list = []
        for k, v in changes.items():
            col = f"`{k}`" if k == "type" else k
            set_parts.append(f"{col} = %s")
            params.append(v)
        params.append(id_neutralisation)

        await tx.execute(
            f"UPDATE trppu_neutralisations SET {', '.join(set_parts)} WHERE id = %s",
            tuple(params),
        )
        version = await increment_version(tx, scenario["id_scenario"])
        await log_api(
            tx,
            api_name="update_neutralisation",
            id_scenario=scenario["id_scenario"],
            caller=caller,
            params={
                "id_neutralisation": id_neutralisation,
                **payload.model_dump(mode="json", exclude_unset=True),
                "nb_jour": nb,
            },
            co_regate=scenario["co_regate"],
        )

    return {
        "id": id_neutralisation,
        "id_scenario": scenario["id_scenario"],
        "version_scenario": version,
        "nb_jour": nb,
    }


# ------------------------------------------------------------------ #
# DELETE
# ------------------------------------------------------------------ #
@router.delete("/neutralisations/{id_neutralisation}")
async def delete_neutralisation(id_neutralisation: int, request: Request):
    caller = get_caller(request)
    async with db_write.transaction() as tx:
        neutre = await _fetch_neutralisation(tx, id_neutralisation)
        scenario = await fetch_scenario(tx, neutre["id_scenario"])
        assert_not_fige(scenario)

        await tx.execute(
            "DELETE FROM trppu_neutralisations WHERE id = %s",
            (id_neutralisation,),
        )
        version = await increment_version(tx, scenario["id_scenario"])
        await log_api(
            tx,
            api_name="delete_neutralisation",
            id_scenario=scenario["id_scenario"],
            caller=caller,
            params={"id_neutralisation": id_neutralisation},
            co_regate=scenario["co_regate"],
        )

    return {
        "id": id_neutralisation,
        "id_scenario": scenario["id_scenario"],
        "version_scenario": version,
        "deleted": True,
    }
