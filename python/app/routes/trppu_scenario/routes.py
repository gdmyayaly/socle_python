"""Routes CRUD et workflow de statut pour la table trppu_scenario."""

import logging

from fastapi import APIRouter, HTTPException, Query, status

from app.db.mysql import db_read, db_write

from .helpers import (
    SELECT_SCENARIO_SQL,
    assert_not_fige,
    default_periode,
    fetch_scenario_or_404,
    increment_version,
    pic_version_exists,
    resolve_default_pic_version,
    site_exists,
)
from .schemas import (
    DuplicateRequest,
    FigeUpdate,
    LbScenarioUpdate,
    NbJoursUpdate,
    PeriodeUpdate,
    ScenarioCreate,
    ScenarioOut,
    StatutUpdate,
)
from .statuts import apply_transition_side_effects, assert_transition_allowed

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/scenarios", tags=["Scenarios"])


@router.get("", response_model=list[ScenarioOut])
async def list_scenarios(
    co_regate: str | None = Query(None, min_length=6, max_length=6),
    co_roc: str | None = Query(None, min_length=6, max_length=6),
    statut: str | None = Query(None),
    est_fige: bool | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    where: list[str] = []
    params: list = []
    if co_regate is not None:
        where.append("co_regate = %s")
        params.append(co_regate)
    if co_roc is not None:
        where.append("co_roc = %s")
        params.append(co_roc)
    if statut is not None:
        where.append("statut = %s")
        params.append(statut)
    if est_fige is not None:
        where.append("est_fige = %s")
        params.append(1 if est_fige else 0)

    sql = SELECT_SCENARIO_SQL
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id_scenario DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    try:
        return await db_read.fetch_all(sql, tuple(params))
    except Exception as e:
        logger.error("Erreur listing scenarios : %s", e)
        raise HTTPException(status_code=500, detail="Erreur listing scenarios.") from e


@router.get("/{id_scenario}", response_model=ScenarioOut)
async def get_scenario(id_scenario: int):
    return await fetch_scenario_or_404(id_scenario)


@router.post("", response_model=ScenarioOut, status_code=status.HTTP_201_CREATED)
async def create_scenario(payload: ScenarioCreate):
    if not await site_exists(payload.co_regate):
        raise HTTPException(
            status_code=422,
            detail=f"Site parent {payload.co_regate} inexistant dans trppu_site.",
        )

    if payload.id_pic_version is not None:
        if not await pic_version_exists(payload.id_pic_version):
            raise HTTPException(
                status_code=422,
                detail=f"id_pic_version {payload.id_pic_version} inexistant.",
            )
        pic_version = payload.id_pic_version
    else:
        pic_version = await resolve_default_pic_version()

    if payload.id_scenario_parent is not None:
        parent = await db_read.fetch_one(
            "SELECT id_scenario FROM trppu_scenario WHERE id_scenario = %s",
            (payload.id_scenario_parent,),
        )
        if not parent:
            raise HTTPException(
                status_code=422,
                detail=f"id_scenario_parent {payload.id_scenario_parent} inexistant.",
            )

    debut, fin = payload.periode_debut, payload.periode_fin
    if debut is None or fin is None:
        d, f = default_periode()
        debut = debut or d
        fin = fin or f

    try:
        async with db_write.transaction() as tx:
            await tx.execute(
                "INSERT INTO trppu_scenario "
                "(co_regate, lb_scenario, co_roc, statut, "
                " periode_debut, periode_fin, "
                " periode_realise_debut, periode_realise_fin, "
                " periode_prev_debut, periode_prev_fin, "
                " nb_jours_semaine, id_pic_version, version_scenario, "
                " id_scenario_parent, est_fige) "
                "VALUES (%s, %s, %s, 'BROUILLON', %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, 0)",
                (
                    payload.co_regate,
                    payload.lb_scenario,
                    payload.co_roc,
                    debut,
                    fin,
                    payload.periode_realise_debut,
                    payload.periode_realise_fin,
                    payload.periode_prev_debut,
                    payload.periode_prev_fin,
                    payload.nb_jours_semaine,
                    pic_version,
                    payload.id_scenario_parent,
                ),
            )
            row = await tx.fetch_one("SELECT LAST_INSERT_ID() AS id")
            id_scenario = int(row["id"])
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Erreur création scenario : %s", e)
        raise HTTPException(status_code=500, detail="Erreur création scenario.") from e

    return await fetch_scenario_or_404(id_scenario)


@router.delete("/{id_scenario}", response_model=ScenarioOut)
async def delete_scenario(id_scenario: int):
    """Soft-delete : transition de statut vers ARCHIVE."""
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_transition_allowed(scenario["statut"], "ARCHIVE")

    try:
        async with db_write.transaction() as tx:
            await apply_transition_side_effects(tx, scenario, "ARCHIVE")
            await increment_version(tx, id_scenario)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Erreur archivage scenario %d : %s", id_scenario, e)
        raise HTTPException(status_code=500, detail="Erreur archivage scenario.") from e

    return await fetch_scenario_or_404(id_scenario)


@router.patch("/{id_scenario}/periodes", response_model=ScenarioOut)
async def update_periodes(id_scenario: int, payload: PeriodeUpdate):
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_not_fige(scenario)

    fields = payload.model_dump(exclude_unset=True)

    new_debut = fields.get("periode_debut", scenario["periode_debut"])
    new_fin = fields.get("periode_fin", scenario["periode_fin"])
    if new_fin < new_debut:
        raise HTTPException(
            status_code=400, detail="periode_fin doit être >= periode_debut."
        )

    new_real_debut = fields.get("periode_realise_debut", scenario["periode_realise_debut"])
    new_real_fin = fields.get("periode_realise_fin", scenario["periode_realise_fin"])
    if new_real_debut and new_real_fin and new_real_fin < new_real_debut:
        raise HTTPException(
            status_code=400,
            detail="periode_realise_fin doit être >= periode_realise_debut.",
        )

    new_prev_debut = fields.get("periode_prev_debut", scenario["periode_prev_debut"])
    new_prev_fin = fields.get("periode_prev_fin", scenario["periode_prev_fin"])
    if new_prev_debut and new_prev_fin and new_prev_fin < new_prev_debut:
        raise HTTPException(
            status_code=400,
            detail="periode_prev_fin doit être >= periode_prev_debut.",
        )

    set_parts = [f"{k} = %s" for k in fields]
    params = list(fields.values()) + [id_scenario]

    try:
        async with db_write.transaction() as tx:
            await tx.execute(
                f"UPDATE trppu_scenario SET {', '.join(set_parts)} "
                "WHERE id_scenario = %s",
                tuple(params),
            )
            await increment_version(tx, id_scenario)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Erreur update periodes %d : %s", id_scenario, e)
        raise HTTPException(status_code=500, detail="Erreur mise à jour périodes.") from e

    return await fetch_scenario_or_404(id_scenario)


@router.patch("/{id_scenario}/nb-jours-semaine", response_model=ScenarioOut)
async def update_nb_jours_semaine(id_scenario: int, payload: NbJoursUpdate):
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_not_fige(scenario)

    try:
        async with db_write.transaction() as tx:
            await tx.execute(
                "UPDATE trppu_scenario SET nb_jours_semaine = %s WHERE id_scenario = %s",
                (payload.nb_jours_semaine, id_scenario),
            )
            await increment_version(tx, id_scenario)
    except Exception as e:
        logger.error("Erreur update nb_jours_semaine %d : %s", id_scenario, e)
        raise HTTPException(
            status_code=500, detail="Erreur mise à jour nb_jours_semaine."
        ) from e

    return await fetch_scenario_or_404(id_scenario)


@router.patch("/{id_scenario}/statut", response_model=ScenarioOut)
async def update_statut(id_scenario: int, payload: StatutUpdate):
    """Change le statut via la machine à états + effets de bord automatiques."""
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_transition_allowed(scenario["statut"], payload.statut)

    try:
        async with db_write.transaction() as tx:
            await apply_transition_side_effects(tx, scenario, payload.statut)
            await increment_version(tx, id_scenario)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Erreur transition statut %d : %s", id_scenario, e)
        raise HTTPException(status_code=500, detail="Erreur transition de statut.") from e

    return await fetch_scenario_or_404(id_scenario)


@router.patch("/{id_scenario}/est-fige", response_model=ScenarioOut)
async def update_est_fige(id_scenario: int, payload: FigeUpdate):
    """Force le flag est_fige (seul moyen de défiger un scénario après PRODUCTION)."""
    await fetch_scenario_or_404(id_scenario)

    try:
        async with db_write.transaction() as tx:
            await tx.execute(
                "UPDATE trppu_scenario SET est_fige = %s WHERE id_scenario = %s",
                (1 if payload.est_fige else 0, id_scenario),
            )
            await increment_version(tx, id_scenario)
    except Exception as e:
        logger.error("Erreur update est_fige %d : %s", id_scenario, e)
        raise HTTPException(status_code=500, detail="Erreur mise à jour est_fige.") from e

    return await fetch_scenario_or_404(id_scenario)


@router.patch("/{id_scenario}/lb-scenario", response_model=ScenarioOut)
async def update_lb_scenario(id_scenario: int, payload: LbScenarioUpdate):
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_not_fige(scenario)

    try:
        async with db_write.transaction() as tx:
            await tx.execute(
                "UPDATE trppu_scenario SET lb_scenario = %s WHERE id_scenario = %s",
                (payload.lb_scenario, id_scenario),
            )
            await increment_version(tx, id_scenario)
    except Exception as e:
        logger.error("Erreur update lb_scenario %d : %s", id_scenario, e)
        raise HTTPException(status_code=500, detail="Erreur mise à jour libellé.") from e

    return await fetch_scenario_or_404(id_scenario)


@router.post(
    "/{id_scenario}/duplicate",
    response_model=ScenarioOut,
    status_code=status.HTTP_201_CREATED,
)
async def duplicate_scenario(id_scenario: int, payload: DuplicateRequest | None = None):
    """Duplique un scénario en nouveau BROUILLON, version 1, est_fige=0.

    id_scenario_parent du clone = id_scenario source.
    """
    source = await fetch_scenario_or_404(id_scenario)

    new_lb = (
        payload.lb_scenario
        if payload and payload.lb_scenario
        else f"{source['lb_scenario']} (copie)"
    )
    if len(new_lb) > 50:
        new_lb = new_lb[:50]

    try:
        async with db_write.transaction() as tx:
            await tx.execute(
                "INSERT INTO trppu_scenario "
                "(co_regate, lb_scenario, co_roc, statut, "
                " periode_debut, periode_fin, "
                " periode_realise_debut, periode_realise_fin, "
                " periode_prev_debut, periode_prev_fin, "
                " nb_jours_semaine, id_pic_version, version_scenario, "
                " id_scenario_parent, est_fige) "
                "VALUES (%s, %s, %s, 'BROUILLON', %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, 0)",
                (
                    source["co_regate"],
                    new_lb,
                    source["co_roc"],
                    source["periode_debut"],
                    source["periode_fin"],
                    source["periode_realise_debut"],
                    source["periode_realise_fin"],
                    source["periode_prev_debut"],
                    source["periode_prev_fin"],
                    source["nb_jours_semaine"],
                    source["id_pic_version"],
                    source["id_scenario"],
                ),
            )
            row = await tx.fetch_one("SELECT LAST_INSERT_ID() AS id")
            new_id = int(row["id"])
    except Exception as e:
        logger.error("Erreur duplicate scenario %d : %s", id_scenario, e)
        raise HTTPException(status_code=500, detail="Erreur duplication scenario.") from e

    return await fetch_scenario_or_404(new_id)


@router.get("/{id_scenario}/history", response_model=list[ScenarioOut])
async def get_history(id_scenario: int):
    """Liste tous les scénarios de la même lignée (ancêtres + descendants)."""
    await fetch_scenario_or_404(id_scenario)

    # 1. Remonter jusqu'à la racine (parent IS NULL ou parent introuvable).
    root_id = id_scenario
    visited: set[int] = {root_id}
    while True:
        row = await db_read.fetch_one(
            "SELECT id_scenario_parent FROM trppu_scenario WHERE id_scenario = %s",
            (root_id,),
        )
        if not row or row["id_scenario_parent"] is None:
            break
        parent_id = int(row["id_scenario_parent"])
        if parent_id in visited:
            break
        visited.add(parent_id)
        root_id = parent_id

    # 2. CTE récursive descendante depuis la racine.
    sql = (
        "WITH RECURSIVE tree AS ("
        " SELECT id_scenario, co_regate, lb_scenario, co_roc, statut, dt_creation,"
        "        dt_validation, dt_mise_en_prod, periode_debut, periode_fin,"
        "        periode_realise_debut, periode_realise_fin,"
        "        periode_prev_debut, periode_prev_fin,"
        "        nb_jours_semaine, id_pic_version, version_scenario,"
        "        id_scenario_parent, est_fige"
        " FROM trppu_scenario WHERE id_scenario = %s"
        " UNION ALL"
        " SELECT s.id_scenario, s.co_regate, s.lb_scenario, s.co_roc, s.statut,"
        "        s.dt_creation, s.dt_validation, s.dt_mise_en_prod,"
        "        s.periode_debut, s.periode_fin,"
        "        s.periode_realise_debut, s.periode_realise_fin,"
        "        s.periode_prev_debut, s.periode_prev_fin,"
        "        s.nb_jours_semaine, s.id_pic_version, s.version_scenario,"
        "        s.id_scenario_parent, s.est_fige"
        " FROM trppu_scenario s INNER JOIN tree t"
        "   ON s.id_scenario_parent = t.id_scenario"
        ") SELECT * FROM tree ORDER BY id_scenario"
    )
    try:
        return await db_read.fetch_all(sql, (root_id,))
    except Exception as e:
        logger.error("Erreur lecture history %d : %s", id_scenario, e)
        raise HTTPException(status_code=500, detail="Erreur lecture history.") from e
