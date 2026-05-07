"""Routes CRUD et workflow de statut pour la table trppu_scenario."""

import logging
import time

from fastapi import APIRouter, HTTPException, Query, status

from app.db.mysql import db_read, db_write
from app.log_utils import safe_preview

from .helpers import (
    SELECT_SCENARIO_SQL,
    assert_not_fige,
    default_periode,
    fetch_scenario_or_404,
    increment_version,
    resolve_default_pic_version,
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
    start = time.perf_counter()
    filters = {
        "co_regate": co_regate,
        "co_roc": co_roc,
        "statut": statut,
        "est_fige": est_fige,
        "limit": limit,
        "offset": offset,
    }
    logger.info("→ list_scenarios (filters=%s)", safe_preview(filters))

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
        rows = await db_read.fetch_all(sql, tuple(params))
    except Exception as e:
        logger.exception("Erreur listing scenarios (filters=%s)", safe_preview(filters))
        raise HTTPException(status_code=500, detail="Erreur listing scenarios.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "list_scenarios OK (count=%d, duration_ms=%.1f)", len(rows), duration_ms
    )
    return rows


@router.get("/{id_scenario}", response_model=ScenarioOut)
async def get_scenario(id_scenario: int):
    start = time.perf_counter()
    logger.info("→ get_scenario (id_scenario=%d)", id_scenario)
    row = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "get_scenario OK (id_scenario=%d, duration_ms=%.1f)", id_scenario, duration_ms
    )
    return row


@router.post("", response_model=ScenarioOut, status_code=status.HTTP_201_CREATED)
async def create_scenario(payload: ScenarioCreate):
    start = time.perf_counter()
    logger.info(
        "→ create_scenario (co_regate=%s, payload=%s)",
        payload.co_regate,
        safe_preview(payload.model_dump(mode="json")),
    )
    # Pas de pre-check FK : MySQL refusera l'INSERT si co_regate / id_pic_version /
    # id_scenario_parent sont introuvables. La stack trace + le payload seront loggés.

    if payload.id_pic_version is not None:
        pic_version = payload.id_pic_version
    else:
        pic_version = await resolve_default_pic_version()
        logger.info(
            "... create_scenario default pic_version resolved (id_pic_version=%d)",
            pic_version,
        )

    debut, fin = payload.periode_debut, payload.periode_fin
    if debut is None or fin is None:
        d, f = default_periode()
        debut = debut or d
        fin = fin or f
        logger.info(
            "... create_scenario default periode applied (debut=%s, fin=%s)", debut, fin
        )

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
        logger.exception(
            "Erreur création scenario (payload=%s)",
            safe_preview(payload.model_dump(mode="json")),
        )
        raise HTTPException(status_code=500, detail="Erreur création scenario.") from e

    created = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "create_scenario OK (id_scenario=%d, co_regate=%s, duration_ms=%.1f)",
        id_scenario,
        payload.co_regate,
        duration_ms,
    )
    return created


@router.delete("/{id_scenario}", response_model=ScenarioOut)
async def delete_scenario(id_scenario: int):
    """Soft-delete : transition de statut vers ARCHIVE."""
    start = time.perf_counter()
    logger.info("→ delete_scenario (id_scenario=%d)", id_scenario)

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_transition_allowed(scenario["statut"], "ARCHIVE")
    logger.info(
        "... delete_scenario transition allowed (from=%s → ARCHIVE)", scenario["statut"]
    )

    try:
        async with db_write.transaction() as tx:
            await apply_transition_side_effects(tx, scenario, "ARCHIVE")
            await increment_version(tx, id_scenario)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur archivage scenario (id_scenario=%d, statut_courant=%s)",
            id_scenario,
            scenario.get("statut"),
        )
        raise HTTPException(status_code=500, detail="Erreur archivage scenario.") from e

    archived = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "delete_scenario OK (id_scenario=%d, statut=ARCHIVE, duration_ms=%.1f)",
        id_scenario,
        duration_ms,
    )
    return archived


@router.patch("/{id_scenario}/periodes", response_model=ScenarioOut)
async def update_periodes(id_scenario: int, payload: PeriodeUpdate):
    start = time.perf_counter()
    fields = payload.model_dump(exclude_unset=True)
    logger.info(
        "→ update_periodes (id_scenario=%d, fields=%s)",
        id_scenario,
        safe_preview(fields),
    )

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_not_fige(scenario)

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
        logger.exception(
            "Erreur update periodes (id_scenario=%d, fields=%s)",
            id_scenario,
            safe_preview(fields),
        )
        raise HTTPException(status_code=500, detail="Erreur mise à jour périodes.") from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "update_periodes OK (id_scenario=%d, fields_updated=%d, duration_ms=%.1f)",
        id_scenario,
        len(fields),
        duration_ms,
    )
    return updated


@router.patch("/{id_scenario}/nb-jours-semaine", response_model=ScenarioOut)
async def update_nb_jours_semaine(id_scenario: int, payload: NbJoursUpdate):
    start = time.perf_counter()
    logger.info(
        "→ update_nb_jours_semaine (id_scenario=%d, nb_jours_semaine=%s)",
        id_scenario,
        payload.nb_jours_semaine,
    )

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
        logger.exception(
            "Erreur update nb_jours_semaine (id_scenario=%d, nb_jours_semaine=%s)",
            id_scenario,
            payload.nb_jours_semaine,
        )
        raise HTTPException(
            status_code=500, detail="Erreur mise à jour nb_jours_semaine."
        ) from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "update_nb_jours_semaine OK (id_scenario=%d, duration_ms=%.1f)",
        id_scenario,
        duration_ms,
    )
    return updated


@router.patch("/{id_scenario}/statut", response_model=ScenarioOut)
async def update_statut(id_scenario: int, payload: StatutUpdate):
    """Change le statut via la machine à états + effets de bord automatiques."""
    start = time.perf_counter()
    logger.info(
        "→ update_statut (id_scenario=%d, target_statut=%s)",
        id_scenario,
        payload.statut,
    )

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_transition_allowed(scenario["statut"], payload.statut)
    logger.info(
        "... update_statut transition allowed (from=%s → %s)",
        scenario["statut"],
        payload.statut,
    )

    try:
        async with db_write.transaction() as tx:
            await apply_transition_side_effects(tx, scenario, payload.statut)
            await increment_version(tx, id_scenario)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur transition statut (id_scenario=%d, from=%s, to=%s)",
            id_scenario,
            scenario.get("statut"),
            payload.statut,
        )
        raise HTTPException(status_code=500, detail="Erreur transition de statut.") from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "update_statut OK (id_scenario=%d, statut=%s, duration_ms=%.1f)",
        id_scenario,
        payload.statut,
        duration_ms,
    )
    return updated


@router.patch("/{id_scenario}/est-fige", response_model=ScenarioOut)
async def update_est_fige(id_scenario: int, payload: FigeUpdate):
    """Force le flag est_fige (seul moyen de défiger un scénario après PRODUCTION)."""
    start = time.perf_counter()
    logger.info(
        "→ update_est_fige (id_scenario=%d, est_fige=%s)",
        id_scenario,
        payload.est_fige,
    )

    await fetch_scenario_or_404(id_scenario)

    try:
        async with db_write.transaction() as tx:
            await tx.execute(
                "UPDATE trppu_scenario SET est_fige = %s WHERE id_scenario = %s",
                (1 if payload.est_fige else 0, id_scenario),
            )
            await increment_version(tx, id_scenario)
    except Exception as e:
        logger.exception(
            "Erreur update est_fige (id_scenario=%d, est_fige=%s)",
            id_scenario,
            payload.est_fige,
        )
        raise HTTPException(status_code=500, detail="Erreur mise à jour est_fige.") from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "update_est_fige OK (id_scenario=%d, est_fige=%s, duration_ms=%.1f)",
        id_scenario,
        payload.est_fige,
        duration_ms,
    )
    return updated


@router.patch("/{id_scenario}/lb-scenario", response_model=ScenarioOut)
async def update_lb_scenario(id_scenario: int, payload: LbScenarioUpdate):
    start = time.perf_counter()
    logger.info(
        "→ update_lb_scenario (id_scenario=%d, lb_scenario=%s)",
        id_scenario,
        safe_preview(payload.lb_scenario),
    )

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
        logger.exception(
            "Erreur update lb_scenario (id_scenario=%d, lb_scenario=%s)",
            id_scenario,
            safe_preview(payload.lb_scenario),
        )
        raise HTTPException(status_code=500, detail="Erreur mise à jour libellé.") from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "update_lb_scenario OK (id_scenario=%d, duration_ms=%.1f)",
        id_scenario,
        duration_ms,
    )
    return updated


@router.post(
    "/{id_scenario}/duplicate",
    response_model=ScenarioOut,
    status_code=status.HTTP_201_CREATED,
)
async def duplicate_scenario(id_scenario: int, payload: DuplicateRequest | None = None):
    """Duplique un scénario en nouveau BROUILLON, version 1, est_fige=0.

    id_scenario_parent du clone = id_scenario source.
    """
    start = time.perf_counter()
    logger.info("→ duplicate_scenario (source_id=%d)", id_scenario)

    source = await fetch_scenario_or_404(id_scenario)

    new_lb = (
        payload.lb_scenario
        if payload and payload.lb_scenario
        else f"{source['lb_scenario']} (copie)"
    )
    if len(new_lb) > 50:
        new_lb = new_lb[:50]
    logger.info("... duplicate_scenario new_lb resolved (lb_scenario=%s)", safe_preview(new_lb))

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
        logger.exception(
            "Erreur duplicate scenario (source_id=%d, new_lb=%s)",
            id_scenario,
            safe_preview(new_lb),
        )
        raise HTTPException(status_code=500, detail="Erreur duplication scenario.") from e

    duplicated = await fetch_scenario_or_404(new_id)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "duplicate_scenario OK (source_id=%d, new_id=%d, duration_ms=%.1f)",
        id_scenario,
        new_id,
        duration_ms,
    )
    return duplicated


@router.get("/{id_scenario}/history", response_model=list[ScenarioOut])
async def get_history(id_scenario: int):
    """Liste tous les scénarios de la même lignée (ancêtres + descendants)."""
    start = time.perf_counter()
    logger.info("→ get_history (id_scenario=%d)", id_scenario)

    await fetch_scenario_or_404(id_scenario)

    # 1. Remonter jusqu'à la racine.
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
    logger.info(
        "... get_history root resolved (id_scenario=%d, root_id=%d)", id_scenario, root_id
    )

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
        rows = await db_read.fetch_all(sql, (root_id,))
    except Exception as e:
        logger.exception(
            "Erreur lecture history (id_scenario=%d, root_id=%d)", id_scenario, root_id
        )
        raise HTTPException(status_code=500, detail="Erreur lecture history.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "get_history OK (id_scenario=%d, count=%d, duration_ms=%.1f)",
        id_scenario,
        len(rows),
        duration_ms,
    )
    return rows
