"""Neutralisations d'un scénario : écriture (DSR-645) + lecture regroupée (DSR-652)."""

import logging
import time
from datetime import date

from fastapi import APIRouter, HTTPException, Query, Response, status

from app.db.mysql import db_read, db_write
from app.log_utils import safe_preview
from app.security.crypto import encrypt_id_rh
from app.routes.trppu_scenario.helpers import assert_not_fige, fetch_scenario_or_404
from app.services.jours_service import compute_nb_jour_neutralise_db

from .helpers import SELECT_NEUTRALISATIONS_SQL, group_neutralisations
from .schemas import NeutralisationCreate, NeutralisationOut, NeutralisationsOut

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/scenarios", tags=["Neutralisations"])


@router.get("/{id_scenario}/neutralisations", response_model=NeutralisationsOut)
async def list_neutralisations(
    id_scenario: int,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-652 : périodes neutralisées regroupées par type (feries / peak / saison)."""
    start = time.perf_counter()
    logger.info(
        "Début lecture neutralisations (id_scenario=%d, id_session_ihm=%s)",
        id_scenario,
        safe_preview(id_session_ihm),
    )
    await fetch_scenario_or_404(id_scenario)
    try:
        rows = await db_read.fetch_all(SELECT_NEUTRALISATIONS_SQL, (id_scenario,))
    except Exception as e:
        logger.exception("Erreur lecture neutralisations (id_scenario=%d)", id_scenario)
        raise HTTPException(status_code=500, detail="Erreur lecture neutralisations.") from e

    result = group_neutralisations(rows)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Lecture neutralisations terminée (id_scenario=%d, feries=%d, peak=%s, saison=%s, duration_ms=%.1f)",
        id_scenario,
        len(result.feries.jours),
        result.peak.actif,
        result.saison.actif,
        duration_ms,
    )
    return result


@router.post(
    "/{id_scenario}/neutralisations",
    response_model=NeutralisationOut,
    status_code=status.HTTP_201_CREATED,
)
async def add_neutralisation(id_scenario: int, payload: NeutralisationCreate):
    """DSR-645 : ajoute une neutralisation (nb_jour calculé serveur).

    - FERIE : 1 jour, nb_jour=1 ; 409 si ce jour est déjà exclu.
    - PEAK / SAISON : 1 seule ligne par scénario (remplacée si déjà présente).
    """
    start = time.perf_counter()
    logger.info(
        "Début ajout neutralisation (id_scenario=%d, type=%s, dt_debut=%s, dt_fin=%s)",
        id_scenario,
        payload.type,
        payload.dt_debut,
        payload.dt_fin,
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_not_fige(scenario)

    if payload.type == "FERIE":
        nb_jour = 1
    else:
        nb_jours_semaine = int(scenario["nb_jours_semaine"] or 6)
        nb_jour = await compute_nb_jour_neutralise_db(
            payload.dt_debut, payload.dt_fin, nb_jours_semaine
        )
    id_rh_token = encrypt_id_rh(payload.id_rh)

    try:
        async with db_write.transaction() as tx:
            if payload.type == "FERIE":
                exists = await tx.fetch_one(
                    "SELECT id FROM trppu_neutralisations "
                    "WHERE id_scenario = %s AND type = 'FERIE' AND dt_debut = %s",
                    (id_scenario, payload.dt_debut),
                )
                if exists:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Le jour férié {payload.dt_debut} est déjà exclu pour ce scénario.",
                    )
                await tx.execute(
                    "INSERT INTO trppu_neutralisations "
                    "(id_scenario, dt_debut, dt_fin, nb_jour, type, id_rh) "
                    "VALUES (%s, %s, %s, %s, 'FERIE', %s)",
                    (id_scenario, payload.dt_debut, payload.dt_fin, nb_jour, id_rh_token),
                )
                action = "created"
            else:
                existing = await tx.fetch_one(
                    "SELECT id FROM trppu_neutralisations "
                    "WHERE id_scenario = %s AND type = %s",
                    (id_scenario, payload.type),
                )
                if existing:
                    await tx.execute(
                        "UPDATE trppu_neutralisations "
                        "SET dt_debut = %s, dt_fin = %s, nb_jour = %s, dt_creation = NOW(), id_rh = %s "
                        "WHERE id = %s",
                        (payload.dt_debut, payload.dt_fin, nb_jour, id_rh_token, existing["id"]),
                    )
                    action = "updated"
                else:
                    await tx.execute(
                        "INSERT INTO trppu_neutralisations "
                        "(id_scenario, dt_debut, dt_fin, nb_jour, type, id_rh) "
                        "VALUES (%s, %s, %s, %s, %s, %s)",
                        (
                            id_scenario,
                            payload.dt_debut,
                            payload.dt_fin,
                            nb_jour,
                            payload.type,
                            id_rh_token,
                        ),
                    )
                    action = "created"
            row = await tx.fetch_one("SELECT LAST_INSERT_ID() AS id")
            new_id = int(row["id"]) if action == "created" else int(existing["id"])
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur ajout neutralisation (id_scenario=%d, type=%s)", id_scenario, payload.type
        )
        raise HTTPException(status_code=500, detail="Erreur ajout neutralisation.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Ajout neutralisation terminé (id_scenario=%d, type=%s, nb_jour=%d, action=%s, duration_ms=%.1f)",
        id_scenario,
        payload.type,
        nb_jour,
        action,
        duration_ms,
    )
    return NeutralisationOut(
        id=new_id,
        type=payload.type,
        dt_debut=payload.dt_debut,
        dt_fin=payload.dt_fin,
        nb_jour=nb_jour,
        action=action,
    )


@router.delete("/{id_scenario}/neutralisations", status_code=status.HTTP_204_NO_CONTENT)
async def delete_neutralisation(
    id_scenario: int,
    type: str = Query(..., description="FERIE | PEAK | SAISON"),
    dt: date | None = Query(None, description="Jour à supprimer (obligatoire pour FERIE)"),
):
    """DSR-645 : suppression. FERIE -> un jour précis (dt) ; PEAK/SAISON -> la ligne du type."""
    start = time.perf_counter()
    type_norm = type.strip().upper()
    if type_norm not in ("FERIE", "PEAK", "SAISON"):
        raise HTTPException(status_code=422, detail="type doit être FERIE, PEAK ou SAISON.")
    if type_norm == "FERIE" and dt is None:
        raise HTTPException(status_code=422, detail="Le paramètre 'dt' est requis pour FERIE.")

    logger.info(
        "Début suppression neutralisation (id_scenario=%d, type=%s, dt=%s)",
        id_scenario,
        type_norm,
        dt,
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_not_fige(scenario)

    try:
        async with db_write.transaction() as tx:
            if type_norm == "FERIE":
                rc = await tx.execute(
                    "DELETE FROM trppu_neutralisations "
                    "WHERE id_scenario = %s AND type = 'FERIE' AND dt_debut = %s",
                    (id_scenario, dt),
                )
            else:
                rc = await tx.execute(
                    "DELETE FROM trppu_neutralisations WHERE id_scenario = %s AND type = %s",
                    (id_scenario, type_norm),
                )
    except Exception as e:
        logger.exception(
            "Erreur suppression neutralisation (id_scenario=%d, type=%s)", id_scenario, type_norm
        )
        raise HTTPException(status_code=500, detail="Erreur suppression neutralisation.") from e

    if not rc:
        raise HTTPException(
            status_code=404,
            detail=f"Aucune neutralisation {type_norm} à supprimer pour ce scénario.",
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Suppression neutralisation terminée (id_scenario=%d, type=%s, duration_ms=%.1f)",
        id_scenario,
        type_norm,
        duration_ms,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
