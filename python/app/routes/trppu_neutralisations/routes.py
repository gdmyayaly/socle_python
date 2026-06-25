"""Neutralisations d'un scénario : écriture (DSR-645) + lecture à plat (DSR-652)."""

import logging
import time
from datetime import date

from fastapi import APIRouter, HTTPException, Query, Response, status

from app.db.mysql import db_read, db_write
from app.log_utils import safe_preview
from app.security.crypto import encrypt_id_rh
from app.routes.trppu_scenario.helpers import assert_editable, fetch_scenario_or_404
from app.services.jours_service import compute_nb_jour_neutralise_db

from .helpers import SELECT_NEUTRALISATIONS_SQL
from .schemas import NeutralisationCreate, NeutralisationItem, NeutralisationOut

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/scenarios", tags=["Neutralisations"])


@router.get("/{id_scenario}/neutralisations", response_model=list[NeutralisationItem])
async def list_neutralisations(
    id_scenario: int,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-652 : périodes neutralisées d'un scénario (liste à plat, 1 ligne par période)."""
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

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Lecture neutralisations terminée (id_scenario=%d, count=%d, duration_ms=%.1f)",
        id_scenario,
        len(rows),
        duration_ms,
    )
    return rows


@router.post(
    "/{id_scenario}/neutralisations",
    response_model=NeutralisationOut,
    status_code=status.HTTP_201_CREATED,
)
async def add_neutralisation(id_scenario: int, payload: NeutralisationCreate):
    """DSR-645 : ajoute une neutralisation (jour ou période), nb_jour calculé serveur.

    - jour unique (dt_debut == dt_fin) -> nb_jour = 1
    - période -> jours ouvrés/ouvrables hors fériés et week-ends (selon nb_jours_semaine)
    409 si la même période (id_scenario, dt_debut, dt_fin) est déjà neutralisée.
    """
    start = time.perf_counter()
    logger.info(
        "Début ajout neutralisation (id_scenario=%d, motif=%s, dt_debut=%s, dt_fin=%s)",
        id_scenario,
        safe_preview(payload.motif),
        payload.dt_debut,
        payload.dt_fin,
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    if payload.dt_debut == payload.dt_fin:
        nb_jour = 1
    else:
        nb_jours_semaine = int(scenario["nb_jours_semaine"] or 6)
        nb_jour = await compute_nb_jour_neutralise_db(
            payload.dt_debut, payload.dt_fin, nb_jours_semaine
        )
    if nb_jour < 1:
        # chk_neutre_jour impose nb_jour > 0 : une période sans jour ouvré n'a pas de sens.
        raise HTTPException(
            status_code=422,
            detail="La période ne déduit aucun jour ouvré (nb_jour=0).",
        )

    id_rh_token = encrypt_id_rh(payload.id_rh)

    try:
        async with db_write.transaction() as tx:
            exists = await tx.fetch_one(
                "SELECT id_neutralisation FROM trppu_neutralisations "
                "WHERE id_scenario = %s AND dt_debut = %s AND dt_fin = %s",
                (id_scenario, payload.dt_debut, payload.dt_fin),
            )
            if exists:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"La période {payload.dt_debut} → {payload.dt_fin} est déjà "
                        f"neutralisée pour ce scénario."
                    ),
                )
            await tx.execute(
                "INSERT INTO trppu_neutralisations "
                "(id_scenario, dt_debut, dt_fin, nb_jour, motif, id_rh) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (id_scenario, payload.dt_debut, payload.dt_fin, nb_jour, payload.motif, id_rh_token),
            )
            row = await tx.fetch_one("SELECT LAST_INSERT_ID() AS id")
            new_id = int(row["id"])
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erreur ajout neutralisation (id_scenario=%d)", id_scenario)
        raise HTTPException(status_code=500, detail="Erreur ajout neutralisation.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Ajout neutralisation terminé (id_scenario=%d, motif=%s, nb_jour=%d, duration_ms=%.1f)",
        id_scenario,
        safe_preview(payload.motif),
        nb_jour,
        duration_ms,
    )
    return NeutralisationOut(
        id=new_id,
        dt_debut=payload.dt_debut,
        dt_fin=payload.dt_fin,
        nb_jour=nb_jour,
        motif=payload.motif,
        action="created",
    )


@router.delete("/{id_scenario}/neutralisations", status_code=status.HTTP_204_NO_CONTENT)
async def delete_neutralisation(
    id_scenario: int,
    dt_debut: date = Query(..., description="Date de début de la période à supprimer"),
    dt_fin: date = Query(..., description="Date de fin de la période à supprimer"),
):
    """DSR-645 : supprime la neutralisation (id_scenario, dt_debut, dt_fin).

    Un jour unique se supprime avec dt_debut == dt_fin (= la date du jour exclu).
    """
    start = time.perf_counter()
    logger.info(
        "Début suppression neutralisation (id_scenario=%d, dt_debut=%s, dt_fin=%s)",
        id_scenario,
        dt_debut,
        dt_fin,
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    try:
        async with db_write.transaction() as tx:
            rc = await tx.execute(
                "DELETE FROM trppu_neutralisations "
                "WHERE id_scenario = %s AND dt_debut = %s AND dt_fin = %s",
                (id_scenario, dt_debut, dt_fin),
            )
    except Exception as e:
        logger.exception("Erreur suppression neutralisation (id_scenario=%d)", id_scenario)
        raise HTTPException(status_code=500, detail="Erreur suppression neutralisation.") from e

    if not rc:
        raise HTTPException(
            status_code=404,
            detail=f"Aucune neutralisation ({dt_debut} → {dt_fin}) à supprimer pour ce scénario.",
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Suppression neutralisation terminée (id_scenario=%d, dt_debut=%s, dt_fin=%s, duration_ms=%.1f)",
        id_scenario,
        dt_debut,
        dt_fin,
        duration_ms,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete(
    "/{id_scenario}/neutralisations/{id_neutralisation}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_neutralisation_by_id(id_scenario: int, id_neutralisation: int):
    """Supprime une neutralisation via son identifiant (id_neutralisation).

    Variante de la suppression par période : cible une ligne précise par son id,
    bornée au scénario (contrôle d'appartenance).
    """
    start = time.perf_counter()
    logger.info(
        "Début suppression neutralisation par id (id_scenario=%d, id_neutralisation=%d)",
        id_scenario,
        id_neutralisation,
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    try:
        async with db_write.transaction() as tx:
            rc = await tx.execute(
                "DELETE FROM trppu_neutralisations "
                "WHERE id_neutralisation = %s AND id_scenario = %s",
                (id_neutralisation, id_scenario),
            )
    except Exception as e:
        logger.exception(
            "Erreur suppression neutralisation par id (id_scenario=%d, id_neutralisation=%d)",
            id_scenario,
            id_neutralisation,
        )
        raise HTTPException(
            status_code=500, detail="Erreur suppression neutralisation."
        ) from e

    if not rc:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Aucune neutralisation (id {id_neutralisation}) à supprimer "
                f"pour le scénario {id_scenario}."
            ),
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Suppression neutralisation par id terminée (id_scenario=%d, id_neutralisation=%d, duration_ms=%.1f)",
        id_scenario,
        id_neutralisation,
        duration_ms,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
