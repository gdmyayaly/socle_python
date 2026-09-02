"""Neutralisations d'un scénario : écriture (DSR-645) + lecture à plat (DSR-652)."""

import logging
import time
from datetime import date

from fastapi import APIRouter, HTTPException, Query, Response, status

from app.db.mysql import db_read, db_write
from app.log_utils import ctx, params_loggables
from app.security.crypto import encrypt_id_rh
from app.services.api_log import ACTION_NEUTRALISATION, enregistrer_appel
from app.routes.trppu_scenario.helpers import (
    assert_editable,
    fetch_scenario_or_404,
    last_insert_id,
)
from app.services.jours_service import compute_nb_jour_neutralise_db

from .helpers import SELECT_NEUTRALISATION_SQL, SELECT_NEUTRALISATIONS_SQL
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
    logger.info("Début lecture neutralisations %s", ctx(id_scenario=id_scenario))
    await fetch_scenario_or_404(id_scenario)
    try:
        rows = await db_read.fetch_all(SELECT_NEUTRALISATIONS_SQL, (id_scenario,))
    except Exception as e:
        logger.exception(
            "Erreur lecture neutralisations %s", ctx(id_scenario=id_scenario)
        )
        raise HTTPException(status_code=500, detail="Erreur lecture neutralisations.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin lecture neutralisations %s",
        ctx(id_scenario=id_scenario, count=len(rows), duration_ms=duration_ms),
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
    logged = params_loggables(payload)
    logger.info(
        "Début ajout neutralisation %s",
        ctx(id_scenario=id_scenario, params=logged),
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
        logger.warning(
            "Rejet ajout neutralisation %s",
            ctx(
                id_scenario=id_scenario,
                dt_debut=payload.dt_debut,
                dt_fin=payload.dt_fin,
                http=422,
                motif="aucun jour ouvré sur la période",
            ),
        )
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
                logger.warning(
                    "Rejet ajout neutralisation %s",
                    ctx(
                        id_scenario=id_scenario,
                        id_neutralisation=exists["id_neutralisation"],
                        dt_debut=payload.dt_debut,
                        dt_fin=payload.dt_fin,
                        http=409,
                        motif="période déjà neutralisée",
                    ),
                )
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"La période {payload.dt_debut} → {payload.dt_fin} est déjà "
                        f"neutralisée pour ce scénario."
                    ),
                )
            rows_inseres = await tx.execute(
                "INSERT INTO trppu_neutralisations "
                "(id_scenario, dt_debut, dt_fin, nb_jour, motif, id_rh) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (id_scenario, payload.dt_debut, payload.dt_fin, nb_jour, payload.motif, id_rh_token),
            )
            new_id = await last_insert_id(tx)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur ajout neutralisation %s",
            ctx(id_scenario=id_scenario, params=logged),
        )
        raise HTTPException(status_code=500, detail="Erreur ajout neutralisation.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_NEUTRALISATION,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={
            "operation": "ajout",
            "id_neutralisation": new_id,
            "nb_jour": nb_jour,
            "params": logged,
        },
    )
    logger.info(
        "Fin ajout neutralisation %s",
        ctx(
            id_scenario=id_scenario,
            id_neutralisation=new_id,
            dt_debut=payload.dt_debut,
            dt_fin=payload.dt_fin,
            nb_jour=nb_jour,
            rows_affected=rows_inseres,
            duration_ms=duration_ms,
        ),
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
        "Début suppression neutralisation %s",
        ctx(id_scenario=id_scenario, dt_debut=dt_debut, dt_fin=dt_fin),
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    # Suppression définitive : l'état est journalisé avant écriture.
    avant = await db_read.fetch_one(
        SELECT_NEUTRALISATION_SQL
        + " WHERE id_scenario = %s AND dt_debut = %s AND dt_fin = %s",
        (id_scenario, dt_debut, dt_fin),
    )
    etat_avant = params_loggables(dict(avant)) if avant else None
    if etat_avant is not None:
        logger.info(
            "État avant suppression neutralisation %s",
            ctx(id_scenario=id_scenario, etat=etat_avant),
        )

    try:
        async with db_write.transaction() as tx:
            rc = await tx.execute(
                "DELETE FROM trppu_neutralisations "
                "WHERE id_scenario = %s AND dt_debut = %s AND dt_fin = %s",
                (id_scenario, dt_debut, dt_fin),
            )
    except Exception as e:
        logger.exception(
            "Erreur suppression neutralisation %s",
            ctx(id_scenario=id_scenario, etat=etat_avant),
        )
        raise HTTPException(status_code=500, detail="Erreur suppression neutralisation.") from e

    if not rc:
        logger.warning(
            "Rejet suppression neutralisation %s",
            ctx(
                id_scenario=id_scenario,
                dt_debut=dt_debut,
                dt_fin=dt_fin,
                http=404,
                motif="aucune neutralisation sur cette période",
            ),
        )
        raise HTTPException(
            status_code=404,
            detail=f"Aucune neutralisation ({dt_debut} → {dt_fin}) à supprimer pour ce scénario.",
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_NEUTRALISATION,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={"operation": "suppression_periode", "etat_avant": etat_avant},
    )
    logger.info(
        "Fin suppression neutralisation %s",
        ctx(
            id_scenario=id_scenario,
            dt_debut=dt_debut,
            dt_fin=dt_fin,
            rows_affected=rc,
            duration_ms=duration_ms,
        ),
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
        "Début suppression neutralisation par id %s",
        ctx(id_scenario=id_scenario, id_neutralisation=id_neutralisation),
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    avant = await db_read.fetch_one(
        SELECT_NEUTRALISATION_SQL
        + " WHERE id_neutralisation = %s AND id_scenario = %s",
        (id_neutralisation, id_scenario),
    )
    etat_avant = params_loggables(dict(avant)) if avant else None
    if etat_avant is not None:
        logger.info(
            "État avant suppression neutralisation %s",
            ctx(id_scenario=id_scenario, etat=etat_avant),
        )

    try:
        async with db_write.transaction() as tx:
            rc = await tx.execute(
                "DELETE FROM trppu_neutralisations "
                "WHERE id_neutralisation = %s AND id_scenario = %s",
                (id_neutralisation, id_scenario),
            )
    except Exception as e:
        logger.exception(
            "Erreur suppression neutralisation par id %s",
            ctx(
                id_scenario=id_scenario,
                id_neutralisation=id_neutralisation,
                etat=etat_avant,
            ),
        )
        raise HTTPException(
            status_code=500, detail="Erreur suppression neutralisation."
        ) from e

    if not rc:
        logger.warning(
            "Rejet suppression neutralisation par id %s",
            ctx(
                id_scenario=id_scenario,
                id_neutralisation=id_neutralisation,
                http=404,
                motif="neutralisation introuvable",
            ),
        )
        raise HTTPException(
            status_code=404,
            detail=(
                f"Aucune neutralisation (id {id_neutralisation}) à supprimer "
                f"pour le scénario {id_scenario}."
            ),
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_NEUTRALISATION,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={
            "operation": "suppression_par_id",
            "id_neutralisation": id_neutralisation,
            "etat_avant": etat_avant,
        },
    )
    logger.info(
        "Fin suppression neutralisation par id %s",
        ctx(
            id_scenario=id_scenario,
            id_neutralisation=id_neutralisation,
            rows_affected=rc,
            duration_ms=duration_ms,
        ),
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
