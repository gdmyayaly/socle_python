"""Endpoints TMH d'un scénario : lecture (DSR-650), création, MAJ batch (DSR-659),
MAJ ciblée (DSR-649), exclusion et suppression.

Depuis la migration du 24/06/2026, un même produit peut figurer plusieurs fois
dans le TMH d'un scénario : les opérations ciblées sont identifiées par `id_tmh`.
"""

import logging
import time

from fastapi import APIRouter, HTTPException, Query, Response

from app.db.mysql import db_read, db_write
from app.log_utils import ctx, params_loggables
from app.routes.trppu_produit.helpers import ensure_produits_exist
from app.routes.trppu_scenario.helpers import assert_editable, fetch_scenario_or_404
from app.security.crypto import encrypt_id_rh
from app.services.api_log import ACTION_ECRITURE_TMH, enregistrer_appel

from .helpers import (
    SELECT_TMH_BY_ID_SQL,
    fetch_tmh,
    insert_tmh_row,
    resolve_libelles_produits,
    upsert_tmh_rows,
)
from .schemas import (
    TmhBatchResult,
    TmhBatchUpdate,
    TmhCreate,
    TmhExclusionUpdate,
    TmhOut,
    TmhVolumeUpdate,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/scenarios", tags=["TMH"])


@router.get("/{id_scenario}/tmh", response_model=list[TmhOut])
async def list_tmh(
    id_scenario: int,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-650 : trafics moyen hebdo d'un scénario (1+ ligne(s) par produit)."""
    start = time.perf_counter()
    logger.info("Début lecture TMH %s", ctx(id_scenario=id_scenario))
    await fetch_scenario_or_404(id_scenario)
    try:
        rows = await fetch_tmh(db_read, id_scenario)
    except Exception as e:
        logger.exception("Erreur lecture TMH %s", ctx(id_scenario=id_scenario))
        raise HTTPException(status_code=500, detail="Erreur lecture TMH.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin lecture TMH %s",
        ctx(id_scenario=id_scenario, count=len(rows), duration_ms=duration_ms),
    )
    return rows


@router.post("/{id_scenario}/tmh", response_model=TmhOut, status_code=201)
async def create_tmh(id_scenario: int, payload: TmhCreate):
    """Crée une nouvelle ligne TMH (le produit peut déjà être présent sur le scénario)."""
    start = time.perf_counter()
    logged = params_loggables(payload)
    logger.info(
        "Début création TMH %s",
        ctx(id_scenario=id_scenario, co_produit=payload.co_produit, params=logged),
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    id_rh_token = encrypt_id_rh(payload.id_rh)
    libelles = await resolve_libelles_produits()
    try:
        async with db_write.transaction() as tx:
            # Le référentiel produits est piloté par Databricks : un objet remonté par les
            # trafics peut ne pas encore exister côté applicatif. On le crée plutôt que de
            # laisser la FK fk_tmh_produit casser la transaction.
            crees = await ensure_produits_exist(tx, [payload.co_produit], libelles)
            if crees:
                logger.info(
                    "Produits créés automatiquement %s",
                    ctx(id_scenario=id_scenario, nb=len(crees), codes=crees),
                )
            new_id = await insert_tmh_row(
                tx,
                id_scenario,
                co_produit=payload.co_produit,
                volume_realise=payload.volume_realise,
                volume_previsionnel=payload.volume_previsionnel,
                moyenne_journaliere=payload.moyenne_journaliere,
                moyenne_hebdo=payload.moyenne_hebdo,
                bl_exclu=payload.exclusion,
                bl_manuel=payload.manuel,
                motif=payload.motif,
                id_rh=id_rh_token,
                volume_previsionnel_recalcule=payload.volume_previsionnel_recalcule,
            )
            row = await tx.fetch_one(SELECT_TMH_BY_ID_SQL, (new_id, id_scenario))
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur création TMH %s", ctx(id_scenario=id_scenario, params=logged)
        )
        raise HTTPException(status_code=500, detail="Erreur création TMH.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_ECRITURE_TMH,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={"operation": "creation", "id_tmh": new_id, "params": logged},
    )
    logger.info(
        "Fin création TMH %s",
        ctx(
            id_scenario=id_scenario,
            id_tmh=new_id,
            co_produit=payload.co_produit,
            duration_ms=duration_ms,
        ),
    )
    return row


@router.put("/{id_scenario}/tmh", response_model=TmhBatchResult)
async def upsert_tmh(id_scenario: int, payload: TmhBatchUpdate):
    """DSR-659 : MAJ (upsert) du lot TMH. Chaque item avec id_tmh est mis à jour,
    sinon une nouvelle ligne est insérée."""
    start = time.perf_counter()
    logger.info(
        "Début MAJ TMH batch %s",
        ctx(
            id_scenario=id_scenario,
            nb_lignes=len(payload.tmh),
            produits=[item.co_produit for item in payload.tmh],
        ),
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    id_rh_token = encrypt_id_rh(payload.id_rh)
    libelles = await resolve_libelles_produits()
    try:
        async with db_write.transaction() as tx:
            crees = await ensure_produits_exist(
                tx, [item.co_produit for item in payload.tmh], libelles
            )
            if crees:
                logger.info(
                    "Produits créés automatiquement %s",
                    ctx(id_scenario=id_scenario, nb=len(crees), codes=crees),
                )
            nb_inserted, nb_updated = await upsert_tmh_rows(
                tx, id_scenario, payload.tmh, id_rh=id_rh_token
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erreur MAJ TMH batch %s", ctx(id_scenario=id_scenario))
        raise HTTPException(status_code=500, detail="Erreur mise à jour TMH.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_ECRITURE_TMH,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={
            "operation": "upsert_batch",
            "inseres": nb_inserted,
            "modifies": nb_updated,
            "lignes": [params_loggables(item) for item in payload.tmh],
        },
    )
    logger.info(
        "Fin MAJ TMH batch %s",
        ctx(
            id_scenario=id_scenario,
            inseres=nb_inserted,
            modifies=nb_updated,
            duration_ms=duration_ms,
        ),
    )
    return TmhBatchResult(
        id_scenario=id_scenario, nb_inserted=nb_inserted, nb_updated=nb_updated
    )


@router.patch("/{id_scenario}/tmh/{id_tmh}", response_model=TmhOut)
async def update_tmh_volume(id_scenario: int, id_tmh: int, payload: TmhVolumeUpdate):
    """DSR-649 : MAJ ciblée d'un trafic initial modifié (volume réalisé + moyennes), par id_tmh."""
    start = time.perf_counter()
    logged = params_loggables(payload)
    logger.info(
        "Début MAJ TMH ciblée %s",
        ctx(id_scenario=id_scenario, id_tmh=id_tmh, params=logged),
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    try:
        async with db_write.transaction() as tx:
            # DSR-649 : modification manuelle d'un trafic initial -> la ligne devient
            # "manuelle" (bl_manuel = 1), cf. DSR-665/648. Une ligne manuelle ne
            # reçoit pas de variation prévisionnelle : le prévisionnel recalculé
            # est réaligné sur la valeur de base (auto-référence SQL).
            # volume_brut suit le nouveau constaté (compute_volume_brut, DSR-689 RG4) :
            # écrit ici à partir du volume reçu et de volume_previsionnel — inchangé
            # par cette requête — plutôt que des colonnes en cours de MAJ, pour ne pas
            # dépendre de l'ordre d'évaluation des affectations du SET.
            rows_maj = await tx.execute(
                "UPDATE trppu_tmh SET volume_realise = %s, moyenne_journaliere = %s, "
                "moyenne_hebdo = %s, motif = %s, bl_manuel = 1, "
                "volume_previsionnel_recalcule = volume_previsionnel, "
                "volume_brut = COALESCE(%s, 0) + COALESCE(volume_previsionnel, 0), "
                "dt_calcul = NOW() "
                "WHERE id_tmh = %s AND id_scenario = %s",
                (
                    payload.volume_realise,
                    payload.moyenne_journaliere,
                    payload.moyenne_hebdo,
                    payload.motif,
                    payload.volume_realise,
                    id_tmh,
                    id_scenario,
                ),
            )
            row = await tx.fetch_one(SELECT_TMH_BY_ID_SQL, (id_tmh, id_scenario))
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur MAJ TMH ciblée %s",
            ctx(id_scenario=id_scenario, id_tmh=id_tmh, params=logged),
        )
        raise HTTPException(status_code=500, detail="Erreur mise à jour TMH.") from e

    if not row:
        logger.warning(
            "Rejet MAJ TMH ciblée %s",
            ctx(
                id_scenario=id_scenario,
                id_tmh=id_tmh,
                http=404,
                motif="ligne TMH introuvable",
            ),
        )
        raise HTTPException(
            status_code=404,
            detail=f"Ligne TMH introuvable (scénario {id_scenario}, id_tmh {id_tmh}).",
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_ECRITURE_TMH,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={"operation": "maj_ciblee", "id_tmh": id_tmh, "params": logged},
    )
    logger.info(
        "Fin MAJ TMH ciblée %s",
        ctx(
            id_scenario=id_scenario,
            id_tmh=id_tmh,
            rows_affected=rows_maj,
            duration_ms=duration_ms,
        ),
    )
    return row


@router.patch("/{id_scenario}/tmh/{id_tmh}/exclusion", response_model=TmhOut)
async def toggle_tmh_exclusion(
    id_scenario: int, id_tmh: int, payload: TmhExclusionUpdate
):
    """Switch de l'exclusion d'une ligne TMH (bl_exclu) du calcul du scénario, par id_tmh."""
    start = time.perf_counter()
    logger.info(
        "Début switch exclusion TMH %s",
        ctx(
            id_scenario=id_scenario,
            id_tmh=id_tmh,
            bl_exclu=payload.bl_exclu,
            motif=payload.motif,
        ),
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    try:
        async with db_write.transaction() as tx:
            rows_maj = await tx.execute(
                "UPDATE trppu_tmh SET bl_exclu = %s, motif = %s, dt_calcul = NOW() "
                "WHERE id_tmh = %s AND id_scenario = %s",
                (1 if payload.bl_exclu else 0, payload.motif, id_tmh, id_scenario),
            )
            row = await tx.fetch_one(SELECT_TMH_BY_ID_SQL, (id_tmh, id_scenario))
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur switch exclusion TMH %s",
            ctx(id_scenario=id_scenario, id_tmh=id_tmh),
        )
        raise HTTPException(
            status_code=500, detail="Erreur mise à jour exclusion TMH."
        ) from e

    if not row:
        logger.warning(
            "Rejet switch exclusion TMH %s",
            ctx(
                id_scenario=id_scenario,
                id_tmh=id_tmh,
                http=404,
                motif="ligne TMH introuvable",
            ),
        )
        raise HTTPException(
            status_code=404,
            detail=f"Ligne TMH introuvable (scénario {id_scenario}, id_tmh {id_tmh}).",
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_ECRITURE_TMH,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={
            "operation": "exclusion",
            "id_tmh": id_tmh,
            "bl_exclu": payload.bl_exclu,
            "motif": payload.motif,
        },
    )
    logger.info(
        "Fin switch exclusion TMH %s",
        ctx(
            id_scenario=id_scenario,
            id_tmh=id_tmh,
            bl_exclu=payload.bl_exclu,
            rows_affected=rows_maj,
            duration_ms=duration_ms,
        ),
    )
    return row


@router.delete("/{id_scenario}/tmh/{id_tmh}", status_code=204)
async def delete_tmh(
    id_scenario: int,
    id_tmh: int,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """Supprime une ligne TMH (utile pour retirer un produit ajouté plusieurs fois)."""
    start = time.perf_counter()
    logger.info(
        "Début suppression TMH %s", ctx(id_scenario=id_scenario, id_tmh=id_tmh)
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    # Suppression définitive : l'état de la ligne est journalisé avant écriture,
    # sans quoi elle devient irrécupérable.
    avant = await db_read.fetch_one(SELECT_TMH_BY_ID_SQL, (id_tmh, id_scenario))
    etat_avant = params_loggables(dict(avant)) if avant else None
    if etat_avant is not None:
        logger.info(
            "État avant suppression TMH %s",
            ctx(id_scenario=id_scenario, id_tmh=id_tmh, etat=etat_avant),
        )

    try:
        async with db_write.transaction() as tx:
            rc = await tx.execute(
                "DELETE FROM trppu_tmh WHERE id_tmh = %s AND id_scenario = %s",
                (id_tmh, id_scenario),
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur suppression TMH %s",
            ctx(id_scenario=id_scenario, id_tmh=id_tmh, etat=etat_avant),
        )
        raise HTTPException(status_code=500, detail="Erreur suppression TMH.") from e

    if not rc:
        logger.warning(
            "Rejet suppression TMH %s",
            ctx(
                id_scenario=id_scenario,
                id_tmh=id_tmh,
                http=404,
                motif="ligne TMH introuvable",
            ),
        )
        raise HTTPException(
            status_code=404,
            detail=f"Ligne TMH introuvable (scénario {id_scenario}, id_tmh {id_tmh}).",
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_ECRITURE_TMH,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={
            "operation": "suppression",
            "id_tmh": id_tmh,
            "etat_avant": etat_avant,
        },
    )
    logger.info(
        "Fin suppression TMH %s",
        ctx(
            id_scenario=id_scenario,
            id_tmh=id_tmh,
            rows_affected=rc,
            duration_ms=duration_ms,
        ),
    )
    return Response(status_code=204)
