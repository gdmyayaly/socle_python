"""Rétention PIC d'un scénario : lecture/merge (DSR-660) + écriture (DSR-661)."""

import logging
import time

from fastapi import APIRouter, HTTPException, Query

from app.db.mysql import db_read, db_write
from app.log_utils import ctx, params_loggables
from app.security.crypto import encrypt_id_rh
from app.routes.trppu_scenario.helpers import (
    assert_editable,
    fetch_scenario_or_404,
)
from app.services.api_log import ACTION_ECRITURE_PIC_COEFFICIENT, enregistrer_appel

from .helpers import (
    dedupliquer_items,
    ensure_scenario_pic_version,
    fetch_coeffs_for_version,
    fetch_scenario_pic_version,
    merge_coeffs,
    resolve_default_pic_version,
    upsert_coef,
)
from .schemas import (
    PicCoefBatchResult,
    PicCoefBatchUpsert,
    PicCoefUpsert,
    PicCoefUpsertResult,
    PicScenarioOut,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/scenarios", tags=["Rétention PIC"])


@router.get("/{id_scenario}/pic-coefficients", response_model=PicScenarioOut)
async def get_pic_coefficients(
    id_scenario: int,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-660 : coefficients PIC par défaut (national) surchargés par ceux du scénario."""
    start = time.perf_counter()
    logger.info("Début lecture coefficients PIC %s", ctx(id_scenario=id_scenario))
    await fetch_scenario_or_404(id_scenario)

    try:
        id_pic_version_defaut = await resolve_default_pic_version(db_read)
        defaults = await fetch_coeffs_for_version(db_read, id_pic_version_defaut)
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
        logger.exception(
            "Erreur lecture coefficients PIC %s", ctx(id_scenario=id_scenario)
        )
        raise HTTPException(status_code=500, detail="Erreur lecture coefficients PIC.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin lecture coefficients PIC %s",
        ctx(
            id_scenario=id_scenario,
            id_pic_version_defaut=id_pic_version_defaut,
            id_pic_version_scenario=id_pic_version_scenario,
            nb_coeffs=len(coefficients),
            nb_surcharges=len(overrides),
            duration_ms=duration_ms,
        ),
    )
    return PicScenarioOut(
        id_pic_version_defaut=id_pic_version_defaut,
        id_pic_version_scenario=id_pic_version_scenario,
        niveau_scenario=niveau_scenario,
        coefficients=coefficients,
    )


@router.put("/{id_scenario}/pic-coefficients", response_model=PicCoefUpsertResult)
async def upsert_pic_coefficient(
    id_scenario: int,
    payload: PicCoefUpsert,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-661 : enregistre un coefficient PIC modifié pour le scénario.

    - Cas 1 : version scénario existante -> UPDATE du coef, ou INSERT si (produit, jour,
      densité) absent.
    - Cas 2 : aucune version scénario -> INSERT trppu_pic_version (niveau SCENARIO)
      puis INSERT du coef. Le tout en transaction.

    Écriture cellule par cellule (sauvegarde à la perte de focus côté IHM) ; pour
    enregistrer un tableau modifié d'un bloc, voir `PUT .../pic-coefficients/batch`.
    """
    start = time.perf_counter()
    logged = params_loggables(payload)
    logger.info(
        "Début enregistrement coefficient PIC %s",
        ctx(
            id_scenario=id_scenario,
            co_produit=payload.co_produit,
            jour_semaine=payload.jour_semaine,
            densite=payload.densite,
            params=logged,
        ),
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)
    id_rh_token = encrypt_id_rh(payload.id_rh)

    try:
        async with db_write.transaction() as tx:
            id_pic_version, version_creee = await ensure_scenario_pic_version(
                tx, id_scenario, scenario["co_regate"], id_rh_token
            )
            # `coef_avant` : seule la branche UPDATE connaît une valeur antérieure.
            operation, coef_avant, id_pic_coef = await upsert_coef(
                tx, id_pic_version, payload, id_rh_token
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur enregistrement coefficient PIC %s",
            ctx(id_scenario=id_scenario, co_produit=payload.co_produit, params=logged),
        )
        raise HTTPException(status_code=500, detail="Erreur enregistrement coefficient PIC.") from e

    if version_creee:
        action = "insert_version_and_coef"
    else:
        action = "update" if operation == "update" else "insert_coef"

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_ECRITURE_PIC_COEFFICIENT,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={
            "operation": action,
            "id_pic_version": id_pic_version,
            "id_pic_coef": id_pic_coef,
            "coef_avant": str(coef_avant) if coef_avant is not None else None,
            "params": logged,
        },
    )
    logger.info(
        "Fin enregistrement coefficient PIC %s",
        ctx(
            id_scenario=id_scenario,
            id_pic_version=id_pic_version,
            id_pic_coef=id_pic_coef,
            co_produit=payload.co_produit,
            action=action,
            coef_avant=coef_avant,
            coef=payload.coef,
            duration_ms=duration_ms,
        ),
    )
    return PicCoefUpsertResult(action=action, id_pic_version=id_pic_version)


@router.put("/{id_scenario}/pic-coefficients/batch", response_model=PicCoefBatchResult)
async def upsert_pic_coefficients_batch(
    id_scenario: int,
    payload: PicCoefBatchUpsert,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """Enregistrement multiple : toutes les cellules modifiées en une seule transaction.

    Même règle métier que l'écriture unitaire (DSR-661), appliquée cellule par
    cellule sur la version PIC du scénario, créée à la volée si elle n'existe pas.
    Le lot est **tout ou rien** : une erreur sur une ligne annule les précédentes et
    la version éventuellement créée, pour qu'un tableau ne soit jamais à moitié
    enregistré. Une cellule répétée dans le lot n'est écrite qu'une fois (dernière
    valeur reçue).
    """
    start = time.perf_counter()
    items = dedupliquer_items(payload.coefficients)
    nb_doublons = len(payload.coefficients) - len(items)
    logged = [params_loggables(item) for item in items]
    logger.info(
        "Début enregistrement lot coefficients PIC %s",
        ctx(
            id_scenario=id_scenario,
            nb_lignes=len(items),
            nb_doublons=nb_doublons or None,
            produits=sorted({item.co_produit for item in items}),
        ),
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)
    id_rh_token = encrypt_id_rh(payload.id_rh)

    nb_inserted = 0
    nb_updated = 0
    try:
        async with db_write.transaction() as tx:
            id_pic_version, version_creee = await ensure_scenario_pic_version(
                tx, id_scenario, scenario["co_regate"], id_rh_token
            )
            for item in items:
                operation, _coef_avant, _id_pic_coef = await upsert_coef(
                    tx, id_pic_version, item, id_rh_token
                )
                if operation == "update":
                    nb_updated += 1
                else:
                    nb_inserted += 1
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur enregistrement lot coefficients PIC %s",
            ctx(id_scenario=id_scenario, nb_lignes=len(items), params=logged),
        )
        raise HTTPException(
            status_code=500, detail="Erreur enregistrement des coefficients PIC."
        ) from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_ECRITURE_PIC_COEFFICIENT,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={
            "operation": "upsert_batch",
            "id_pic_version": id_pic_version,
            "version_creee": version_creee,
            "inseres": nb_inserted,
            "modifies": nb_updated,
            "lignes": logged,
        },
    )
    logger.info(
        "Fin enregistrement lot coefficients PIC %s",
        ctx(
            id_scenario=id_scenario,
            id_pic_version=id_pic_version,
            version_creee=version_creee,
            inseres=nb_inserted,
            modifies=nb_updated,
            duration_ms=duration_ms,
        ),
    )
    return PicCoefBatchResult(
        id_scenario=id_scenario,
        id_pic_version=id_pic_version,
        version_creee=version_creee,
        nb_inserted=nb_inserted,
        nb_updated=nb_updated,
    )
