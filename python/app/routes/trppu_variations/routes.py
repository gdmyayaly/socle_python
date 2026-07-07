"""Variations prévisionnelles d'un scénario : lecture (DSR-651) + écriture (DSR-646)."""

import logging
import time
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query, Response, status

from app.db.mysql import db_read, db_write
from app.log_utils import safe_preview
from app.routes.trppu_scenario.helpers import assert_editable, fetch_scenario_or_404
from app.security.crypto import encrypt_id_rh

from .helpers import SELECT_VARIATIONS_SQL, fetch_variation
from .schemas import VariationOut, VariationUpsert, VariationUpsertResult

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/scenarios", tags=["Variations prévisionnelles"])


@router.get("/{id_scenario}/variations", response_model=list[VariationOut])
async def list_variations(
    id_scenario: int,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-651 : variations prévisionnelles d'un scénario.

    La liste est pilotée par les produits du TMH (co_produit distincts ayant au moins
    une ligne non exclue) : chaque produit est renvoyé avec sa variation stockée, ou
    0 % par défaut (le 0 % n'est pas stocké en base). Un scénario sans TMH — ou dont
    tous les produits sont exclus — renvoie une liste vide.
    """
    start = time.perf_counter()
    logger.info(
        "Début lecture variations (id_scenario=%d, id_session_ihm=%s)",
        id_scenario,
        safe_preview(id_session_ihm),
    )
    await fetch_scenario_or_404(id_scenario)
    try:
        rows = await db_read.fetch_all(SELECT_VARIATIONS_SQL, (id_scenario, id_scenario))
    except Exception as e:
        logger.exception("Erreur lecture variations (id_scenario=%d)", id_scenario)
        raise HTTPException(status_code=500, detail="Erreur lecture variations.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Lecture variations terminée (id_scenario=%d, count=%d, duration_ms=%.1f)",
        id_scenario,
        len(rows),
        duration_ms,
    )
    return rows


@router.put("/{id_scenario}/variations/{co_produit}", response_model=VariationUpsertResult)
async def upsert_variation(id_scenario: int, co_produit: str, payload: VariationUpsert):
    """DSR-646 : ajoute/modifie une variation ; la supprime si variation_pct == 0."""
    start = time.perf_counter()
    logger.info(
        "Début upsert variation (id_scenario=%d, co_produit=%s, variation_pct=%s)",
        id_scenario,
        co_produit,
        payload.variation_pct,
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    is_zero = payload.variation_pct == Decimal("0")
    id_rh_token = encrypt_id_rh(payload.id_rh)  # traçabilité (migration 004)

    try:
        async with db_write.transaction() as tx:
            existing = await fetch_variation(tx, id_scenario, co_produit)
            if is_zero:
                if existing:
                    await tx.execute(
                        "DELETE FROM trppu_scenario_variations_prev "
                        "WHERE id_scenario = %s AND co_produit = %s",
                        (id_scenario, co_produit),
                    )
                    action = "deleted"
                else:
                    action = "noop"  # déjà à 0 % (non stocké) : rien à faire
            elif existing:
                # dt_creation n'est PAS réécrit : on conserve la date de création d'origine
                # (la table n'a pas de colonne dt_maj dans le schéma).
                await tx.execute(
                    "UPDATE trppu_scenario_variations_prev "
                    "SET variation_pct = %s, id_rh = %s "
                    "WHERE id_scenario = %s AND co_produit = %s",
                    (payload.variation_pct, id_rh_token, id_scenario, co_produit),
                )
                action = "updated"
            else:
                await tx.execute(
                    "INSERT INTO trppu_scenario_variations_prev "
                    "(id_scenario, co_produit, variation_pct, id_rh) "
                    "VALUES (%s, %s, %s, %s)",
                    (id_scenario, co_produit, payload.variation_pct, id_rh_token),
                )
                action = "created"
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur upsert variation (id_scenario=%d, co_produit=%s)", id_scenario, co_produit
        )
        raise HTTPException(status_code=500, detail="Erreur mise à jour variation.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Upsert variation terminé (id_scenario=%d, co_produit=%s, action=%s, duration_ms=%.1f)",
        id_scenario,
        co_produit,
        action,
        duration_ms,
    )
    return VariationUpsertResult(
        co_produit=co_produit,
        variation_pct=None if action in ("deleted", "noop") else payload.variation_pct,
        action=action,
    )


@router.delete("/{id_scenario}/variations/{co_produit}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_variation(id_scenario: int, co_produit: str):
    """DSR-646 : suppression explicite d'une variation."""
    start = time.perf_counter()
    logger.info(
        "Début suppression variation (id_scenario=%d, co_produit=%s)", id_scenario, co_produit
    )
    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    try:
        async with db_write.transaction() as tx:
            rc = await tx.execute(
                "DELETE FROM trppu_scenario_variations_prev "
                "WHERE id_scenario = %s AND co_produit = %s",
                (id_scenario, co_produit),
            )
    except Exception as e:
        logger.exception(
            "Erreur suppression variation (id_scenario=%d, co_produit=%s)",
            id_scenario,
            co_produit,
        )
        raise HTTPException(status_code=500, detail="Erreur suppression variation.") from e

    if not rc:
        raise HTTPException(
            status_code=404,
            detail=f"Variation introuvable (scénario {id_scenario}, produit {co_produit}).",
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Suppression variation terminée (id_scenario=%d, co_produit=%s, duration_ms=%.1f)",
        id_scenario,
        co_produit,
        duration_ms,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
