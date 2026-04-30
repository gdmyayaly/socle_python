"""Helpers transverses pour les routes scénarios : audit, garde figé, versioning, validation."""

import json
import logging
from datetime import date, datetime
from typing import Any

from fastapi import HTTPException, Request

from app.config import MAX_DATE_RANGE_DAYS

logger = logging.getLogger(__name__)

STATUTS_FIGES = ("VERROUILLE", "ARCHIVE")


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def err(status_code: int, message: str) -> HTTPException:
    """Construit une HTTPException au format standard du projet."""
    return HTTPException(
        status_code=status_code,
        detail={"error": True, "message": message, "code": status_code},
    )


def get_caller(request: Request) -> str:
    """Retourne l'utilisateur appelant via le header X-User (fallback 'system')."""
    return request.headers.get("X-User") or "system"


async def fetch_scenario(tx, id_scenario: int) -> dict[str, Any]:
    """Charge un scénario ou lève 404."""
    row = await tx.fetch_one(
        "SELECT * FROM trppu_scenario WHERE id_scenario = %s",
        (id_scenario,),
    )
    if not row:
        raise err(404, f"Scénario {id_scenario} introuvable.")
    return row


def assert_not_fige(scenario: dict[str, Any]) -> None:
    """Lève 409 si le scénario est figé (statut VERROUILLE/ARCHIVE ou est_fige=True)."""
    if scenario.get("est_fige") or scenario.get("statut") in STATUTS_FIGES:
        raise err(
            409,
            f"Scénario {scenario['id_scenario']} figé "
            f"(statut={scenario['statut']}), modification interdite.",
        )


async def increment_version(tx, id_scenario: int) -> int:
    """Incrémente version_scenario et retourne la nouvelle valeur."""
    await tx.execute(
        "UPDATE trppu_scenario SET version_scenario = version_scenario + 1 "
        "WHERE id_scenario = %s",
        (id_scenario,),
    )
    row = await tx.fetch_one(
        "SELECT version_scenario FROM trppu_scenario WHERE id_scenario = %s",
        (id_scenario,),
    )
    return row["version_scenario"]


async def log_api(
    tx,
    api_name: str,
    id_scenario: int | None,
    caller: str,
    params: dict[str, Any] | None = None,
    co_regate: str | None = None,
) -> None:
    """Insère une trace dans trppu_api_log dans la transaction courante."""
    payload = json.dumps(params or {}, default=_json_default, ensure_ascii=False)
    await tx.execute(
        "INSERT INTO trppu_api_log "
        "(api_name, id_scenario, regate, dt_appel, caller, params) "
        "VALUES (%s, %s, %s, NOW(), %s, %s)",
        (api_name, id_scenario, co_regate, caller, payload),
    )


async def last_insert_id(tx) -> int:
    row = await tx.fetch_one("SELECT LAST_INSERT_ID() AS id")
    return int(row["id"])


def validate_periode(periode_debut: date, periode_fin: date) -> None:
    """Vérifie cohérence d'une fenêtre [debut, fin]."""
    if periode_fin < periode_debut:
        raise err(400, "periode_fin doit être postérieure ou égale à periode_debut.")
    if (periode_fin - periode_debut).days > MAX_DATE_RANGE_DAYS:
        raise err(
            400,
            f"L'écart entre les dates ne doit pas dépasser {MAX_DATE_RANGE_DAYS} jours "
            "(24 mois).",
        )


def validate_window_inside_scenario(
    dt_debut: date,
    dt_fin: date,
    scenario: dict[str, Any],
    label: str = "fenêtre",
) -> None:
    """Vérifie qu'une fenêtre [dt_debut, dt_fin] est incluse dans la période du scénario."""
    if dt_fin < dt_debut:
        raise err(400, f"{label} : dt_fin doit être postérieure ou égale à dt_debut.")
    p_deb: date = scenario["periode_debut"]
    p_fin: date = scenario["periode_fin"]
    if dt_debut < p_deb or dt_fin > p_fin:
        raise err(
            400,
            f"{label} hors période du scénario "
            f"({p_deb.isoformat()} → {p_fin.isoformat()}).",
        )


async def assert_pic_version_exists(tx, id_pic_version: int) -> None:
    """Vérifie que l'id_pic_version existe (pas de FK déclarée au schéma)."""
    row = await tx.fetch_one(
        "SELECT 1 AS ok FROM trppu_pic_version WHERE id_pic_version = %s",
        (id_pic_version,),
    )
    if not row:
        raise err(409, f"id_pic_version {id_pic_version} inexistant.")


async def assert_produit_exists(tx, co_produit: str) -> None:
    """Vérifie que le produit existe."""
    row = await tx.fetch_one(
        "SELECT 1 AS ok FROM trppu_produit WHERE co_produit = %s",
        (co_produit,),
    )
    if not row:
        raise err(409, f"co_produit '{co_produit}' inexistant.")
