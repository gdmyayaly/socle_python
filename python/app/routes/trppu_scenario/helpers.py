"""Helpers pour trppu_scenario : SQL constants, FK checks, défauts métier."""

from datetime import date, timedelta
from typing import Any

from fastapi import HTTPException

from app.db.mysql import db_read

SELECT_SCENARIO_SQL = (
    "SELECT id_scenario, co_regate, lb_scenario, co_roc, statut, dt_creation, "
    "dt_validation, dt_mise_en_prod, periode_debut, periode_fin, "
    "periode_realise_debut, periode_realise_fin, periode_prev_debut, periode_prev_fin, "
    "nb_jours_semaine, id_pic_version, version_scenario, id_scenario_parent, est_fige "
    "FROM trppu_scenario"
)


def default_periode() -> tuple[date, date]:
    """Période par défaut : today - 1 an → today + 1 an."""
    today = date.today()
    return today - timedelta(days=365), today + timedelta(days=365)


async def resolve_default_pic_version() -> int:
    """Première trppu_pic_version avec est_par_defaut=1, sinon id_pic_version=1.

    Lève 422 si aucune ligne candidate.
    """
    row = await db_read.fetch_one(
        "SELECT id_pic_version FROM trppu_pic_version "
        "WHERE est_par_defaut = 1 ORDER BY id_pic_version LIMIT 1"
    )
    if row:
        return int(row["id_pic_version"])
    row = await db_read.fetch_one(
        "SELECT id_pic_version FROM trppu_pic_version WHERE id_pic_version = 1"
    )
    if row:
        return int(row["id_pic_version"])
    raise HTTPException(
        status_code=422,
        detail=(
            "Aucun id_pic_version par défaut disponible : "
            "renseigner trppu_pic_version (est_par_defaut=1) ou fournir id_pic_version."
        ),
    )


async def fetch_scenario_or_404(id_scenario: int) -> dict[str, Any]:
    row = await db_read.fetch_one(
        SELECT_SCENARIO_SQL + " WHERE id_scenario = %s", (id_scenario,)
    )
    if not row:
        raise HTTPException(
            status_code=404, detail=f"Scénario {id_scenario} introuvable."
        )
    return row


def assert_not_fige(scenario: dict[str, Any]) -> None:
    """Lève HTTP 409 si le scénario est figé.

    Le PATCH /est-fige est la seule manière de défiger ; il n'utilise donc pas ce check.
    Le PATCH /statut a sa propre logique (transitions autorisées) et ne se sert pas non plus
    de ce check : un scénario PRODUCTION reste archivable.
    """
    if scenario.get("est_fige"):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Scénario {scenario['id_scenario']} figé "
                f"(statut={scenario['statut']}), modification interdite."
            ),
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
    return int(row["version_scenario"])
