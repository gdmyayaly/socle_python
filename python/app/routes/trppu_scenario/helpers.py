"""Helpers pour trppu_scenario : SQL constants, défauts métier, recalcul des bornes."""

from datetime import date, timedelta
from typing import Any

from fastapi import HTTPException

from app.db.mysql import db_read

SELECT_SCENARIO_SQL = (
    "SELECT id_scenario, co_regate, lb_scenario, co_roc, statut, dt_creation, "
    "dt_validation, dt_mise_en_prod, periode_debut, periode_fin, "
    "periode_realise_debut, periode_realise_fin, periode_prev_debut, periode_prev_fin, "
    "nb_jours_semaine, id_pic_version, version_scenario, est_fige "
    "FROM trppu_scenario"
)


def default_periode() -> tuple[date, date]:
    """Période par défaut : today - 1 an a today + 1 an."""
    today = date.today()
    return today - timedelta(days=365), today + timedelta(days=365)


def recompute_realise_prev(
    periode_debut: date,
    periode_fin: date,
    today: date | None = None,
) -> tuple[date | None, date | None, date | None, date | None]:
    """Recalcule les bornes réalisé / prévision en fonction de la période et de today.

    Renvoie (realise_debut, realise_fin, prev_debut, prev_fin).

    Règles :
    - La portion réalisée (passée + présent) = [periode_debut, min(today, periode_fin)]
      si periode_debut <= today, sinon (None, None).
    - La portion prévision (futur + présent) = [max(today, periode_debut), periode_fin]
      si periode_fin >= today, sinon (None, None).
    - Cas où today est dans la période : realise_fin == prev_debut == today
      (les deux périodes se touchent sur la journée du jour).
    """
    today = today or date.today()
    if periode_debut <= today:
        realise_debut = periode_debut
        realise_fin = min(today, periode_fin)
    else:
        realise_debut = None
        realise_fin = None
    if periode_fin >= today:
        prev_debut = max(today, periode_debut)
        prev_fin = periode_fin
    else:
        prev_debut = None
        prev_fin = None
    return realise_debut, realise_fin, prev_debut, prev_fin


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
    de ce check : un scénario EN PRODUCTION reste archivable.
    """
    if scenario.get("est_fige"):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Scénario {scenario['id_scenario']} figé "
                f"(statut={scenario['statut']}), modification interdite."
            ),
        )


async def ensure_site_exists(
    tx,
    co_regate: str,
    co_roc: str,
    lb_regate: str,
    type_site: str,
) -> bool:
    """Garantit la présence du site dans trppu_site avant insert d'un scénario.

    Retourne True si une ligne a été insérée, False si le site existait déjà.
    Aucun UPDATE n'est fait sur un site déjà présent.
    """
    row = await tx.fetch_one(
        "SELECT co_regate FROM trppu_site WHERE co_regate = %s", (co_regate,)
    )
    if row:
        return False

    await tx.execute(
        "INSERT INTO trppu_site (co_regate, lb_regate, type_site, co_roc) "
        "VALUES (%s, %s, %s, %s)",
        (co_regate, lb_regate, type_site, co_roc),
    )
    return True


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
