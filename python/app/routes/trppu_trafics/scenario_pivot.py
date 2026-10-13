"""Rattachement d'une récupération de trafics à un scénario : écriture de sa date pivot.

`GET /trafics/get_trafics_pivot?id_scenario=…` écrit `trppu_scenario.dt_pivot` et les
bornes `periode_realise_*` / `periode_prev_*` : la date pivot qui a réellement servi à
séparer le réalisé du prévisionnel dans Databricks. Les bornes sont les plages de
`split_by_pivot`, donc réalisé = [debut, pivot - 1] et prév = [pivot, fin]. Le PUT
scénario (si `date_pivot` est transmis) et le PATCH /periodes appliquent la même règle
via `trppu_scenario.helpers.enregistrer_pivot_periode`.

Le contrôle a lieu AVANT les requêtes Databricks (pas de lecture coûteuse pour un
scénario qu'on ne pourra pas mettre à jour), l'écriture APRÈS leur succès (un échec
Databricks ne doit pas laisser un pivot qui ne correspond à aucun trafic récupéré).
"""

import logging
from datetime import date, datetime
from typing import Any

from app.db.mysql import db_write
from app.log_utils import ctx
from app.routes.trppu_scenario.helpers import enregistrer_pivot, fetch_scenario_or_404
from app.routes.trppu_scenario.statuts import STATUTS_EDITABLES
from app.routes.trppu_trafics.errors import erreur_400, erreur_409, erreur_500
from app.services.api_log import ACTION_MAJ_SCENARIO, enregistrer_appel

logger = logging.getLogger(__name__)


async def controler_scenario_pivot(id_scenario: int, co_regate: str) -> dict[str, Any]:
    """Scénario existant (404), modifiable (409) et du même site que les trafics (400)."""
    scenario = await fetch_scenario_or_404(id_scenario)

    if scenario["statut"] not in STATUTS_EDITABLES:
        logger.warning(
            "Rejet lecture trafics pivot %s",
            ctx(
                id_scenario=id_scenario,
                statut=scenario["statut"],
                http=409,
                motif="statut non éditable",
            ),
        )
        raise erreur_409(
            f"Date pivot non enregistrable : le scénario {id_scenario} est au statut "
            f"'{scenario['statut']}' (attendu : {' ou '.join(STATUTS_EDITABLES)})."
        )

    co_regate_scenario = (scenario.get("co_regate") or "").strip()
    if co_regate_scenario != co_regate.strip():
        logger.warning(
            "Rejet lecture trafics pivot %s",
            ctx(
                id_scenario=id_scenario,
                co_regate=co_regate,
                co_regate_scenario=co_regate_scenario,
                http=400,
                motif="co_regate différent de celui du scénario",
            ),
        )
        raise erreur_400(
            f"co_regate {co_regate} ne correspond pas au site du scénario "
            f"{id_scenario} ({co_regate_scenario})."
        )
    return scenario


def _en_dates(plage: tuple[datetime, datetime] | None) -> tuple[date, date] | None:
    return (plage[0].date(), plage[1].date()) if plage else None


async def enregistrer_pivot_scenario(
    scenario: dict[str, Any],
    dt_pivot: datetime,
    reel_range: tuple[datetime, datetime] | None,
    prev_range: tuple[datetime, datetime] | None,
) -> dict[str, Any]:
    """Écrit dt_pivot + bornes réalisé/prév ; retourne les valeurs écrites (réponse API)."""
    id_scenario = scenario["id_scenario"]
    pivot = dt_pivot.date()
    reel = _en_dates(reel_range)
    prev = _en_dates(prev_range)
    valeurs = {
        "dt_pivot": pivot,
        "periode_realise_debut": reel[0] if reel else None,
        "periode_realise_fin": reel[1] if reel else None,
        "periode_prev_debut": prev[0] if prev else None,
        "periode_prev_fin": prev[1] if prev else None,
    }

    try:
        async with db_write.transaction() as tx:
            rows_maj = await enregistrer_pivot(tx, id_scenario, pivot, reel, prev)
    except Exception as e:
        logger.exception(
            "Erreur enregistrement pivot scénario %s",
            ctx(id_scenario=id_scenario, **valeurs),
        )
        raise erreur_500(
            "Trafics récupérés mais erreur lors de l'enregistrement de la date pivot "
            "du scénario."
        ) from e

    logger.info(
        "Pivot scénario enregistré %s",
        ctx(id_scenario=id_scenario, rows_affected=rows_maj, **valeurs),
    )
    await enregistrer_appel(
        api_name=ACTION_MAJ_SCENARIO,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={"cible": "pivot", **{k: v and v.isoformat() for k, v in valeurs.items()}},
    )
    return valeurs
