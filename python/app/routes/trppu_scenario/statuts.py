"""Machine à états des scénarios + effets de bord automatiques.

Chaque refus de transition est journalisé en WARNING avant de lever le HTTP :
sans cela, un 409/422 renvoyé à l'IHM ne laisse aucune trace côté serveur et
l'incident n'est pas rejouable (cf. api_docs/CONVENTION-LOGS.md).
"""

import logging
from typing import Any

from fastapi import HTTPException

from app.log_utils import ctx

logger = logging.getLogger(__name__)

# Ordre et valeurs alignés sur l'enum trppu_scenario.statut (db/db_new.sql).
STATUTS = ("EN COURS", "SIMULATION", "VALIDE", "EN PRODUCTION", "ARCHIVE")

# Statuts « de travail » : un scénario y est éditable et peut être validé.
# SIMULATION se comporte exactement comme EN COURS dans la machine à états.
STATUTS_EDITABLES = ("EN COURS", "SIMULATION")

# Le figement suit le statut : PATCH /statut fige le scénario à la validation et
# à la mise en production, et le défige au retour vers un statut de travail
# (cf. apply_transition_side_effects). Le batch YB05 ne calcule que les scénarios
# VALIDE et figés.

# Transitions accessibles via PATCH /statut. ARCHIVE n'est atteignable depuis
# aucun statut. VALIDE -> EN PRODUCTION impose en plus les contrôles DSR-707 C4/C5,
# appliqués par la route (assert_trafics_calcules, assert_aucun_scenario_en_production).
ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "EN COURS":      {"SIMULATION", "VALIDE"},
    "SIMULATION":    {"EN COURS", "VALIDE"},
    "VALIDE":        {"EN COURS", "SIMULATION", "EN PRODUCTION"},
    "EN PRODUCTION": set(),
    "ARCHIVE":       set(),
}


def assert_transition_allowed(current: str, target: str) -> None:
    """Lève HTTP 422 si la transition n'est pas autorisée via PATCH /statut."""
    if target not in STATUTS:
        logger.warning(
            "Rejet transition statut %s",
            ctx(depuis=current, vers=target, http=422, motif="statut cible inconnu"),
        )
        raise HTTPException(
            status_code=422,
            detail=f"Statut '{target}' inconnu. Valeurs : {', '.join(STATUTS)}.",
        )
    if current == target:
        logger.warning(
            "Rejet transition statut %s",
            ctx(depuis=current, vers=target, http=422, motif="statut déjà courant"),
        )
        raise HTTPException(
            status_code=422,
            detail=f"Le scénario est déjà au statut '{current}'.",
        )

    allowed = ALLOWED_TRANSITIONS.get(current, set())
    if target not in allowed:
        allowed_str = ", ".join(sorted(allowed)) if allowed else "(aucune)"
        logger.warning(
            "Rejet transition statut %s",
            ctx(
                depuis=current,
                vers=target,
                autorisees=sorted(allowed),
                http=422,
                motif="transition interdite",
            ),
        )
        raise HTTPException(
            status_code=422,
            detail=(
                f"Transition '{current}' -> '{target}' interdite. "
                f"Transitions autorisées depuis '{current}' : {allowed_str}."
            ),
        )


async def apply_transition_side_effects(tx, scenario: dict[str, Any], target: str) -> None:
    """Applique l'UPDATE du statut + effets de bord automatiques.

    - VALIDE : est_fige = 1, et pose dt_validation = NOW() si NULL.
    - EN PRODUCTION : pose dt_mise_en_prod = NOW(), est_fige = 1, et dt_validation si NULL
      (pour respecter chk_scen_prod : dt_mise_en_prod >= dt_validation).
    - EN COURS / SIMULATION : est_fige = 0.
    """
    id_scenario = scenario["id_scenario"]

    if target == "VALIDE":
        effets = ["dt_validation si NULL", "est_fige=1"]
        rows = await tx.execute(
            "UPDATE trppu_scenario SET statut = %s, est_fige = 1, "
            "dt_validation = COALESCE(dt_validation, NOW()) "
            "WHERE id_scenario = %s",
            (target, id_scenario),
        )
    elif target == "EN PRODUCTION":
        effets = ["dt_validation si NULL", "dt_mise_en_prod", "est_fige=1"]
        rows = await tx.execute(
            "UPDATE trppu_scenario SET statut = %s, "
            "dt_validation = COALESCE(dt_validation, NOW()), "
            "dt_mise_en_prod = NOW(), est_fige = 1 "
            "WHERE id_scenario = %s",
            (target, id_scenario),
        )
    elif target in STATUTS_EDITABLES:
        effets = ["est_fige=0"]
        rows = await tx.execute(
            "UPDATE trppu_scenario SET statut = %s, est_fige = 0 WHERE id_scenario = %s",
            (target, id_scenario),
        )
    else:
        effets = []
        rows = await tx.execute(
            "UPDATE trppu_scenario SET statut = %s WHERE id_scenario = %s",
            (target, id_scenario),
        )

    # Les effets de bord sont implicites côté appelant : sans cette ligne, une
    # colonne posée automatiquement (est_fige, dt_mise_en_prod) est intraçable.
    logger.info(
        "Effets de bord de transition appliqués %s",
        ctx(
            id_scenario=id_scenario,
            depuis=scenario.get("statut"),
            vers=target,
            effets=effets,
            rows_affected=rows,
        ),
    )
