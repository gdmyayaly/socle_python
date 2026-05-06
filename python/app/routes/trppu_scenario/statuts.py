"""Machine à états des scénarios + effets de bord automatiques."""

from typing import Any

from fastapi import HTTPException

STATUTS = ("BROUILLON", "SIMULATION", "VALIDE", "PRODUCTION", "ARCHIVE")

ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "BROUILLON":  {"SIMULATION", "ARCHIVE"},
    "SIMULATION": {"VALIDE", "BROUILLON", "ARCHIVE"},
    "VALIDE":     {"PRODUCTION", "SIMULATION", "ARCHIVE"},
    "PRODUCTION": {"ARCHIVE"},
    "ARCHIVE":    set(),
}


def assert_transition_allowed(current: str, target: str) -> None:
    """Lève HTTP 422 si la transition n'est pas autorisée."""
    if target not in STATUTS:
        raise HTTPException(
            status_code=422,
            detail=f"Statut '{target}' inconnu. Valeurs : {', '.join(STATUTS)}.",
        )
    if current == target:
        raise HTTPException(
            status_code=422,
            detail=f"Le scénario est déjà au statut '{current}'.",
        )
    allowed = ALLOWED_TRANSITIONS.get(current, set())
    if target not in allowed:
        allowed_str = ", ".join(sorted(allowed)) if allowed else "(aucune)"
        raise HTTPException(
            status_code=422,
            detail=(
                f"Transition '{current}' → '{target}' interdite. "
                f"Transitions autorisées depuis '{current}' : {allowed_str}."
            ),
        )


async def apply_transition_side_effects(tx, scenario: dict[str, Any], target: str) -> None:
    """Applique l'UPDATE du statut + effets de bord (dt_validation, dt_mise_en_prod, est_fige).

    - VALIDE : pose dt_validation = NOW() si NULL.
    - PRODUCTION : pose dt_mise_en_prod = NOW(), est_fige = 1, et dt_validation si NULL
      (pour respecter chk_scen_prod : dt_mise_en_prod >= dt_validation).
    - autres : juste l'UPDATE du statut.
    """
    id_scenario = scenario["id_scenario"]

    if target == "VALIDE":
        await tx.execute(
            "UPDATE trppu_scenario SET statut = %s, "
            "dt_validation = COALESCE(dt_validation, NOW()) "
            "WHERE id_scenario = %s",
            (target, id_scenario),
        )
    elif target == "PRODUCTION":
        await tx.execute(
            "UPDATE trppu_scenario SET statut = %s, "
            "dt_validation = COALESCE(dt_validation, NOW()), "
            "dt_mise_en_prod = NOW(), est_fige = 1 "
            "WHERE id_scenario = %s",
            (target, id_scenario),
        )
    else:
        await tx.execute(
            "UPDATE trppu_scenario SET statut = %s WHERE id_scenario = %s",
            (target, id_scenario),
        )
