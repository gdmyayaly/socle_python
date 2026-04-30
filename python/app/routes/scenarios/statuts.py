"""Machine à états des scénarios + effets de bord (verrouillage, snapshot PIC)."""

import logging
from typing import Any

from app.routes.scenarios.helpers import err

logger = logging.getLogger(__name__)

STATUTS = ("EN COURS", "SIMULATION", "VALIDE", "VERROUILLE", "ARCHIVE")

ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "EN COURS":   {"SIMULATION", "ARCHIVE"},
    "SIMULATION": {"VALIDE", "EN COURS", "ARCHIVE"},
    "VALIDE":     {"VERROUILLE", "SIMULATION", "ARCHIVE"},
    "VERROUILLE": {"ARCHIVE"},
    "ARCHIVE":    set(),
}


def assert_transition_allowed(current: str, target: str) -> None:
    """Lève 422 si la transition n'est pas autorisée."""
    if target not in STATUTS:
        raise err(422, f"Statut '{target}' inconnu. Valeurs : {', '.join(STATUTS)}.")
    if current == target:
        raise err(422, f"Le scénario est déjà au statut '{current}'.")
    allowed = ALLOWED_TRANSITIONS.get(current, set())
    if target not in allowed:
        allowed_str = ", ".join(sorted(allowed)) if allowed else "(aucune)"
        raise err(
            422,
            f"Transition '{current}' → '{target}' interdite. "
            f"Transitions autorisées depuis '{current}' : {allowed_str}.",
        )


async def _snapshot_pic_coeffs(tx, id_scenario: int, id_pic_version: int) -> int:
    """Copie les coefficients PIC actifs vers trppu_scenario_pic_coeffs.

    Pour chaque (co_produit, jour_semaine), on prend la ligne avec la `dt_effet`
    la plus récente (règle par défaut — voir analyse_db_scenario.md §9.3).

    Retourne le nombre de lignes copiées.
    """
    # Idempotence : on purge un éventuel snapshot précédent (re-verrouillage).
    await tx.execute(
        "DELETE FROM trppu_scenario_pic_coeffs WHERE id_scenario = %s",
        (id_scenario,),
    )
    await tx.execute(
        """
        INSERT INTO trppu_scenario_pic_coeffs
            (id_scenario, co_produit, jour_semaine,
             coef_dense, coef_faible1, coef_faible2)
        SELECT %s, pc.co_produit, pc.jour_semaine,
               pc.coef_dense, pc.coef_faible1, pc.coef_faible2
        FROM trppu_pic_coefficients pc
        INNER JOIN (
            SELECT id_pic_version, co_produit, jour_semaine,
                   MAX(dt_effet) AS dt_effet_max
            FROM trppu_pic_coefficients
            WHERE id_pic_version = %s
            GROUP BY id_pic_version, co_produit, jour_semaine
        ) latest
          ON latest.id_pic_version = pc.id_pic_version
         AND latest.co_produit = pc.co_produit
         AND latest.jour_semaine = pc.jour_semaine
         AND latest.dt_effet_max = pc.dt_effet
        WHERE pc.id_pic_version = %s
        """,
        (id_scenario, id_pic_version, id_pic_version),
    )
    row = await tx.fetch_one(
        "SELECT COUNT(*) AS n FROM trppu_scenario_pic_coeffs WHERE id_scenario = %s",
        (id_scenario,),
    )
    return int(row["n"])


async def apply_transition_side_effects(
    tx, scenario: dict[str, Any], target: str
) -> dict[str, Any]:
    """Applique les effets de bord d'une transition validée.

    Retourne un dict d'informations complémentaires (ex. nb de coefs snapshotés).
    """
    info: dict[str, Any] = {}
    id_scenario = scenario["id_scenario"]

    if target == "VALIDE":
        # Pose dt_validation si pas déjà rempli
        await tx.execute(
            "UPDATE trppu_scenario "
            "SET statut = %s, "
            "    dt_validation = COALESCE(dt_validation, NOW()) "
            "WHERE id_scenario = %s",
            (target, id_scenario),
        )
        return info

    if target == "VERROUILLE":
        # Snapshot des coefs PIC + figeage
        n = await _snapshot_pic_coeffs(tx, id_scenario, scenario["id_pic_version"])
        info["pic_coeffs_snapshotes"] = n
        await tx.execute(
            "UPDATE trppu_scenario "
            "SET statut = %s, est_fige = TRUE, dt_mise_en_prod = NOW() "
            "WHERE id_scenario = %s",
            (target, id_scenario),
        )
        return info

    # Cas génériques (SIMULATION, EN COURS, ARCHIVE)
    await tx.execute(
        "UPDATE trppu_scenario SET statut = %s WHERE id_scenario = %s",
        (target, id_scenario),
    )
    return info
