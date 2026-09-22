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

# Le figement (est_fige = 1) n'est posé que par la mise en production, qui passe
# exclusivement par OPTIPACC (DSR-707). L'IHM ne pilote plus le figement : le
# mapping statut -> est_fige de DSR-669 (PATCH /{id}/figement) a été retiré, il
# permettait de figer un scénario hors production et le rendait non modifiable.
# Cf. api_docs/dsr/resolutions/DSR-669_resolution.md.

# Transitions accessibles via PATCH /statut.
# La transition VALIDE -> EN PRODUCTION est volontairement absente : elle passe
# uniquement par POST /mise-en-prod (cf. INTERNAL_TRANSITIONS).
ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "EN COURS":      {"SIMULATION", "VALIDE", "ARCHIVE"},
    "SIMULATION":    {"EN COURS", "VALIDE", "ARCHIVE"},
    "VALIDE":        {"EN COURS", "SIMULATION", "ARCHIVE"},
    "EN PRODUCTION": {"ARCHIVE"},
    "ARCHIVE":       set(),
}

# Transitions internes, déclenchables uniquement par leur endpoint dédié.
INTERNAL_TRANSITIONS: dict[str, set[str]] = {
    "VALIDE": {"EN PRODUCTION"},
}


def _internal_route_for(current: str, target: str) -> str | None:
    """Retourne le nom de l'endpoint dédié si la transition n'est accessible que par lui.

    Deux routes mènent à EN PRODUCTION : celle de l'IHM (date = NOW()) et celle
    d'OPTIPACC (DSR-707, date de mise en œuvre fournie par l'appelant). Les deux
    sont citées pour que le message d'erreur reste actionnable quel que soit
    l'appelant.
    """
    if current == "VALIDE" and target == "EN PRODUCTION":
        return (
            "POST /trppu-api/scenarios/{id_scenario}/mise-en-prod"
            " ou POST /trppu-api/optipacc/scenario/mise-en-production"
        )
    return None


def assert_transition_allowed(current: str, target: str) -> None:
    """Lève HTTP 422 si la transition n'est pas autorisée via PATCH /statut.

    Si la transition existe en interne (cf. INTERNAL_TRANSITIONS), renvoie un
    message indiquant la route dédiée à utiliser.
    """
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

    if target in INTERNAL_TRANSITIONS.get(current, set()):
        route = _internal_route_for(current, target)
        logger.warning(
            "Rejet transition statut %s",
            ctx(
                depuis=current,
                vers=target,
                route_dediee=route,
                http=409,
                motif="transition réservée à une route dédiée",
            ),
        )
        raise HTTPException(
            status_code=409,
            detail=(
                f"Transition '{current}' -> '{target}' non accessible via PATCH /statut. "
                f"Utilisez : {route}."
            ),
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


def assert_internal_transition_allowed(current: str, target: str) -> None:
    """Variante pour les routes internes (/mise-en-prod) : autorise les transitions
    listées dans INTERNAL_TRANSITIONS en plus des transitions publiques.
    """
    if target not in STATUTS:
        logger.warning(
            "Rejet transition statut interne %s",
            ctx(depuis=current, vers=target, http=422, motif="statut cible inconnu"),
        )
        raise HTTPException(
            status_code=422,
            detail=f"Statut '{target}' inconnu. Valeurs : {', '.join(STATUTS)}.",
        )
    if current == target:
        logger.warning(
            "Rejet transition statut interne %s",
            ctx(depuis=current, vers=target, http=422, motif="statut déjà courant"),
        )
        raise HTTPException(
            status_code=422,
            detail=f"Le scénario est déjà au statut '{current}'.",
        )
    public = ALLOWED_TRANSITIONS.get(current, set())
    internal = INTERNAL_TRANSITIONS.get(current, set())
    if target not in (public | internal):
        allowed_str = ", ".join(sorted(public | internal)) if (public | internal) else "(aucune)"
        logger.warning(
            "Rejet transition statut interne %s",
            ctx(
                depuis=current,
                vers=target,
                autorisees=sorted(public | internal),
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

    - VALIDE : pose dt_validation = NOW() si NULL.
    - EN PRODUCTION : pose dt_mise_en_prod = NOW(), est_fige = 1, et dt_validation si NULL
      (pour respecter chk_scen_prod : dt_mise_en_prod >= dt_validation).
    - autres : juste l'UPDATE du statut.
    """
    id_scenario = scenario["id_scenario"]

    if target == "VALIDE":
        effets = ["dt_validation si NULL"]
        rows = await tx.execute(
            "UPDATE trppu_scenario SET statut = %s, "
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
