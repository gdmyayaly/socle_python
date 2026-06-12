"""Machine à états des scénarios + effets de bord automatiques."""

import unicodedata
from typing import Any

from fastapi import HTTPException

STATUTS = ("EN COURS", "VALIDE", "EN PRODUCTION", "ARCHIVE")

# DSR-669 : mapping d'un statut IHM reçu vers le flag de figement (est_fige).
# "validé"/"simulation" -> figé (True) ; "en cours" -> défigé (False).
# Clés normalisées (majuscules, sans accent) — cf. _normalize_statut.
FIGE_PAR_STATUT: dict[str, bool] = {
    "VALIDE": True,
    "SIMULATION": True,
    "EN COURS": False,
}


def _normalize_statut(statut: str) -> str:
    """Normalise un libellé de statut : majuscules, sans accent, espaces compactés."""
    sans_accent = "".join(
        c for c in unicodedata.normalize("NFD", statut) if unicodedata.category(c) != "Mn"
    )
    return " ".join(sans_accent.upper().split())


def resolve_fige_from_statut(statut: str) -> bool:
    """DSR-669 : déduit le figement (True/False) à partir du statut reçu.

    Insensible à la casse et aux accents. Lève HTTP 422 si le statut n'est pas
    reconnu (paramètre 'inconnu'), sans réaliser aucune action.
    """
    cle = _normalize_statut(statut)
    if cle not in FIGE_PAR_STATUT:
        attendus = ", ".join(sorted(FIGE_PAR_STATUT))
        raise HTTPException(
            status_code=422,
            detail=f"Statut '{statut}' inconnu. Valeurs acceptées : {attendus}.",
        )
    return FIGE_PAR_STATUT[cle]

# Transitions accessibles via PATCH /statut.
# La transition VALIDE -> EN PRODUCTION est volontairement absente : elle passe
# uniquement par POST /mise-en-prod (cf. INTERNAL_TRANSITIONS).
ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "EN COURS":      {"VALIDE", "ARCHIVE"},
    "VALIDE":        {"EN COURS", "ARCHIVE"},
    "EN PRODUCTION": {"ARCHIVE"},
    "ARCHIVE":       set(),
}

# Transitions internes, déclenchables uniquement par leur endpoint dédié.
INTERNAL_TRANSITIONS: dict[str, set[str]] = {
    "VALIDE": {"EN PRODUCTION"},
}


def _internal_route_for(current: str, target: str) -> str | None:
    """Retourne le nom de l'endpoint dédié si la transition n'est accessible que par lui."""
    if current == "VALIDE" and target == "EN PRODUCTION":
        return "POST /trppu-api/scenarios/{id_scenario}/mise-en-prod"
    return None


def assert_transition_allowed(current: str, target: str) -> None:
    """Lève HTTP 422 si la transition n'est pas autorisée via PATCH /statut.

    Si la transition existe en interne (cf. INTERNAL_TRANSITIONS), renvoie un
    message indiquant la route dédiée à utiliser.
    """
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

    if target in INTERNAL_TRANSITIONS.get(current, set()):
        route = _internal_route_for(current, target)
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
        raise HTTPException(
            status_code=422,
            detail=f"Statut '{target}' inconnu. Valeurs : {', '.join(STATUTS)}.",
        )
    if current == target:
        raise HTTPException(
            status_code=422,
            detail=f"Le scénario est déjà au statut '{current}'.",
        )
    public = ALLOWED_TRANSITIONS.get(current, set())
    internal = INTERNAL_TRANSITIONS.get(current, set())
    if target not in (public | internal):
        allowed_str = ", ".join(sorted(public | internal)) if (public | internal) else "(aucune)"
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
        await tx.execute(
            "UPDATE trppu_scenario SET statut = %s, "
            "dt_validation = COALESCE(dt_validation, NOW()) "
            "WHERE id_scenario = %s",
            (target, id_scenario),
        )
    elif target == "EN PRODUCTION":
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
