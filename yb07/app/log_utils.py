"""Utilitaires de logging : identifiant de corrélation (ContextVar) et bloc `ctx(...)`.

Grammaire des messages : `docs/CONVENTION-LOGS.md`.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Any

# --- Identifiant de corrélation ---------------------------------------------
# Posé en début de traitement, relu par `JsonFormatter` sur chaque ligne, y compris
# celles des couches basses ; isolé par tâche asyncio.
_id_traitement: ContextVar[int | None] = ContextVar("id_traitement", default=None)


def set_id_traitement(value: int | None) -> Token:
    """Pose l'identifiant (non entier -> None) et retourne le token de reset."""
    if value is not None:
        try:
            value = int(value)
        except (TypeError, ValueError):
            value = None
    return _id_traitement.set(value)


def get_id_traitement() -> int | None:
    """Identifiant du contexte courant, None hors traitement."""
    return _id_traitement.get()


def reset_id_traitement(token: Token) -> None:
    """Restaure la valeur précédente du contexte (à appeler en fin de traitement)."""
    _id_traitement.reset(token)


def safe_preview(obj: Any, max_len: int = 500) -> str:
    """`repr()` tronqué pour les logs, sans jamais lever."""
    try:
        s = repr(obj)
    except Exception:
        s = "<unrepresentable>"
    if len(s) <= max_len:
        return s
    return s[:max_len] + f"...[truncated {len(s) - max_len} chars]"


# --- Bloc de contexte des messages ------------------------------------------
#
# Grammaire :
#     Début  <action> (<cle>=<valeur>, …)
#     Fin    <action> (<cle>=<valeur>, …, duration_ms=<f>)
#     Rejet  <action> (<cle>=<valeur>, …, motif=…)
#     Erreur <action> (<cle>=<valeur>, …)      # toujours via logger.exception

# Longueur max d'une valeur dans un bloc de contexte.
CTX_VALEUR_MAX_LEN = 300


def _rendre_valeur(valeur: Any) -> str:
    """Rend une valeur sans jamais lever : scalaires tels quels, le reste via `safe_preview`."""
    if isinstance(valeur, float):
        return f"{valeur:.1f}"
    if isinstance(valeur, (bool, int)):
        return str(valeur)
    if isinstance(valeur, str):
        if len(valeur) <= CTX_VALEUR_MAX_LEN:
            return valeur
        return valeur[:CTX_VALEUR_MAX_LEN] + f"...[tronqué {len(valeur) - CTX_VALEUR_MAX_LEN} car.]"
    return safe_preview(valeur, max_len=CTX_VALEUR_MAX_LEN)


class _Contexte:
    """Bloc `(cle=valeur, …)` rendu paresseusement, seulement si le log est émis."""

    __slots__ = ("_champs",)

    def __init__(self, champs: dict[str, Any]) -> None:
        self._champs = champs

    def __str__(self) -> str:
        try:
            rendus = [
                f"{cle}={_rendre_valeur(valeur)}"
                for cle, valeur in self._champs.items()
                if valeur is not None
            ]
        except Exception:  # un __repr__ exotique ne doit jamais casser un log
            return "(contexte illisible)"
        return "(" + ", ".join(rendus) + ")"

    __repr__ = __str__


def ctx(**champs: Any) -> _Contexte:
    """Bloc de contexte de log (ordre préservé, `None` omis, valeurs longues tronquées).

    À passer en argument de `%s`, jamais concaténé : sinon le rendu paresseux est perdu.
    """
    return _Contexte(champs)


__all__ = [
    "CTX_VALEUR_MAX_LEN",
    "ctx",
    "get_id_traitement",
    "reset_id_traitement",
    "safe_preview",
    "set_id_traitement",
]
