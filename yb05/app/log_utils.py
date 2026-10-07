"""Utilitaires de logging (portage réduit de `python/app/log_utils.py`).

Convention : `yb05/docs/CONVENTION-LOGS.md`.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Any

# --- Identifiant de corrélation ---------------------------------------------
#
# Scénario en cours, ajouté par `JsonFormatter` à chaque ligne (y compris `app.db.mysql`).
# Un ContextVar est isolé par tâche asyncio : les workers du mode ALL ne se mélangent pas.
_id_scenario: ContextVar[int | None] = ContextVar("id_scenario", default=None)


def set_id_scenario(value: int | None) -> Token:
    """Pose le scénario du contexte (non entier -> None) et retourne le token de reset."""
    if value is not None:
        try:
            value = int(value)
        except (TypeError, ValueError):
            value = None
    return _id_scenario.set(value)


def get_id_scenario() -> int | None:
    """Scénario du contexte courant, None hors traitement."""
    return _id_scenario.get()


def reset_id_scenario(token: Token) -> None:
    """Restaure la valeur précédente du contexte (à appeler en fin de traitement)."""
    _id_scenario.reset(token)


# --- Site du scénario ------------------------------------------------------------
#
# (co_regate, co_roc) du scénario en cours, posés dès qu'ils sont connus et relus par
# `JsonFormatter` : recherche Kibana par site.
_site: ContextVar[tuple[str | None, str | None]] = ContextVar("site", default=(None, None))


def _code(valeur: Any) -> str | None:
    """Code normalisé (texte sans blancs), None si absent : un log ne doit jamais lever."""
    if valeur is None:
        return None
    try:
        texte = str(valeur).strip()
    except Exception:
        return None
    return texte or None


def set_site(co_regate: Any, co_roc: Any) -> Token:
    """Pose le site et le ROC du contexte courant et retourne le token de reset."""
    return _site.set((_code(co_regate), _code(co_roc)))


def get_site() -> tuple[str | None, str | None]:
    """(co_regate, co_roc) du contexte courant, (None, None) hors traitement."""
    return _site.get()


def reset_site(token: Token) -> None:
    """Restaure le site précédent du contexte (à appeler en fin de traitement)."""
    _site.reset(token)


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
#     Rejet  <action> (<cle>=<valeur>, …, verdict=…, motif=…)
#     Erreur <action> (<cle>=<valeur>, …)      # toujours via logger.exception

# Longueur max d'une valeur de contexte (plusieurs valeurs par ligne).
CTX_VALEUR_MAX_LEN = 300


def _rendre_valeur(valeur: Any) -> str:
    """Scalaires tels quels (sans guillemets), le reste via `safe_preview` ; ne lève jamais."""
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
    """Bloc `(cle=valeur, …)` rendu paresseusement : rien n'est calculé si le niveau est coupé."""

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
    """Bloc de contexte d'un log (ordre préservé, None omis) ; à passer en argument de `%s`."""
    return _Contexte(champs)


__all__ = [
    "CTX_VALEUR_MAX_LEN",
    "ctx",
    "get_id_scenario",
    "get_site",
    "reset_id_scenario",
    "reset_site",
    "safe_preview",
    "set_id_scenario",
    "set_site",
]
