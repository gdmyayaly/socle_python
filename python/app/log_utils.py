"""Petits utilitaires de logging partagés."""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Any

# Longueur max retenue pour l'id de session IHM : l'IHM envoie un UUID (36
# caractères), on borne pour qu'un query param arbitraire ne gonfle pas chaque
# ligne de log.
ID_SESSION_IHM_MAX_LEN = 64

# Id de session IHM de la requête courante (cf. session-ihm.interceptor.ts côté
# front, qui le pose en query param sur tous les appels /trppu-api).
# Alimenté une fois par requête par le middleware HTTP de `app.main`, puis relu
# par `JsonFormatter` pour l'ajouter à *chaque* ligne de log — y compris celles
# émises hors des endpoints (middleware, handlers d'exception, erreurs SQL).
# Les ContextVar sont isolés par tâche asyncio (une tâche par requête) et sont
# propagés par `run_in_threadpool`, donc les logs des appels synchrones
# (Databricks) conservent l'identifiant.
_id_session_ihm: ContextVar[str | None] = ContextVar("id_session_ihm", default=None)


def set_id_session_ihm(value: str | None) -> Token:
    """Pose l'id de session IHM du contexte courant et retourne le token de reset.

    Normalise l'entrée : valeur vide ou blanche -> None, valeur tronquée à
    `ID_SESSION_IHM_MAX_LEN`.
    """
    if value is not None:
        value = value.strip()[:ID_SESSION_IHM_MAX_LEN] or None
    return _id_session_ihm.set(value)


def get_id_session_ihm() -> str | None:
    """Id de session IHM du contexte courant, None hors requête HTTP."""
    return _id_session_ihm.get()


def reset_id_session_ihm(token: Token) -> None:
    """Restaure la valeur précédente du contexte (à appeler en fin de requête)."""
    _id_session_ihm.reset(token)


def safe_preview(obj: Any, max_len: int = 500) -> str:
    """Représentation tronquée d'un payload pour les logs.

    - Évite de dump des MB en cas de gros body.
    - Encapsule `repr()` pour ne jamais lever d'exception.
    """
    try:
        s = repr(obj)
    except Exception:
        s = "<unrepresentable>"
    if len(s) <= max_len:
        return s
    return s[:max_len] + f"...[truncated {len(s) - max_len} chars]"
