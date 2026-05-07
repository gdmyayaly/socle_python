"""Petits utilitaires de logging partagés."""

from __future__ import annotations

from typing import Any


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
