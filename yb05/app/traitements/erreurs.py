"""Erreurs métier des traitements YB05."""

from __future__ import annotations


class TraitementImpossible(RuntimeError):
    """Donnée manquante ou hors schéma : on échoue plutôt que de produire un trafic faux.

    Le message, destiné à l'exploitant, doit dire quoi corriger.
    """


__all__ = ["TraitementImpossible"]
