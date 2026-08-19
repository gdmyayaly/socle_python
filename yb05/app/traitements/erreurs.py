"""Erreurs métier des traitements YB05."""

from __future__ import annotations


class TraitementImpossible(RuntimeError):
    """Le traitement ne peut pas aboutir sur des données correctes.

    Levée plutôt que corrigée à la volée dès qu'une donnée manque ou dépasse ce que le schéma
    accepte : un trafic faux se propage à tout le calcul des scénarios et ne se voit plus.
    Le message est destiné à l'exploitant — il doit dire quoi corriger.
    """


__all__ = ["TraitementImpossible"]
