"""Erreur commune aux traitements."""


class TraitementImpossible(RuntimeError):
    """Donnée manquante ou hors domaine : on lève plutôt que de réparer en silence."""
