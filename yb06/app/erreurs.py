"""Erreur commune aux traitements."""


class TraitementImpossible(RuntimeError):
    """Levée dès qu'une donnée manque ou sort du domaine attendu.

    On lève plutôt que de réparer silencieusement : une valeur rafistolée se propage dans
    tous les calculs qui suivent, sans laisser de trace. Le message est destiné à
    l'exploitant — il doit dire quoi corriger.
    """
