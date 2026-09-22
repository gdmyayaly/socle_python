"""Petits utilitaires de logging partagés.

Deux briques : un identifiant de corrélation propagé par ContextVar et ajouté à chaque
ligne par `JsonFormatter`, et le bloc de contexte `ctx(...)` qui normalise la partie
`(cle=valeur, …)` des messages. La grammaire attendue est décrite dans
`docs/CONVENTION-LOGS.md`.

Rien ici ne connaît le métier du module : les deux briques sont utilisables telles quelles.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Any

# --- Identifiant de corrélation ---------------------------------------------
#
# Un batch n'a pas de session HTTP : son pendant est l'unité de travail en cours
# (un identifiant d'enregistrement, de lot, de traitement…).
#
# Quand plusieurs unités sont traitées par des tâches asyncio concurrentes, leurs
# lignes de log s'entrelacent. Le ContextVar est posé une fois en début de
# traitement puis relu par `JsonFormatter` pour l'ajouter à *chaque* ligne — y
# compris celles émises par les couches basses (`app.db.mysql`), qui n'ont aucun
# moyen de connaître l'unité en cours. Les ContextVar sont isolés par tâche
# asyncio : une tâche ne voit jamais l'identifiant d'une autre.
_id_traitement: ContextVar[int | None] = ContextVar("id_traitement", default=None)


def set_id_traitement(value: int | None) -> Token:
    """Pose l'identifiant du contexte courant et retourne le token de reset.

    Normalise l'entrée : une valeur non convertible en entier vaut `None` plutôt
    que de faire échouer le traitement qu'elle ne fait qu'annoter.
    """
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
    """Représentation tronquée d'un objet pour les logs.

    - Évite de dump des Mo en cas de gros volume.
    - Encapsule `repr()` pour ne jamais lever.
    """
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

# Longueur max retenue pour une valeur rendue dans un bloc de contexte. Plus courte
# que le `safe_preview` par défaut : un contexte agrège plusieurs valeurs sur une
# même ligne.
CTX_VALEUR_MAX_LEN = 300


def _rendre_valeur(valeur: Any) -> str:
    """Rend une valeur pour un bloc de contexte, sans jamais lever.

    Les scalaires sont rendus tels quels (un `id_traitement` ne doit pas se retrouver
    entre guillemets) ; tout le reste passe par `safe_preview`, qui borne la
    longueur et encapsule `repr()`.
    """
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
    """Bloc `(cle=valeur, …)` rendu paresseusement.

    Le rendu n'a lieu que si l'enregistrement est réellement émis : `logging`
    n'appelle `__str__` qu'au moment de construire le message. Passer par une
    simple fonction évaluerait le contexte même quand le niveau est désactivé —
    coûteux sur les `logger.debug` des étapes de calcul, qui ne sont visibles
    qu'avec `-v`.
    """

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
    """Bloc de contexte normalisé pour un message de log.

    Ordre des arguments préservé (identifiants d'abord, `duration_ms` en dernier),
    valeurs `None` omises, valeurs longues tronquées.

        logger.info("Fin chargement %s", ctx(lignes=1240, duration_ms=8421.0))
        -> "Fin chargement (lignes=1240, duration_ms=8421.0)"

    À passer en **argument** de `%s`, jamais concaténé au message : sinon le rendu
    paresseux est perdu.
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
