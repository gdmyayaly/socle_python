"""Petits utilitaires de logging partagés.

Portage réduit de `python/app/log_utils.py` (module YS04). La grammaire des messages
est commune aux deux modules — cf. `python/api_docs/CONVENTION-LOGS.md` et, pour les
écarts propres au batch, `yb05/docs/CONVENTION-LOGS.md`.

Ce qui n'est **pas** porté : `params_loggables`, `CHAMPS_SENSIBLES` et `diff_champs`.
YB05 ne manipule aucun `id_rh` — la contrainte de non-journalisation qui les motive
côté API ne s'applique pas ici, et les états avant/après sont déjà portés par
`Rapport.etats`.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Any

# --- Identifiant de corrélation ---------------------------------------------
#
# YB05 est un batch : il n'a pas d'`id_session_ihm` comme l'API YS04. Son pendant
# est le scénario en cours de traitement.
#
# En mode ALL, `NB_WORKER` scénarios sont traités par des tâches asyncio
# concurrentes et leurs lignes de log s'entrelacent. Le ContextVar est posé une
# fois par le worker (cf. `orchestrateur._worker`) puis relu par `JsonFormatter`
# pour l'ajouter à *chaque* ligne — y compris celles émises par les couches
# basses (`app.db.mysql`), qui n'ont aucun moyen de connaître le scénario.
# Les ContextVar sont isolés par tâche asyncio : un worker ne voit jamais le
# scénario d'un autre.
_id_scenario: ContextVar[int | None] = ContextVar("id_scenario", default=None)


def set_id_scenario(value: int | None) -> Token:
    """Pose le scénario du contexte courant et retourne le token de reset.

    Normalise l'entrée : une valeur non convertible en entier vaut `None` plutôt
    que de faire échouer le traitement qu'elle ne fait qu'annoter.
    """
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
#     Rejet  <action> (<cle>=<valeur>, …, verdict=…, motif=…)
#     Erreur <action> (<cle>=<valeur>, …)      # toujours via logger.exception

# Longueur max retenue pour une valeur rendue dans un bloc de contexte. Plus courte
# que le `safe_preview` par défaut : un contexte agrège plusieurs valeurs sur une
# même ligne.
CTX_VALEUR_MAX_LEN = 300


def _rendre_valeur(valeur: Any) -> str:
    """Rend une valeur pour un bloc de contexte, sans jamais lever.

    Les scalaires sont rendus tels quels (un `id_scenario` ne doit pas se retrouver
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

        logger.info("Fin calcul trafics PDI %s", ctx(lignes=1240, duration_ms=8421.0))
        -> "Fin calcul trafics PDI (lignes=1240, duration_ms=8421.0)"

    À passer en **argument** de `%s`, jamais concaténé au message : sinon le rendu
    paresseux est perdu.
    """
    return _Contexte(champs)


__all__ = [
    "CTX_VALEUR_MAX_LEN",
    "ctx",
    "get_id_scenario",
    "reset_id_scenario",
    "safe_preview",
    "set_id_scenario",
]
