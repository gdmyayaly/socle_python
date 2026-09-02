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


# --- Convention de log (cf. api_docs/CONVENTION-LOGS.md) ---------------------
#
# Grammaire unique de `app_message`, une seule forme par phase :
#
#     Début  <action> (<cle>=<valeur>, ...)
#     Fin    <action> (<cle>=<valeur>, ..., duration_ms=<f>)
#     Rejet  <action> (<cle>=<valeur>, ..., http=<code>, motif=<texte>)
#     Erreur <action> (<cle>=<valeur>, ...)      # toujours via logger.exception
#
# « Fin » (et non « terminé/terminée ») supprime la dérive de genre qui rendait
# les messages non regroupables dans Kibana.

# Champs à ne jamais faire figurer dans un log.
#
# ⚠️ `encrypt_id_rh` est un *passthrough* quand `ID_RH_CRYPTO_KEY` est vide
# (cf. app/security/crypto.py) : une variable nommée `id_rh_token` n'est donc pas
# garantie chiffrée. Elle ne doit pas être loguée davantage que l'id_rh en clair.
CHAMPS_SENSIBLES: frozenset[str] = frozenset(
    {"id_rh", "id_rh_creation", "id_rh_maj", "cle"}
)

# Longueur max d'une valeur rendue dans un bloc de contexte. Plus court que le
# `safe_preview` par défaut : un contexte agrège plusieurs valeurs sur une ligne.
CTX_VALEUR_MAX_LEN = 300


def _rendre_valeur(valeur: Any) -> str:
    """Rend une valeur pour un bloc de contexte, sans jamais lever.

    Les scalaires sont rendus tels quels (un `id_scenario` ne doit pas se
    retrouver entre guillemets) ; tout le reste passe par `safe_preview`, qui
    borne la longueur et encapsule `repr()`.
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
    """Bloc `(cle=valeur, ...)` rendu paresseusement.

    Le rendu n'a lieu que si l'enregistrement est réellement émis : `logging`
    n'appelle `__str__` qu'au moment de construire le message. Passer par une
    simple fonction évaluerait le contexte même quand le niveau est désactivé —
    coûteux sur les `logger.debug(...)` des étapes intermédiaires.
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

    Ordre des arguments préservé (identifiants d'abord, `duration_ms` en
    dernier), valeurs `None` omises, valeurs longues tronquées.

        logger.info("Fin création scénario %s", ctx(id_scenario=52, duration_ms=84.2))
        -> "Fin création scénario (id_scenario=52, duration_ms=84.2)"
    """
    return _Contexte(champs)


def params_loggables(
    payload: Any, *, exclude: frozenset[str] | set[str] = CHAMPS_SENSIBLES
) -> dict[str, Any]:
    """Payload sérialisable pour les logs, privé de ses champs sensibles.

    Accepte un modèle Pydantic v2 ou un dict. Généralise l'idiome
    `payload.model_dump(mode="json", exclude={"id_rh"})` à tous les packages :
    la règle « ne jamais logger l'id_rh en clair » ne dépend plus du soin
    apporté à chaque appel.
    """
    if hasattr(payload, "model_dump"):
        donnees = payload.model_dump(mode="json")
    elif isinstance(payload, dict):
        donnees = dict(payload)
    else:
        return {"payload": safe_preview(payload)}
    return {cle: valeur for cle, valeur in donnees.items() if cle not in exclude}


def diff_champs(
    avant: dict[str, Any],
    apres: dict[str, Any],
    *,
    exclude: frozenset[str] | set[str] = CHAMPS_SENSIBLES,
) -> dict[str, list[Any]]:
    """`{champ: [avant, après]}` restreint aux champs réellement modifiés.

    Brique de reconstitution des UPDATE : les endpoints relisent déjà la ligne
    avant d'écrire, ce delta rend la modification rejouable depuis les seuls logs.
    Seules les clés présentes dans `apres` sont comparées (un UPDATE partiel ne
    doit pas faire apparaître les colonnes qu'il ne touche pas).
    """
    delta: dict[str, list[Any]] = {}
    for cle, valeur_apres in apres.items():
        if cle in exclude:
            continue
        valeur_avant = avant.get(cle)
        if valeur_avant != valeur_apres:
            delta[cle] = [valeur_avant, valeur_apres]
    return delta
