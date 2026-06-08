"""Client de l'API jours fermés (tournées) — récupère les jours fériés / fermés.

Remplace l'ancienne table `trppu_jours_feries`. La source de vérité est désormais
l'API externe :

- ``GET {base}/get?annee=2025`` — jours fermés d'une année
- ``GET {base}/get``            — tous les jours fermés

Réponse (tableau) : seul ``ymd_date`` (``YYYY-MM-DD``) est exploité ; le champ
``action`` et les autres champs sont ignorés (toute entrée = jour fermé).

Cache : in-memory par année avec TTL (cf. ``JOURS_FERMES_CACHE_TTL``) pour éviter
de retaper l'API à chaque calcul de jours.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import date

import httpx

from app.config import (
    JOURS_FERMES_API_BASE_URL,
    JOURS_FERMES_API_CA_BUNDLE,
    JOURS_FERMES_API_PASSWORD,
    JOURS_FERMES_API_TIMEOUT,
    JOURS_FERMES_API_USERNAME,
    JOURS_FERMES_API_VERIFY_SSL,
    JOURS_FERMES_CACHE_TTL,
)

logger = logging.getLogger(__name__)


class JoursFermesAPIError(Exception):
    """L'API jours fermés est indisponible ou a renvoyé une réponse invalide."""


# Cache par année : {annee: (expiry_ts, feries)}
_cache: dict[int, tuple[float, set[date]]] = {}
_cache_lock = asyncio.Lock()


def _parse_feries(payload: object) -> set[date]:
    """Extrait l'ensemble des dates fermées depuis la réponse de l'API."""
    if not isinstance(payload, list):
        raise JoursFermesAPIError(
            f"Réponse jours fermés invalide : tableau attendu, reçu {type(payload).__name__}."
        )
    feries: set[date] = set()
    for item in payload:
        try:
            ymd = item["ymd_date"]  # ex: "2025-01-01" ; les autres champs sont ignorés
            feries.add(date.fromisoformat(ymd))
        except (KeyError, TypeError, ValueError) as exc:
            raise JoursFermesAPIError(
                f"Entrée jours fermés invalide ({item!r}) : {exc}"
            ) from exc
    return feries


async def fetch_feries_annee(annee: int) -> set[date]:
    """Jours fermés d'une année donnée (avec cache + TTL)."""
    now = time.time()
    async with _cache_lock:
        cached = _cache.get(annee)
        if cached is not None and cached[0] > now:
            return set(cached[1])

    if not JOURS_FERMES_API_BASE_URL:
        raise JoursFermesAPIError(
            "JOURS_FERMES_API_BASE_URL non configurée : impossible de récupérer les jours fermés."
        )

    url = f"{JOURS_FERMES_API_BASE_URL.rstrip('/')}/get"
    auth = (
        httpx.BasicAuth(JOURS_FERMES_API_USERNAME, JOURS_FERMES_API_PASSWORD)
        if JOURS_FERMES_API_USERNAME
        else None
    )
    # verify : False (désactivé) > bundle CA dédié > vérification standard (certifi)
    if not JOURS_FERMES_API_VERIFY_SSL:
        verify: bool | str = False
    elif JOURS_FERMES_API_CA_BUNDLE:
        verify = JOURS_FERMES_API_CA_BUNDLE
    else:
        verify = True
    try:
        async with httpx.AsyncClient(
            timeout=JOURS_FERMES_API_TIMEOUT, auth=auth, verify=verify
        ) as client:
            response = await client.get(url, params={"annee": annee})
            response.raise_for_status()
            payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("Appel API jours fermés échoué (annee=%s) : %s", annee, exc)
        raise JoursFermesAPIError(
            f"API jours fermés indisponible (annee={annee}) : {exc}"
        ) from exc

    feries = _parse_feries(payload)
    async with _cache_lock:
        _cache[annee] = (time.time() + JOURS_FERMES_CACHE_TTL, set(feries))
    return feries


async def fetch_feries_between(debut: date, fin: date) -> set[date]:
    """Jours fermés entre deux dates incluses (agrège les années couvertes)."""
    feries: set[date] = set()
    for annee in range(debut.year, fin.year + 1):
        feries |= await fetch_feries_annee(annee)
    return {f for f in feries if debut <= f <= fin}
