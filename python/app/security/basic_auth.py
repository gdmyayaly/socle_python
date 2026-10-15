"""Authentification HTTP Basic des routes de l'API.

Identifiants : variables d'environnement / config `TRPPU_USER` et `TRPPU_SECRET`.
- **Les deux vides** (valeur par défaut) : authentification DÉSACTIVÉE, toutes les
  requêtes passent (usage local / tests). Un WARNING le signale au démarrage.
- **Un seul des deux renseigné** : configuration incomplète, toutes les requêtes
  sont refusées (401) plutôt que d'accepter un mot de passe ou un login vide.
- Sinon : l'en-tête `Authorization: Basic …` doit porter exactement ces identifiants.

Identifiants en ASCII uniquement : `HTTPBasic` (FastAPI) décode l'en-tête en ASCII
et répond lui-même 401 sur tout autre caractère, avant cette dépendance.

La comparaison se fait en temps constant (`secrets.compare_digest`), login et mot de
passe toujours comparés tous les deux pour ne pas révéler lequel est faux.

Appliqué dans `app/main.py` via `dependencies=[Depends(verifier_basic_auth)]` sur les
routers métier ; `/`, `/health*` et la documentation Swagger restent ouverts.
"""

from __future__ import annotations

import logging
import secrets

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from app.config import TRPPU_SECRET, TRPPU_USER
from app.log_utils import ctx

logger = logging.getLogger(__name__)

# auto_error=False : l'absence d'en-tête est traitée (et tracée) ici, pas par FastAPI.
_basic = HTTPBasic(auto_error=False)


def auth_desactivee() -> bool:
    """Vrai si aucun identifiant n'est configuré."""
    return not TRPPU_USER and not TRPPU_SECRET


def _identifiants_valides(credentials: HTTPBasicCredentials) -> bool:
    if not TRPPU_USER or not TRPPU_SECRET:
        return False
    user_ok = secrets.compare_digest(
        credentials.username.encode("utf-8"), TRPPU_USER.encode("utf-8")
    )
    secret_ok = secrets.compare_digest(
        credentials.password.encode("utf-8"), TRPPU_SECRET.encode("utf-8")
    )
    return user_ok and secret_ok


def verifier_basic_auth(
    request: Request,
    credentials: HTTPBasicCredentials | None = Depends(_basic),
) -> None:
    """Dépendance FastAPI : lève 401 si les identifiants Basic sont absents ou faux."""
    if auth_desactivee():
        return
    if credentials is None:
        motif = "identifiants absents"
    elif not _identifiants_valides(credentials):
        motif = "identifiants invalides"
    else:
        return
    logger.warning(
        "Rejet authentification %s",
        ctx(methode=request.method, path=request.url.path, http=401, motif=motif),
    )
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentification requise",
        headers={"WWW-Authenticate": "Basic"},
    )
