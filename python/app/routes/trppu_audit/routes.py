"""Audit id_rh : retrouve toutes les actions d'un id_rh enregistrées en base.

POST /trppu-api/audit/actions-id-rh : reçoit un id_rh **chiffré** + la **clé de
déchiffrement**, déchiffre les id_rh stockés dans toutes les tables porteuses et
renvoie la liste des actions correspondantes.

⚠️ Endpoint d'**administration / traçabilité** : il déchiffre des données et exige la
clé de déchiffrement. La clé et l'id_rh en clair ne sont **jamais journalisés**.
"""

import logging
import time
from datetime import datetime

from cryptography.fernet import Fernet
from fastapi import APIRouter, HTTPException

from app.db.mysql import db_read
from app.security.crypto import build_fernet

from .helpers import collect_actions, looks_like_fernet, safe_decrypt
from .schemas import AuditOut, AuditRequest

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/audit", tags=["Audit id_rh"])


@router.post("/actions-id-rh", response_model=AuditOut)
async def actions_id_rh(payload: AuditRequest):
    """Retourne toutes les actions en base de l'id_rh fourni (chiffré) via la clé donnée."""
    start = time.perf_counter()
    logger.info("Début audit id_rh (recherche des actions)")  # ni clé ni id_rh loggés

    # 1. Construire l'instance Fernet depuis la clé fournie.
    try:
        fernet: Fernet = build_fernet(payload.cle)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    # 2. Résoudre l'id_rh cible (déchiffrer le token fourni).
    clear_target, ok = safe_decrypt(fernet, payload.id_rh)
    if not ok and looks_like_fernet(payload.id_rh):
        # Le token ressemble à du Fernet mais n'a pas pu être déchiffré -> clé incorrecte.
        raise HTTPException(
            status_code=400,
            detail="Clé de déchiffrement incorrecte : le token id_rh fourni n'a pas pu être déchiffré.",
        )
    if not clear_target:
        raise HTTPException(status_code=400, detail="id_rh fourni invalide ou vide.")

    # 3. Balayer les tables et collecter les actions.
    try:
        actions = await collect_actions(db_read, fernet, clear_target)
    except Exception as e:
        logger.exception("Erreur audit id_rh (collecte des actions)")
        raise HTTPException(status_code=500, detail="Erreur lors de l'audit id_rh.") from e

    # Tri chronologique décroissant (actions sans date traitées comme les plus anciennes).
    actions.sort(key=lambda a: a["date"] or datetime.min, reverse=True)

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Audit id_rh terminé (nb_actions=%d, duration_ms=%.1f)", len(actions), duration_ms
    )
    return AuditOut(id_rh=clear_target, nb_actions=len(actions), actions=actions)
