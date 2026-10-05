"""Audit id_rh : retrouve toutes les actions d'un id_rh enregistrées en base.

POST /trppu-api/audit/actions-id-rh : reçoit un id_rh **chiffré** + la **clé de
déchiffrement**, déchiffre les id_rh stockés dans toutes les tables porteuses et
renvoie la liste des actions correspondantes.

POST /trppu-api/audit/agrebals-pdi (DSR-737) : Agrébals et PDI d'un scénario (ceux
réellement utilisés par son calcul) ou d'un site (données actives). Consultation seule.

⚠️ Endpoint d'**administration / traçabilité** : il déchiffre des données et exige la
clé de déchiffrement. La clé et l'id_rh en clair ne sont **jamais journalisés**.
"""

import logging
import time
from datetime import datetime

from cryptography.fernet import Fernet
from fastapi import APIRouter, HTTPException

from app.db.mysql import db_read
from app.log_utils import ctx, set_co_regate
from app.security.crypto import build_fernet

from .helpers import (
    SELECT_AGREBALS_PDI_SCENARIO_SQL,
    SELECT_AGREBALS_PDI_SITE_SQL,
    SELECT_SCENARIO_AUDIT_SQL,
    SELECT_SITE_AUDIT_SQL,
    agrebals_du_site,
    collect_actions,
    looks_like_fernet,
    regrouper_par_agrebal,
    safe_decrypt,
)
from .schemas import (
    TYPE_SCENARIO,
    AgrebalsPdiOut,
    AgrebalsPdiRequest,
    AuditOut,
    AuditRequest,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/audit", tags=["Audit"])


@router.post("/actions-id-rh", response_model=AuditOut)
async def actions_id_rh(payload: AuditRequest):
    """Retourne toutes les actions en base de l'id_rh fourni (chiffré) via la clé donnée."""
    start = time.perf_counter()
    # Message sans bloc de contexte : la clé de déchiffrement et l'id_rh sont les
    # deux seuls paramètres de cet endpoint, et tous deux sont sensibles.
    logger.info("Début audit id_rh")

    # 1. Construire l'instance Fernet depuis la clé fournie.
    try:
        fernet: Fernet = build_fernet(payload.cle)
    except ValueError as e:
        logger.warning(
            "Rejet audit id_rh %s",
            ctx(http=400, motif="clé de déchiffrement invalide"),
        )
        raise HTTPException(status_code=400, detail=str(e)) from e

    # 2. Résoudre l'id_rh cible (déchiffrer le token fourni).
    clear_target, ok = safe_decrypt(fernet, payload.id_rh)
    if not ok and looks_like_fernet(payload.id_rh):
        # Le token ressemble à du Fernet mais n'a pas pu être déchiffré -> clé incorrecte.
        logger.warning(
            "Rejet audit id_rh %s",
            ctx(http=400, motif="token id_rh non déchiffrable avec la clé fournie"),
        )
        raise HTTPException(
            status_code=400,
            detail="Clé de déchiffrement incorrecte : le token id_rh fourni n'a pas pu être déchiffré.",
        )
    if not clear_target:
        logger.warning(
            "Rejet audit id_rh %s", ctx(http=400, motif="id_rh vide ou invalide")
        )
        raise HTTPException(status_code=400, detail="id_rh fourni invalide ou vide.")

    # 3. Balayer les tables et collecter les actions.
    try:
        actions = await collect_actions(db_read, fernet, clear_target)
    except Exception as e:
        logger.exception("Erreur audit id_rh %s", ctx(etape="collecte des actions"))
        raise HTTPException(status_code=500, detail="Erreur lors de l'audit id_rh.") from e

    # Tri chronologique décroissant (actions sans date traitées comme les plus anciennes).
    actions.sort(key=lambda a: a["date"] or datetime.min, reverse=True)

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    # Les ressources concernées sont journalisables (ce ne sont que des noms de
    # tables) et disent quelles données l'utilisateur audité a touchées.
    logger.info(
        "Fin audit id_rh %s",
        ctx(
            nb_actions=len(actions),
            ressources=sorted({a["ressource"] for a in actions}),
            duration_ms=duration_ms,
        ),
    )
    return AuditOut(id_rh=clear_target, nb_actions=len(actions), actions=actions)


MESSAGE_SCENARIO_NON_CALCULE = (
    "Scénario non calculé : aucun Agrébal n'a encore servi à un calcul de trafic."
)
MESSAGE_SITE_SANS_AGREBAL = "Aucun Agrébal actif sur ce site."


@router.post("/agrebals-pdi", response_model=AgrebalsPdiOut)
async def agrebals_pdi(payload: AgrebalsPdiRequest):
    """DSR-737 : Agrébals et PDI associés à un scénario ou à un site.

    - **`idScenario`** — les Agrébals et PDI réellement utilisés par le calcul du
      scénario, lus dans `trppu_trafic_pdi` (photo du calcul YB05) : un Agrébal modifié
      ou supprimé depuis est restitué tel qu'il a servi (RG-003, CA-03). Un scénario
      pas encore calculé rend une liste vide et un `message`.
    - **`codeRegate`** — les Agrébals actifs du site (`agrebal_deleteddAt IS NULL`) et
      leurs PDI (RG-002).

    Consultation uniquement (RG-006) : trois SELECT au plus, sur `db_read`.
    """
    start = time.perf_counter()
    type_recherche = payload.type_recherche
    logger.info(
        "Début audit agrébals PDI %s",
        ctx(
            type_recherche=type_recherche,
            id_scenario=payload.id_scenario,
            co_regate=payload.code_regate,
        ),
    )

    message = None
    try:
        if type_recherche == TYPE_SCENARIO:
            scenario = await db_read.fetch_one(
                SELECT_SCENARIO_AUDIT_SQL, (payload.id_scenario,)
            )
            if not scenario:  # CA-04
                _rejeter(type_recherche, payload, "Scénario introuvable.")
            co_regate = str(scenario["co_regate"])
            set_co_regate(co_regate)
            site = await db_read.fetch_one(SELECT_SITE_AUDIT_SQL, (co_regate,))
            agrebals = regrouper_par_agrebal(
                await db_read.fetch_all(
                    SELECT_AGREBALS_PDI_SCENARIO_SQL, (payload.id_scenario,)
                )
            )
            if not agrebals:
                message = MESSAGE_SCENARIO_NON_CALCULE
        else:
            co_regate = payload.code_regate
            site = await db_read.fetch_one(SELECT_SITE_AUDIT_SQL, (co_regate,))
            if not site:  # CA-05
                _rejeter(type_recherche, payload, "Site introuvable.")
            set_co_regate(co_regate)
            agrebals = agrebals_du_site(
                await db_read.fetch_all(SELECT_AGREBALS_PDI_SITE_SQL, (co_regate,))
            )
            if not agrebals:
                message = MESSAGE_SITE_SANS_AGREBAL
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur audit agrébals PDI %s",
            ctx(
                type_recherche=type_recherche,
                id_scenario=payload.id_scenario,
                co_regate=payload.code_regate,
            ),
        )
        raise HTTPException(
            status_code=500,
            detail="Erreur lors de la récupération des Agrébals et PDI.",
        ) from e

    nb_pdis = sum(len(a["pdis"]) for a in agrebals)
    logger.info(
        "Fin audit agrébals PDI %s",
        ctx(
            type_recherche=type_recherche,
            id_scenario=payload.id_scenario,
            co_regate=co_regate,
            nb_agrebals=len(agrebals),
            nb_pdis=nb_pdis,
            constat=message,
            duration_ms=round((time.perf_counter() - start) * 1000, 1),
        ),
    )
    return AgrebalsPdiOut(
        type_recherche=type_recherche,
        id_scenario=payload.id_scenario,
        code_regate=co_regate,
        libelle_site=(site or {}).get("lb_regate"),
        nb_agrebals=len(agrebals),
        nb_pdis=nb_pdis,
        agrebals=agrebals,
        message=message,
    )


def _rejeter(type_recherche: str, payload: AgrebalsPdiRequest, detail: str) -> None:
    """404 explicite (CA-04, CA-05), tracé avant d'être levé."""
    logger.warning(
        "Rejet audit agrébals PDI %s",
        ctx(
            type_recherche=type_recherche,
            id_scenario=payload.id_scenario,
            co_regate=payload.code_regate,
            http=404,
            motif=detail,
        ),
    )
    raise HTTPException(status_code=404, detail=detail)
