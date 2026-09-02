"""Persistance des appels d'écriture dans `trppu_api_log`.

La table existe au schéma (`db/db_new.sql:84-96`) mais n'était alimentée par
aucune route — c'est la proposition IMP-5 de `api_docs/dsr/README_ameliorations.md`.
Elle complète les logs Kibana d'une trace **durable en base**, indépendante de la
rétention des logs fichiers, pour reconstituer une donnée après incident.

Trois règles de conception, toutes motivées par le cas d'usage « post-mortem » :

1. **Écritures seules** (POST/PUT/PATCH/DELETE). Persister les lectures
   saturerait la table sans rien apporter.
2. **Hors de la transaction métier**, une fois son issue connue : une trace
   annulée par le rollback serait inutile précisément quand on en a besoin.
3. **Best-effort** : un échec d'écriture de l'audit est redescendu en WARNING et
   n'interrompt jamais la requête métier.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from app.db.mysql import db_write
from app.log_utils import CHAMPS_SENSIBLES, get_id_session_ihm

logger = logging.getLogger(__name__)

# Bornes des colonnes (cf. db/db_new.sql:84-96).
API_NAME_MAX_LEN = 50
CALLER_MAX_LEN = 120
REGATE_LEN = 6
# La colonne `params` est un JSON : on borne le sérialisé pour qu'un gros payload
# (upload Excel, TMH multi-produits) ne gonfle pas la table.
PARAMS_MAX_LEN = 4000

# Vocabulaire d'actions, partagé avec le module d'audit
# (app/routes/trppu_audit/helpers.py) : la table et l'endpoint d'audit doivent
# désigner une même opération par un même code.
ACTION_CREATION_SCENARIO = "CREATION_SCENARIO"
ACTION_MAJ_SCENARIO = "MAJ_SCENARIO"
ACTION_SUPPRESSION_SCENARIO = "SUPPRESSION_SCENARIO"
ACTION_ARCHIVAGE_SCENARIO = "ARCHIVAGE_SCENARIO"
ACTION_DUPLICATION_SCENARIO = "DUPLICATION_SCENARIO"
ACTION_TRANSITION_STATUT = "TRANSITION_STATUT"
ACTION_CREATION_PIC_VERSION = "CREATION_PIC_VERSION"
ACTION_MAJ_PIC_VERSION = "MAJ_PIC_VERSION"
ACTION_SUPPRESSION_PIC_VERSION = "SUPPRESSION_PIC_VERSION"
ACTION_ECRITURE_PIC_COEFFICIENT = "ECRITURE_PIC_COEFFICIENT"
ACTION_SUPPRESSION_PIC_COEFFICIENT = "SUPPRESSION_PIC_COEFFICIENT"
ACTION_NEUTRALISATION = "NEUTRALISATION"
ACTION_ECRITURE_TMH = "ECRITURE_TMH"
ACTION_ECRITURE_VARIATION = "ECRITURE_VARIATION"
ACTION_ECRITURE_COMPTAGE = "ECRITURE_COMPTAGE"
ACTION_ECRITURE_SITE = "ECRITURE_SITE"
ACTION_ECRITURE_PRODUIT = "ECRITURE_PRODUIT"
ACTION_IMPORT_EXCEL = "IMPORT_EXCEL"

INSERT_API_LOG_SQL = (
    "INSERT INTO trppu_api_log (api_name, id_scenario, regate, dt_appel, caller, params) "
    "VALUES (%s, %s, %s, NOW(), %s, %s)"
)


def _borner(valeur: str | None, max_len: int) -> str | None:
    if valeur is None:
        return None
    valeur = str(valeur).strip()
    return valeur[:max_len] or None


def _serialiser_params(params: Any, id_scenario: int | None) -> str | None:
    """Sérialise les paramètres en JSON, sans champ sensible et borné.

    `id_scenario` est recopié dans le JSON : la FK `trppu_api_log.id_scenario`
    est détachée (mise à NULL) quand le scénario est supprimé, et c'est cette
    copie qui permet alors de savoir de quel scénario l'appel parlait.
    """
    if params is None and id_scenario is None:
        return None
    contenu: dict[str, Any] = {}
    if isinstance(params, dict):
        contenu = {c: v for c, v in params.items() if c not in CHAMPS_SENSIBLES}
    elif params is not None:
        contenu = {"params": repr(params)}
    if id_scenario is not None:
        contenu.setdefault("id_scenario", id_scenario)
    try:
        texte = json.dumps(contenu, ensure_ascii=False, default=str)
    except Exception:
        texte = json.dumps({"params": "<non sérialisable>"}, ensure_ascii=False)
    if len(texte) > PARAMS_MAX_LEN:
        texte = json.dumps(
            {"tronque": True, "apercu": texte[:PARAMS_MAX_LEN]}, ensure_ascii=False
        )
    return texte


async def enregistrer_appel(
    *,
    api_name: str,
    id_scenario: int | None = None,
    regate: str | None = None,
    params: Any = None,
) -> None:
    """Enregistre un appel d'écriture dans `trppu_api_log`.

    `api_name` reprend le vocabulaire d'actions du module d'audit
    (`app/routes/trppu_audit/helpers.py`) — CREATION_SCENARIO, MAJ_SCENARIO,
    ECRITURE_TMH… — pour que la table et l'endpoint d'audit parlent la même langue.

    `caller` est alimenté par l'`id_session_ihm` du contexte : les routes ne sont
    pas authentifiées, c'est le seul identifiant d'appelant disponible.

    N'échoue jamais : toute erreur est loguée en WARNING et absorbée.
    """
    try:
        await db_write.execute(
            INSERT_API_LOG_SQL,
            (
                _borner(api_name, API_NAME_MAX_LEN),
                id_scenario,
                _borner(regate, REGATE_LEN),
                _borner(get_id_session_ihm(), CALLER_MAX_LEN),
                _serialiser_params(params, id_scenario),
            ),
        )
    except Exception:
        # L'audit ne doit jamais faire échouer le métier : on trace et on continue.
        logger.warning(
            "Écriture trppu_api_log impossible (api_name=%s, id_scenario=%s) "
            "— appel métier non impacté.",
            api_name,
            id_scenario,
            exc_info=True,
        )
