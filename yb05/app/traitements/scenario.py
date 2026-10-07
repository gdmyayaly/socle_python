"""Accès au scénario commun à DSR-701/702/703 : lecture, verrou, référentiel, journal.

Colonnes écrites en minuscules (`Calcul_trafic_en_cours` au schéma) : MySQL y est insensible.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from app.log_utils import ctx, safe_preview, set_site

logger = logging.getLogger(__name__)

# Motifs de calcul autorisés par l'enum `trppu_recalcul_log.raison`.
RAISON_INITIAL = "INITIAL"
RAISONS = ("AGREBAL", "CLE_REPARTITION", "MANUEL", RAISON_INITIAL)

STATUT_VALIDE = "VALIDE"

SELECT_SCENARIO_SQL = """
    SELECT id_scenario,
           co_roc,
           co_regate,
           lb_scenario,
           statut,
           est_fige,
           nb_jours_semaine,
           id_pic_version,
           id_referentiel,
           id_version_cle,
           trafic_pdi_calcule,
           trafic_agrebal_calcule,
           calcul_trafic_en_cours
      FROM trppu_scenario
     WHERE id_scenario = %s
"""

COUNT_COEFFICIENTS_PIC_SQL = """
    SELECT COUNT(*) AS nb
      FROM trppu_pic_coefficients
     WHERE id_pic_version = %s
"""

SELECT_VERSION_CLE_ACTIVE_SQL = """
    SELECT id_version_cle, id_referentiel
      FROM trppu_version_cle
     WHERE co_regate = %s
       AND actif = 'O'
     ORDER BY id_version_cle DESC
     LIMIT 1
"""

# DSR-701 règle 11 : les Agrébals du site sont lus dans `trppu_agrebal_pdi` (et non
# `trppu_trafic_agrebal`, table calculée). Cf. docs/DIAGNOSTIC-DSR-701-703.md.
SELECT_AGREBALS_DU_SITE_SQL = """
    SELECT agrebal_id,
           agrebal_uuid,
           agrebal_pdiQuantity,
           agrebal_pdiList
      FROM trppu_agrebal_pdi
     WHERE agrebal_code_regate = %s
       AND agrebal_deleteddAt IS NULL
     ORDER BY agrebal_id
"""

SELECT_DERNIERE_RAISON_SQL = """
    SELECT raison
      FROM trppu_recalcul_log
     WHERE id_scenario = %s
     ORDER BY dt_recalcul DESC, id_log DESC
     LIMIT 1
"""

PRENDRE_VERROU_SQL = """
    UPDATE trppu_scenario
       SET calcul_trafic_en_cours = 1
     WHERE id_scenario = %s
       AND calcul_trafic_en_cours = 0
"""

LIBERER_VERROU_SQL = """
    UPDATE trppu_scenario
       SET calcul_trafic_en_cours = 0
     WHERE id_scenario = %s
"""

INSERT_RECALCUL_LOG_SQL = """
    INSERT INTO trppu_recalcul_log (id_scenario, dt_recalcul, raison, commentaire)
    VALUES (%s, NOW(), %s, %s)
"""


# ---------------------------------------------------------------------------
# Lectures
# ---------------------------------------------------------------------------


async def charger_scenario(db_lecture, id_scenario: int) -> dict[str, Any] | None:
    """Le scénario, ou None (DSR-701 règle 1) ; pose site et ROC dans le contexte de log.

    Le reset du contexte appartient à l'appelant qui l'a ouvert.
    """
    scenario = await db_lecture.fetch_one(SELECT_SCENARIO_SQL, (id_scenario,))
    if scenario:
        set_site(scenario.get("co_regate"), scenario.get("co_roc"))
    return scenario


async def compter_coefficients_pic(db_lecture, id_pic_version: int) -> int:
    ligne = await db_lecture.fetch_one(COUNT_COEFFICIENTS_PIC_SQL, (id_pic_version,))
    return int(ligne["nb"]) if ligne else 0


async def version_cle_active(db_lecture, co_regate: str) -> dict[str, Any] | None:
    """Version de clés active du site et son référentiel (DSR-701 règles 9-10, DSR-702 ét. 3).

    Seule source du référentiel : `trppu_referentiel` n'est plus utilisée.
    """
    return await db_lecture.fetch_one(SELECT_VERSION_CLE_ACTIVE_SQL, (co_regate,))


def referentiel_de_la_version(version: dict[str, Any] | None) -> int | None:
    """Référentiel de la version de clés ; nul ou 0 = absent (None), cf. DSR-700."""
    if not version:
        return None
    try:
        referentiel = int(version.get("id_referentiel") or 0)
    except (TypeError, ValueError):
        return None
    return referentiel or None


async def agrebals_du_site(db_lecture, co_regate: str) -> list[dict[str, Any]]:
    """Agrébals actifs du site, `agrebal_pdiList` (JSON `[{"pdi_id": …}]`) désérialisé en Python.

    Pas de `JSON_TABLE` : aucune dépendance à une version de MySQL.
    """
    lignes = await db_lecture.fetch_all(SELECT_AGREBALS_DU_SITE_SQL, (co_regate,))
    for ligne in lignes:
        ligne["pdi_ids"] = _extraire_pdi_ids(ligne.get("agrebal_pdiList"))
    return lignes


def _extraire_pdi_ids(brut: Any) -> list[int]:
    """Identifiants de PDI portés par un `agrebal_pdiList`, liste vide si illisible."""
    if not brut:
        return []
    if isinstance(brut, (str, bytes, bytearray)):
        try:
            brut = json.loads(brut)
        except (ValueError, TypeError):
            logger.warning(
                "Agrébal ignoré %s",
                ctx(motif="agrebal_pdiList illisible", brut=safe_preview(brut, 120)),
            )
            return []
    if not isinstance(brut, list):
        return []
    ids: list[int] = []
    for element in brut:
        pdi = element.get("pdi_id") if isinstance(element, dict) else element
        if pdi is None:
            continue
        try:
            ids.append(int(pdi))
        except (ValueError, TypeError):
            continue
    return ids


async def determiner_raison(db_lecture, scenario: dict[str, Any]) -> str:
    """Raison de la dernière demande de recalcul, `INITIAL` s'il n'y en a pas."""
    ligne = await db_lecture.fetch_one(
        SELECT_DERNIERE_RAISON_SQL, (scenario["id_scenario"],)
    )
    if ligne is None:
        return RAISON_INITIAL
    raison = str(ligne["raison"])
    return raison if raison in RAISONS else RAISON_INITIAL


# ---------------------------------------------------------------------------
# Écritures
# ---------------------------------------------------------------------------


async def prendre_verrou(db_ecriture, id_scenario: int) -> bool:
    """Pose `calcul_trafic_en_cours = 1` ; True si le verrou est obtenu.

    L'`UPDATE … WHERE calcul_trafic_en_cours = 0` porte l'exclusion mutuelle ; exécuté seul,
    donc commité aussitôt (dans la transaction du calcul, il serait invisible des autres).
    """
    # retries=1 : rejouée après une coupure post-commit, la pose conclurait à tort
    # « calcul déjà en cours ».
    lignes = await db_ecriture.execute(PRENDRE_VERROU_SQL, (id_scenario,), retries=1)
    obtenu = bool(lignes)
    if obtenu:
        logger.info("Verrou de calcul obtenu %s", ctx(id_scenario=id_scenario))
    else:
        logger.warning(
            "Verrou de calcul non obtenu %s",
            ctx(id_scenario=id_scenario, motif="calcul déjà en cours"),
        )
    return obtenu


async def liberer_verrou(db_ecriture, id_scenario: int) -> None:
    """Remet `calcul_trafic_en_cours = 0` dans tous les chemins d'échec.

    Non protégée exprès : un verrou non libéré bloque le scénario, l'échec doit rester visible.
    """
    lignes = await db_ecriture.execute(LIBERER_VERROU_SQL, (id_scenario,))
    logger.info(
        "Verrou de calcul libéré %s",
        ctx(id_scenario=id_scenario, rows_affected=lignes),
    )


async def journaliser(db_ecriture, id_scenario: int, raison: str, commentaire: str) -> None:
    """Trace le calcul (ou son échec) dans `trppu_recalcul_log`, hors transaction annulée.

    Best-effort : un échec d'écriture passe en WARNING pour ne pas masquer l'erreur métier.
    """
    try:
        # retries=1 : un INSERT rejoué après une coupure post-commit doublerait la trace.
        await db_ecriture.execute(
            INSERT_RECALCUL_LOG_SQL, (id_scenario, raison, commentaire[:255]), retries=1
        )
    except Exception:
        logger.warning(
            "Écriture trppu_recalcul_log impossible %s",
            ctx(
                id_scenario=id_scenario,
                raison=raison,
                consequence="traitement non impacté",
            ),
            exc_info=True,
        )


__all__ = [
    "RAISONS",
    "RAISON_INITIAL",
    "STATUT_VALIDE",
    "agrebals_du_site",
    "charger_scenario",
    "compter_coefficients_pic",
    "determiner_raison",
    "journaliser",
    "liberer_verrou",
    "prendre_verrou",
    "referentiel_de_la_version",
    "version_cle_active",
]
