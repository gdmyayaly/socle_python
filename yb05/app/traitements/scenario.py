"""Accès au scénario et à son contexte de calcul : verrou, flags, référentiel, journal.

Tout ce qui est commun à DSR-701, DSR-702 et DSR-703 vit ici, pour que les trois traitements
lisent le scénario de la même façon et posent le même verrou.

Note sur la casse des colonnes : le schéma déclare `Calcul_trafic_en_cours` avec une majuscule.
Les noms de colonnes MySQL étant insensibles à la casse, tout est écrit en minuscules ici — la
seule chose qui compte est que le nom soit reconnaissable d'un `grep` à l'autre.
"""

from __future__ import annotations

import json
import logging
from typing import Any

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

SELECT_DERNIER_REFERENTIEL_SQL = """
    SELECT id_referentiel
      FROM trppu_referentiel
     WHERE co_regate = %s
     ORDER BY id_referentiel DESC
     LIMIT 1
"""

# DSR-701 règle 11 désigne `trppu_trafic_agrebal` — table des trafics CALCULÉS, que la règle 6
# exige justement vide avant un premier calcul. Les Agrébals d'un site vivent dans
# `trppu_agrebal_pdi` ; c'est elle qui est interrogée. Cf. docs/DIAGNOSTIC-DSR-701-703.md.
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
    """Le scénario, ou None s'il n'existe pas (DSR-701 règle 1)."""
    return await db_lecture.fetch_one(SELECT_SCENARIO_SQL, (id_scenario,))


async def compter_coefficients_pic(db_lecture, id_pic_version: int) -> int:
    ligne = await db_lecture.fetch_one(COUNT_COEFFICIENTS_PIC_SQL, (id_pic_version,))
    return int(ligne["nb"]) if ligne else 0


async def version_cle_active(db_lecture, co_regate: str) -> dict[str, Any] | None:
    """Version de clés active du site (DSR-701 règle 9, DSR-702 étape 3)."""
    return await db_lecture.fetch_one(SELECT_VERSION_CLE_ACTIVE_SQL, (co_regate,))


async def dernier_referentiel(db_lecture, co_regate: str) -> int | None:
    """« Référentiel actif » du site.

    `trppu_referentiel` ne porte aucune colonne `actif` : la convention retenue est celle du
    ticket, le plus grand `id_referentiel` du site. Un référentiel national (`co_regate IS
    NULL`) n'est donc jamais retourné — écart signalé dans le rapport d'éligibilité.
    """
    ligne = await db_lecture.fetch_one(SELECT_DERNIER_REFERENTIEL_SQL, (co_regate,))
    return int(ligne["id_referentiel"]) if ligne else None


async def agrebals_du_site(db_lecture, co_regate: str) -> list[dict[str, Any]]:
    """Agrébals actifs du site, avec leur liste de PDI déjà désérialisée.

    `agrebal_pdiList` est un JSON `[{"pdi_id": …}, …]` — la colonne générée
    `agrebal_pdi_ids` du schéma confirme le nom de la propriété. La désérialisation est faite
    ici, en Python, plutôt qu'avec `JSON_TABLE` : le code ne dépend ainsi d'aucune version
    particulière de MySQL.
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
            logger.warning("agrebal_pdiList illisible, agrébal ignoré")
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
    """Motif du calcul, au sens de `trppu_recalcul_log.raison`.

    Règle du ticket : `INITIAL` si les deux flags de trafic sont à 0 et qu'aucune demande de
    recalcul n'a été enregistrée ; sinon la raison de la dernière demande.
    """
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
    """Pose `calcul_trafic_en_cours = 1` et dit si le verrou a été obtenu.

    L'`UPDATE` conditionnel est ce qui porte l'exclusion mutuelle : deux processus lancés en
    même temps voient forcément l'un 1 ligne affectée, l'autre 0. Il est exécuté **seul**, donc
    commité immédiatement — dans la transaction du calcul, le verrou resterait invisible des
    autres processus jusqu'au commit final, c'est-à-dire trop tard.
    """
    lignes = await db_ecriture.execute(PRENDRE_VERROU_SQL, (id_scenario,))
    return bool(lignes)


async def liberer_verrou(db_ecriture, id_scenario: int) -> None:
    """Remet `calcul_trafic_en_cours = 0` — à appeler dans tous les chemins d'échec."""
    await db_ecriture.execute(LIBERER_VERROU_SQL, (id_scenario,))


async def journaliser(db_ecriture, id_scenario: int, raison: str, commentaire: str) -> None:
    """Écrit la trace du calcul (ou de son échec) dans `trppu_recalcul_log`.

    Sur un échec, l'écriture doit se faire HORS de la transaction annulée : une trace
    d'incident qui disparaît avec le rollback ne sert à rien.
    """
    await db_ecriture.execute(
        INSERT_RECALCUL_LOG_SQL, (id_scenario, raison, commentaire[:255])
    )


__all__ = [
    "RAISONS",
    "RAISON_INITIAL",
    "STATUT_VALIDE",
    "agrebals_du_site",
    "charger_scenario",
    "compter_coefficients_pic",
    "dernier_referentiel",
    "determiner_raison",
    "journaliser",
    "liberer_verrou",
    "prendre_verrou",
    "version_cle_active",
]
