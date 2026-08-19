"""DSR-701 — contrôle d'éligibilité d'un scénario au calcul des trafics.

Douze règles, aucune écriture : le traitement ne fait que lire et rendre un verdict (CA-05 du
ticket). C'est la raison pour laquelle la fonction ne reçoit que `db_read` — la violation est
impossible, pas seulement interdite.

Les onze règles qui suivent la première sont **toutes** évaluées, même quand l'une échoue :
l'intérêt d'un mode contrôle est de rendre d'un coup la liste complète des motifs bloquants,
pas de s'arrêter au premier. Seule l'absence de scénario court-circuite le reste, puisque plus
rien n'est alors évaluable.
"""

from __future__ import annotations

import logging

from app.db.mysql import db_read
from app.traitements import scenario as scn
from app.traitements.rapport import ELIGIBLE, NON_ELIGIBLE, Rapport

logger = logging.getLogger(__name__)

TITRE = "Contrôle d'éligibilité YB05"


async def controle_eligibilite(id_scenario: int, *, db_lecture=db_read) -> Rapport:
    """Évalue les douze règles d'éligibilité et retourne le rapport correspondant."""
    rapport = Rapport(titre=TITRE, id_scenario=id_scenario)

    # Règle 1 — le scénario doit exister.
    scenario = await scn.charger_scenario(db_lecture, id_scenario)
    if scenario is None:
        rapport.ko("Scénario inexistant", libelle="Scénario trouvé")
        rapport.statut = NON_ELIGIBLE
        logger.warning("Éligibilité : scénario %s inexistant", id_scenario)
        return rapport
    rapport.ok("Scénario trouvé")

    _regles_sur_le_scenario(rapport, scenario)
    await _regles_sur_les_donnees(rapport, scenario, db_lecture)

    rapport.statut = ELIGIBLE if rapport.reussi else NON_ELIGIBLE
    logger.info(
        "Éligibilité du scénario %s : %s (%d motif(s))",
        id_scenario,
        rapport.statut,
        len(rapport.motifs),
    )
    return rapport


def _regles_sur_le_scenario(rapport: Rapport, scenario: dict) -> None:
    """Règles 2 à 7 — tout est dans la ligne de `trppu_scenario` déjà chargée."""
    rapport.ajouter(
        scenario["statut"] == scn.STATUT_VALIDE,
        "Statut VALIDE",
        "Le scénario n'est pas validé",
    )
    rapport.ajouter(
        bool(scenario["est_fige"]),
        "Scénario figé",
        "Le scénario n'est pas figé",
    )
    rapport.ajouter(
        not scenario["calcul_trafic_en_cours"],
        "Aucun calcul en cours",
        "Un calcul de trafic est déjà en cours",
    )
    rapport.ajouter(
        not scenario["trafic_pdi_calcule"],
        "Trafic PDI non calculé",
        "Les trafics PDI sont déjà calculés",
    )
    rapport.ajouter(
        not scenario["trafic_agrebal_calcule"],
        "Trafic Agrébal non calculé",
        "Les trafics Agrébal sont déjà calculés",
    )
    rapport.ajouter(
        (scenario["id_pic_version"] or 0) > 0,
        "Version PIC trouvée",
        "Aucune version PIC associée au scénario",
    )


async def _regles_sur_les_donnees(rapport: Rapport, scenario: dict, db_lecture) -> None:
    """Règles 8 à 12 — les données que le calcul consommera existent-elles ?

    L'ordre suit l'exemple de sortie du ticket, qui affiche le référentiel avant la version de
    clés, alors que sa numérotation fait l'inverse. C'est la sortie que l'exploitant compare.
    """
    co_regate = scenario["co_regate"]

    # Règle 8 — coefficients de rétention de la version PIC du scénario.
    nb_coefs = await scn.compter_coefficients_pic(db_lecture, scenario["id_pic_version"])
    rapport.ajouter(
        nb_coefs > 0,
        f"Coefficients de rétention disponibles ({nb_coefs})",
        "Aucun coefficient de rétention trouvé pour la version PIC du scénario",
    )

    # Règle 9 — version de clés active du site (lue avant la règle 10, qui s'y compare).
    version = await scn.version_cle_active(db_lecture, co_regate)

    # Règle 10 — référentiel actif du site.
    referentiel = await scn.dernier_referentiel(db_lecture, co_regate)
    if referentiel is None:
        rapport.ko(
            "Aucun référentiel actif disponible",
            libelle="Référentiel actif disponible",
        )
    elif version is not None and version["id_referentiel"] != referentiel:
        # Le ticket exige que les deux requêtes renvoient le même identifiant, sans dire quoi
        # faire lorsqu'elles diffèrent. Un écart signifie que la version active repose sur un
        # référentiel dépassé : calculer produirait des trafics à partir de clés périmées.
        rapport.ko(
            f"Le référentiel de la version de clés active ({version['id_referentiel']}) "
            f"n'est pas le dernier référentiel du site ({referentiel})",
            libelle="Référentiel actif disponible",
        )
    else:
        rapport.ok(f"Référentiel actif disponible ({referentiel})")

    rapport.ajouter(
        version is not None,
        f"Version de clés active disponible ({version['id_version_cle']})"
        if version
        else "Version de clés active disponible",
        "Aucune version de clés active disponible",
    )

    # Règles 11 et 12 — les Agrébals du site, et les PDI qu'ils portent.
    agrebals = await scn.agrebals_du_site(db_lecture, co_regate)
    rapport.ajouter(
        bool(agrebals),
        f"Agrébals trouvés ({len(agrebals)})",
        "Aucun Agrébal trouvé sur le site",
    )
    nb_pdi = sum(len(a["pdi_ids"]) for a in agrebals)
    rapport.ajouter(
        nb_pdi > 0,
        f"PDI trouvés ({nb_pdi})",
        "Aucun PDI rattaché aux Agrébals du site",
    )


__all__ = ["TITRE", "controle_eligibilite"]
