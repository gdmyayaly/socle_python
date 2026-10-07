"""DSR-704 — mode `ALL` : traite les scénarios éligibles sur `NB_WORKER` workers.

CA-09 : aucune règle métier ici. La réservation est le verrou posé par `calcul_trafic_pdi` ;
seul le filet de sécurité (libération du verrou) écrit.
"""

from __future__ import annotations

import asyncio
import logging
import time

from app.config import NB_WORKER
from app.db.mysql import db_read, db_write
from app.log_utils import ctx, reset_id_scenario, reset_site, set_id_scenario, set_site
from app.traitements import scenario as scn
from app.traitements.eligibilite import controle_eligibilite
from app.traitements.rapport import ECHEC, NON_ELIGIBLE, SUCCES, Bilan
from app.traitements.trafic_agrebal import calcul_trafic_agrebal
from app.traitements.trafic_pdi import calcul_trafic_pdi

logger = logging.getLogger(__name__)

TITRE = "YB05 - Mode ALL"

# Critères du ticket ; un scénario déjà calculé n'y répond plus, d'où la rejouabilité.
SELECT_SCENARIOS_ELIGIBLES_SQL = """
    SELECT id_scenario, co_regate, co_roc
      FROM trppu_scenario
     WHERE statut = 'VALIDE'
       AND est_fige = 1
       AND calcul_trafic_en_cours = 0
       AND trafic_pdi_calcule = 0
       AND trafic_agrebal_calcule = 0
     ORDER BY id_scenario
"""

# PDI calculé sans Agrébal : jamais repris automatiquement, seulement signalé au bilan.
SELECT_SCENARIOS_A_MOITIE_CALCULES_SQL = """
    SELECT id_scenario
      FROM trppu_scenario
     WHERE statut = 'VALIDE'
       AND est_fige = 1
       AND calcul_trafic_en_cours = 0
       AND trafic_pdi_calcule = 1
       AND trafic_agrebal_calcule = 0
     ORDER BY id_scenario
"""


async def executer_tout(
    id_scenario: int | None = None,
    *,
    nb_workers: int | None = None,
    db_lecture=db_read,
    db_ecriture=db_write,
) -> Bilan:
    """Traite un scénario, ou tous les scénarios éligibles. Ne lève pas : rend un bilan."""
    nb_workers = nb_workers if nb_workers is not None else NB_WORKER
    if id_scenario is not None:
        # Un seul scénario : le parallélisme n'a aucun objet (CA-02).
        nb_workers = 1

    bilan = Bilan(nb_workers=nb_workers)
    debut = time.monotonic()

    logger.info(
        "Début mode ALL %s",
        ctx(id_scenario=id_scenario, nb_workers=nb_workers),
    )

    sites: dict[int, tuple] = {}
    try:
        bilan.scenarios_trouves = await _lister_scenarios(db_lecture, id_scenario, sites)
        if id_scenario is None:
            bilan.scenarios_a_moitie_calcules = await _lister_a_moitie_calcules(db_lecture)
    except Exception as erreur:  # noqa: BLE001 — une erreur système ne rend pas de stacktrace
        logger.exception(
            "Erreur mode ALL %s", ctx(etape="recherche des scénarios éligibles")
        )
        bilan.erreur = str(erreur)
        bilan.duree_s = time.monotonic() - debut
        return bilan

    logger.info(
        "Scénarios à traiter %s",
        ctx(
            nb_scenarios=len(bilan.scenarios_trouves),
            nb_workers=nb_workers,
            a_moitie_calcules=len(bilan.scenarios_a_moitie_calcules),
        ),
    )

    file: asyncio.Queue[int] = asyncio.Queue()
    for identifiant in bilan.scenarios_trouves:
        file.put_nowait(identifiant)

    await asyncio.gather(
        *(
            _worker(numero + 1, file, bilan, db_lecture, db_ecriture, debut, sites)
            for numero in range(nb_workers)
        )
    )

    # L'ordre d'arrivée dépend du parallélisme ; le bilan, lui, doit rester lisible.
    bilan.resultats.sort(key=lambda resultat: resultat.id_scenario)
    bilan.duree_s = time.monotonic() - debut

    logger.info(
        "Fin mode ALL %s",
        ctx(
            nb_scenarios=len(bilan.scenarios_trouves),
            succes=len(bilan.succes),
            echecs=len(bilan.echecs),
            non_eligibles=len(bilan.non_eligibles),
            duration_ms=bilan.duree_s * 1000,
        ),
    )
    return bilan


async def _lister_scenarios(
    db_lecture, id_scenario: int | None, sites: dict[int, tuple] | None = None
) -> list[int]:
    """Identifiants à traiter ; `sites` reçoit au passage (co_regate, co_roc) de chacun."""
    if id_scenario is not None:
        return [id_scenario]
    lignes = await db_lecture.fetch_all(SELECT_SCENARIOS_ELIGIBLES_SQL)
    ids = []
    for ligne in lignes:
        identifiant = int(ligne["id_scenario"])
        ids.append(identifiant)
        if sites is not None:
            sites[identifiant] = (ligne.get("co_regate"), ligne.get("co_roc"))
    return ids


async def _lister_a_moitie_calcules(db_lecture) -> list[int]:
    lignes = await db_lecture.fetch_all(SELECT_SCENARIOS_A_MOITIE_CALCULES_SQL)
    return [int(ligne["id_scenario"]) for ligne in lignes]


async def _worker(
    numero: int,
    file: asyncio.Queue,
    bilan: Bilan,
    db_lecture,
    db_ecriture,
    debut: float,
    sites: dict[int, tuple] | None = None,
) -> None:
    """Vide la file, un scénario à la fois, jusqu'à épuisement."""
    while True:
        try:
            id_scenario = file.get_nowait()
        except asyncio.QueueEmpty:
            return

        # Contexte de log par scénario : les workers s'entrelacent.
        jeton = set_id_scenario(id_scenario)
        # Site inconnu (scénario unique) : posé plus tard par `charger_scenario`.
        jeton_site = set_site(*(sites or {}).get(id_scenario, (None, None)))
        logger.info("Début traitement scénario %s", ctx(worker=numero))
        try:
            await _traiter(id_scenario, bilan, db_lecture, db_ecriture)
        except Exception as erreur:  # noqa: BLE001 — CA-08 : un scénario ne fait pas tomber la file
            logger.exception("Erreur traitement scénario %s", ctx(worker=numero))
            bilan.ajouter(id_scenario, ECHEC, str(erreur))
        finally:
            reset_site(jeton_site)
            reset_id_scenario(jeton)
            file.task_done()
        _journaliser_avancement(bilan, debut)


async def _traiter(id_scenario: int, bilan: Bilan, db_lecture, db_ecriture) -> None:
    """Éligibilité, trafics PDI puis trafics Agrébal pour un scénario."""
    # Étape 1 — éligibilité. Non éligible ≠ échec dans le bilan ; aucun verrou posé.
    eligibilite = await controle_eligibilite(id_scenario, db_lecture=db_lecture)
    if not eligibilite.reussi:
        logger.warning(
            "Rejet traitement scénario %s",
            ctx(
                verdict=NON_ELIGIBLE,
                etape="éligibilité",
                motifs=eligibilite.motifs,
            ),
        )
        bilan.ajouter(id_scenario, NON_ELIGIBLE, " ; ".join(eligibilite.motifs))
        return

    # Étape 2 — trafics PDI, qui pose le verrou. Double contrôle d'éligibilité voulu : le
    # second rend `calcul-trafic-pdi` sûr lancé seul.
    rapport_pdi = await calcul_trafic_pdi(
        id_scenario, db_lecture=db_lecture, db_ecriture=db_ecriture
    )
    if not rapport_pdi.reussi:
        logger.warning(
            "Rejet traitement scénario %s",
            ctx(verdict=ECHEC, etape="trafics PDI", motif=_motif(rapport_pdi)),
        )
        bilan.ajouter(id_scenario, ECHEC, _motif(rapport_pdi))
        return

    # Étape 3 — calcul des trafics Agrébal, qui libère le scénario en fin de course.
    rapport_agrebal = await calcul_trafic_agrebal(
        id_scenario, db_lecture=db_lecture, db_ecriture=db_ecriture
    )
    if rapport_agrebal.reussi:
        logger.info("Fin traitement scénario %s", ctx(verdict=SUCCES))
        bilan.ajouter(id_scenario, SUCCES)
        return

    # Filet de sécurité : DSR-703 ne libère pas un verrou qu'il ne détient pas ; ici l'étape 2
    # vient de le poser, on le libère pour qu'aucun scénario ne reste verrouillé.
    logger.warning(
        "Rejet traitement scénario %s",
        ctx(
            verdict=ECHEC,
            etape="trafics Agrébal",
            motif=_motif(rapport_agrebal),
            filet="libération du verrou",
        ),
    )
    await scn.liberer_verrou(db_ecriture, id_scenario)
    bilan.ajouter(id_scenario, ECHEC, _motif(rapport_agrebal))


def _journaliser_avancement(bilan: Bilan, debut: float) -> None:
    """Log d'avancement (progression, débit) après chaque scénario."""
    traites = len(bilan.resultats)
    total = len(bilan.scenarios_trouves)
    ecoule = max(time.monotonic() - debut, 1e-9)
    logger.info(
        "Avancement mode ALL %s",
        ctx(
            traites=traites,
            total=total,
            pct=round(100 * traites / total, 1) if total else None,
            succes=len(bilan.succes),
            echecs=len(bilan.echecs),
            non_eligibles=len(bilan.non_eligibles),
            debit_scenarios_min=round(60 * traites / ecoule, 1),
            duration_ms=round(ecoule * 1000, 1),
        ),
    )


def _motif(rapport) -> str:
    """Cause de l'échec, telle qu'elle sera lue dans le bilan."""
    if rapport.erreur:
        return rapport.erreur.splitlines()[0]
    return " ; ".join(rapport.motifs) if rapport.motifs else "échec sans motif"


__all__ = ["TITRE", "executer_tout"]
