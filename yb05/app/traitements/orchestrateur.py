"""DSR-704 — mode `ALL` : orchestration de la chaîne de calcul des trafics.

Le mode nominal d'exploitation : trouver les scénarios à calculer, les traiter sur `NB_WORKER`
workers, rendre un bilan.

Son CA-09 est la contrainte structurante — « le mode ALL ne doit contenir aucune règle métier
propre au calcul ». Ce module ne décide donc rien : ni de l'éligibilité, ni du verrou, ni de la
formule, ni de la journalisation. Il choisit quoi lancer, quand, et combien de fois en même
temps. Tout le reste appartient à DSR-701, DSR-702 et DSR-703.

Deux conséquences directes :

* **la réservation du ticket n'est pas réimplémentée**. Elle existe déjà : `calcul_trafic_pdi`
  pose le verrou par un `UPDATE … WHERE calcul_trafic_en_cours = 0`, atomique, dont zéro ligne
  affectée signifie qu'un autre worker a été plus rapide. C'est exactement le mécanisme décrit
  par le ticket, et il est déjà couvert par des tests ;
* **rien n'est écrit ici**, sauf le filet de sécurité ci-dessous, qui répare au lieu de décider.
"""

from __future__ import annotations

import asyncio
import logging
import time

from app.config import NB_WORKER
from app.db.mysql import db_read, db_write
from app.traitements import scenario as scn
from app.traitements.eligibilite import controle_eligibilite
from app.traitements.rapport import ECHEC, NON_ELIGIBLE, SUCCES, Bilan
from app.traitements.trafic_agrebal import calcul_trafic_agrebal
from app.traitements.trafic_pdi import calcul_trafic_pdi

logger = logging.getLogger(__name__)

TITRE = "YB05 - Mode ALL"

# Critères du ticket, mot pour mot. Un scénario déjà calculé n'y répond plus : c'est ce qui rend
# le mode ALL rejouable sans précaution particulière.
SELECT_SCENARIOS_ELIGIBLES_SQL = """
    SELECT id_scenario
      FROM trppu_scenario
     WHERE statut = 'VALIDE'
       AND est_fige = 1
       AND calcul_trafic_en_cours = 0
       AND trafic_pdi_calcule = 0
       AND trafic_agrebal_calcule = 0
     ORDER BY id_scenario
"""

# Hors critères ci-dessus, donc jamais repris automatiquement : un scénario dont le calcul PDI a
# abouti mais pas l'Agrébal. Lu uniquement pour le signaler dans le bilan.
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

    try:
        bilan.scenarios_trouves = await _lister_scenarios(db_lecture, id_scenario)
        if id_scenario is None:
            bilan.scenarios_a_moitie_calcules = await _lister_a_moitie_calcules(db_lecture)
    except Exception as erreur:  # noqa: BLE001 — une erreur système ne rend pas de stacktrace
        logger.exception("Mode ALL : recherche des scénarios impossible")
        bilan.erreur = str(erreur)
        bilan.duree_s = time.monotonic() - debut
        return bilan

    logger.info(
        "Mode ALL : %d scénario(s) à traiter, %d worker(s)",
        len(bilan.scenarios_trouves),
        nb_workers,
    )

    file: asyncio.Queue[int] = asyncio.Queue()
    for identifiant in bilan.scenarios_trouves:
        file.put_nowait(identifiant)

    await asyncio.gather(
        *(
            _worker(numero + 1, file, bilan, db_lecture, db_ecriture)
            for numero in range(nb_workers)
        )
    )

    # L'ordre d'arrivée dépend du parallélisme ; le bilan, lui, doit rester lisible.
    bilan.resultats.sort(key=lambda resultat: resultat.id_scenario)
    bilan.duree_s = time.monotonic() - debut

    logger.info(
        "Mode ALL terminé : %d succès, %d échec(s), %d non éligible(s) en %.1fs",
        len(bilan.succes),
        len(bilan.echecs),
        len(bilan.non_eligibles),
        bilan.duree_s,
    )
    return bilan


async def _lister_scenarios(db_lecture, id_scenario: int | None) -> list[int]:
    if id_scenario is not None:
        return [id_scenario]
    lignes = await db_lecture.fetch_all(SELECT_SCENARIOS_ELIGIBLES_SQL)
    return [int(ligne["id_scenario"]) for ligne in lignes]


async def _lister_a_moitie_calcules(db_lecture) -> list[int]:
    lignes = await db_lecture.fetch_all(SELECT_SCENARIOS_A_MOITIE_CALCULES_SQL)
    return [int(ligne["id_scenario"]) for ligne in lignes]


async def _worker(numero: int, file: asyncio.Queue, bilan: Bilan, db_lecture, db_ecriture) -> None:
    """Vide la file, un scénario à la fois, jusqu'à épuisement."""
    while True:
        try:
            id_scenario = file.get_nowait()
        except asyncio.QueueEmpty:
            return

        logger.info("Worker %d -> scénario %s", numero, id_scenario)
        try:
            await _traiter(id_scenario, bilan, db_lecture, db_ecriture)
        except Exception as erreur:  # noqa: BLE001 — CA-08 : un scénario ne fait pas tomber la file
            logger.exception("Worker %d : scénario %s interrompu", numero, id_scenario)
            bilan.ajouter(id_scenario, ECHEC, str(erreur))
        finally:
            file.task_done()


async def _traiter(id_scenario: int, bilan: Bilan, db_lecture, db_ecriture) -> None:
    """Les trois étapes du ticket, pour un scénario."""
    # Étape 1 — éligibilité. Non éligible : abandon, pas échec. Le ticket distingue les deux dans
    # son bilan, et aucun verrou n'a été posé à ce stade.
    eligibilite = await controle_eligibilite(id_scenario, db_lecture=db_lecture)
    if not eligibilite.reussi:
        bilan.ajouter(id_scenario, NON_ELIGIBLE, " ; ".join(eligibilite.motifs))
        return

    # Étape 2 — calcul des trafics PDI. C'est lui qui pose le verrou, donc qui réserve.
    #
    # L'éligibilité est ainsi contrôlée deux fois : ici, et à l'entrée de calcul_trafic_pdi.
    # C'est VOULU — le second contrôle est ce qui rend la commande calcul-trafic-pdi sûre
    # lancée seule, et le premier ne coûte que quelques SELECT. Ne pas « optimiser » l'un des
    # deux sans mesurer ce qu'on y perd.
    rapport_pdi = await calcul_trafic_pdi(
        id_scenario, db_lecture=db_lecture, db_ecriture=db_ecriture
    )
    if not rapport_pdi.reussi:
        bilan.ajouter(id_scenario, ECHEC, _motif(rapport_pdi))
        return

    # Étape 3 — calcul des trafics Agrébal, qui libère le scénario en fin de course.
    rapport_agrebal = await calcul_trafic_agrebal(
        id_scenario, db_lecture=db_lecture, db_ecriture=db_ecriture
    )
    if rapport_agrebal.reussi:
        bilan.ajouter(id_scenario, SUCCES)
        return

    # Filet de sécurité — le ticket exige qu'un scénario ne reste JAMAIS verrouillé. Or DSR-703
    # s'arrête sur ses contrôles préalables sans libérer le verrou, et il a raison : lancé seul,
    # il ne le détient pas et le relâcher couperait le calcul d'un autre processus. Ici, nous
    # savons qu'il est à nous, puisque l'étape 2 vient de le poser.
    await scn.liberer_verrou(db_ecriture, id_scenario)
    bilan.ajouter(id_scenario, ECHEC, _motif(rapport_agrebal))


def _motif(rapport) -> str:
    """Cause de l'échec, telle qu'elle sera lue dans le bilan."""
    if rapport.erreur:
        return rapport.erreur.splitlines()[0]
    return " ; ".join(rapport.motifs) if rapport.motifs else "échec sans motif"


__all__ = ["TITRE", "executer_tout"]
