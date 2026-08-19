"""DSR-703 — calcul des trafics Agrébal d'un scénario, par agrégation des trafics PDI.

Aucune formule métier ici : ni coefficient de rétention, ni clé de répartition (CA-06). Le
traitement somme les trafics PDI déjà calculés par DSR-702, par
(scénario, Agrébal, produit, jour, couleur PIC).

L'agrégation est faite **en SQL** — trois `INSERT … SELECT … GROUP BY`, un par couleur. Les
volumes ne transitent jamais par l'application : c'est plus rapide d'un ordre de grandeur sur
les volumes en jeu, et `volume` étant un `decimal(12,4)`, aucun arrondi n'est introduit.

Le verrou n'est pas repris : ce traitement tourne sous celui posé par DSR-702, et c'est sa mise
à jour finale qui le libère — `calcul_trafic_en_cours = 0` marque la fin du calcul complet.
"""

from __future__ import annotations

import logging

from app.db.mysql import db_read, db_write
from app.traitements import scenario as scn
from app.traitements.erreurs import TraitementImpossible
from app.traitements.rapport import ECHEC, SUCCES, Rapport

logger = logging.getLogger(__name__)

TITRE = "Calcul des trafics Agrébal"

COULEURS = (("DENSE", "dense"), ("FAIBLE1", "faible1"), ("FAIBLE2", "faible2"))

# `trppu_trafic_agrebal.id_agrebal` est un `int` alors que `trppu_trafic_pdi.id_agrebal` est un
# `bigint` : au-delà de cette borne, l'insertion échouerait (ou tronquerait). Contrôlé avant.
ID_AGREBAL_MAX = 2147483647

SELECT_ETAT_PDI_SQL = """
    SELECT COUNT(*)              AS nb_lignes,
           MAX(id_agrebal)       AS id_agrebal_max,
           SUM(dense)            AS total_dense,
           SUM(faible1)          AS total_faible1,
           SUM(faible2)          AS total_faible2
      FROM trppu_trafic_pdi
     WHERE id_scenario = %s
"""

DELETE_TRAFIC_AGREBAL_SQL = "DELETE FROM trppu_trafic_agrebal WHERE id_scenario = %s"

# Une insertion par couleur PIC ; `{colonne}` est l'une des trois colonnes de trafic PDI, jamais
# une valeur venue de l'extérieur (cf. COULEURS).
INSERT_AGREGAT_SQL = """
    INSERT INTO trppu_trafic_agrebal
        (id_scenario, co_regate, id_agrebal, agrebal_uuid, co_produit, jour_semaine,
         couleur_pic, volume)
    SELECT id_scenario,
           co_regate,
           id_agrebal,
           agrebal_uuid,
           co_produit,
           jour_semaine,
           %s,
           SUM({colonne})
      FROM trppu_trafic_pdi
     WHERE id_scenario = %s
     GROUP BY id_scenario, co_regate, id_agrebal, agrebal_uuid, co_produit, jour_semaine
"""

MARQUER_AGREBAL_CALCULE_SQL = """
    UPDATE trppu_scenario
       SET trafic_agrebal_calcule = 1,
           calcul_trafic_en_cours = 0
     WHERE id_scenario = %s
"""

SELECT_TOTAL_AGREBAL_SQL = """
    SELECT COUNT(*) AS nb_lignes, COALESCE(SUM(volume), 0) AS total
      FROM trppu_trafic_agrebal
     WHERE id_scenario = %s
"""


async def calcul_trafic_agrebal(
    id_scenario: int, *, db_lecture=db_read, db_ecriture=db_write
) -> Rapport:
    """Agrège les trafics PDI du scénario en trafics Agrébal. Ne lève pas : rend un rapport."""
    rapport = Rapport(titre=TITRE, id_scenario=id_scenario)

    # Contrôle préalable 1 — le scénario existe.
    scenario = await scn.charger_scenario(db_lecture, id_scenario)
    if scenario is None:
        rapport.ko("Scénario inexistant", libelle="Scénario trouvé")
        rapport.statut = ECHEC
        return rapport
    rapport.ok("Scénario trouvé")

    # Contrôle préalable 2 — les trafics PDI existent, flag ET données.
    etat = await db_lecture.fetch_one(SELECT_ETAT_PDI_SQL, (id_scenario,))
    nb_pdi = int(etat["nb_lignes"]) if etat else 0
    if not scenario["trafic_pdi_calcule"] or nb_pdi == 0:
        rapport.ko("Trafics PDI non calculés", libelle="Trafics PDI calculés")
        rapport.erreur = (
            "Calcul des trafics Agrébal impossible.\n"
            f"Les trafics PDI du scénario {id_scenario} n'ont pas été calculés.\n"
            f"Exécuter préalablement : YB05 CALCUL_TRAFIC_PDI {id_scenario}"
        )
        rapport.statut = ECHEC
        return rapport
    rapport.ok("Trafics PDI calculés")
    rapport.ok(f"{nb_pdi} lignes PDI lues")

    raison = await scn.determiner_raison(db_lecture, scenario)

    try:
        nb_lignes = await _agreger(rapport, id_scenario, etat, raison, db_lecture, db_ecriture)
    except Exception as erreur:  # noqa: BLE001 — le scénario doit être déverrouillé quoi qu'il arrive
        logger.exception("Calcul des trafics Agrébal du scénario %s en échec", id_scenario)
        await scn.liberer_verrou(db_ecriture, id_scenario)
        await scn.journaliser(
            db_ecriture,
            id_scenario,
            raison,
            f"Recalcul Agrébal interrompu : {erreur}",
        )
        rapport.erreur = str(erreur)
        rapport.statut = ECHEC
        rapport.etats["TRAFIC_PDI_CALCULE"] = 1
        rapport.etats["TRAFIC_AGREBAL_CALCULE"] = 0
        rapport.etats["CALCUL_TRAFIC_EN_COURS"] = 0
        return rapport

    rapport.ok(f"{nb_lignes} lignes Agrébal créées")
    rapport.etats["TRAFIC_AGREBAL_CALCULE"] = 1
    rapport.etats["CALCUL_TRAFIC_EN_COURS"] = 0
    # Les données sont écrites et le scénario déverrouillé, mais un écart de totaux reste une
    # anomalie : le verdict la reflète, sinon l'exploitant lirait SUCCES sur un calcul douteux.
    rapport.statut = SUCCES if rapport.reussi else ECHEC
    return rapport


async def _agreger(
    rapport: Rapport, id_scenario: int, etat: dict, raison: str, db_lecture, db_ecriture
) -> int:
    """Purge, agrégation, journalisation et déverrouillage, en une seule transaction."""
    id_agrebal_max = etat.get("id_agrebal_max") or 0
    if int(id_agrebal_max) > ID_AGREBAL_MAX:
        raise TraitementImpossible(
            f"id_agrebal {id_agrebal_max} au-delà de la capacité de "
            f"trppu_trafic_agrebal.id_agrebal (int, maximum {ID_AGREBAL_MAX})"
        )

    nb_lignes = 0
    async with db_ecriture.transaction() as tx:
        await tx.execute(DELETE_TRAFIC_AGREBAL_SQL, (id_scenario,))
        for couleur, colonne in COULEURS:
            nb_lignes += await tx.execute(
                INSERT_AGREGAT_SQL.format(colonne=colonne), (couleur, id_scenario)
            )
        await tx.execute(
            scn.INSERT_RECALCUL_LOG_SQL, (id_scenario, raison, _commentaire(raison))
        )
        await tx.execute(MARQUER_AGREBAL_CALCULE_SQL, (id_scenario,))

    await _controler_totaux(rapport, id_scenario, etat, db_lecture)

    logger.info(
        "Trafics Agrébal du scénario %s : %d lignes écrites (raison %s)",
        id_scenario,
        nb_lignes,
        raison,
    )
    return nb_lignes


async def _controler_totaux(rapport: Rapport, id_scenario: int, etat: dict, db_lecture) -> None:
    """La somme des volumes Agrébal doit égaler la somme des trafics PDI.

    C'est le seul contrôle qui vérifie réellement l'agrégation : un écart signale un PDI compté
    deux fois ou un `GROUP BY` incomplet, ce qu'aucun décompte de lignes ne montrerait.
    """
    total_pdi = sum(
        int(etat.get(cle) or 0)
        for cle in ("total_dense", "total_faible1", "total_faible2")
    )
    ligne = await db_lecture.fetch_one(SELECT_TOTAL_AGREBAL_SQL, (id_scenario,))
    total_agrebal = int(ligne["total"]) if ligne else 0
    rapport.ajouter(
        total_pdi == total_agrebal,
        f"Totaux cohérents ({total_agrebal})",
        f"Écart entre trafics PDI ({total_pdi}) et trafics Agrébal ({total_agrebal})",
    )


def _commentaire(raison: str) -> str:
    """Commentaires de `trppu_recalcul_log`, repris mot pour mot du ticket."""
    libelles = {
        scn.RAISON_INITIAL: "Premier calcul des trafics Agrébal",
        "AGREBAL": "Recalcul Agrébal exécuté suite évolution Agrébal",
        "CLE_REPARTITION": (
            "Recalcul Agrébal exécuté suite activation d'une nouvelle version de clés"
        ),
        "MANUEL": "Recalcul manuel des trafics Agrébal",
    }
    return libelles.get(raison, "Calcul des trafics Agrébal")


__all__ = ["TITRE", "calcul_trafic_agrebal"]
