"""DSR-703 — trafics Agrébal : somme des trafics PDI par Agrébal × produit × jour × couleur.

Aucune formule métier (CA-06) ; agrégation en SQL (`INSERT … SELECT … GROUP BY` par couleur).
Tourne sous le verrou posé par DSR-702, que sa mise à jour finale libère.
"""

from __future__ import annotations

import logging
import time

from app.db.mysql import db_read, db_write
from app.log_utils import ctx
from app.traitements import scenario as scn
from app.traitements.erreurs import TraitementImpossible
from app.traitements.rapport import ECHEC, SUCCES, Rapport

logger = logging.getLogger(__name__)

TITRE = "Calcul des trafics Agrébal"

COULEURS = (("DENSE", "dense"), ("FAIBLE1", "faible1"), ("FAIBLE2", "faible2"))

# `id_agrebal` : `int` côté Agrébal, `bigint` côté PDI — contrôlé avant insertion.
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

# Une insertion par couleur ; `{colonne}` vient toujours de COULEURS, jamais de l'extérieur.
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

# Une ligne par Agrébal calculé : sert au contrôle des totaux ET au décompte des Agrébals.
SELECT_TOTAL_AGREBAL_SQL = """
    SELECT id_agrebal, COUNT(*) AS nb_lignes, COALESCE(SUM(volume), 0) AS total
      FROM trppu_trafic_agrebal
     WHERE id_scenario = %s
     GROUP BY id_agrebal
"""

# Agrébals non calculés détaillés dans le rapport et les logs ; le total reste exact.
MAX_AGREBALS_DETAILLES = 20


async def calcul_trafic_agrebal(
    id_scenario: int, *, db_lecture=db_read, db_ecriture=db_write
) -> Rapport:
    """Agrège les trafics PDI du scénario en trafics Agrébal. Ne lève pas : rend un rapport."""
    debut = time.perf_counter()
    rapport = Rapport(titre=TITRE, id_scenario=id_scenario)
    logger.info("Début calcul trafics Agrébal %s", ctx(id_scenario=id_scenario))

    # Contrôle préalable 1 — le scénario existe.
    scenario = await scn.charger_scenario(db_lecture, id_scenario)
    if scenario is None:
        logger.warning(
            "Rejet calcul trafics Agrébal %s",
            ctx(verdict=ECHEC, motif="scénario inexistant"),
        )
        rapport.ko("Scénario inexistant", libelle="Scénario trouvé")
        rapport.statut = ECHEC
        return rapport
    rapport.ok("Scénario trouvé")

    # Contrôle préalable 2 — les trafics PDI existent, flag ET données.
    etat = await db_lecture.fetch_one(SELECT_ETAT_PDI_SQL, (id_scenario,))
    nb_pdi = int(etat["nb_lignes"]) if etat else 0
    if not scenario["trafic_pdi_calcule"] or nb_pdi == 0:
        logger.warning(
            "Rejet calcul trafics Agrébal %s",
            ctx(
                verdict=ECHEC,
                motif="trafics PDI non calculés",
                trafic_pdi_calcule=bool(scenario["trafic_pdi_calcule"]),
                lignes_pdi=nb_pdi,
            ),
        )
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
    # Agrébals actifs du site : ce qui est attendu au calcul (même source que DSR-701/702).
    a_calculer = list(
        dict.fromkeys(
            int(a["agrebal_id"])
            for a in await scn.agrebals_du_site(db_lecture, scenario["co_regate"])
        )
    )
    rapport.ok(f"{len(a_calculer)} Agrébal(s) actif(s) à calculer sur le site")
    logger.info(
        "Avancement calcul trafics Agrébal %s",
        ctx(phase="agrébals à calculer", agrebals_a_calculer=len(a_calculer)),
    )

    try:
        nb_lignes, calcules = await _agreger(
            rapport, id_scenario, etat, raison, db_lecture, db_ecriture
        )
    except Exception as erreur:  # noqa: BLE001 — le scénario doit être déverrouillé quoi qu'il arrive
        logger.exception("Erreur calcul trafics Agrébal %s", ctx(raison=raison))
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
    non_calcules = _signaler_agrebals(rapport, a_calculer, calcules)
    rapport.etats["TRAFIC_AGREBAL_CALCULE"] = 1
    rapport.etats["CALCUL_TRAFIC_EN_COURS"] = 0
    rapport.etats["NB_AGREBAL_A_CALCULER"] = len(a_calculer)
    rapport.etats["NB_AGREBAL_CALCULES"] = len(calcules)
    # Un écart de totaux rend le verdict ECHEC, même données écrites.
    rapport.statut = SUCCES if rapport.reussi else ECHEC
    logger.info(
        "Fin calcul trafics Agrébal %s",
        ctx(
            verdict=rapport.statut,
            lignes=nb_lignes,
            lignes_pdi=nb_pdi,
            agrebals_a_calculer=len(a_calculer),
            agrebals_calcules=len(calcules),
            agrebals_non_calcules=len(non_calcules),
            raison=raison,
            duration_ms=(time.perf_counter() - debut) * 1000,
        ),
    )
    return rapport


async def _agreger(
    rapport: Rapport, id_scenario: int, etat: dict, raison: str, db_lecture, db_ecriture
) -> tuple[int, list[int]]:
    """Purge, agrégation, journalisation et déverrouillage, en une seule transaction.

    Retourne le nombre de lignes écrites et les Agrébals effectivement calculés.
    """
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
            debut_couleur = time.perf_counter()
            lignes_couleur = await tx.execute(
                INSERT_AGREGAT_SQL.format(colonne=colonne), (couleur, id_scenario)
            )
            nb_lignes += lignes_couleur
            logger.info(
                "Avancement calcul trafics Agrébal %s",
                ctx(
                    couleur=couleur,
                    lignes=lignes_couleur,
                    duration_ms=round((time.perf_counter() - debut_couleur) * 1000, 1),
                ),
            )
        await tx.execute(
            scn.INSERT_RECALCUL_LOG_SQL, (id_scenario, raison, _commentaire(raison))
        )
        await tx.execute(MARQUER_AGREBAL_CALCULE_SQL, (id_scenario,))

    calcules = await _controler_totaux(rapport, id_scenario, etat, db_lecture)

    logger.info(
        "Trafics Agrébal écrits %s",
        ctx(lignes=nb_lignes, couleurs=len(COULEURS), agrebals=len(calcules), raison=raison),
    )
    return nb_lignes, calcules


async def _controler_totaux(
    rapport: Rapport, id_scenario: int, etat: dict, db_lecture
) -> list[int]:
    """Vérifie que la somme des volumes Agrébal égale celle des trafics PDI.

    Retourne les Agrébals présents dans `trppu_trafic_agrebal` (une ligne de la requête chacun).
    """
    total_pdi = sum(
        int(etat.get(cle) or 0)
        for cle in ("total_dense", "total_faible1", "total_faible2")
    )
    lignes = await db_lecture.fetch_all(SELECT_TOTAL_AGREBAL_SQL, (id_scenario,))
    total_agrebal = sum(int(ligne["total"] or 0) for ligne in lignes)
    rapport.ajouter(
        total_pdi == total_agrebal,
        f"Totaux cohérents ({total_agrebal})",
        f"Écart entre trafics PDI ({total_pdi}) et trafics Agrébal ({total_agrebal})",
    )
    return sorted(int(ligne["id_agrebal"]) for ligne in lignes)


def _signaler_agrebals(
    rapport: Rapport, a_calculer: list[int], calcules: list[int]
) -> list[int]:
    """Rapport (non bloquant) et logs : Agrébals à calculer contre Agrébals calculés.

    La cause d'un manque se lit au calcul PDI (`Agrébals non calculés`) : l'agrégation ne
    fait que regrouper `trppu_trafic_pdi`, elle ne peut pas perdre un Agrébal par elle-même.
    """
    ensemble_calcules = set(calcules)
    non_calcules = [a for a in a_calculer if a not in ensemble_calcules]
    hors_site = sorted(ensemble_calcules - set(a_calculer))

    rapport.ok(f"Agrébals : {len(calcules)} calculé(s) sur {len(a_calculer)} à calculer")
    if non_calcules:
        rapport.alerte(
            f"{len(non_calcules)} Agrébal(s) non calculé(s) "
            f"({non_calcules[:MAX_AGREBALS_DETAILLES]}) — cause au calcul des trafics PDI"
        )
        logger.warning(
            "Agrébals non calculés %s",
            ctx(
                nb=len(non_calcules),
                agrebals=non_calcules[:MAX_AGREBALS_DETAILLES],
                cause="voir « Agrébals non calculés » du calcul trafics PDI",
            ),
        )
    if hors_site:
        # Agrébal supprimé ou changé de site entre le calcul PDI et celui-ci.
        rapport.alerte(
            f"{len(hors_site)} Agrébal(s) calculé(s) mais plus actif(s) sur le site "
            f"({hors_site[:MAX_AGREBALS_DETAILLES]})"
        )
        logger.warning(
            "Agrébals calculés hors site %s",
            ctx(nb=len(hors_site), agrebals=hors_site[:MAX_AGREBALS_DETAILLES]),
        )
    return non_calcules


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
