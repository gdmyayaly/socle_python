"""DSR-702 — calcul des trafics PDI d'un scénario (et DSR-700, par son étape 3).

    trafic = TMH × coefficient de rétention / 100 × clé de répartition du PDI

Le coefficient est stocké en pourcentage dans `trppu_pic_coefficients.coef` (52 = 52 %) ;
il est ramené en fraction (0.52) dès sa lecture, dans `_charger_coefficients`. Tout ce qui
suit (calcul, logs `coefs=`, formule des échecs) manipule donc la fraction.

par PDI × produit × jour × densité, écrit dans `trppu_trafic_pdi`. DSR-700 = étape 3
(référentiel et version de clés mémorisés dans le scénario avant le calcul, CA-03).

Ordre des écritures : verrou commité seul (sinon invisible des autres processus),
traçabilité commitée seule, flags remis à 0, purge puis insertion **par lots commités un à
un** (`TAILLE_LOT` lignes), enfin trace + `trafic_pdi_calcule = 1` ensemble.

Pourquoi des lots : sous réplication de groupe MySQL, une transaction unique sur un gros
scénario dépasse `group_replication_transaction_size_limit` et le COMMIT est refusé
(erreur 3100 « Error on observer while running replication hook 'before_commit' »).
Contrepartie : un échec en cours d'écriture laisse un état partiel. Il reste sans effet
parce que `trafic_pdi_calcule = 0` jusqu'au dernier commit (rien ne le prend pour un
calcul fini), que les lignes partielles sont purgées sur l'échec quand c'est possible, et
que tout nouveau calcul repurge le scénario avant d'écrire.
"""

from __future__ import annotations

import logging
import time
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, NamedTuple

from app.config import CLES_PAR_PRODUIT, TAILLE_LOT, TRAFIC_MAX
from app.db.mysql import db_read, db_write
from app.log_utils import ctx
from app.traitements import scenario as scn
from app.traitements.eligibilite import controle_eligibilite
from app.traitements.erreurs import TraitementImpossible
from app.traitements.rapport import ECHEC, SUCCES, Rapport

logger = logging.getLogger(__name__)

TITRE = "Calcul des trafics PDI"

# Jours ouvrables, dans l'ordre de l'enum `jour_semaine` du schéma.
JOURS_SEMAINE = ("LUNDI", "MARDI", "MERCREDI", "JEUDI", "VENDREDI", "SAMEDI")

# `trppu_pic_coefficients.densite` (0, 1, 2) vers les colonnes de `trppu_trafic_pdi`.
# Association déduite de l'ordre des colonnes : à confirmer par le métier.
COLONNES_PAR_DENSITE = {0: "dense", 1: "faible1", 2: "faible2"}

# Familles de clés de `trppu_cles_repartition_calcule`, cibles du mapping CLES_PAR_PRODUIT.
COLONNE_PAR_FAMILLE = {
    "colis": "cle_colis",
    "oo": "cle_oo",
    "3s": "cle_3s",
    "potentielip": "cle_potentielip",
}

# Plafond des colonnes de trafic : `TRAFIC_MAX` (app/config.py). Renseigné, on échoue
# explicitement au-delà plutôt que de laisser MySQL refuser ou tronquer ; vide, aucun contrôle
# et l'erreur MySQL remonte. Colonnes ex-`smallint unsigned` (65 535), trop étroites pour un
# gros site (scénario 15, site 133950 en prod : 108 716) — élargies en `decimal(12,4)` par
# `db/fix_trafic_pdi_capacite.sql`. Le trafic reste arrondi à l'entier (OPTIPACC le restitue
# en `int`).

# TMH retenu : somme des lignes par produit, hors `bl_exclu` (ligne) et
# `trppu_scenario_exclusions` (produit).
SELECT_TMH_SQL = """
    SELECT t.co_produit,
           SUM(t.moyenne_hebdo) AS tmh
      FROM trppu_tmh t
     WHERE t.id_scenario = %s
       AND t.bl_exclu = 0
       AND NOT EXISTS (SELECT 1
                         FROM trppu_scenario_exclusions e
                        WHERE e.id_scenario = t.id_scenario
                          AND e.co_produit = t.co_produit)
     GROUP BY t.co_produit
     ORDER BY t.co_produit
"""

SELECT_COEFFICIENTS_SQL = """
    SELECT co_produit, jour_semaine, densite, coef
      FROM trppu_pic_coefficients
     WHERE id_pic_version = %s
"""

SELECT_CLES_SQL = """
    SELECT id_pdi, cle_colis, cle_oo, cle_3s, cle_potentielip
      FROM trppu_cles_repartition_calcule
     WHERE id_version_cle = %s
       AND co_regate_site = %s
"""

UPDATE_TRACABILITE_SQL = """
    UPDATE trppu_scenario
       SET id_referentiel = %s,
           id_version_cle = %s
     WHERE id_scenario = %s
"""

# Purge par lots (`LIMIT`) : chaque DELETE est commité seul, comme l'insertion.
DELETE_TRAFIC_AGREBAL_SQL = "DELETE FROM trppu_trafic_agrebal WHERE id_scenario = %s LIMIT %s"
DELETE_TRAFIC_PDI_SQL = "DELETE FROM trppu_trafic_pdi WHERE id_scenario = %s LIMIT %s"

RESET_FLAGS_SQL = """
    UPDATE trppu_scenario
       SET trafic_pdi_calcule = 0,
           trafic_agrebal_calcule = 0
     WHERE id_scenario = %s
"""

INSERT_TRAFIC_PDI_SQL = """
    INSERT INTO trppu_trafic_pdi
        (id_scenario, co_regate, id_agrebal, agrebal_uuid, id_pdi, co_produit,
         jour_semaine, dense, faible1, faible2, dt_calcul)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
"""

MARQUER_PDI_CALCULE_SQL = """
    UPDATE trppu_scenario
       SET trafic_pdi_calcule = 1
     WHERE id_scenario = %s
"""


async def calcul_trafic_pdi(
    id_scenario: int, *, db_lecture=db_read, db_ecriture=db_write
) -> Rapport:
    """Calcule et enregistre les trafics PDI du scénario. Ne lève pas : rend un rapport."""
    debut = time.perf_counter()
    rapport = Rapport(titre=TITRE, id_scenario=id_scenario)
    logger.info("Début calcul trafics PDI %s", ctx(id_scenario=id_scenario))

    # Étape 1 — éligibilité. Non éligible : aucune écriture, aucun verrou (CA-01).
    eligibilite = await controle_eligibilite(id_scenario, db_lecture=db_lecture)
    if not eligibilite.reussi:
        logger.warning(
            "Rejet calcul trafics PDI %s",
            ctx(verdict=ECHEC, motif="scénario non éligible", motifs=eligibilite.motifs),
        )
        rapport.ko(
            "Contrôle d'éligibilité : scénario non éligible au calcul",
            libelle="Contrôle d'éligibilité",
        )
        for motif in eligibilite.motifs:
            rapport.ko(motif)
        rapport.statut = ECHEC
        rapport.etats["TRAFIC_PDI_CALCULE"] = 0
        return rapport
    rapport.ok("Contrôle d'éligibilité")

    scenario = await scn.charger_scenario(db_lecture, id_scenario)
    raison = await scn.determiner_raison(db_lecture, scenario)

    # Étape 2 — verrou. 0 ligne affectée : un autre processus détient le scénario.
    if not await scn.prendre_verrou(db_ecriture, id_scenario):
        logger.warning(
            "Rejet calcul trafics PDI %s",
            ctx(verdict=ECHEC, motif="verrou déjà pris par un autre calcul"),
        )
        rapport.ko("Un calcul de trafic est déjà en cours", libelle="Verrou du scénario")
        rapport.statut = ECHEC
        rapport.etats["TRAFIC_PDI_CALCULE"] = 0
        return rapport
    rapport.ok("Verrou posé (CALCUL_TRAFIC_EN_COURS = 1)")
    logger.debug("Verrou posé %s", ctx(raison=raison))

    try:
        nb_lignes, agrebals = await _calculer(rapport, scenario, raison, db_lecture, db_ecriture)
    except Exception as erreur:  # noqa: BLE001 — tout échec doit libérer le verrou
        logger.exception("Erreur calcul trafics PDI %s", ctx(raison=raison))
        await scn.liberer_verrou(db_ecriture, id_scenario)
        await scn.journaliser(
            db_ecriture,
            id_scenario,
            raison,
            f"Recalcul interrompu - {erreur}",
        )
        rapport.erreur = str(erreur)
        rapport.statut = ECHEC
        rapport.etats["TRAFIC_PDI_CALCULE"] = 0
        rapport.etats["CALCUL_TRAFIC_EN_COURS"] = 0
        return rapport

    rapport.ok(f"{nb_lignes} lignes trafic PDI calculées")
    logger.info(
        "Fin calcul trafics PDI %s",
        ctx(
            lignes=nb_lignes,
            agrebals_a_calculer=len(agrebals.a_calculer),
            agrebals_calcules=len(agrebals.calcules),
            raison=raison,
            duration_ms=(time.perf_counter() - debut) * 1000,
        ),
    )
    rapport.statut = SUCCES
    rapport.etats["TRAFIC_PDI_CALCULE"] = 1
    # Volontairement laissé à 1 : c'est le calcul des trafics Agrébal qui libère le scénario.
    rapport.etats["CALCUL_TRAFIC_EN_COURS"] = 1
    rapport.etats["NB_AGREBAL_A_CALCULER"] = len(agrebals.a_calculer)
    rapport.etats["NB_AGREBAL_CALCULES"] = len(agrebals.calcules)
    return rapport


async def _calculer(
    rapport: Rapport, scenario: dict, raison: str, db_lecture, db_ecriture
) -> tuple[int, "BilanAgrebals"]:
    """Étapes 3 à 7. Toute anomalie lève : l'appelant libère le verrou et journalise."""
    id_scenario = scenario["id_scenario"]
    co_regate = scenario["co_regate"]
    debut_phase = time.perf_counter()

    # Étape 3 — traçabilité (DSR-700, CA-03), commitée avant le calcul. Référentiel lu dans
    # `trppu_version_cle` (`trppu_referentiel` n'est plus utilisée).
    version = await scn.version_cle_active(db_lecture, co_regate)
    if version is None:
        raise TraitementImpossible(f"Aucune version de clés active pour le site {co_regate}")
    id_referentiel = scn.referentiel_de_la_version(version)
    if id_referentiel is None:
        raise TraitementImpossible(
            f"La version de clés active du site {co_regate} ne porte aucun référentiel"
        )
    id_version_cle = int(version["id_version_cle"])
    await db_ecriture.execute(
        UPDATE_TRACABILITE_SQL, (id_referentiel, id_version_cle, id_scenario)
    )
    rapport.ok(f"Référentiel associé ({id_referentiel})")
    rapport.ok(f"Version de clés associée ({id_version_cle})")
    logger.info(
        "Avancement calcul trafics PDI %s",
        ctx(
            phase="traçabilité",
            id_referentiel=id_referentiel,
            id_version_cle=id_version_cle,
        ),
    )
    debut_phase = time.perf_counter()

    # Étape 5 — chargement (avant la purge : rien n'est détruit si une donnée manque).
    tmh = await _charger_tmh(db_lecture, id_scenario)
    rapport.ok(f"TMH chargés ({len(tmh)} produit(s))")

    coefficients = await _charger_coefficients(db_lecture, scenario["id_pic_version"])
    rapport.ok(f"Coefficients de rétention chargés ({len(coefficients)} combinaison(s))")

    cles = await _charger_cles(db_lecture, id_version_cle, co_regate)
    rapport.ok(f"Clés de répartition chargées ({len(cles)} PDI)")

    agrebal_par_pdi, agrebals_du_site = await _charger_mapping_agrebal(db_lecture, co_regate)
    rapport.ok(
        f"Mapping Agrébal/PDI chargé ({len(agrebal_par_pdi)} PDI, "
        f"{len(agrebals_du_site)} Agrébal(s) actif(s) sur le site)"
    )

    logger.info(
        "Avancement calcul trafics PDI %s",
        ctx(
            phase="chargement",
            tmh=len(tmh),
            coefficients=len(coefficients),
            cles=len(cles),
            agrebal_par_pdi=len(agrebal_par_pdi),
            duration_ms=_ms(debut_phase),
        ),
    )
    # Entrées du calcul, de quoi le rejouer à la main (cf. db/verif_trafic_pdi.sql).
    logger.info(
        "Avancement calcul trafics PDI %s",
        ctx(
            phase="entrées",
            id_pic_version=scenario["id_pic_version"],
            id_version_cle=id_version_cle,
            nb_jours_semaine=scenario.get("nb_jours_semaine"),
            tmh_hebdo={produit: str(valeur) for produit, valeur in tmh.items()},
            cle_par_produit={produit: _colonne_cle_ou_none(produit) for produit in tmh},
            trafic_max=TRAFIC_MAX,
        ),
    )
    debut_phase = time.perf_counter()

    _signaler_ecarts_de_perimetre(rapport, cles, agrebal_par_pdi)

    jours = _jours_a_calculer(scenario, coefficients)
    rapport.ok(f"Jours calculés : {', '.join(jours)}")
    logger.debug("Jours à calculer %s", ctx(nb=len(jours), jours=jours))

    lignes = _construire_lignes(scenario, tmh, coefficients, cles, agrebal_par_pdi, jours)
    logger.info(
        "Avancement calcul trafics PDI %s",
        ctx(
            phase="construction",
            lignes=len(lignes),
            jours=len(jours),
            taille_lot=TAILLE_LOT,
            duration_ms=_ms(debut_phase),
        ),
    )
    debut_phase = time.perf_counter()
    bilan_agrebals = _bilan_agrebals(agrebals_du_site, agrebal_par_pdi, cles, lignes)
    _signaler_bilan_agrebals(rapport, bilan_agrebals)
    if not lignes:
        raise TraitementImpossible(
            "Aucun trafic PDI à écrire : vérifier les TMH, les coefficients et les clés"
        )

    # Étape 4 — invalidation d'abord : dès ce commit, plus rien ne prend les trafics du
    # scénario pour un calcul fini (`trafic_pdi_calcule = 0` jusqu'au marquage final).
    await db_ecriture.execute(RESET_FLAGS_SQL, (id_scenario,))
    purges_agrebal = await _purger(db_ecriture, DELETE_TRAFIC_AGREBAL_SQL, id_scenario)
    purges_pdi = await _purger(db_ecriture, DELETE_TRAFIC_PDI_SQL, id_scenario)
    logger.info(
        "Avancement calcul trafics PDI %s",
        ctx(
            phase="purge",
            trafics_agrebal=purges_agrebal,
            trafics_pdi=purges_pdi,
            duration_ms=_ms(debut_phase),
        ),
    )
    debut_phase = time.perf_counter()

    # Étape 7 — insertion, un lot = une transaction. Pas de rejeu automatique d'un lot en
    # échec : aucune clé unique n'empêcherait de le doubler si son COMMIT était passé.
    nb_lots = 0
    try:
        for depart in range(0, len(lignes), TAILLE_LOT):
            lot = lignes[depart : depart + TAILLE_LOT]
            async with db_ecriture.transaction() as tx:
                await tx.execute_many(INSERT_TRAFIC_PDI_SQL, lot)
            nb_lots += 1
            if len(lignes) > TAILLE_LOT:
                # Plusieurs lots : l'écriture peut durer, on montre qu'elle avance.
                logger.info(
                    "Avancement calcul trafics PDI %s",
                    ctx(
                        phase="écriture",
                        lots=nb_lots,
                        lignes=depart + len(lot),
                        lignes_total=len(lignes),
                        duration_ms=_ms(debut_phase),
                    ),
                )
        async with db_ecriture.transaction() as tx:
            await tx.execute(
                scn.INSERT_RECALCUL_LOG_SQL,
                (id_scenario, raison, _commentaire(raison, len(lignes))),
            )
            await tx.execute(MARQUER_PDI_CALCULE_SQL, (id_scenario,))
    except Exception:
        await _purger_lignes_partielles(db_ecriture, id_scenario, nb_lots)
        raise

    logger.info(
        "Trafics PDI écrits %s",
        ctx(lignes=len(lignes), raison=raison, lots=nb_lots, duration_ms=_ms(debut_phase)),
    )
    return len(lignes), bilan_agrebals


async def _purger(db_ecriture, requete: str, id_scenario: int) -> int:
    """`DELETE … LIMIT TAILLE_LOT` répété jusqu'à épuisement ; chaque lot commité seul.

    Rejouable sans risque (un DELETE déjà passé ne trouve simplement plus rien).
    """
    total = 0
    while True:
        lignes = await db_ecriture.execute(requete, (id_scenario, TAILLE_LOT))
        total += lignes
        if lignes < TAILLE_LOT:
            return total


async def _purger_lignes_partielles(db_ecriture, id_scenario: int, lots_commites: int) -> None:
    """Après un échec d'écriture : retire les lots déjà commités. Best-effort.

    Si la purge échoue aussi (base indisponible), les lignes restent, mais
    `trafic_pdi_calcule = 0` les signale comme inexploitables et le prochain calcul les purge.
    """
    try:
        supprimees = await _purger(db_ecriture, DELETE_TRAFIC_PDI_SQL, id_scenario)
    except Exception as erreur:  # noqa: BLE001 — ne pas masquer l'erreur d'origine
        logger.warning(
            "Purge des trafics PDI partiels impossible %s",
            ctx(lots_commites=lots_commites, erreur=str(erreur)),
        )
        return
    logger.warning(
        "Trafics PDI partiels purgés %s",
        ctx(lots_commites=lots_commites, rows_affected=supprimees),
    )


def _ms(depuis: float) -> float:
    """Durée écoulée depuis `depuis` (perf_counter), en millisecondes arrondies."""
    return round((time.perf_counter() - depuis) * 1000, 1)


# ---------------------------------------------------------------------------
# Chargements
# ---------------------------------------------------------------------------


async def _charger_tmh(db_lecture, id_scenario: int) -> dict[str, Decimal]:
    """TMH par produit, produits à TMH nul ou absent écartés."""
    lignes = await db_lecture.fetch_all(SELECT_TMH_SQL, (id_scenario,))
    tmh = {
        ligne["co_produit"]: Decimal(str(ligne["tmh"]))
        for ligne in lignes
        if ligne["tmh"] is not None and Decimal(str(ligne["tmh"])) > 0
    }
    if not tmh:
        raise TraitementImpossible(
            f"Aucun TMH exploitable pour le scénario {id_scenario} "
            "(lignes exclues, produits exclus, ou moyennes hebdomadaires nulles)"
        )
    return tmh


async def _charger_coefficients(
    db_lecture, id_pic_version: int
) -> dict[tuple[str, str], dict[int, Decimal]]:
    """Coefficients par (produit, jour) puis densité ; dates non filtrées (clé unique).

    `coef` est un pourcentage en base : divisé par 100 ici, une seule fois.
    """
    lignes = await db_lecture.fetch_all(SELECT_COEFFICIENTS_SQL, (id_pic_version,))
    coefficients: dict[tuple[str, str], dict[int, Decimal]] = {}
    for ligne in lignes:
        cle = (ligne["co_produit"], ligne["jour_semaine"])
        densite = int(ligne["densite"])
        coefficients.setdefault(cle, {})[densite] = Decimal(str(ligne["coef"])) / 100
    if not coefficients:
        raise TraitementImpossible(
            f"Aucun coefficient de rétention trouvé pour la version PIC {id_pic_version}"
        )
    return coefficients


async def _charger_cles(
    db_lecture, id_version_cle: int, co_regate: str
) -> dict[int, dict[str, Decimal]]:
    """Clés de répartition de la version, par PDI."""
    lignes = await db_lecture.fetch_all(SELECT_CLES_SQL, (id_version_cle, co_regate))
    if not lignes:
        raise TraitementImpossible(
            f"Aucune clé de répartition trouvée pour la version {id_version_cle}"
        )
    return {
        int(ligne["id_pdi"]): {
            colonne: Decimal(str(ligne[colonne])) for colonne in COLONNE_PAR_FAMILLE.values()
        }
        for ligne in lignes
    }


async def _charger_mapping_agrebal(
    db_lecture, co_regate: str
) -> tuple[dict[int, tuple[int, str]], list[dict[str, Any]]]:
    """PDI -> (id_agrebal, agrebal_uuid), et les Agrébals actifs du site (bilan Agrébal).

    Un PDI cité par plusieurs Agrébals est rattaché au **dernier** lu (ordre `agrebal_id`) :
    les autres le perdent. Comportement inchangé, rendu visible par `_bilan_agrebals`.
    """
    agrebals = await scn.agrebals_du_site(db_lecture, co_regate)
    mapping: dict[int, tuple[int, str]] = {}
    for agrebal in agrebals:
        for id_pdi in agrebal["pdi_ids"]:
            mapping[id_pdi] = (int(agrebal["agrebal_id"]), agrebal["agrebal_uuid"])
    if not mapping:
        raise TraitementImpossible(f"Aucun PDI rattaché aux Agrébals du site {co_regate}")
    return mapping, agrebals


# ---------------------------------------------------------------------------
# Calcul
# ---------------------------------------------------------------------------


def _signaler_ecarts_de_perimetre(
    rapport: Rapport, cles: dict[int, Any], agrebal_par_pdi: dict[int, Any]
) -> None:
    """Signale au rapport les PDI ignorés car présents d'un seul côté (clés / Agrébals)."""
    sans_agrebal = sorted(set(cles) - set(agrebal_par_pdi))
    sans_cle = sorted(set(agrebal_par_pdi) - set(cles))
    if sans_agrebal:
        rapport.ok(
            f"{len(sans_agrebal)} PDI avec clé mais sans Agrébal, ignorés "
            f"(ex. {sans_agrebal[:5]})"
        )
    if sans_cle:
        rapport.ok(
            f"{len(sans_cle)} PDI rattachés à un Agrébal mais sans clé, ignorés "
            f"(ex. {sans_cle[:5]})"
        )


# Agrébals non calculés détaillés dans le rapport et les logs ; le total reste exact.
MAX_AGREBALS_DETAILLES = 20


class BilanAgrebals(NamedTuple):
    """Agrébals du site attendus au calcul, ceux qui ont produit du trafic, et pourquoi pas les
    autres. `trppu_trafic_agrebal` n'étant qu'un `GROUP BY` de `trppu_trafic_pdi`, un Agrébal
    absent ici le sera aussi au calcul Agrébal : c'est à cette étape que se lit la cause."""

    a_calculer: list[int]
    calcules: list[int]
    non_calcules: dict[int, str]
    """id_agrebal -> motif."""
    pdi_partages: dict[int, list[int]]
    """id_pdi cité par plusieurs Agrébals -> ces Agrébals (seul le dernier le garde)."""


def _bilan_agrebals(
    agrebals: list[dict[str, Any]],
    agrebal_par_pdi: dict[int, tuple[int, str]],
    cles: dict[int, Any],
    lignes: list[tuple],
) -> BilanAgrebals:
    """Compare les Agrébals actifs du site à ceux présents dans les lignes PDI construites."""
    a_calculer = list(dict.fromkeys(int(a["agrebal_id"]) for a in agrebals))
    # Position 2 des lignes : id_agrebal (cf. INSERT_TRAFIC_PDI_SQL).
    presents = {int(ligne[2]) for ligne in lignes}
    calcules = [id_agrebal for id_agrebal in a_calculer if id_agrebal in presents]

    citations: dict[int, list[int]] = {}
    for agrebal in agrebals:
        for id_pdi in agrebal["pdi_ids"]:
            citations.setdefault(id_pdi, []).append(int(agrebal["agrebal_id"]))
    pdi_partages = {
        id_pdi: list(dict.fromkeys(ids))
        for id_pdi, ids in citations.items()
        if len(set(ids)) > 1
    }

    non_calcules: dict[int, str] = {}
    for agrebal in agrebals:
        id_agrebal = int(agrebal["agrebal_id"])
        if id_agrebal in presents or id_agrebal in non_calcules:
            continue
        non_calcules[id_agrebal] = _motif_agrebal_non_calcule(
            id_agrebal, agrebal["pdi_ids"], agrebal_par_pdi, cles
        )
    return BilanAgrebals(a_calculer, calcules, non_calcules, pdi_partages)


def _motif_agrebal_non_calcule(
    id_agrebal: int,
    pdi_ids: list[int],
    agrebal_par_pdi: dict[int, tuple[int, str]],
    cles: dict[int, Any],
) -> str:
    if not pdi_ids:
        return "aucun PDI dans agrebal_pdiList (vide ou illisible)"
    propres = [p for p in pdi_ids if agrebal_par_pdi.get(p, (None,))[0] == id_agrebal]
    if not propres:
        return f"ses {len(pdi_ids)} PDI sont tous rattachés à un autre Agrébal"
    if not any(p in cles for p in propres):
        return (
            f"aucun de ses {len(propres)} PDI n'a de clé de répartition "
            f"(ex. {sorted(propres)[:5]})"
        )
    return "aucune ligne de trafic produite"


def _signaler_bilan_agrebals(rapport: Rapport, bilan: BilanAgrebals) -> None:
    """Rapport (non bloquant) et logs : Agrébals à calculer, calculés, et pourquoi pas les autres."""
    rapport.ok(
        f"Agrébals : {len(bilan.calcules)} calculé(s) sur {len(bilan.a_calculer)} à calculer"
    )
    if bilan.non_calcules:
        detail = "; ".join(
            f"{id_agrebal} : {motif}"
            for id_agrebal, motif in list(bilan.non_calcules.items())[:MAX_AGREBALS_DETAILLES]
        )
        rapport.alerte(f"{len(bilan.non_calcules)} Agrébal(s) non calculé(s) — {detail}")
    if bilan.pdi_partages:
        exemples = dict(list(bilan.pdi_partages.items())[:5])
        rapport.alerte(
            f"{len(bilan.pdi_partages)} PDI cité(s) par plusieurs Agrébals, rattaché(s) au "
            f"dernier seulement (ex. PDI -> Agrébals : {exemples})"
        )

    logger.info(
        "Avancement calcul trafics PDI %s",
        ctx(
            phase="agrébals",
            agrebals_a_calculer=len(bilan.a_calculer),
            agrebals_calcules=len(bilan.calcules),
            agrebals_non_calcules=len(bilan.non_calcules),
            pdi_partages=len(bilan.pdi_partages),
        ),
    )
    if bilan.non_calcules:
        logger.warning(
            "Agrébals non calculés %s",
            ctx(
                nb=len(bilan.non_calcules),
                agrebals=dict(list(bilan.non_calcules.items())[:MAX_AGREBALS_DETAILLES]),
            ),
        )
    if bilan.pdi_partages:
        logger.warning(
            "PDI partagés entre Agrébals %s",
            ctx(
                nb=len(bilan.pdi_partages),
                consequence="PDI rattaché au dernier Agrébal lu",
                pdi=dict(list(bilan.pdi_partages.items())[:MAX_AGREBALS_DETAILLES]),
            ),
        )


def _jours_a_calculer(scenario: dict, coefficients: dict[tuple[str, str], Any]) -> list[str]:
    """Jours de la semaine du scénario (5 ou 6) coefficientés par au moins un produit.

    Un jour retenu sans coefficient pour un produit est refusé par `_construire_lignes`.
    """
    nb_jours = scenario.get("nb_jours_semaine") or 6
    attendus = JOURS_SEMAINE[: 6 if int(nb_jours) >= 6 else 5]
    coefficientes = {jour for _, jour in coefficients}
    jours = [jour for jour in attendus if jour in coefficientes]
    if not jours:
        raise TraitementImpossible(
            "Aucun jour calculable : les coefficients de la version PIC ne couvrent aucun "
            f"jour de la semaine du scénario ({', '.join(attendus)})"
        )
    return jours


def _construire_lignes(
    scenario: dict,
    tmh: dict[str, Decimal],
    coefficients: dict[tuple[str, str], dict[int, Decimal]],
    cles: dict[int, dict[str, Decimal]],
    agrebal_par_pdi: dict[int, tuple[int, str]],
    jours: list[str],
) -> list[tuple]:
    """Toutes les lignes de `trppu_trafic_pdi` à insérer, dans l'ordre des paramètres du SQL.

    trafic(PDI, produit, jour, densité) = arrondi(TMH hebdo × coef(produit, jour, densité)
    × clé du PDI pour la famille du produit). Une ligne INFO par produit/jour résume le calcul
    (totaux, plus gros PDI) ; `-v` ajoute une ligne DEBUG par PDI avec chaque facteur.
    """
    id_scenario = scenario["id_scenario"]
    co_regate = scenario["co_regate"]
    lignes: list[tuple] = []
    manquants: list[str] = []
    densites = sorted(COLONNES_PAR_DENSITE)

    for produit, valeur_tmh in tmh.items():
        colonne_cle = _colonne_cle(produit)
        for jour in jours:
            coefs = coefficients.get((produit, jour))
            if not coefs:
                manquants.append(f"{produit}/{jour}")
                continue
            facteurs = {d: coefs.get(d, Decimal(0)) for d in densites}
            nb_pdi = 0
            somme_cles = Decimal(0)
            totaux = dict.fromkeys(densites, 0)
            plus_gros: tuple[int, Decimal, list[int]] | None = None
            for id_pdi, cles_du_pdi in cles.items():
                agrebal = agrebal_par_pdi.get(id_pdi)
                if agrebal is None:
                    continue
                cle = cles_du_pdi[colonne_cle]
                trafics = [
                    _arrondir(
                        _Calcul(id_pdi, produit, jour, densite, valeur_tmh, facteurs[densite], cle)
                    )
                    for densite in densites
                ]
                logger.debug(
                    "Avancement calcul trafics PDI %s",
                    ctx(
                        phase="calcul PDI",
                        id_pdi=id_pdi,
                        produit=produit,
                        jour=jour,
                        tmh_hebdo=str(valeur_tmh),
                        colonne_cle=colonne_cle,
                        cle=str(cle),
                        coefs=_par_colonne(facteurs, str),
                        trafics=_par_colonne(dict(zip(densites, trafics))),
                    ),
                )
                nb_pdi += 1
                somme_cles += cle
                for densite, trafic in zip(densites, trafics):
                    totaux[densite] += trafic
                if plus_gros is None or cle > plus_gros[1]:
                    plus_gros = (id_pdi, cle, trafics)
                lignes.append(
                    (
                        id_scenario,
                        co_regate,
                        agrebal[0],
                        agrebal[1],
                        id_pdi,
                        produit,
                        jour,
                        *trafics,
                    )
                )
            # Contrôle manuel : total ≈ TMH × coef × somme des clés (écart = arrondis).
            logger.info(
                "Avancement calcul trafics PDI %s",
                ctx(
                    phase="détail",
                    produit=produit,
                    jour=jour,
                    tmh_hebdo=str(valeur_tmh),
                    colonne_cle=colonne_cle,
                    coefs=_par_colonne(facteurs, str),
                    nb_pdi=nb_pdi,
                    somme_cles=str(somme_cles),
                    totaux=_par_colonne(totaux),
                    pdi_max=plus_gros[0] if plus_gros else None,
                    cle_max=str(plus_gros[1]) if plus_gros else None,
                    trafics_max=(
                        _par_colonne(dict(zip(densites, plus_gros[2]))) if plus_gros else None
                    ),
                ),
            )

    if manquants:
        raise TraitementImpossible(
            "Coefficients de rétention manquants pour "
            f"{len(manquants)} combinaison(s) produit/jour : {', '.join(manquants[:10])}"
        )
    return lignes


def _par_colonne(valeurs: dict[int, Any], conversion=None) -> dict[str, Any]:
    """`{0: v, 1: v, 2: v}` -> `{"dense": v, "faible1": v, "faible2": v}` pour les logs."""
    return {
        COLONNES_PAR_DENSITE[densite]: conversion(valeur) if conversion else valeur
        for densite, valeur in valeurs.items()
    }


def _colonne_cle(produit: str) -> str:
    """Colonne de clé du produit ; produit inconnu = échec plutôt que trafic faux."""
    famille = CLES_PAR_PRODUIT.get(produit.upper())
    if famille is None:
        raise TraitementImpossible(
            f"Produit {produit} absent de CLES_PAR_PRODUIT : impossible de savoir quelle clé "
            "de répartition lui appliquer (familles attendues : "
            f"{', '.join(sorted(COLONNE_PAR_FAMILLE))})"
        )
    colonne = COLONNE_PAR_FAMILLE.get(famille)
    if colonne is None:
        raise TraitementImpossible(
            f"Famille de clé inconnue '{famille}' pour le produit {produit} "
            f"(attendu : {', '.join(sorted(COLONNE_PAR_FAMILLE))})"
        )
    return colonne


class _Calcul(NamedTuple):
    """Facteurs d'un trafic PDI, repris tels quels dans les messages d'échec."""

    id_pdi: int
    produit: str
    jour: str
    densite: int
    tmh: Decimal
    coef: Decimal
    cle: Decimal

    @property
    def valeur(self) -> Decimal:
        return self.tmh * self.coef * self.cle

    def formule(self) -> str:
        """`PDI 1001, dense : TMH 1000 × coef 0.5 × clé 0.6 = 300.0` — rejouable à la main."""
        return (
            f"PDI {self.id_pdi}, {COLONNES_PAR_DENSITE[self.densite]} : "
            f"TMH {self.tmh} × coef {self.coef} × clé {self.cle} = {self.valeur}"
        )


def _colonne_cle_ou_none(produit: str) -> str | None:
    """Colonne de clé du produit pour les logs ; None si inconnue (l'échec vient plus loin)."""
    try:
        return _colonne_cle(produit)
    except TraitementImpossible:
        return None


def _arrondir(calcul: _Calcul) -> int:
    """Arrondi à l'entier, avec refus explicite au-delà de la capacité de la colonne."""
    entier = int(calcul.valeur.quantize(Decimal(1), rounding=ROUND_HALF_UP))
    if entier < 0:
        raise TraitementImpossible(
            f"Trafic négatif calculé pour {calcul.produit}/{calcul.jour} : "
            f"{calcul.formule()}"
        )
    if TRAFIC_MAX is not None and entier > TRAFIC_MAX:
        raise TraitementImpossible(
            f"Trafic {entier} hors capacité de la colonne pour {calcul.produit}/{calcul.jour} "
            f"(maximum {TRAFIC_MAX} — TRAFIC_MAX) : {calcul.formule()}"
        )
    return entier


def _commentaire(raison: str, nb_lignes: int) -> str:
    """Commentaire de `trppu_recalcul_log`, reprenant les libellés du ticket."""
    libelles = {
        scn.RAISON_INITIAL: "Premier calcul des trafics du scénario",
        "AGREBAL": "Recalcul des trafics exécuté suite évolution Agrébal",
        "CLE_REPARTITION": (
            "Recalcul des trafics exécuté suite activation d'une nouvelle version de clés"
        ),
        "MANUEL": "Recalcul manuel du scénario",
    }
    return f"{libelles.get(raison, 'Calcul des trafics du scénario')} - {nb_lignes} lignes PDI"


__all__ = ["TITRE", "calcul_trafic_pdi"]
