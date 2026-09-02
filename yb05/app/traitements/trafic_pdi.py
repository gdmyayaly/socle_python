"""DSR-702 — calcul des trafics PDI d'un scénario (et DSR-700, par son étape 3).

    trafic = TMH × coefficient de rétention × clé de répartition du PDI

pour chaque combinaison PDI × produit × jour × densité, écrite dans `trppu_trafic_pdi`.

DSR-700 n'a pas de traitement propre : son exigence — un scénario mémorise le référentiel et la
version de clés utilisés, et les conserve — est satisfaite par l'étape 3 ci-dessous, qui écrit
`id_referentiel` et `id_version_cle` dans le scénario avant le premier calcul.

Ordre des écritures, et pourquoi :

1. le verrou (`calcul_trafic_en_cours = 1`) est posé et commité **seul**, sinon il resterait
   invisible des autres processus jusqu'au commit final — trop tard pour empêcher un doublon ;
2. le référentiel et la version de clés sont écrits ensuite, également commités seuls (CA-03 :
   mémorisés avant le premier calcul) ;
3. purge et insertion des trafics forment **une seule transaction** : à aucun moment la base ne
   doit contenir un demi-calcul.
"""

from __future__ import annotations

import logging
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from app.config import CLES_PAR_PRODUIT
from app.db.mysql import db_read, db_write
from app.traitements import scenario as scn
from app.traitements.eligibilite import controle_eligibilite
from app.traitements.erreurs import TraitementImpossible
from app.traitements.rapport import ECHEC, SUCCES, Rapport

logger = logging.getLogger(__name__)

TITRE = "Calcul des trafics PDI"

# Jours ouvrables, dans l'ordre de l'enum `jour_semaine` du schéma.
JOURS_SEMAINE = ("LUNDI", "MARDI", "MERCREDI", "JEUDI", "VENDREDI", "SAMEDI")

# `trppu_pic_coefficients.densite` (0, 1, 2) vers les colonnes de `trppu_trafic_pdi`.
# L'association n'est écrite nulle part : elle suit l'ordre des colonnes de la table cible et
# la contrainte `chk_pic_densite`. À confirmer par le métier.
COLONNES_PAR_DENSITE = {0: "dense", 1: "faible1", 2: "faible2"}

# Familles de clés de `trppu_cles_repartition_calcule`, cibles du mapping CLES_PAR_PRODUIT.
COLONNE_PAR_FAMILLE = {
    "colis": "cle_colis",
    "oo": "cle_oo",
    "3s": "cle_3s",
    "potentielip": "cle_potentielip",
}

# `dense`, `faible1` et `faible2` sont des `smallint unsigned` : au-delà, MySQL refuse la ligne
# (mode strict) ou la tronque en silence. On préfère l'échec explicite.
TRAFIC_MAX = 65535

TAILLE_LOT = 5000

# TMH retenu : somme des lignes non exclues, hors produits mis de côté par le scénario.
# `trppu_tmh` autorise plusieurs lignes par produit depuis la migration du 24/06/2026, et deux
# mécanismes d'exclusion coexistent — `bl_exclu` sur la ligne, `trppu_scenario_exclusions` sur
# le produit.
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

DELETE_TRAFIC_AGREBAL_SQL = "DELETE FROM trppu_trafic_agrebal WHERE id_scenario = %s"
DELETE_TRAFIC_PDI_SQL = "DELETE FROM trppu_trafic_pdi WHERE id_scenario = %s"

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
    rapport = Rapport(titre=TITRE, id_scenario=id_scenario)

    # Étape 1 — éligibilité. Non éligible : aucune écriture, aucun verrou (CA-01).
    eligibilite = await controle_eligibilite(id_scenario, db_lecture=db_lecture)
    if not eligibilite.reussi:
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

    # Étape 2 — verrou. 0 ligne affectée : un autre processus a pris le scénario entre le
    # contrôle et maintenant. Il est propriétaire, on ne touche à rien.
    if not await scn.prendre_verrou(db_ecriture, id_scenario):
        rapport.ko("Un calcul de trafic est déjà en cours", libelle="Verrou du scénario")
        rapport.statut = ECHEC
        rapport.etats["TRAFIC_PDI_CALCULE"] = 0
        return rapport
    rapport.ok("Verrou posé (CALCUL_TRAFIC_EN_COURS = 1)")

    try:
        nb_lignes = await _calculer(rapport, scenario, raison, db_lecture, db_ecriture)
    except Exception as erreur:  # noqa: BLE001 — tout échec doit libérer le verrou
        logger.exception("Calcul des trafics PDI du scénario %s en échec", id_scenario)
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
    rapport.statut = SUCCES
    rapport.etats["TRAFIC_PDI_CALCULE"] = 1
    # Volontairement laissé à 1 : c'est le calcul des trafics Agrébal qui libère le scénario.
    rapport.etats["CALCUL_TRAFIC_EN_COURS"] = 1
    return rapport


async def _calculer(
    rapport: Rapport, scenario: dict, raison: str, db_lecture, db_ecriture
) -> int:
    """Étapes 3 à 7. Toute anomalie lève : l'appelant libère le verrou et journalise."""
    id_scenario = scenario["id_scenario"]
    co_regate = scenario["co_regate"]

    # Étape 3 — traçabilité (DSR-700). Commitée avant le calcul, comme l'exige le CA-03.
    version = await scn.version_cle_active(db_lecture, co_regate)
    referentiel = await scn.dernier_referentiel(db_lecture, co_regate)
    if version is None:
        raise TraitementImpossible(f"Aucune version de clés active pour le site {co_regate}")
    id_version_cle = int(version["id_version_cle"])
    id_referentiel = referentiel if referentiel is not None else int(version["id_referentiel"])
    await db_ecriture.execute(
        UPDATE_TRACABILITE_SQL, (id_referentiel, id_version_cle, id_scenario)
    )
    rapport.ok(f"Référentiel associé ({id_referentiel})")
    rapport.ok(f"Version de clés associée ({id_version_cle})")

    # Étape 5 — chargement (avant la purge : rien n'est détruit si une donnée manque).
    tmh = await _charger_tmh(db_lecture, id_scenario)
    rapport.ok(f"TMH chargés ({len(tmh)} produit(s))")

    coefficients = await _charger_coefficients(db_lecture, scenario["id_pic_version"])
    rapport.ok(f"Coefficients de rétention chargés ({len(coefficients)} combinaison(s))")

    cles = await _charger_cles(db_lecture, id_version_cle, co_regate)
    rapport.ok(f"Clés de répartition chargées ({len(cles)} PDI)")

    agrebal_par_pdi = await _charger_mapping_agrebal(db_lecture, co_regate)
    rapport.ok(f"Mapping Agrébal/PDI chargé ({len(agrebal_par_pdi)} PDI)")

    _signaler_ecarts_de_perimetre(rapport, cles, agrebal_par_pdi)

    jours = _jours_a_calculer(scenario, coefficients)
    rapport.ok(f"Jours calculés : {', '.join(jours)}")

    lignes = _construire_lignes(scenario, tmh, coefficients, cles, agrebal_par_pdi, jours)
    if not lignes:
        raise TraitementImpossible(
            "Aucun trafic PDI à écrire : vérifier les TMH, les coefficients et les clés"
        )

    # Étapes 4 et 7 — purge puis insertion, dans une seule transaction.
    async with db_ecriture.transaction() as tx:
        await tx.execute(DELETE_TRAFIC_AGREBAL_SQL, (id_scenario,))
        await tx.execute(DELETE_TRAFIC_PDI_SQL, (id_scenario,))
        await tx.execute(RESET_FLAGS_SQL, (id_scenario,))
        for debut in range(0, len(lignes), TAILLE_LOT):
            await tx.execute_many(INSERT_TRAFIC_PDI_SQL, lignes[debut : debut + TAILLE_LOT])
        await tx.execute(
            scn.INSERT_RECALCUL_LOG_SQL,
            (id_scenario, raison, _commentaire(raison, len(lignes))),
        )
        await tx.execute(MARQUER_PDI_CALCULE_SQL, (id_scenario,))

    logger.info(
        "Trafics PDI du scénario %s : %d lignes écrites (raison %s)",
        id_scenario,
        len(lignes),
        raison,
    )
    return len(lignes)


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
    """Coefficients indexés par (produit, jour) puis par densité.

    `dt_effet` et `dt_fin` ne sont pas filtrés : la clé unique
    `(id_pic_version, co_produit, jour_semaine, densite)` garantit déjà une seule ligne par
    combinaison, ces deux dates ne discriminent donc rien.
    """
    lignes = await db_lecture.fetch_all(SELECT_COEFFICIENTS_SQL, (id_pic_version,))
    coefficients: dict[tuple[str, str], dict[int, Decimal]] = {}
    for ligne in lignes:
        cle = (ligne["co_produit"], ligne["jour_semaine"])
        densite = int(ligne["densite"])
        coefficients.setdefault(cle, {})[densite] = Decimal(str(ligne["coef"]))
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


async def _charger_mapping_agrebal(db_lecture, co_regate: str) -> dict[int, tuple[int, str]]:
    """PDI -> (id_agrebal, agrebal_uuid), à partir des Agrébals actifs du site."""
    agrebals = await scn.agrebals_du_site(db_lecture, co_regate)
    mapping: dict[int, tuple[int, str]] = {}
    for agrebal in agrebals:
        for id_pdi in agrebal["pdi_ids"]:
            mapping[id_pdi] = (int(agrebal["agrebal_id"]), agrebal["agrebal_uuid"])
    if not mapping:
        raise TraitementImpossible(f"Aucun PDI rattaché aux Agrébals du site {co_regate}")
    return mapping


# ---------------------------------------------------------------------------
# Calcul
# ---------------------------------------------------------------------------


def _signaler_ecarts_de_perimetre(
    rapport: Rapport, cles: dict[int, Any], agrebal_par_pdi: dict[int, Any]
) -> None:
    """Compte les PDI connus d'un seul des deux côtés.

    Un PDI sans Agrébal ne peut pas être écrit (`id_agrebal` est NOT NULL) et un PDI sans clé
    n'a pas de trafic : dans les deux cas c'est un référentiel désynchronisé, pas une ligne à
    inventer. Le rapport le dit plutôt que de le taire.
    """
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


def _jours_a_calculer(scenario: dict, coefficients: dict[tuple[str, str], Any]) -> list[str]:
    """Jours du calcul : la semaine du scénario, limitée aux jours réellement coefficientés.

    Aucun ticket ne dit quels jours calculer. `nb_jours_semaine` (5 ou 6) donne la semaine
    d'exploitation du scénario ; les coefficients disent ce qui est paramétré.

    La tolérance s'arrête ici, au niveau de la **version PIC** : un jour qu'aucun produit ne
    coefficiente n'est simplement pas calculé. En revanche, une fois le jour retenu, un produit
    qui n'a pas de coefficient pour ce jour est une anomalie de paramétrage — c'est
    `_construire_lignes` qui la refuse. Les deux cas sont différents : ne pas exploiter le
    samedi est un choix, oublier le samedi d'un seul produit est un oubli.
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
    """Toutes les lignes de `trppu_trafic_pdi` à insérer, dans l'ordre des paramètres du SQL."""
    id_scenario = scenario["id_scenario"]
    co_regate = scenario["co_regate"]
    lignes: list[tuple] = []
    manquants: list[str] = []

    for produit, valeur_tmh in tmh.items():
        colonne_cle = _colonne_cle(produit)
        for jour in jours:
            coefs = coefficients.get((produit, jour))
            if not coefs:
                manquants.append(f"{produit}/{jour}")
                continue
            for id_pdi, cles_du_pdi in cles.items():
                agrebal = agrebal_par_pdi.get(id_pdi)
                if agrebal is None:
                    continue
                cle = cles_du_pdi[colonne_cle]
                trafics = [
                    _arrondir(valeur_tmh * coefs.get(densite, Decimal(0)) * cle, produit, jour)
                    for densite in sorted(COLONNES_PAR_DENSITE)
                ]
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

    if manquants:
        raise TraitementImpossible(
            "Coefficients de rétention manquants pour "
            f"{len(manquants)} combinaison(s) produit/jour : {', '.join(manquants[:10])}"
        )
    return lignes


def _colonne_cle(produit: str) -> str:
    """Colonne de clé à utiliser pour un produit, via le mapping de configuration."""
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


def _arrondir(valeur: Decimal, produit: str, jour: str) -> int:
    """Arrondi à l'entier, avec refus explicite au-delà de la capacité de la colonne."""
    entier = int(valeur.quantize(Decimal(1), rounding=ROUND_HALF_UP))
    if entier < 0:
        raise TraitementImpossible(
            f"Trafic négatif calculé pour {produit}/{jour} : {valeur}"
        )
    if entier > TRAFIC_MAX:
        raise TraitementImpossible(
            f"Trafic {entier} hors capacité de la colonne pour {produit}/{jour} "
            f"(maximum {TRAFIC_MAX} — smallint unsigned)"
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
