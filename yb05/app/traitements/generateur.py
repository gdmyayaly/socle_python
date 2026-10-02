"""Scénarios de test complets, générés avec de fausses données, pour éprouver yb05 de bout en
bout : `generer-scenarios N`, puis `all`.

Chaque scénario reçoit tout ce que DSR-701 contrôle et que DSR-702/703 consomment :

    trppu_site                       un site de test dédié (ZT0001, ZT0002…)
    trppu_produit                    les produits de CLES_PAR_PRODUIT (créés s'ils manquent)
    trppu_pic_version                une version PIC de niveau SITE
    trppu_pic_coefficients           produit × jour (LUNDI…SAMEDI) × densité (0, 1, 2)
    trppu_version_cle                une version de clés active, avec son référentiel
                                     (règles 9 et 10)
    trppu_cles_repartition_calcule   une clé par PDI ; chaque famille somme à 1
    trppu_agrebal_pdi                des Agrébals qui se partagent les PDI (règles 11 et 12)
    trppu_scenario                   VALIDE, figé, non calculé, non verrouillé (règles 2 à 8)
    trppu_tmh                        un TMH par produit, non exclu

Tout est **marqué** — libellés `TEST YB05`, sites `ZT…`, référentiel 900000, PDI au-delà de
9·10¹² et Agrébals au-delà de 9·10⁸, hors de toute plage réelle — et `supprimer_scenarios_test` efface
exactement ces données. La génération est refusée en production (`APP_ENV=prod`).

Une seule transaction pour l'ensemble : une génération interrompue ne laisse rien.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import date
from decimal import Decimal
from typing import Any

from app.config import APP_ENV, CLES_PAR_PRODUIT
from app.db.mysql import db_read, db_write
from app.log_utils import ctx
from app.traitements.rapport import ECHEC, SUCCES, Rapport

logger = logging.getLogger(__name__)

TITRE = "Génération de scénarios de test YB05"
TITRE_SUPPRESSION = "Suppression des scénarios de test YB05"

MARQUEUR = "TEST YB05"
PREFIXE_SITE = "ZT"  # co_regate char(6) : ZT0001 … ZT9999
PREFIXE_ROC = "ZR"
LIBELLE_SITE = "SITE TEST YB05"
NUMERO_MAX = 9999
NOMBRE_MAX = 500
PDI_PAR_SITE_MAX = 999
AGREBALS_PAR_SITE_MAX = 99

# Hors de toute plage réelle : un PDI ou un Agrébal de test ne peut jamais en masquer un vrai.
PDI_BASE = 9_000_000_000_000
AGREBAL_BASE = 900_000_000
# Référentiel porté par les versions et les clés de test. `trppu_referentiel` n'est pas
# alimentée : le référentiel est lu dans la version de clés (DSR-701 règle 10).
REFERENTIEL_TEST = 900_000

JOURS = ("LUNDI", "MARDI", "MERCREDI", "JEUDI", "VENDREDI", "SAMEDI")
# Coefficient de base par densité (0 = dense, 1 = faible1, 2 = faible2), modulé par jour.
COEF_PAR_DENSITE = {0: Decimal("0.2000"), 1: Decimal("0.1500"), 2: Decimal("0.1000")}
FAMILLES = ("colis", "oo", "3s", "potentielip")
PRECISION_CLE = Decimal("0.000000000000000001")  # decimal(24,18)

DERNIER_NUMERO_SQL = """
SELECT MAX(co) AS dernier FROM (
    SELECT co_regate AS co FROM trppu_site WHERE co_regate LIKE 'ZT%%'
    UNION ALL
    SELECT co_regate FROM trppu_scenario WHERE co_regate LIKE 'ZT%%'
    UNION ALL
    SELECT co_regate FROM trppu_version_cle WHERE co_regate LIKE 'ZT%%'
) x
"""
DERNIER_ID_SQL = "SELECT LAST_INSERT_ID() AS id"

INSERT_PRODUIT_SQL = """
INSERT INTO trppu_produit (co_produit, lb_produit) VALUES (%s, %s)
ON DUPLICATE KEY UPDATE co_produit = co_produit
"""
INSERT_SITE_SQL = """
INSERT INTO trppu_site (co_regate, lb_regate, type_site, co_roc) VALUES (%s, %s, 'PPDC', %s)
"""
INSERT_PIC_VERSION_SQL = """
INSERT INTO trppu_pic_version
    (lb_pic_version, niveau, co_regate, id_scenario, dt_activation, commentaire,
     est_par_defaut, id_rh_creation)
VALUES (%s, 'SITE', %s, 0, NOW(), %s, 0, 'GENERATEUR')
"""
INSERT_COEFFICIENT_SQL = """
INSERT INTO trppu_pic_coefficients
    (id_pic_version, co_produit, jour_semaine, dt_effet, coef, densite, id_rh)
VALUES (%s, %s, %s, NOW(), %s, %s, 'GENERATEUR')
"""
INSERT_VERSION_CLE_SQL = """
INSERT INTO trppu_version_cle (id_referentiel, libelle, co_regate, actif, commentaire)
VALUES (%s, %s, %s, 'O', %s)
"""
INSERT_CLE_SQL = """
INSERT INTO trppu_cles_repartition_calcule
    (id_version_cle, id_referentiel, id_pdi, co_regate_site,
     cle_colis, cle_oo, cle_3s, cle_potentielip)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""
INSERT_AGREBAL_SQL = """
INSERT INTO trppu_agrebal_pdi
    (agrebal_id, agrebal_uuid, agrebal_nom, agrebal_code_roc, agrebal_code_regate,
     agrebal_pdiQuantity, agrebal_pdiList, agrebal_event, agrebal_createdAt, agrebal_updatedAt)
VALUES (%s, %s, %s, %s, %s, %s, %s, 'TEST', NOW(), NOW())
"""
INSERT_SCENARIO_SQL = """
INSERT INTO trppu_scenario
    (co_roc, co_regate, lb_scenario, statut, dt_creation, dt_validation,
     periode_debut, periode_fin, nb_jours_semaine, id_pic_version, version_scenario,
     est_fige, trafic_pdi_calcule, trafic_agrebal_calcule, Calcul_trafic_en_cours,
     id_referentiel, id_version_cle, id_rh_creation)
VALUES (%s, %s, %s, 'VALIDE', NOW(), NOW(), %s, %s, %s, %s, 1, 1, 0, 0, 0, 0, 0, 'GENERATEUR')
"""
INSERT_TMH_SQL = """
INSERT INTO trppu_tmh
    (id_scenario, co_produit, volume_realise, volume_previsionnel, moyenne_journaliere,
     moyenne_hebdo, bl_exclu, bl_manuel, id_rh)
VALUES (%s, %s, %s, %s, %s, %s, 0, 0, 'GENERATEUR')
"""

# Suppression : sites de test d'abord identifiés, puis tout ce qui s'y rattache, enfants avant
# parents (clés étrangères sans ON DELETE CASCADE sur plusieurs tables filles du scénario).
SITES_DE_TEST_SQL = """
SELECT co_regate FROM trppu_site WHERE co_regate LIKE 'ZT%%' AND lb_regate LIKE %s
"""
SCENARIOS_DES_SITES_SQL = "SELECT id_scenario FROM trppu_scenario WHERE co_regate IN ({sites})"
TABLES_FILLES_DU_SCENARIO = (
    "trppu_trafic_agrebal",
    "trppu_trafic_pdi",
    "trppu_recalcul_log",
    "trppu_tmh",
    "trppu_scenario_exclusions",
    "trppu_scenario_comptages_manuels",
    "trppu_scenario_pic_coeffs",
    "trppu_scenario_variations_prev",
    "trppu_neutralisations",
    "trppu_api_log",
)
SUPPRESSIONS_PAR_SITE = (
    ("trppu_cles_repartition_calcule", "co_regate_site"),
    ("trppu_version_cle", "co_regate"),
    ("trppu_agrebal_pdi", "agrebal_code_regate"),
)


# ---------------------------------------------------------------------------
# Génération
# ---------------------------------------------------------------------------


async def generer_scenarios(
    nombre: int,
    *,
    nb_pdi: int = 20,
    nb_agrebals: int = 3,
    nb_jours: int = 5,
    db_lecture=db_read,
    db_ecriture=db_write,
) -> Rapport:
    """Crée `nombre` scénarios complets, prêts pour `all`. Ne lève pas : rend un rapport."""
    debut = time.perf_counter()
    rapport = Rapport(titre=TITRE, id_scenario=0)
    logger.info(
        "Début génération scénarios de test %s",
        ctx(nombre=nombre, nb_pdi=nb_pdi, nb_agrebals=nb_agrebals, nb_jours=nb_jours),
    )

    motif = _parametres_invalides(nombre, nb_pdi, nb_agrebals, nb_jours)
    if motif:
        return _refus(rapport, motif)

    try:
        ligne = await db_lecture.fetch_one(DERNIER_NUMERO_SQL)
        premier = _numero(ligne.get("dernier") if ligne else None) + 1
        if premier + nombre - 1 > NUMERO_MAX:
            return _refus(
                rapport,
                f"Plus assez de codes de site de test libres ({PREFIXE_SITE}{premier:04d} à "
                f"{PREFIXE_SITE}{NUMERO_MAX}) : lancer d'abord supprimer-scenarios-test.",
            )

        crees: list[tuple[int, str]] = []
        async with db_ecriture.transaction() as tx:
            for code in CLES_PAR_PRODUIT:
                await tx.execute(INSERT_PRODUIT_SQL, (code, f"Produit {code}"))
            for numero in range(premier, premier + nombre):
                id_scenario = await _creer_scenario(tx, numero, nb_pdi, nb_agrebals, nb_jours)
                crees.append((id_scenario, _site(numero)))
                logger.info(
                    "Fin génération scénario de test %s",
                    ctx(
                        id_scenario=id_scenario,
                        co_regate=_site(numero),
                        co_roc=f"{PREFIXE_ROC}{numero:04d}",
                    ),
                )
    except Exception as erreur:  # noqa: BLE001 - la CLI ne doit jamais rendre de stacktrace
        logger.exception("Erreur génération scénarios de test %s", ctx(nombre=nombre))
        rapport.erreur = f"{type(erreur).__name__} : {erreur}"
        rapport.ko("Génération annulée : rien n'a été écrit (transaction annulée)")
        rapport.statut = ECHEC
        return rapport

    for id_scenario, site in crees[:50]:
        rapport.ok(
            f"Scénario {id_scenario} — site {site}, {nb_pdi} PDI, {nb_agrebals} Agrébal(s), "
            f"{len(CLES_PAR_PRODUIT)} produit(s), {nb_jours} jours"
        )
    if len(crees) > 50:
        rapport.ok(f"… et {len(crees) - 50} autre(s) scénario(s)")
    ids = [id_scenario for id_scenario, _ in crees]
    rapport.etats["SCENARIOS_GENERES"] = len(ids)
    rapport.etats["PREMIER_SCENARIO"] = ids[0]
    rapport.etats["DERNIER_SCENARIO"] = ids[-1]
    rapport.etats["A_LANCER"] = "python -m app.main all"
    rapport.etats["NETTOYAGE"] = "python -m app.main supprimer-scenarios-test"
    rapport.statut = SUCCES
    logger.info(
        "Fin génération scénarios de test %s",
        ctx(
            nombre=len(ids),
            premier=ids[0],
            dernier=ids[-1],
            duration_ms=round((time.perf_counter() - debut) * 1000, 1),
        ),
    )
    return rapport


def _parametres_invalides(nombre: int, nb_pdi: int, nb_agrebals: int, nb_jours: int) -> str:
    if APP_ENV.strip().lower() == "prod":
        return "Génération refusée en production (APP_ENV=prod) : données de test interdites."
    if not 1 <= nombre <= NOMBRE_MAX:
        return f"Nombre de scénarios hors bornes : {nombre} (1 à {NOMBRE_MAX})."
    if not 1 <= nb_pdi <= PDI_PAR_SITE_MAX:
        return f"Nombre de PDI hors bornes : {nb_pdi} (1 à {PDI_PAR_SITE_MAX})."
    if not 1 <= nb_agrebals <= min(AGREBALS_PAR_SITE_MAX, nb_pdi):
        return (
            f"Nombre d'Agrébals hors bornes : {nb_agrebals} (1 à "
            f"{min(AGREBALS_PAR_SITE_MAX, nb_pdi)}, au plus un par PDI)."
        )
    if nb_jours not in (5, 6):
        return f"Semaine de {nb_jours} jours : 5 ou 6 attendu (contrainte du scénario)."
    return ""


def _refus(rapport: Rapport, motif: str) -> Rapport:
    logger.warning("Rejet génération scénarios de test %s", ctx(motif=motif))
    rapport.ko(motif)
    rapport.statut = ECHEC
    return rapport


def _site(numero: int) -> str:
    return f"{PREFIXE_SITE}{numero:04d}"


def _numero(code: Any) -> int:
    """Numéro d'un code de site de test, 0 s'il n'y en a pas encore."""
    try:
        return int(str(code)[len(PREFIXE_SITE) :]) if code else 0
    except ValueError:
        return 0


async def _dernier_id(tx) -> int:
    ligne = await tx.fetch_one(DERNIER_ID_SQL)
    return int(ligne["id"])


async def _creer_scenario(tx, numero: int, nb_pdi: int, nb_agrebals: int, nb_jours: int) -> int:
    """Un scénario et tout son contexte. Rend l'identifiant du scénario."""
    site, roc = _site(numero), f"{PREFIXE_ROC}{numero:04d}"
    commentaire = f"{MARQUEUR} - généré"
    await tx.execute(INSERT_SITE_SQL, (site, f"{LIBELLE_SITE} {numero:04d}", roc))

    # Version PIC et ses coefficients : tous les produits, tous les jours, trois densités.
    await tx.execute(INSERT_PIC_VERSION_SQL, (MARQUEUR, site, commentaire))
    id_pic_version = await _dernier_id(tx)
    await tx.execute_many(
        INSERT_COEFFICIENT_SQL,
        [
            (id_pic_version, code, jour, _coefficient(densite, rang_jour), densite)
            for code in CLES_PAR_PRODUIT
            for rang_jour, jour in enumerate(JOURS)
            for densite in COEF_PAR_DENSITE
        ],
    )

    # Version de clés active, qui porte le référentiel (règles 9 et 10).
    id_referentiel = REFERENTIEL_TEST
    await tx.execute(INSERT_VERSION_CLE_SQL, (id_referentiel, MARQUEUR, site, commentaire))
    id_version_cle = await _dernier_id(tx)

    pdis = [PDI_BASE + numero * 1000 + rang for rang in range(1, nb_pdi + 1)]
    cles = {famille: _cles(nb_pdi, decalage) for decalage, famille in enumerate(FAMILLES)}
    await tx.execute_many(
        INSERT_CLE_SQL,
        [
            (id_version_cle, id_referentiel, pdi, site, *(cles[f][rang] for f in FAMILLES))
            for rang, pdi in enumerate(pdis)
        ],
    )

    # Agrébals : les PDI répartis à tour de rôle, chacun dans exactement un Agrébal.
    for rang in range(nb_agrebals):
        portes = pdis[rang::nb_agrebals]
        await tx.execute(
            INSERT_AGREBAL_SQL,
            (
                AGREBAL_BASE + numero * 100 + rang,
                str(uuid.uuid4()),
                f"{MARQUEUR} {numero:04d}-{rang + 1}",
                roc,
                site,
                len(portes),
                json.dumps([{"pdi_id": pdi} for pdi in portes]),
            ),
        )

    annee = date.today().year
    await tx.execute(
        INSERT_SCENARIO_SQL,
        (
            roc,
            site,
            f"{MARQUEUR} {numero:04d}",
            date(annee, 1, 1),
            date(annee, 12, 31),
            nb_jours,
            id_pic_version,
        ),
    )
    id_scenario = await _dernier_id(tx)
    await tx.execute_many(
        INSERT_TMH_SQL,
        [
            (id_scenario, code, *_tmh(rang, numero))
            for rang, code in enumerate(CLES_PAR_PRODUIT)
        ],
    )
    return id_scenario


def _coefficient(densite: int, rang_jour: int) -> Decimal:
    """Coefficient de rétention : base de la densité, +5 % par jour (decimal(7,4))."""
    return (COEF_PAR_DENSITE[densite] * (1 + Decimal("0.05") * rang_jour)).quantize(
        Decimal("0.0001")
    )


def _cles(nb_pdi: int, decalage: int) -> list[Decimal]:
    """Clés d'une famille : poids 1…N décalés selon la famille, normalisés à une somme de 1.

    Le dernier PDI absorbe l'arrondi : la somme vaut exactement 1, comme l'exige le CA3 de
    DSR-699.
    """
    poids = [((rang + decalage) % nb_pdi) + 1 for rang in range(nb_pdi)]
    total = Decimal(sum(poids))
    cles = [(Decimal(p) / total).quantize(PRECISION_CLE) for p in poids]
    cles[-1] = Decimal(1) - sum(cles[:-1])
    return cles


def _tmh(rang_produit: int, numero: int) -> tuple[int, int, Decimal, Decimal]:
    """(volume réalisé, volume prévisionnel, moyenne journalière, moyenne hebdo).

    Moyenne hebdo de 1 000 à quelques milliers : trafic PDI = TMH × coef × clé reste très
    en deçà de la capacité de la colonne (65 535).
    """
    hebdo = Decimal(1000 * (rang_produit + 1) + (numero % 10) * 100)
    return int(hebdo * 52), int(hebdo * 52), (hebdo / 6).quantize(Decimal("0.01")), hebdo


# ---------------------------------------------------------------------------
# Suppression
# ---------------------------------------------------------------------------


async def supprimer_scenarios_test(*, db_lecture=db_read, db_ecriture=db_write) -> Rapport:
    """Efface tout ce que `generer_scenarios` a créé — et seulement cela."""
    debut = time.perf_counter()
    rapport = Rapport(titre=TITRE_SUPPRESSION, id_scenario=0)
    logger.info("Début suppression scénarios de test %s", ctx(marqueur=MARQUEUR))
    try:
        sites = [
            ligne["co_regate"]
            for ligne in await db_lecture.fetch_all(SITES_DE_TEST_SQL, (f"{LIBELLE_SITE}%",))
        ]
        if not sites:
            rapport.ok("Aucun scénario de test en base : rien à supprimer")
            rapport.statut = SUCCES
            return rapport

        marqueurs_sites = ", ".join(["%s"] * len(sites))
        scenarios = [
            int(ligne["id_scenario"])
            for ligne in await db_lecture.fetch_all(
                SCENARIOS_DES_SITES_SQL.format(sites=marqueurs_sites), tuple(sites)
            )
        ]
        bilan: dict[str, int] = {}
        async with db_ecriture.transaction() as tx:
            if scenarios:
                marqueurs = ", ".join(["%s"] * len(scenarios))
                for table in TABLES_FILLES_DU_SCENARIO:
                    bilan[table] = await tx.execute(
                        f"DELETE FROM {table} WHERE id_scenario IN ({marqueurs})",
                        tuple(scenarios),
                    )
                bilan["trppu_scenario"] = await tx.execute(
                    f"DELETE FROM trppu_scenario WHERE id_scenario IN ({marqueurs})",
                    tuple(scenarios),
                )
            for table, colonne in SUPPRESSIONS_PAR_SITE:
                bilan[table] = await tx.execute(
                    f"DELETE FROM {table} WHERE {colonne} IN ({marqueurs_sites})", tuple(sites)
                )
            # Les coefficients suivent leur version (ON DELETE CASCADE).
            bilan["trppu_pic_version"] = await tx.execute(
                f"DELETE FROM trppu_pic_version WHERE lb_pic_version = %s "
                f"AND co_regate IN ({marqueurs_sites})",
                (MARQUEUR, *sites),
            )
            bilan["trppu_site"] = await tx.execute(
                f"DELETE FROM trppu_site WHERE co_regate IN ({marqueurs_sites})", tuple(sites)
            )
    except Exception as erreur:  # noqa: BLE001 - la CLI ne doit jamais rendre de stacktrace
        logger.exception("Erreur suppression scénarios de test %s", ctx(marqueur=MARQUEUR))
        rapport.erreur = f"{type(erreur).__name__} : {erreur}"
        rapport.ko("Suppression annulée : rien n'a été effacé (transaction annulée)")
        rapport.statut = ECHEC
        return rapport

    rapport.ok(f"{len(sites)} site(s) de test, {len(scenarios)} scénario(s)")
    for table, lignes in bilan.items():
        if lignes:
            rapport.ok(f"{table} : {lignes} ligne(s) supprimée(s)")
    rapport.etats["SITES_SUPPRIMES"] = len(sites)
    rapport.etats["SCENARIOS_SUPPRIMES"] = len(scenarios)
    rapport.statut = SUCCES
    logger.info(
        "Fin suppression scénarios de test %s",
        ctx(
            sites=len(sites),
            scenarios=len(scenarios),
            duration_ms=round((time.perf_counter() - debut) * 1000, 1),
        ),
    )
    return rapport


__all__ = ["generer_scenarios", "supprimer_scenarios_test"]
