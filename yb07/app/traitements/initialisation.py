"""Chaîne d'initialisation des clés de répartition des PDI (DSR-696 à DSR-699), en six étapes.

L'ordre est impératif : DSR-699 écarte sans erreur les sites sans agrégat ou sans version, et
le CA4 interdit ensuite de recalculer. D'où des prérequis vérifiés avant toute écriture, un
arrêt au premier contrôle en échec et un rapport qui nomme l'étape de reprise. Ne lève pas :
rend un `Rapport` dont la CLI déduit le code de retour.
"""

from __future__ import annotations

import logging
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.config import (
    CHARGEMENT_LOCK_WAIT_TIMEOUT,
    CHARGEMENT_TAILLE_LOT,
    INIT_LOG_TOUS_LES_SITES,
    INIT_MAX_ANOMALIES_LOGUEES,
    INIT_VERSIONS_TAILLE_LOT,
)
from app.db.mysql import db_write
from app.db.sql_parametres import injecter_parametres, instructions_parametrees
from app.db.sql_script import SqlScriptError, statement_preview
from app.erreurs import TraitementImpossible
from app.log_utils import ctx
from app.traitements import controles_init, index_chargement
from app.traitements.cles_repartition import charger_cles_repartition
from app.traitements.rapport import ECHEC, SUCCES, Rapport

logger = logging.getLogger(__name__)

TITRE = "INITIALISATION DES CLES DE REPARTITION"

CHARGEMENT = "chargement"
MIGRATION = "migration"
CORRECTIF = "correctif"
AGREGATS = "agregats"
VERSIONS = "versions"
CLES = "cles"

#: Ordre d'exécution ; sert aussi de `choices=` à argparse et d'index à `--depuis`.
ETAPES = (CHARGEMENT, MIGRATION, CORRECTIF, AGREGATS, VERSIONS, CLES)

#: Fichier et mode transactionnel de chaque étape scriptée. Migration et correctif hors
#: transaction : leurs `ALTER` passent par `PREPARE`/`EXECUTE`, invisibles à `is_ddl`, mais
#: le COMMIT implicite a bien lieu.
SCRIPTS: dict[str, tuple[str, bool]] = {
    MIGRATION: ("DSR-696-699_migration.sql", False),
    CORRECTIF: ("fix_error.sql", False),
    AGREGATS: ("DSR-696_site_trafic.sql", True),
    VERSIONS: ("DSR-698_version_cle.sql", True),
    # Par lots en autocommit (cf. `_etape_cles`) : limite de la réplication de groupe.
    CLES: ("DSR-699_cles_calculees.sql", False),
}

#: Étapes dont le script modifie le schéma (ALTER) : leur attente de verrou est bornée.
SCRIPTS_DDL = frozenset({MIGRATION, CORRECTIF})

#: Prérequis selon l'étape de départ. Les garde-fous du calcul des clés (dénominateurs nuls,
#: périmètre déjà calculé) sont joués par l'étape elle-même : le chargement ne purge pas
#: `trppu_cles_repartition_calcule`.
PREREQUIS: dict[str, tuple[str, ...]] = {
    CHARGEMENT: (),
    MIGRATION: ("lignes",),
    CORRECTIF: ("lignes",),
    AGREGATS: ("lignes", "schema"),
    VERSIONS: ("lignes", "schema", "agregats"),
    CLES: ("lignes", "schema", "agregats", "versions"),
}

#: Résolu depuis le module : un batch lancé par un ordonnanceur ne choisit pas son `cwd`.
REPERTOIRE_SQL = Path(__file__).resolve().parents[2] / "db"

ENCODAGE_SQL = "utf-8-sig"

SITES_DU_REFERENTIEL_SQL = """
SELECT co_regate_site FROM trppu_trafic_site WHERE id_referentiel = %s ORDER BY co_regate_site
"""

#: Code régate fictif employé par la marche à blanc — six caractères, comme la colonne.
CO_REGATE_A_BLANC = "000000"

APERCU_INSERT_AGREGATS = "INSERT INTO trppu_trafic_site"
APERCU_INSERT_VERSION = "INSERT INTO trppu_version_cle"
APERCU_INSERT_CLES = "INSERT INTO trppu_cles_repartition_calcule"

#: Index unique de `trppu_cles_repartition_calcule`, recréé avant le calcul s'il manque.
TABLE_CLES = "trppu_cles_repartition_calcule"
INDEX_UNIQUE_CLES = "uq_crc_version_pdi"
DEFINITION_INDEX_UNIQUE_CLES = "UNIQUE KEY `uq_crc_version_pdi` (`id_version_cle`, `id_pdi`)"
# Sur l'instance d'écriture : il décide d'un DDL.
INDEX_UNIQUE_CLES_PRESENT_SQL = """
SELECT COUNT(*) AS nb
  FROM information_schema.STATISTICS
 WHERE TABLE_SCHEMA = DATABASE()
   AND TABLE_NAME = 'trppu_cles_repartition_calcule'
   AND INDEX_NAME = 'uq_crc_version_pdi'
"""


# Étendue des `id` découpée en lots par l'étape « cles » (lecture instantanée par la PK).
BORNES_CLES_REPARTITION_SQL = (
    "SELECT MIN(id) AS id_min, MAX(id) AS id_max FROM trppu_cles_repartition"
)

#: Lots de l'étape « cles » par connexion ; chaque lot reste commité seul (autocommit).
LOTS_CLES_PAR_CONNEXION = 100


@dataclass
class _Etat:
    """Ce qu'une étape transmet aux suivantes, et les dépendances injectables."""

    id_referentiel: int
    rapport: Rapport
    db_lecture: Any
    db_ecriture: Any
    scripts: dict[str, str]
    fichier: str | None = None
    commentaire: str = ""
    libelle: str | None = None
    dry_run: bool = False
    controles_longs: bool = True
    ignorer_erreurs: bool = False

    #: PDI actifs rendus par le « chargement » (CA1 de DSR-699) ; `None` en reprise.
    lignes_actives: int | None = None
    #: Agrégats par référentiel avant l'étape « agregats » (preuve du CA5).
    photo_referentiels: dict[Any, int] = field(default_factory=dict)
    #: Sites du référentiel, lus une fois par l'étape « versions ».
    nb_sites: int = 0


# ---------------------------------------------------------------------------
# Traitement
# ---------------------------------------------------------------------------


async def initialiser_cles_repartition(
    id_referentiel: int,
    *,
    fichier: str | None = None,
    depuis: str | None = None,
    etape: str | None = None,
    commentaire: str | None = None,
    libelle: str | None = None,
    dry_run: bool = False,
    controles_longs: bool = True,
    ignorer_erreurs: bool = False,
    db_lecture=db_write,
    db_ecriture=db_write,
) -> Rapport:
    """Joue la chaîne entière, à partir de `depuis`, ou la seule `etape` (exclusifs).

    `dry_run` découpe les scripts sans écrire (chargement sauté). Lectures par défaut sur
    l'instance d'écriture : le réplica peut être en retard (le CA3 de DSR-698 y avait compté
    2000 versions sur 2022).
    """
    debut = time.perf_counter()
    rapport = Rapport(
        titre=TITRE,
        id_traitement=id_referentiel,
        libelle_identifiant="Référentiel",
    )

    logger.info(
        "Début initialisation clés de répartition %s",
        ctx(
            id_referentiel=id_referentiel,
            depuis=depuis,
            etape=etape,
            dry_run=dry_run,
            skip_errors=ignorer_erreurs or None,
        ),
    )

    jouees: list[str] = []
    try:
        a_jouer = _resoudre_etapes(depuis, etape)

        # Scripts lus avant toute écriture : un fichier illisible échoue base intacte.
        etat = _Etat(
            id_referentiel=id_referentiel,
            rapport=rapport,
            db_lecture=db_lecture,
            db_ecriture=db_ecriture,
            scripts=_lire_scripts(a_jouer),
            fichier=fichier,
            commentaire=commentaire or f"Initialisation référentiel {id_referentiel}",
            libelle=libelle,
            dry_run=dry_run,
            controles_longs=controles_longs,
            ignorer_erreurs=ignorer_erreurs,
        )

        if not await controles_init.verifier_prerequis(
            rapport, db_lecture, id_referentiel, PREREQUIS[a_jouer[0]]
        ):
            rapport.statut = ECHEC
            return rapport

        for rang, nom in enumerate(a_jouer, start=1):
            debut_etape = time.perf_counter()
            logger.info(
                "Début étape initialisation %s",
                ctx(
                    id_referentiel=id_referentiel,
                    etape=nom,
                    rang=rang,
                    total=len(a_jouer),
                ),
            )

            reussie = await _IMPLEMENTATIONS[nom](etat, rang, len(a_jouer))

            duree_ms = round((time.perf_counter() - debut_etape) * 1000, 1)
            jouees.append(nom)
            rapport.etats[f"DUREE_MS_{nom.upper()}"] = duree_ms
            logger.info(
                "Fin étape initialisation %s",
                ctx(
                    id_referentiel=id_referentiel,
                    etape=nom,
                    verdict=SUCCES if reussie else ECHEC,
                    duration_ms=duree_ms,
                ),
            )

            if not reussie:
                # La chaîne est séquentielle : ce qui suit consommerait ce qui vient d'échouer.
                rapport.etats["REPRENDRE_A"] = nom
                break

    except TraitementImpossible as erreur:
        logger.warning(
            "Rejet initialisation clés de répartition %s",
            ctx(id_referentiel=id_referentiel, motif=str(erreur)),
        )
        rapport.ko(str(erreur))
    except SqlScriptError as erreur:
        logger.exception(
            "Erreur initialisation clés de répartition %s",
            ctx(id_referentiel=id_referentiel, source=erreur.source, instruction=erreur.index),
        )
        rapport.erreur = _message_script(erreur)
    except Exception as erreur:  # noqa: BLE001 - la CLI ne doit jamais rendre de stacktrace
        logger.exception(
            "Erreur initialisation clés de répartition %s",
            ctx(id_referentiel=id_referentiel),
        )
        rapport.erreur = f"{type(erreur).__name__} : {erreur}"

    rapport.etats["ETAPES_JOUEES"] = ",".join(jouees)
    rapport.statut = SUCCES if rapport.reussi else ECHEC

    duration_ms = round((time.perf_counter() - debut) * 1000, 1)
    logger.info(
        "Fin initialisation clés de répartition %s",
        ctx(
            id_referentiel=id_referentiel,
            etapes=len(jouees),
            verdict=rapport.statut,
            duration_ms=duration_ms,
        ),
    )
    return rapport


# ---------------------------------------------------------------------------
# Sélection des étapes et lecture des scripts
# ---------------------------------------------------------------------------


def _resoudre_etapes(depuis: str | None, etape: str | None) -> tuple[str, ...]:
    """Étapes à jouer, dans l'ordre (revalidées : appelable hors CLI)."""
    if depuis and etape:
        raise TraitementImpossible("« depuis » et « etape » s'excluent : n'en passer qu'un.")
    for valeur in (depuis, etape):
        if valeur is not None and valeur not in ETAPES:
            raise TraitementImpossible(
                f"Étape inconnue : {valeur}. Attendu : {', '.join(ETAPES)}."
            )
    if etape:
        return (etape,)
    if depuis:
        return ETAPES[ETAPES.index(depuis) :]
    return ETAPES


def _lire_scripts(a_jouer: tuple[str, ...]) -> dict[str, str]:
    """Lit les scripts des étapes retenues. Un fichier manquant échoue ici, avant la base."""
    scripts: dict[str, str] = {}
    for nom in a_jouer:
        if nom not in SCRIPTS:
            continue
        chemin = REPERTOIRE_SQL / SCRIPTS[nom][0]
        try:
            scripts[nom] = chemin.read_text(encoding=ENCODAGE_SQL)
        except (OSError, UnicodeDecodeError) as erreur:
            raise TraitementImpossible(
                f"Script illisible pour l'étape « {nom} » : {chemin} ({erreur})"
            ) from erreur
    return scripts


def _message_script(erreur: SqlScriptError) -> str:
    """Message d'échec d'un script, avec un aperçu tronqué et jamais le SQL complet."""
    apercu = statement_preview(erreur.statement) if erreur.statement else ""
    args = getattr(erreur.original, "args", ())
    conseil = (
        f" — verrou non obtenu en {CHARGEMENT_LOCK_WAIT_TIMEOUT} s : une autre session "
        "(client SQL resté en transaction, API) utilise la table. La libérer "
        "(SHOW FULL PROCESSLIST), puis relancer l'étape."
        if args and args[0] == 1205
        else ""
    )
    return (
        f"Échec de {erreur.source}, instruction {erreur.index}"
        + (f" [{apercu}]" if apercu else "")
        + f" : {erreur.original}{conseil}"
    )


# ---------------------------------------------------------------------------
# Étapes
# ---------------------------------------------------------------------------


async def _executer_script(
    etat: _Etat, nom: str, parametres: dict[str, Any], *, suffixe_label: str = ""
):
    """Injecte les paramètres du script de l'étape et le joue sur la base d'écriture.

    `suffixe_label` distingue dans les logs les exécutions répétées (un site, par exemple).
    """
    fichier, transactional = SCRIPTS[nom]
    texte = injecter_parametres(etat.scripts[nom], parametres)
    if nom in SCRIPTS_DDL:
        # Attente de verrou bornée : sinon l'ALTER attend jusqu'à un an et bloque la table.
        texte = f"SET SESSION lock_wait_timeout = {CHARGEMENT_LOCK_WAIT_TIMEOUT};\n{texte}"
    return await etat.db_ecriture.execute_sql_script(
        texte,
        label=f"db/{fichier}{suffixe_label}",
        transactional=transactional,
        dry_run=etat.dry_run,
        # SELECT de constat pour l'exécution manuelle, coûteux (24 M de lignes) : les
        # contrôles utiles sont rejoués par `controles_init`.
        skip_selects=True,
    )


def _ligne_etape(rang: int, total: int, nom: str, suite: str) -> str:
    return f"Étape {rang}/{total} {nom} — {suite}"


async def _etape_chargement(etat: _Etat, rang: int, total: int) -> bool:
    """Délègue au traitement existant, qui porte le streaming S3 et les lots commités."""
    if etat.dry_run:
        etat.rapport.ok(
            _ligne_etape(rang, total, CHARGEMENT, "non jouée (--dry-run, pas de mode à blanc)")
        )
        return True

    sous_rapport = await charger_cles_repartition(
        etat.id_referentiel,
        etat.fichier,
        ignorer_erreurs=etat.ignorer_erreurs,
    )

    etat.rapport.ajouter(
        sous_rapport.reussi,
        _ligne_etape(
            rang,
            total,
            CHARGEMENT,
            f"{sous_rapport.etats.get('LIGNES_CHARGEES', 0)} ligne(s)",
        ),
        f"Chargement en échec : {'; '.join(sous_rapport.motifs) or sous_rapport.erreur}",
    )
    etat.rapport.controles.extend(sous_rapport.controles)
    etat.rapport.etats.update(sous_rapport.etats)
    etat.rapport.avertissements.extend(sous_rapport.avertissements)
    if sous_rapport.erreur and not etat.rapport.erreur:
        etat.rapport.erreur = sous_rapport.erreur

    lignes_actives = sous_rapport.etats.get("LIGNES_ACTIVES")
    if isinstance(lignes_actives, int):
        etat.lignes_actives = lignes_actives

    return sous_rapport.reussi


async def _etape_migration(etat: _Etat, rang: int, total: int) -> bool:
    resultat = await _executer_script(etat, MIGRATION, {})
    etat.rapport.ok(
        _ligne_etape(
            rang, total, MIGRATION, f"{resultat.total_count} instruction(s), hors transaction"
        )
    )
    if etat.dry_run:
        return True
    await controles_init.controler_migration(etat.rapport, etat.db_lecture)
    return etat.rapport.reussi


async def _etape_correctif(etat: _Etat, rang: int, total: int) -> bool:
    resultat = await _executer_script(
        etat, CORRECTIF, {"id_referentiel": etat.id_referentiel}
    )
    etat.rapport.ok(
        _ligne_etape(
            rang, total, CORRECTIF, f"{resultat.total_count} instruction(s), hors transaction"
        )
    )
    if etat.dry_run:
        return True
    await controles_init.controler_correctif(etat.rapport, etat.db_lecture)
    return etat.rapport.reussi


async def _etape_agregats(etat: _Etat, rang: int, total: int) -> bool:
    # Photo avant écriture, comparée à celle d'après pour prouver le CA5.
    if not etat.dry_run:
        etat.photo_referentiels = await controles_init.photo_referentiels(etat.db_lecture)

    resultat = await _executer_script(
        etat, AGREGATS, {"id_referentiel": etat.id_referentiel, "co_regate": None}
    )

    if etat.dry_run:
        etat.rapport.ok(
            _ligne_etape(rang, total, AGREGATS, f"{resultat.total_count} instruction(s)")
        )
        return True

    ecrits = controles_init.rowcount(resultat, APERCU_INSERT_AGREGATS)
    etat.rapport.ok(_ligne_etape(rang, total, AGREGATS, f"{ecrits} site(s) agrégé(s)"))
    await controles_init.controler_agregats(
        etat.rapport,
        etat.db_lecture,
        etat.id_referentiel,
        etat.photo_referentiels,
        ecrits,
    )
    return etat.rapport.reussi


async def _etape_versions(etat: _Etat, rang: int, total: int) -> bool:
    """DSR-698 site par site, pour les sites de `trppu_trafic_site` (ayant un dénominateur)."""
    if etat.dry_run:
        # Marche à blanc sans lire la base : un site fictif suffit à valider le découpage.
        resultat = await _executer_script(
            etat, VERSIONS, _parametres_version(etat, CO_REGATE_A_BLANC)
        )
        etat.rapport.ok(
            _ligne_etape(
                rang,
                total,
                VERSIONS,
                f"{resultat.total_count} instruction(s) par site — non jouées (--dry-run)",
            )
        )
        return True

    sites = [
        ligne["co_regate_site"]
        for ligne in await etat.db_lecture.fetch_all(
            SITES_DU_REFERENTIEL_SQL, (etat.id_referentiel,)
        )
    ]
    etat.nb_sites = len(sites)

    if not sites:
        etat.rapport.ko(
            f"Aucun site agrégé pour le référentiel {etat.id_referentiel} : "
            f"jouer l'étape « agregats »."
        )
        return False

    debut = time.perf_counter()
    bilan = _BilanVersions()
    traites = 0
    # Une connexion et une transaction par lot de sites : la connexion et le commit coûtent.
    for debut_lot in range(0, len(sites), INIT_VERSIONS_TAILLE_LOT):
        lot = [str(site) for site in sites[debut_lot : debut_lot + INIT_VERSIONS_TAILLE_LOT]]
        await _versionner_lot(etat, lot, bilan)
        precedent, traites = traites, traites + len(lot)
        if traites // INIT_LOG_TOUS_LES_SITES > precedent // INIT_LOG_TOUS_LES_SITES:
            _journaliser_avancement(
                etat, traites, len(sites), bilan.creees, bilan.echecs, debut
            )
    creees, deja_a_jour = bilan.creees, bilan.deja_a_jour
    echecs, motifs, causes = bilan.echecs, bilan.motifs, bilan.causes

    etat.rapport.ajouter(
        not echecs,
        _ligne_etape(
            rang,
            total,
            VERSIONS,
            f"{len(sites)} site(s) : {creees} créée(s), {deja_a_jour} déjà à jour",
        ),
        f"Versions : {len(echecs)} site(s) en échec sur {len(sites)} "
        f"(premiers : {', '.join(echecs[:5])}). Le calcul des clés n'est pas joué — le CA4 de "
        f"DSR-699 figerait définitivement les sites restants.",
    )
    etat.rapport.etats["SITES_TRAITES"] = len(sites)
    etat.rapport.etats["VERSIONS_CREEES"] = creees
    etat.rapport.etats["SITES_VERSIONS_KO"] = len(echecs)
    if echecs:
        _rapporter_echecs_versions(etat, echecs, motifs, causes, len(sites))

    await controles_init.controler_versions(
        etat.rapport, etat.db_lecture, etat.id_referentiel, len(sites)
    )
    return etat.rapport.reussi


@dataclass
class _BilanVersions:
    """Compteurs de l'étape « versions », alimentés lot par lot."""

    creees: int = 0
    deja_a_jour: int = 0
    echecs: list[str] = field(default_factory=list)
    # Motif par site et causes regroupées (une cause touche souvent tous les sites).
    motifs: dict[str, str] = field(default_factory=dict)
    causes: Counter = field(default_factory=Counter)


async def _versionner_lot(etat: _Etat, lot: list[str], bilan: _BilanVersions) -> None:
    """Crée les versions d'un lot de sites dans une transaction.

    En cas d'échec le lot est annulé puis rejoué site par site, pour n'écarter que les sites
    fautifs ; sans risque, le script saute un site déjà versionné.
    """
    fichier, transactional = SCRIPTS[VERSIONS]
    unites = [
        (
            f"db/{fichier}@{site}",
            instructions_parametrees(etat.scripts[VERSIONS], _parametres_version(etat, site)),
        )
        for site in lot
    ]
    try:
        resultat = await etat.db_ecriture.execute_sql_units(
            unites, transactional=transactional, skip_selects=True
        )
    except SqlScriptError as erreur:
        logger.warning(
            "Rejet lot de versions %s",
            ctx(
                id_referentiel=etat.id_referentiel,
                sites=len(lot),
                premier_site=lot[0],
                site_en_echec=erreur.source.rsplit("@", 1)[-1],
                suite="lot annulé, rejoué site par site",
            ),
        )
        for site in lot:
            await _versionner_site(etat, site, bilan)
        return

    for site, (label, _) in zip(lot, unites):
        if _insertions(resultat, label, APERCU_INSERT_VERSION) > 0:
            bilan.creees += 1
        else:
            bilan.deja_a_jour += 1
    logger.info(
        "Fin lot de versions %s",
        ctx(
            id_referentiel=etat.id_referentiel,
            sites=len(lot),
            premier_site=lot[0],
            dernier_site=lot[-1],
            duration_ms=round(resultat.duration_ms, 1),
        ),
    )


async def _versionner_site(etat: _Etat, site: str, bilan: _BilanVersions) -> None:
    """Un site, dans sa propre transaction — chemin de reprise d'un lot annulé."""
    try:
        resultat = await _executer_script(
            etat, VERSIONS, _parametres_version(etat, site), suffixe_label=f"@{site}"
        )
    except SqlScriptError as erreur:
        # On poursuit les autres sites ; l'étape échouera et bloquera le calcul des clés.
        bilan.echecs.append(site)
        bilan.motifs[site] = _message_script(erreur)
        bilan.causes[_cause(erreur)] += 1
        logger.warning(
            "Rejet création de version %s",
            ctx(
                id_referentiel=etat.id_referentiel,
                co_regate=site,
                motif=_message_script(erreur),
            ),
        )
        return
    if controles_init.rowcount(resultat, APERCU_INSERT_VERSION) > 0:
        bilan.creees += 1
    else:
        bilan.deja_a_jour += 1


def _insertions(resultat, label: str, debut_apercu: str) -> int:
    """Lignes écrites par l'instruction `debut_apercu` du script `label` d'un lot."""
    cible = " ".join(debut_apercu.split()).upper()
    for instruction in resultat.statements:
        if instruction.source == label and " ".join(
            instruction.preview.split()
        ).upper().startswith(cible):
            return instruction.rowcount
    return -1


def _cause(erreur: SqlScriptError) -> str:
    """Cause d'un échec sans les valeurs propres au site : clé de regroupement du rapport."""
    origine = erreur.original
    args = getattr(origine, "args", ())
    code = f"({args[0]}) " if args and isinstance(args[0], int) else ""
    texte = str(args[1]) if len(args) > 1 else str(origine)
    texte = re.sub(r"'[^']*'", "'…'", texte)
    return f"{code}{texte} [instruction {erreur.index}]"


def _rapporter_echecs_versions(
    etat: _Etat,
    echecs: list[str],
    motifs: dict[str, str],
    causes: Counter[str],
    nb_sites: int,
) -> None:
    """Rapport exploitable après un échec de l'étape « versions » : causes, sites, reprise."""
    rapport = etat.rapport
    rapport.avertissements.append(
        f"Versions — {len(echecs)} site(s) en échec sur {nb_sites}, par cause :"
    )
    rapport.avertissements += [
        f"    {nombre} site(s) : {cause}" for cause, nombre in causes.most_common()
    ]
    detailles = echecs[:INIT_MAX_ANOMALIES_LOGUEES]
    rapport.avertissements.append(
        f"Versions — détail des sites en échec ({len(detailles)} sur {len(echecs)}) :"
    )
    rapport.avertissements += [f"    site {site} : {motifs[site]}" for site in detailles]
    rapport.avertissements.append(
        "Versions — reprise : corriger la cause, puis relancer "
        f"« init {etat.id_referentiel} --depuis versions ». Les sites déjà versionnés sont "
        f"sautés : seuls les {len(echecs)} en échec seront rejoués."
    )
    suite = "…" if len(echecs) > len(detailles) else ""
    rapport.etats["SITES_VERSIONS_KO_LISTE"] = ",".join(detailles) + suite


def _parametres_version(etat: _Etat, site: Any) -> dict[str, Any]:
    return {
        "id_referentiel": etat.id_referentiel,
        "co_regate": site,
        "commentaire": etat.commentaire,
        "libelle": etat.libelle,
    }


def _journaliser_avancement(
    etat: _Etat, traites: int, total: int, creees: int, echecs: list[str], debut: float
) -> None:
    ecoule = max(time.perf_counter() - debut, 1e-9)
    logger.info(
        "Avancement création des versions %s",
        ctx(
            id_referentiel=etat.id_referentiel,
            sites=traites,
            sites_total=total,
            pct=round(100 * traites / total, 1),
            versions_creees=creees,
            sites_ko=len(echecs),
            debit_sites_s=round(traites / ecoule, 1),
            duration_ms=round(ecoule * 1000, 1),
        ),
    )


async def _etape_cles(etat: _Etat, rang: int, total: int) -> bool:
    """DSR-699 par lots d'`id`, chacun commité seul (autocommit).

    Une transaction unique dépasse `group_replication_transaction_size_limit` (erreur 3231).
    Reprenable : relancer `--etape cles` n'insère que les clés absentes (`NOT EXISTS`), sans
    modifier les existantes (CA4). `uq_crc_version_pdi` sert cette anti-jointure.
    """
    if etat.dry_run:
        resultat = await _executer_script(etat, CLES, _parametres_cles(etat, 0, None))
        etat.rapport.ok(
            _ligne_etape(rang, total, CLES, f"{resultat.total_count} instruction(s) par lot")
        )
        return True

    if not await controles_init.verifier_prerequis(
        etat.rapport, etat.db_lecture, etat.id_referentiel, ("denominateurs",)
    ):
        return False
    deja = await controles_init.verifier_reprise_cles(
        etat.rapport, etat.db_lecture, etat.id_referentiel, etat.lignes_actives
    )
    if deja is None:
        return False

    present = await etat.db_ecriture.fetch_one(INDEX_UNIQUE_CLES_PRESENT_SQL)
    if not (present and present["nb"]):
        await _recreer_index_cles(etat)
        if not etat.rapport.reussi:
            return False

    bornes = await etat.db_ecriture.fetch_one(BORNES_CLES_REPARTITION_SQL)
    id_min, id_max = (bornes or {}).get("id_min"), (bornes or {}).get("id_max")
    if id_min is None or id_max is None:
        etat.rapport.ko(
            "trppu_cles_repartition est vide : aucune clé à calculer. Jouer l'étape « chargement »."
        )
        return False

    ecrites = await _calculer_cles_par_lots(etat, int(id_min), int(id_max))
    etat.rapport.etats["CLES_CALCULEES"] = ecrites

    if ecrites == 0:
        etat.rapport.ko(
            "Aucune clé écrite alors que des PDI actifs étaient attendus : jointure sans "
            "correspondance (site sans agrégat ou sans version active). Rien n'a été modifié."
        )
        return False

    suite = f"{ecrites} clé(s) calculée(s)"
    if deja:
        suite += f" — reprise, {deja} déjà présente(s)"
    etat.rapport.ok(_ligne_etape(rang, total, CLES, suite))
    await controles_init.controler_cles(
        etat.rapport,
        etat.db_lecture,
        etat.id_referentiel,
        deja + ecrites,
        lignes_attendues=etat.lignes_actives,
        controles_longs=etat.controles_longs,
    )
    return etat.rapport.reussi


def _parametres_cles(etat: _Etat, id_debut: int, id_fin: int | None) -> dict[str, Any]:
    parametres = {
        "id_referentiel": etat.id_referentiel,
        "co_regate": None,
        "id_debut": id_debut,
    }
    if id_fin is not None:
        parametres["id_fin"] = id_fin
    return parametres


async def _calculer_cles_par_lots(etat: _Etat, id_min: int, id_max: int) -> int:
    """Joue le calcul par tranches d'`id` en autocommit ; rend le nombre de clés écrites."""
    debut = time.perf_counter()
    fichier, _ = SCRIPTS[CLES]
    tranches = [
        (borne, min(borne + CHARGEMENT_TAILLE_LOT, id_max))
        for borne in range(id_min - 1, id_max, CHARGEMENT_TAILLE_LOT)
    ]
    logger.info(
        "Début calcul des clés par lots %s",
        ctx(
            id_referentiel=etat.id_referentiel,
            lots=len(tranches),
            taille_lot=CHARGEMENT_TAILLE_LOT,
            id_min=id_min,
            id_max=id_max,
        ),
    )

    ecrites = 0
    lots_faits = 0
    for depart in range(0, len(tranches), LOTS_CLES_PAR_CONNEXION):
        paquet = tranches[depart : depart + LOTS_CLES_PAR_CONNEXION]
        unites = [
            (
                f"db/{fichier}@{bas + 1}-{haut}",
                instructions_parametrees(
                    etat.scripts[CLES], _parametres_cles(etat, bas, haut)
                ),
            )
            for bas, haut in paquet
        ]
        try:
            resultat = await etat.db_ecriture.execute_sql_units(
                unites, transactional=False, skip_selects=True
            )
        except SqlScriptError:
            logger.warning(
                "Rejet calcul des clés par lots %s",
                ctx(
                    id_referentiel=etat.id_referentiel,
                    lots_commites=lots_faits,
                    cles_ecrites=ecrites,
                    suite="relancer --etape cles : seules les clés manquantes seront écrites",
                ),
            )
            etat.rapport.avertissements.append(
                f"Calcul des clés interrompu après {lots_faits} lot(s) commité(s) "
                f"({ecrites} clé(s) écrite(s), conservées). Relancer « --etape cles » : "
                f"seules les clés manquantes seront calculées."
            )
            raise
        for label, _ in unites:
            ecrites += max(_insertions(resultat, label, APERCU_INSERT_CLES), 0)
        lots_faits += len(paquet)

        ecoule = max(time.perf_counter() - debut, 1e-9)
        logger.info(
            "Avancement calcul des clés %s",
            ctx(
                id_referentiel=etat.id_referentiel,
                lots=lots_faits,
                lots_total=len(tranches),
                pct=round(100 * lots_faits / len(tranches), 1),
                cles_ecrites=ecrites,
                debit_lignes_s=round(ecrites / ecoule, 1),
                duration_ms=round(ecoule * 1000, 1),
            ),
        )
    return ecrites


async def _recreer_index_cles(etat: _Etat) -> None:
    """Recrée `uq_crc_version_pdi` s'il manque ; un échec (invariant cassé) va au rapport."""
    debut = time.perf_counter()
    logger.info("Début reconstruction index clés %s", ctx(index=INDEX_UNIQUE_CLES))
    try:
        await index_chargement.executer_ddl(
            f"ALTER TABLE {TABLE_CLES} ADD {DEFINITION_INDEX_UNIQUE_CLES}",
            etape="index-reconstruction",
            prefixe="cles",
            table=TABLE_CLES,
            db=etat.db_ecriture,
        )
    except Exception as erreur:  # noqa: BLE001 - porté au rapport, jamais de stacktrace
        logger.exception("Erreur reconstruction index clés %s", ctx(index=INDEX_UNIQUE_CLES))
        etat.rapport.ko(
            f"Index {INDEX_UNIQUE_CLES} non recréé : {erreur}. Le recréer avant toute "
            f"lecture des clés : ALTER TABLE {TABLE_CLES} ADD {DEFINITION_INDEX_UNIQUE_CLES}"
        )
        return
    logger.info(
        "Fin reconstruction index clés %s",
        ctx(
            index=INDEX_UNIQUE_CLES,
            duration_ms=round((time.perf_counter() - debut) * 1000, 1),
        ),
    )


_IMPLEMENTATIONS = {
    CHARGEMENT: _etape_chargement,
    MIGRATION: _etape_migration,
    CORRECTIF: _etape_correctif,
    AGREGATS: _etape_agregats,
    VERSIONS: _etape_versions,
    CLES: _etape_cles,
}


__all__ = ["ETAPES", "SCRIPTS", "initialiser_cles_repartition"]
