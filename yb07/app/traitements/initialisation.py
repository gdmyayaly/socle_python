"""Chaîne d'initialisation des clés de répartition des PDI — tickets DSR-696 à DSR-699.

    CSV (S3) ─chargement─→ trppu_cles_repartition
                                  │
                      migration + correctif : le schéma que la suite exige
                                  │
                           agregats ─→ trppu_trafic_site      (les DÉNOMINATEURS)
                                  │
                           versions ─→ trppu_version_cle      (les CONTENEURS)
                                  │
                              cles ─→ trppu_cles_repartition_calcule

Six étapes, un ordre non négociable. L'enjeu n'est pas la difficulté de chacune — elles sont
toutes écrites — mais le fait qu'une erreur d'enchaînement **ne se voit pas** : DSR-699 joint
les trois tables et écarte sans un mot les sites auxquels il manque un agrégat ou une version.
Le référentiel est alors incomplet, et le CA4 du même ticket interdit de le recalculer.

D'où la forme de ce traitement : des prérequis joués avant toute écriture, une étape qui s'arrête
net dès qu'un contrôle échoue, et un rapport qui nomme l'étape à laquelle reprendre. Sur une
chaîne dont deux étapes se comptent en heures, c'est la sortie la plus utile.

Les scripts vivent dans `db/` et gardent leur forme SQL — ils sont relus en recette, et leurs
contrôles d'acceptation font foi. Ce module ne les réécrit pas : il substitue leurs paramètres
de session au vol (`app/db/sql_parametres.py`) et rejoue en Python les contrôles dont le socle
perd le résultat (`controles_init.py`).

Le traitement **ne lève pas** : il rend un `Rapport` dont `reussi` vaut `False`. C'est la CLI qui
en déduit le code de retour du processus.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.config import INIT_LOG_TOUS_LES_SITES
from app.db.mysql import db_read, db_write
from app.db.sql_parametres import injecter_parametres
from app.db.sql_script import SqlScriptError, statement_preview
from app.erreurs import TraitementImpossible
from app.log_utils import ctx
from app.traitements import controles_init
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

#: Ordre d'exécution. Sert aussi de `choices=` à argparse et d'index à `--depuis` — une seule
#: source de vérité, pour que la CLI ne puisse pas diverger de la chaîne.
ETAPES = (CHARGEMENT, MIGRATION, CORRECTIF, AGREGATS, VERSIONS, CLES)

#: Fichier et mode d'exécution de chaque étape scriptée.
#:
#: `transactional=False` sur la migration et le correctif n'est pas un réglage de confort :
#: leurs `ALTER` voyagent dans une chaîne exécutée par `PREPARE`/`EXECUTE`, ce qui les rend
#: rejouables mais **invisibles** à la détection de DDL du socle (`is_ddl`). L'avertissement
#: « DDL en mode transactionnel » ne se déclencherait donc pas, alors que le COMMIT implicite,
#: lui, a bien lieu — on ouvrirait une transaction qui ne protège rien.
SCRIPTS: dict[str, tuple[str, bool]] = {
    MIGRATION: ("DSR-696-699_migration.sql", False),
    CORRECTIF: ("fix_error.sql", False),
    AGREGATS: ("DSR-696_site_trafic.sql", True),
    VERSIONS: ("DSR-698_version_cle.sql", True),
    CLES: ("DSR-699_cles_calculees.sql", True),
}

#: Prérequis à vérifier selon l'étape de **départ**. Ils ne portent que sur le point d'entrée :
#: les prérequis des étapes suivantes sont produits par les précédentes au cours du même passage.
#:
#: Les deux garde-fous propres au calcul des clés — dénominateurs nuls et périmètre déjà calculé —
#: n'apparaissent pas ici : ils sont joués par l'étape elle-même, à chaque fois. Ils dépendent en
#: effet d'une table que le chargement ne purge pas (`trppu_cles_repartition_calcule`), donc une
#: chaîne complète relancée sur un référentiel déjà calculé doit s'y heurter, elle aussi.
PREREQUIS: dict[str, tuple[str, ...]] = {
    CHARGEMENT: (),
    MIGRATION: ("lignes",),
    CORRECTIF: ("lignes",),
    AGREGATS: ("lignes", "schema"),
    VERSIONS: ("lignes", "schema", "agregats"),
    CLES: ("lignes", "schema", "agregats", "versions"),
}

#: Répertoire des scripts, résolu depuis le module et non depuis le répertoire courant : un
#: batch lancé par un ordonnanceur ne choisit pas son `cwd`.
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

    #: Nombre de PDI actifs, rendu par l'étape « chargement ». Rend le CA1 de DSR-699 gratuit
    #: quand la chaîne tourne de bout en bout ; vaut `None` en reprise.
    lignes_actives: int | None = None
    #: Nombre d'agrégats par référentiel avant l'étape « agregats » — sert à prouver le CA5.
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
    db_lecture=db_read,
    db_ecriture=db_write,
) -> Rapport:
    """Joue la chaîne d'initialisation, en totalité ou à partir d'une étape.

    `depuis` reprend à cette étape et enchaîne les suivantes ; `etape` n'en joue qu'une. Les deux
    s'excluent. `dry_run` lit et découpe les scripts sans rien écrire — l'étape « chargement »
    est alors sautée, elle n'a pas de mode à blanc.
    """
    debut = time.perf_counter()
    rapport = Rapport(
        titre=TITRE,
        id_traitement=id_referentiel,
        libelle_identifiant="Référentiel",
    )

    logger.info(
        "Début initialisation clés de répartition %s",
        ctx(id_referentiel=id_referentiel, depuis=depuis, etape=etape, dry_run=dry_run),
    )

    jouees: list[str] = []
    try:
        a_jouer = _resoudre_etapes(depuis, etape)

        # Lecture et découpage AVANT toute écriture : un fichier absent ou mal encodé doit
        # échouer sans que la base ait été touchée.
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
    """Étapes à jouer, dans l'ordre.

    La validation des noms revient à argparse (`choices=ETAPES`), qui refuse avant toute
    connexion. Les contrôles ci-dessous sont la bretelle : ce traitement est aussi appelable
    comme bibliothèque.
    """
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
    """Message d'échec d'un script, sans jamais le SQL complet.

    `erreur.statement` porte l'instruction entière ; le rapport part en JSON vers une supervision
    et les logs vers Kibana. On s'en tient à l'aperçu tronqué, comme le socle le fait lui-même.
    """
    apercu = statement_preview(erreur.statement) if erreur.statement else ""
    return (
        f"Échec de {erreur.source}, instruction {erreur.index}"
        + (f" [{apercu}]" if apercu else "")
        + f" : {erreur.original}"
    )


# ---------------------------------------------------------------------------
# Étapes
# ---------------------------------------------------------------------------


async def _executer_script(
    etat: _Etat, nom: str, parametres: dict[str, Any], *, suffixe_label: str = ""
):
    """Injecte les paramètres du script de l'étape et le joue sur la base d'écriture.

    `suffixe_label` distingue les exécutions répétées d'un même script : sans lui, les lignes
    `Début`/`Fin script SQL` du socle seraient identiques pour les milliers de sites de
    l'étape « versions », et `id_traitement` vaut le référentiel, pas le site.
    """
    fichier, transactional = SCRIPTS[nom]
    texte = injecter_parametres(etat.scripts[nom], parametres)
    return await etat.db_ecriture.execute_sql_script(
        texte,
        label=f"db/{fichier}{suffixe_label}",
        transactional=transactional,
        dry_run=etat.dry_run,
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

    sous_rapport = await charger_cles_repartition(etat.id_referentiel, etat.fichier)

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
    # Photo prise avant l'écriture : comparée à celle d'après, elle prouve le CA5, ce que la
    # simple lecture de la table après coup ne saurait pas faire.
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
    """DSR-698, un site à la fois — la règle de gestion du ticket raisonne par site.

    La liste vient de `trppu_trafic_site`, et non de `trppu_cles_repartition` : quelques milliers
    de lignes servies par `uq_site_trafic` au lieu d'un balayage de 24 M entrées, et ce sont
    exactement les sites qui ont un dénominateur, donc ceux que le calcul des clés saura traiter.
    """
    if etat.dry_run:
        # La liste des sites n'est pas lue : la marche à blanc doit pouvoir valider le
        # découpage et la substitution des paramètres sans dépendre de l'état de la base.
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
    creees = 0
    deja_a_jour = 0
    echecs: list[str] = []

    for traites, site in enumerate(sites, start=1):
        try:
            resultat = await _executer_script(
                etat, VERSIONS, _parametres_version(etat, site), suffixe_label=f"@{site}"
            )
        except SqlScriptError as erreur:
            # Un site n'explique pas le suivant : on poursuit, quitte à échouer l'étape. Ce qui
            # est interdit, c'est de passer au calcul des clés — voir plus bas.
            echecs.append(str(site))
            logger.warning(
                "Rejet création de version %s",
                ctx(
                    id_referentiel=etat.id_referentiel,
                    co_regate=site,
                    motif=_message_script(erreur),
                ),
            )
        else:
            if controles_init.rowcount(resultat, APERCU_INSERT_VERSION) > 0:
                creees += 1
            else:
                deja_a_jour += 1
            logger.debug(
                "Fin création de version %s",
                ctx(id_referentiel=etat.id_referentiel, co_regate=site),
            )

        if traites % INIT_LOG_TOUS_LES_SITES == 0:
            _journaliser_avancement(etat, traites, len(sites), creees, echecs, debut)

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

    await controles_init.controler_versions(
        etat.rapport, etat.db_lecture, etat.id_referentiel, len(sites)
    )
    return etat.rapport.reussi


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
    """DSR-699. Deux garde-fous avant d'écrire, parce que l'écriture est un cliquet.

    Ils sont joués à chaque passage, et pas seulement en reprise : le chargement purge
    `trppu_cles_repartition`, jamais `trppu_cles_repartition_calcule`. Une chaîne complète
    relancée sur un référentiel déjà calculé se heurterait donc au CA4 sans que rien ne le dise,
    le script se terminant en succès après avoir écrit zéro ligne.
    """
    if not etat.dry_run and not await controles_init.verifier_prerequis(
        etat.rapport, etat.db_lecture, etat.id_referentiel, ("denominateurs", "non_calcule")
    ):
        return False

    resultat = await _executer_script(
        etat, CLES, {"id_referentiel": etat.id_referentiel, "co_regate": None}
    )

    if etat.dry_run:
        etat.rapport.ok(
            _ligne_etape(rang, total, CLES, f"{resultat.total_count} instruction(s)")
        )
        return True

    ecrites = controles_init.rowcount(resultat, APERCU_INSERT_CLES)
    etat.rapport.etats["CLES_CALCULEES"] = ecrites

    if ecrites == 0:
        etat.rapport.ko(
            "Aucune clé écrite alors que des PDI actifs étaient attendus : périmètre déjà "
            "calculé, ou jointure sans correspondance. Rien n'a été modifié."
        )
        return False

    etat.rapport.ok(_ligne_etape(rang, total, CLES, f"{ecrites} clé(s) calculée(s)"))
    await controles_init.controler_cles(
        etat.rapport,
        etat.db_lecture,
        etat.id_referentiel,
        ecrites,
        lignes_attendues=etat.lignes_actives,
        controles_longs=etat.controles_longs,
    )
    return etat.rapport.reussi


_IMPLEMENTATIONS = {
    CHARGEMENT: _etape_chargement,
    MIGRATION: _etape_migration,
    CORRECTIF: _etape_correctif,
    AGREGATS: _etape_agregats,
    VERSIONS: _etape_versions,
    CLES: _etape_cles,
}


__all__ = ["ETAPES", "SCRIPTS", "initialiser_cles_repartition"]
