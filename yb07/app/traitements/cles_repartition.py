"""Chargement du référentiel des clés de répartition des PDI, depuis un CSV sur S3.

    fichier CSV (S3)  →  trppu_cles_repartition

Les règles de gestion viennent du chargement historique, écrit en SQL pur dans le projet
voisin (`yb05/db/DSR-697_chargement_cles_repartition.sql`) :

    RG1  toutes les lignes chargées portent le même `id_referentiel`
    RG3  champs vides convertis en NULL — co_regate_etablissement, lb_etablissement,
         nb_pre, potentielip
    RG5  unicité (id_pdi, id_referentiel) — garantie en base par `uk_pdi_ref`
    RG6  purge du référentiel cible avant chargement

Deux écarts assumés avec ce script, tous deux dus au format de fichier, qui a évolué :

* le CSV porte désormais lui-même `id_referentiel`, `date_debut_validite` et
  `date_fin_validite` ; le script les forçait (RG2). **Le fichier fait foi**, et le
  paramètre de la commande ne sert qu'à cibler la purge — toute ligne portant un autre
  référentiel fait échouer le chargement, c'est le seul moyen de ne pas charger un fichier
  sous un référentiel qui n'est pas le sien ;
* le chargement se fait **par lots commités**, pas en une transaction unique. Sur 22 M de
  lignes, le journal d'annulation, la durée de connexion et le coût du ROLLBACK sont le
  vrai risque. Un échec laisse donc un chargement partiel : c'est la purge (RG6) qui rend
  la commande rejouable, et le rapport dit combien de lignes étaient passées.

Le traitement **ne lève pas** : il rend un `Rapport` dont `reussi` vaut `False`. C'est la
CLI qui en déduit le code de retour du processus.
"""

from __future__ import annotations

import asyncio
import csv
import logging
import time
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterator

from app.config import (
    CHARGEMENT_LOG_TOUTES_LES,
    CHARGEMENT_TAILLE_LOT,
    CSV_CLES_REPARTITION,
    CSV_DELIMITEUR,
    CSV_ENCODAGE,
    S3_BUCKET,
)
from app.db.mysql import db_read, db_write
from app.erreurs import TraitementImpossible
from app.log_utils import ctx
from app.services import s3
from app.traitements.rapport import ECHEC, SUCCES, Rapport

logger = logging.getLogger(__name__)

TITRE = "CHARGEMENT DES CLES DE REPARTITION"

# En-tête attendu, dans l'ordre du fichier. Le vérifier au premier enregistrement fait
# échouer un fichier au mauvais format tout de suite, et non à la millionième ligne.
COLONNES_CSV = (
    "id",
    "pdi_rattache",
    "trafic_colis",
    "trafic_oo",
    "trafic_3s",
    "nature",
    "regate_site",
    "type",
    "libelle_site",
    "regate_etab",
    "libelle_etab",
    "regate_dex",
    "libelle_dex",
    "nb_pre",
    "potentielip",
    "id_referentiel",
    "date_debut_validite",
    "date_fin_validite",
)

# `id` (auto-incrément) n'est pas alimenté : la colonne `id` du CSV est le PDI, elle part
# dans `id_pdi`.
INSERT_SQL = """
INSERT INTO trppu_cles_repartition
    (id_pdi, pdi_rattache, trafic_colis, trafic_oo, trafic_3s, nature,
     co_regate_site, type_site, lb_regate, co_regate_etablissement, lb_etablissement,
     co_regate_dex, lb_dex, nb_pre, potentielip, id_referentiel,
     date_debut_validite, date_fin_validite)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# RG6. `DELETE` et non `TRUNCATE` : `TRUNCATE` viderait **tous** les référentiels,
# remettrait l'auto-incrément à zéro et, étant du DDL, interdirait tout retour arrière.
PURGE_SQL = "DELETE FROM trppu_cles_repartition WHERE id_referentiel = %s"

REFERENTIEL_DECLARE_SQL = (
    "SELECT COUNT(*) AS nb FROM trppu_referentiel WHERE id_referentiel = %s"
)
LIGNES_PRESENTES_SQL = (
    "SELECT COUNT(*) AS nb FROM trppu_cles_repartition WHERE id_referentiel = %s"
)
CONTROLE_FINAL_SQL = """
SELECT COUNT(*)                             AS nb_lignes,
       COUNT(DISTINCT id_pdi)               AS nb_pdi_distincts,
       SUM(date_fin_validite IS NULL)       AS nb_actives,
       MIN(date_debut_validite)             AS debut_validite_min
  FROM trppu_cles_repartition
 WHERE id_referentiel = %s
"""


# ---------------------------------------------------------------------------
# Conversion d'une ligne CSV
# ---------------------------------------------------------------------------


def _texte(valeur: str | None) -> str:
    return (valeur or "").strip()


def _obligatoire(ligne: dict[str, str], colonne: str, numero: int) -> str:
    """Champ NOT NULL en base : un vide doit faire échouer le chargement.

    C'est la transposition du `sql_mode` durci du script SQL — hors mode strict, MySQL
    convertit silencieusement, et mieux vaut une erreur au chargement qu'un trafic ramené
    à zéro sans que personne ne le sache.
    """
    valeur = _texte(ligne.get(colonne))
    if not valeur:
        raise TraitementImpossible(
            f"Ligne {numero} : la colonne '{colonne}' est vide alors qu'elle est obligatoire."
        )
    return valeur


def _entier(ligne: dict[str, str], colonne: str, numero: int) -> int:
    valeur = _obligatoire(ligne, colonne, numero)
    try:
        return int(valeur)
    except ValueError as erreur:
        raise TraitementImpossible(
            f"Ligne {numero} : '{colonne}' = '{valeur}' n'est pas un entier."
        ) from erreur


def _entier_optionnel(ligne: dict[str, str], colonne: str, numero: int) -> int | None:
    """RG3 : un champ vide vaut NULL, jamais 0 — 0 et « inconnu » ne se confondent pas."""
    valeur = _texte(ligne.get(colonne))
    if not valeur:
        return None
    try:
        return int(valeur)
    except ValueError as erreur:
        raise TraitementImpossible(
            f"Ligne {numero} : '{colonne}' = '{valeur}' n'est pas un entier."
        ) from erreur


def _decimal(ligne: dict[str, str], colonne: str, numero: int) -> Decimal:
    valeur = _obligatoire(ligne, colonne, numero)
    try:
        return Decimal(valeur)
    except InvalidOperation as erreur:
        raise TraitementImpossible(
            f"Ligne {numero} : '{colonne}' = '{valeur}' n'est pas un décimal."
        ) from erreur


def _date(ligne: dict[str, str], colonne: str, numero: int) -> date:
    valeur = _obligatoire(ligne, colonne, numero)
    try:
        return datetime.strptime(valeur, "%Y-%m-%d").date()
    except ValueError as erreur:
        raise TraitementImpossible(
            f"Ligne {numero} : '{colonne}' = '{valeur}' n'est pas une date AAAA-MM-JJ."
        ) from erreur


def _date_optionnelle(ligne: dict[str, str], colonne: str, numero: int) -> date | None:
    if not _texte(ligne.get(colonne)):
        return None
    return _date(ligne, colonne, numero)


def _texte_optionnel(ligne: dict[str, str], colonne: str) -> str | None:
    """RG3 : chaîne vide convertie en NULL.

    Sans cette conversion, `co_regate_etablissement` vaudrait `''` et se propagerait comme
    une clé de jointure fantôme sur l'établissement.
    """
    return _texte(ligne.get(colonne)) or None


def convertir(ligne: dict[str, str], numero: int, id_referentiel: int) -> tuple[Any, ...]:
    """Transforme une ligne du CSV en paramètres d'insertion, dans l'ordre de `INSERT_SQL`.

    `numero` est le numéro de ligne dans le fichier, en-tête compris : c'est ce que
    l'exploitant lit dans son éditeur.
    """
    referentiel_ligne = _entier(ligne, "id_referentiel", numero)
    if referentiel_ligne != id_referentiel:
        raise TraitementImpossible(
            f"Ligne {numero} : le fichier porte le référentiel {referentiel_ligne} "
            f"alors que le chargement vise le référentiel {id_referentiel}."
        )

    return (
        _entier(ligne, "id", numero),  # -> id_pdi
        _entier(ligne, "pdi_rattache", numero),
        _decimal(ligne, "trafic_colis", numero),
        _decimal(ligne, "trafic_oo", numero),
        _decimal(ligne, "trafic_3s", numero),
        _obligatoire(ligne, "nature", numero),
        _obligatoire(ligne, "regate_site", numero),
        _obligatoire(ligne, "type", numero),
        _obligatoire(ligne, "libelle_site", numero),
        _texte_optionnel(ligne, "regate_etab"),
        _texte_optionnel(ligne, "libelle_etab"),
        _obligatoire(ligne, "regate_dex", numero),
        _obligatoire(ligne, "libelle_dex", numero),
        _entier_optionnel(ligne, "nb_pre", numero),
        _entier_optionnel(ligne, "potentielip", numero),
        referentiel_ligne,
        _date(ligne, "date_debut_validite", numero),
        _date_optionnelle(ligne, "date_fin_validite", numero),
    )


def verifier_entete(colonnes: list[str] | None) -> None:
    """Refuse un fichier dont l'en-tête ne correspond pas, avant toute écriture."""
    lues = tuple(_texte(colonne) for colonne in (colonnes or []))
    if lues != COLONNES_CSV:
        raise TraitementImpossible(
            "En-tête du fichier inattendu.\n"
            f"  attendu : {';'.join(COLONNES_CSV)}\n"
            f"  lu      : {';'.join(lues) if lues else '(fichier vide)'}"
        )


def _lire_lot(lecteur: Iterator[dict[str, str]], taille: int) -> list[dict[str, str]]:
    """Consomme jusqu'à `taille` lignes du lecteur CSV.

    Isolée pour être appelée dans un thread : la lecture tire sur le réseau et bloquerait
    la boucle asyncio.
    """
    lot: list[dict[str, str]] = []
    for ligne in lecteur:
        lot.append(ligne)
        if len(lot) >= taille:
            break
    return lot


# ---------------------------------------------------------------------------
# Traitement
# ---------------------------------------------------------------------------


async def charger_cles_repartition(
    id_referentiel: int, fichier: str | None = None
) -> Rapport:
    """Charge `trppu_cles_repartition` depuis le CSV du bucket S3.

    `fichier` surcharge `CSV_CLES_REPARTITION` pour un rechargement ponctuel.
    """
    debut = time.perf_counter()
    nom_fichier = fichier or CSV_CLES_REPARTITION
    rapport = Rapport(
        titre=TITRE,
        id_traitement=id_referentiel,
        libelle_identifiant="Référentiel",
    )

    logger.info(
        "Début chargement clés de répartition %s",
        ctx(id_referentiel=id_referentiel, fichier=nom_fichier, bucket=S3_BUCKET),
    )

    if not nom_fichier:
        motif = "Aucun fichier : renseigner CSV_CLES_REPARTITION ou passer --fichier."
        logger.warning("Rejet chargement clés de répartition %s", ctx(motif=motif))
        rapport.ko(motif)
        rapport.statut = ECHEC
        return rapport

    cle = s3.chemin_objet(nom_fichier)
    lignes_inserees = 0
    lots = 0

    try:
        # --- Garde-fous, avant toute écriture -----------------------------
        #
        # Aucune clé étrangère ne relie `trppu_cles_repartition` à `trppu_referentiel` :
        # sans ce contrôle, on charge 22 M de lignes sous un référentiel qui n'existe pas
        # et rien ne le signale. C'est ici, et seulement ici, que l'écart se voit.
        declare = await db_read.fetch_one(REFERENTIEL_DECLARE_SQL, (id_referentiel,))
        if not (declare and declare["nb"]):
            motif = (
                f"Le référentiel {id_referentiel} n'est pas déclaré dans trppu_referentiel."
            )
            logger.warning(
                "Rejet chargement clés de répartition %s",
                ctx(id_referentiel=id_referentiel, motif=motif),
            )
            rapport.ko(motif)
            rapport.statut = ECHEC
            return rapport
        rapport.ok(f"Référentiel {id_referentiel} déclaré")

        presentes = await db_read.fetch_one(LIGNES_PRESENTES_SQL, (id_referentiel,))
        nb_presentes = int(presentes["nb"]) if presentes else 0

        # Localiser le fichier avant de purger : découvrir son absence après avoir
        # supprimé 22 M de lignes coûterait un rechargement complet.
        taille = await asyncio.to_thread(s3.verifier_presence, cle)
        rapport.ok(f"Fichier '{cle}' présent sur S3 ({taille} octets)")

        # --- RG6 : purge du référentiel cible ------------------------------
        supprimees = await db_write.execute(PURGE_SQL, (id_referentiel,))
        logger.info(
            "Purge du référentiel effectuée %s",
            ctx(id_referentiel=id_referentiel, lignes=supprimees),
        )
        rapport.ok(f"Purge du référentiel : {supprimees} ligne(s) supprimée(s)")

        # --- Lecture en streaming et insertion par lots ---------------------
        lignes_inserees, lots = await _charger(cle, id_referentiel, debut)

    except TraitementImpossible as erreur:
        logger.warning(
            "Rejet chargement clés de répartition %s",
            ctx(
                id_referentiel=id_referentiel,
                fichier=nom_fichier,
                lignes=lignes_inserees,
                motif=str(erreur),
            ),
        )
        rapport.ko(str(erreur))
        rapport.statut = ECHEC
        rapport.etats["LIGNES_CHARGEES"] = lignes_inserees
        return rapport
    except Exception as erreur:  # noqa: BLE001 - la CLI ne doit jamais rendre de stacktrace
        logger.exception(
            "Erreur chargement clés de répartition %s",
            ctx(id_referentiel=id_referentiel, fichier=nom_fichier, lignes=lignes_inserees),
        )
        rapport.erreur = _message_erreur(erreur)
        rapport.statut = ECHEC
        rapport.etats["LIGNES_CHARGEES"] = lignes_inserees
        return rapport

    rapport.ok(f"{lignes_inserees} ligne(s) insérée(s) en {lots} lot(s)")
    await _controles_finaux(rapport, id_referentiel, lignes_inserees)

    rapport.etats["LIGNES_CHARGEES"] = lignes_inserees
    rapport.etats["LIGNES_PRECEDENTES"] = nb_presentes
    rapport.statut = SUCCES if rapport.reussi else ECHEC

    duration_ms = round((time.perf_counter() - debut) * 1000, 1)
    logger.info(
        "Fin chargement clés de répartition %s",
        ctx(
            id_referentiel=id_referentiel,
            fichier=nom_fichier,
            lignes=lignes_inserees,
            lots=lots,
            verdict=rapport.statut,
            duration_ms=duration_ms,
        ),
    )
    return rapport


async def _charger(cle: str, id_referentiel: int, debut: float) -> tuple[int, int]:
    """Lit le CSV en streaming et insère par lots. Retourne (lignes, lots)."""
    lignes_inserees = 0
    lots = 0
    prochain_jalon = CHARGEMENT_LOG_TOUTES_LES

    with s3.ouvrir_objet(cle, encodage=CSV_ENCODAGE) as flux:
        lecteur = csv.DictReader(flux, delimiter=CSV_DELIMITEUR)
        verifier_entete(lecteur.fieldnames)

        while True:
            brutes = await asyncio.to_thread(_lire_lot, lecteur, CHARGEMENT_TAILLE_LOT)
            if not brutes:
                break

            # `lecteur.line_num` porte le numéro de la dernière ligne lue, en-tête compris :
            # on remonte au premier enregistrement du lot pour numéroter chaque ligne.
            premiere = lecteur.line_num - len(brutes) + 1
            valeurs = [
                convertir(ligne, premiere + decalage, id_referentiel)
                for decalage, ligne in enumerate(brutes)
            ]

            try:
                async with db_write.transaction() as tx:
                    await tx.execute_many(INSERT_SQL, valeurs)
            except Exception as erreur:  # noqa: BLE001 - retraduit puis relancé
                raise _traduire_erreur_insertion(erreur, premiere) from erreur

            lignes_inserees += len(valeurs)
            lots += 1

            if lignes_inserees >= prochain_jalon:
                _journaliser_avancement(id_referentiel, lignes_inserees, lots, debut)
                prochain_jalon += CHARGEMENT_LOG_TOUTES_LES

    return lignes_inserees, lots


def _journaliser_avancement(
    id_referentiel: int, lignes: int, lots: int, debut: float
) -> None:
    """Trace la progression d'un chargement long.

    Le batch tourne sous ordonnanceur, sans `-v` : sans ces lignes, un chargement d'une
    heure ne laisserait aucune trace de son avancement, et l'exploitant ne saurait pas
    distinguer un traitement lent d'un traitement bloqué. Le total de lignes est inconnu
    (le fichier est lu en streaming, jamais compté d'avance) : on journalise un volume et
    un débit, pas un pourcentage.
    """
    ecoule = time.perf_counter() - debut
    logger.info(
        "Avancement chargement clés de répartition %s",
        ctx(
            id_referentiel=id_referentiel,
            lignes=lignes,
            lots=lots,
            debit_lignes_s=round(lignes / ecoule, 1) if ecoule > 0 else None,
            duration_ms=round(ecoule * 1000, 1),
        ),
    )


def _traduire_erreur_insertion(erreur: Exception, premiere_ligne: int) -> Exception:
    """Retraduit les erreurs d'insertion les plus fréquentes en message actionnable."""
    message = str(erreur)
    if "1062" in message or "Duplicate entry" in message:
        return TraitementImpossible(
            f"Doublon de PDI détecté à partir de la ligne {premiere_ligne} : le fichier "
            "porte deux fois le même couple (PDI, référentiel). Le dédoublonnage amont est "
            "insuffisant — dédoublonner le fichier avant de recharger."
        )
    if "1264" in message or "Out of range" in message:
        return TraitementImpossible(
            f"Valeur hors bornes à partir de la ligne {premiere_ligne} : {message}"
        )
    return erreur


def _message_erreur(erreur: Exception) -> str:
    """Message d'erreur destiné à l'exploitant, jamais une stacktrace."""
    return f"{type(erreur).__name__} : {erreur}"


async def _controles_finaux(
    rapport: Rapport, id_referentiel: int, lignes_inserees: int
) -> None:
    """Relit la table pour confirmer ce qui a été écrit.

    Les doublons `(id_pdi, id_referentiel)` ne sont pas comptés : `uk_pdi_ref` les rejette
    déjà au moment de l'insertion, les recompter sur 22 M de lignes ne dirait rien de plus.
    """
    controle = await db_read.fetch_one(CONTROLE_FINAL_SQL, (id_referentiel,))
    if not controle:
        rapport.ko("Contrôle final impossible : aucune ligne relue.")
        return

    nb_lignes = int(controle["nb_lignes"] or 0)
    nb_pdi = int(controle["nb_pdi_distincts"] or 0)
    nb_actives = int(controle["nb_actives"] or 0)

    rapport.ajouter(
        nb_lignes == lignes_inserees,
        f"Volumétrie en base : {nb_lignes} ligne(s)",
        f"Volumétrie incohérente : {nb_lignes} ligne(s) en base pour "
        f"{lignes_inserees} insérée(s).",
    )
    rapport.ajouter(
        nb_pdi == nb_lignes,
        f"PDI distincts : {nb_pdi}",
        f"{nb_lignes - nb_pdi} ligne(s) en doublon de PDI dans le référentiel.",
    )
    rapport.ok(f"Lignes actives (date_fin_validite NULL) : {nb_actives}")
    rapport.etats["DATE_DEBUT_VALIDITE_MIN"] = controle["debut_validite_min"]
