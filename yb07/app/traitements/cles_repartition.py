"""Chargement du référentiel des clés de répartition des PDI, depuis un CSV.

    fichier CSV (S3 ou disque local)  →  trppu_cles_repartition

La source nominale est le bucket S3. Un fichier du disque local peut la remplacer
(`chemin_local`) : seul l'accès au fichier change, les règles ci-dessous s'appliquent à
l'identique.

Les règles de gestion viennent du chargement historique, écrit en SQL pur dans le projet
voisin (`yb05/db/DSR-697_chargement_cles_repartition.sql`) :

    RG1  toutes les lignes chargées portent le même `id_referentiel`
    RG3  champs vides convertis en NULL — co_regate_etablissement, lb_etablissement,
         nb_pre, potentielip
    RG5  unicité (id_pdi, id_referentiel) — garantie en base par `uk_pdi_ref`
    RG6  purge avant chargement — par `TRUNCATE TABLE` (cf. plus bas)

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

Index (cf. `index_chargement.py`) : par défaut ils restent en place et les lignes sont
insérées par lots — plus lent, mais sans instruction longue. `CHARGEMENT_RETIRER_INDEX=true`
les retire le temps du chargement et les reconstruit en une passe triée, les doublons étant
traités juste avant de recréer l'index unique. Dans les deux cas, le lot suivant est lu et
converti pendant l'insertion du courant.

Sites dont un total de trafic est nul : ils sont chargés comme les autres ; le calcul des
clés (DSR-699) leur enregistre des clés à 0 et le rapport de `init` les liste.

La purge RG6 est un `TRUNCATE TABLE`, quasi instantané là où un `DELETE` de 22 M de lignes
prend longtemps. Il vide **toute** la table et ne s'annule pas (DDL, commit implicite) : il
n'est donc joué que si la table ne contient aucun autre référentiel que celui chargé —
vérifié avant toute écriture — et il exige le droit `DROP` sur la table.

Avec `ignorer_erreurs` (option `--skip-errors`), une ligne non conforme — champ obligatoire
vide, valeur mal formée, autre référentiel, doublon ou valeur refusée par MySQL — est écartée
au lieu d'arrêter le chargement. Elle figure dans les avertissements du rapport, et le total
dans `LIGNES_IGNOREES`. Un en-tête faux ou une panne technique (connexion, verrou, droits)
restent bloquants : ce ne sont pas des défauts d'une ligne.

Reprise : une fois le fichier entièrement chargé, un échec de la suite (reconstruction
des index, doublons, contrôles — typiquement une coupure de connexion
pendant un ALTER de plusieurs minutes) **ne vide plus la table**. Les lignes restent, et
`finaliser_chargement` (commande `finaliser-chargement`) reprend là où le chargement s'est
arrêté, sans relire le fichier.

Le traitement **ne lève pas** : il rend un `Rapport` dont `reussi` vaut `False`. C'est la
CLI qui en déduit le code de retour du processus.
"""

from __future__ import annotations

import asyncio
import csv
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterator

from app.config import (
    CHARGEMENT_LOG_TOUTES_LES,
    CHARGEMENT_MAX_REJETS_DETAILLES,
    CHARGEMENT_RETIRER_INDEX,
    CHARGEMENT_TAILLE_LOT,
    CSV_CLES_REPARTITION,
    CSV_DELIMITEUR,
    CSV_ENCODAGE,
    S3_BUCKET,
)
from app.db.mysql import db_read, db_write
from app.erreurs import TraitementImpossible
from app.log_utils import ctx
from app.services import fichier_local, s3
from app.traitements import index_chargement
from app.traitements.rapport import ECHEC, SUCCES, Rapport

logger = logging.getLogger(__name__)

TITRE = "CHARGEMENT DES CLES DE REPARTITION"
TITRE_FINALISATION = "FINALISATION DU CHARGEMENT DES CLES DE REPARTITION"

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

# RG6. La purge est un `TRUNCATE` (`index_chargement.vider_table`) : il vide **tous** les
# référentiels et, étant du DDL, interdit tout retour arrière. Il n'est joué que derrière ce
# garde-fou, servi par `idx_cr_ref_actif` (id_referentiel en tête) : un parcours d'index.
AUTRE_REFERENTIEL_SQL = """
SELECT id_referentiel FROM trppu_cles_repartition WHERE id_referentiel <> %s LIMIT 1
"""

# Existence d'au moins une ligne : sans index, s'arrête à la première ligne trouvée.
REFERENTIEL_CHARGE_SQL = (
    "SELECT 1 AS ok FROM trppu_cles_repartition WHERE id_referentiel = %s LIMIT 1"
)

LIGNES_PRESENTES_SQL = (
    "SELECT COUNT(*) AS nb FROM trppu_cles_repartition WHERE id_referentiel = %s"
)
# Erreurs MySQL imputables à UNE ligne : doublon (1062), valeur hors bornes (1264), date
# invalide (1292), valeur incorrecte (1366), texte trop long (1406). Toute autre erreur
# (connexion, verrou, droits) arrête le chargement, même avec --skip-errors.
CODES_ERREUR_LIGNE = frozenset({1062, 1264, 1292, 1366, 1406})

# Couverte par `idx_cr_ref_actif` (id_referentiel, date_fin_validite, …) : un parcours
# d'index, sans lire les 22 M de lignes de la table. Ni `COUNT(DISTINCT id_pdi)` — il
# exigeait une table temporaire de 22 M de valeurs, alors que `uk_pdi_ref`, qui vient d'être
# recréé, garantit déjà l'unicité — ni `MIN(date_debut_validite)`, calculé à la lecture.
CONTROLE_FINAL_SQL = """
SELECT COUNT(*)                             AS nb_lignes,
       SUM(date_fin_validite IS NULL)       AS nb_actives
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
        nombre = Decimal(valeur)
    except InvalidOperation as erreur:
        raise TraitementImpossible(
            f"Ligne {numero} : '{colonne}' = '{valeur}' n'est pas un décimal."
        ) from erreur
    # `Decimal` accepte « NaN » et « Infinity », que MySQL refuserait à l'insertion.
    if not nombre.is_finite():
        raise TraitementImpossible(
            f"Ligne {numero} : '{colonne}' = '{valeur}' n'est pas un décimal."
        )
    return nombre


def _date(ligne: dict[str, str], colonne: str, numero: int) -> date:
    valeur = _obligatoire(ligne, colonne, numero)
    # Chemin rapide pour la forme canonique AAAA-MM-JJ : `fromisoformat` est en C, dix fois
    # plus rapide que `strptime` — sur 22 M de lignes × 2 dates, cela se compte en minutes.
    # Les autres formes passent par `strptime`, qui garde exactement l'ancien comportement.
    if len(valeur) == 10 and valeur[4] == "-" and valeur[7] == "-":
        try:
            return date.fromisoformat(valeur)
        except ValueError:
            pass
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
# Lignes écartées (--skip-errors)
# ---------------------------------------------------------------------------


@dataclass
class _Rejets:
    """Lignes écartées par `--skip-errors` : toutes comptées, les premières détaillées."""

    id_referentiel: int
    total: int = 0
    details: list[str] = field(default_factory=list)
    # La conversion tourne dans un thread pendant que l'insertion signale ses propres rejets.
    _verrou: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def ajouter(self, numero: int | None, motif: str) -> None:
        """`numero` : ligne du fichier, None pour un doublon trouvé en base (sans ligne)."""
        with self._verrou:
            self.total += 1
            if len(self.details) >= CHARGEMENT_MAX_REJETS_DETAILLES:
                return
            self.details.append(motif)
        logger.warning(
            "Rejet ligne clés de répartition %s",
            ctx(id_referentiel=self.id_referentiel, ligne=numero, motif=motif),
        )

    def avertissements(self) -> list[str]:
        restants = self.total - len(self.details)
        if restants <= 0:
            return list(self.details)
        return [
            *self.details,
            f"… et {restants} autre(s) ligne(s) ignorée(s), non détaillée(s) "
            f"(CHARGEMENT_MAX_REJETS_DETAILLES = {CHARGEMENT_MAX_REJETS_DETAILLES}).",
        ]


def _code_mysql(erreur: Exception) -> int | None:
    """Code d'erreur MySQL porté par une exception pymysql/aiomysql, sinon None."""
    args = getattr(erreur, "args", ())
    return args[0] if args and isinstance(args[0], int) else None


def _motif_insertion(erreur: Exception, numero: int) -> str:
    if _code_mysql(erreur) == 1062:
        return (
            f"Ligne {numero} : doublon de PDI — le couple (PDI, référentiel) figure déjà "
            "plus haut dans le fichier."
        )
    return f"Ligne {numero} : valeur refusée par MySQL — {erreur}"


# ---------------------------------------------------------------------------
# Traitement
# ---------------------------------------------------------------------------


async def charger_cles_repartition(
    id_referentiel: int,
    fichier: str | None = None,
    *,
    chemin_local: str | None = None,
    ignorer_erreurs: bool = False,
) -> Rapport:
    """Charge `trppu_cles_repartition` depuis le CSV du bucket S3, ou d'un fichier local.

    `fichier` surcharge `CSV_CLES_REPARTITION` pour un rechargement ponctuel depuis S3.
    `chemin_local`, s'il est renseigné, désigne un fichier du disque : S3 n'est alors pas
    sollicité et `fichier` est ignoré. `ignorer_erreurs` écarte les lignes non conformes
    au lieu d'échouer (cf. docstring du module).
    """
    debut = time.perf_counter()
    nom_fichier = chemin_local or fichier or CSV_CLES_REPARTITION
    rapport = Rapport(
        titre=TITRE,
        id_traitement=id_referentiel,
        libelle_identifiant="Référentiel",
    )

    logger.info(
        "Début chargement clés de répartition %s",
        ctx(
            id_referentiel=id_referentiel,
            fichier=nom_fichier,
            source="local" if chemin_local else "s3",
            bucket=None if chemin_local else S3_BUCKET,
            skip_errors=ignorer_erreurs or None,
        ),
    )
    rejets = _Rejets(id_referentiel) if ignorer_erreurs else None

    if not nom_fichier:
        motif = "Aucun fichier : renseigner CSV_CLES_REPARTITION ou passer --fichier."
        logger.warning("Rejet chargement clés de répartition %s", ctx(motif=motif))
        rapport.ko(motif)
        rapport.statut = ECHEC
        return rapport

    if chemin_local:
        cle = chemin_local
        localiser = fichier_local.verifier_presence
        ouvrir = fichier_local.ouvrir
        libelle_source = "en local"
    else:
        cle = s3.chemin_objet(nom_fichier)
        localiser = s3.verifier_presence
        ouvrir = s3.ouvrir_objet
        libelle_source = "sur S3"
    lignes_inserees = 0
    lots = 0
    # Vrai tant que les index secondaires sont retirés : un échec doit alors remettre la
    # table dans un état propre (vide, index en place) plutôt que la laisser sans index.
    index_a_restaurer = False
    # Vrai dès que le fichier est entièrement en base : un échec ensuite ne vide plus la
    # table, 45 minutes de chargement valent mieux qu'une remise à zéro.
    donnees_chargees = False

    try:
        # --- Garde-fous, avant toute écriture -----------------------------
        #
        # Pas de contrôle sur `trppu_referentiel` : la table est vouée à disparaître. Le seul
        # garde-fou sur le référentiel est la concordance ligne à ligne avec le fichier (RG1).
        presentes = await db_write.fetch_one(LIGNES_PRESENTES_SQL, (id_referentiel,))
        nb_presentes = int(presentes["nb"]) if presentes else 0

        # Avant la localisation du fichier et toute écriture : la purge est un TRUNCATE,
        # qui ne se rattrape pas et ne doit jamais emporter un autre référentiel. Lu sur
        # l'instance d'écriture, comme le comptage ci-dessus : une réplique en retard ne doit
        # pas décider d'un TRUNCATE.
        autre = await db_write.fetch_one(AUTRE_REFERENTIEL_SQL, (id_referentiel,))
        if autre:
            raise TraitementImpossible(
                f"Chargement refusé : la table porte aussi le référentiel "
                f"{autre['id_referentiel']}, que la purge (TRUNCATE) effacerait."
            )
        rapport.ok(f"Aucun autre référentiel que {id_referentiel} dans la table")

        # Localiser le fichier avant de purger : découvrir son absence après avoir
        # supprimé 22 M de lignes coûterait un rechargement complet.
        taille = await asyncio.to_thread(localiser, cle)
        rapport.ok(f"Fichier '{cle}' présent {libelle_source} ({taille} octets)")

        # --- RG6 : purge -------------------------------------------------
        await index_chargement.vider_table()
        # TRUNCATE ne rend pas de nombre de lignes : c'est le comptage fait juste avant, que
        # le garde-fou garantit être le contenu entier de la table.
        supprimees = nb_presentes
        logger.info(
            "Purge du référentiel effectuée %s",
            ctx(id_referentiel=id_referentiel, lignes=supprimees),
        )
        rapport.ok(f"Purge (TRUNCATE) : {supprimees} ligne(s) supprimée(s)")

        # --- Index : conservés (défaut) ou retirés le temps du chargement ------
        if CHARGEMENT_RETIRER_INDEX:
            index_a_restaurer = await index_chargement.retirer_index()
            if index_a_restaurer:
                rapport.ok("Index secondaires retirés pendant le chargement")
            else:
                rapport.avertissements.append(
                    "Index secondaires conservés pendant le chargement (droit ALTER "
                    "manquant) : chargement nettement plus lent."
                )
        else:
            # Table vide : recréer un index absent (chargement précédent interrompu) est
            # instantané, et garantit que l'unicité est contrôlée dès la première ligne.
            recrees = await index_chargement.completer_index()
            rapport.ok(
                "Index conservés pendant le chargement (insertion par lots, sans "
                "reconstruction finale)"
                + (f" — {len(recrees)} opération(s) de remise en place" if recrees else "")
            )

        # --- Lecture en streaming et insertion par lots ---------------------
        lignes_inserees, lots, date_debut_min = await _charger(
            ouvrir, cle, id_referentiel, debut, rejets
        )
        donnees_chargees = True

        # --- Index et doublons (si les index avaient été retirés) ------------
        lignes_inserees -= await _finaliser(
            rapport,
            id_referentiel,
            rejets=rejets,
            index_retires=index_a_restaurer,
        )
        index_a_restaurer = False

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
        rapport.etats["LIGNES_CHARGEES"] = await _apres_echec(
            rapport,
            id_referentiel,
            index_a_restaurer,
            lignes_inserees,
            donnees_chargees=donnees_chargees,
            ignorer_erreurs=ignorer_erreurs,
        )
        _reporter_rejets(rapport, rejets)
        return rapport
    except Exception as erreur:  # noqa: BLE001 - la CLI ne doit jamais rendre de stacktrace
        logger.exception(
            "Erreur chargement clés de répartition %s",
            ctx(id_referentiel=id_referentiel, fichier=nom_fichier, lignes=lignes_inserees),
        )
        rapport.erreur = _message_erreur(erreur)
        rapport.statut = ECHEC
        rapport.etats["LIGNES_CHARGEES"] = await _apres_echec(
            rapport,
            id_referentiel,
            index_a_restaurer,
            lignes_inserees,
            donnees_chargees=donnees_chargees,
            ignorer_erreurs=ignorer_erreurs,
        )
        _reporter_rejets(rapport, rejets)
        return rapport

    rapport.ok(f"{lignes_inserees} ligne(s) insérée(s) en {lots} lot(s)")
    if rejets is not None:
        # Écarter des lignes est toléré, n'en garder aucune ne l'est pas : un fichier dont
        # toutes les lignes sont fausses n'est pas un référentiel.
        rapport.ajouter(
            lignes_inserees > 0 or rejets.total == 0,
            f"{rejets.total} ligne(s) non conforme(s) ignorée(s) (--skip-errors)",
            f"Aucune ligne conforme : les {rejets.total} ligne(s) du fichier ont été "
            "écartées.",
        )
    await _controles_finaux(rapport, id_referentiel, lignes_inserees, date_debut_min)

    rapport.etats["LIGNES_CHARGEES"] = lignes_inserees
    rapport.etats["LIGNES_PRECEDENTES"] = nb_presentes
    _reporter_rejets(rapport, rejets)
    rapport.statut = SUCCES if rapport.reussi else ECHEC

    duration_ms = round((time.perf_counter() - debut) * 1000, 1)
    logger.info(
        "Fin chargement clés de répartition %s",
        ctx(
            id_referentiel=id_referentiel,
            fichier=nom_fichier,
            lignes=lignes_inserees,
            lots=lots,
            lignes_ignorees=rejets.total if rejets else None,
            verdict=rapport.statut,
            duration_ms=duration_ms,
        ),
    )
    return rapport


def commande_finalisation(id_referentiel: int, ignorer_erreurs: bool) -> str:
    """Commande de reprise, avec les mêmes options que le chargement interrompu."""
    options = " --skip-errors" * ignorer_erreurs
    return f"python -m app.main finaliser-chargement {id_referentiel}{options}"


async def _apres_echec(
    rapport: Rapport,
    id_referentiel: int,
    index_a_restaurer: bool,
    lignes: int,
    *,
    donnees_chargees: bool,
    ignorer_erreurs: bool,
) -> int:
    """Suite à donner à un échec. Retourne les lignes restant en base.

    Fichier entièrement chargé : les lignes sont conservées et la reprise se fait par
    `finaliser-chargement`, sans relire le fichier. Chargement interrompu en cours de route,
    index retirés : la table est vidée et ses index recréés (instantané), un chargement
    partiel n'ayant aucune valeur.
    """
    if donnees_chargees:
        rapport.avertissements.append(
            f"Fichier entièrement chargé ({lignes} ligne(s)) : les données sont CONSERVÉES. "
            "Une fois la cause corrigée, reprendre sans recharger : "
            + commande_finalisation(id_referentiel, ignorer_erreurs)
        )
        rapport.etats["REPRENDRE_PAR"] = "finaliser-chargement"
        return lignes
    if not index_a_restaurer:
        return lignes
    if await index_chargement.remettre_table_vide():
        rapport.avertissements.append(
            "Chargement interrompu : la table a été vidée et ses index recréés. Relancer le "
            "chargement une fois la cause corrigée."
        )
        return 0
    rapport.avertissements.append(
        "Chargement interrompu ET remise en état impossible : la table peut être sans ses "
        "index secondaires. Relancer le chargement, qui les recrée."
    )
    return lignes


async def _finaliser(
    rapport: Rapport,
    id_referentiel: int,
    *,
    rejets: _Rejets | None,
    index_retires: bool,
) -> int:
    """Tout ce qui suit le chargement des lignes. Rend le nombre de lignes retirées.

    Commun au chargement et à sa reprise (`finaliser_chargement`) : index reconstruits et
    doublons traités si les index avaient été retirés.
    Chaque étape est reprenable — elle ne refait pas ce qui est déjà fait.
    """
    retirees = 0
    if index_retires:
        doublons = await index_chargement.reconstruire_index(
            ecarter=rejets is not None,
            signaler=(lambda motif: rejets.ajouter(None, motif)) if rejets else None,
        )
        if doublons.pdi and rejets is None:
            rapport.avertissements.extend(doublons.details)
            raise TraitementImpossible(
                f"{doublons.pdi} PDI en doublon dans le fichier "
                f"({doublons.lignes_en_trop} ligne(s) en trop, dont {doublons.conflits} "
                "aux données différentes). Dédoublonner le fichier "
                "(scripts/nettoyer_csv_cles.py), ou finaliser avec --skip-errors pour ne "
                "garder que la première occurrence."
            )
        retirees += doublons.lignes_en_trop
        rapport.ok("Index secondaires reconstruits")
        if doublons.pdi:
            rapport.ok(
                f"{doublons.lignes_en_trop} doublon(s) de PDI écarté(s) — "
                f"{doublons.identiques} identique(s), {doublons.conflits} en conflit"
            )
    return retirees


async def finaliser_chargement(
    id_referentiel: int,
    *,
    ignorer_erreurs: bool = False,
) -> Rapport:
    """Reprend un chargement dont les lignes sont en base mais la suite a échoué.

    Reconstruit les index qui manquent, traite les doublons, puis joue les contrôles
    finaux — sans relire le fichier. Sans effet nuisible sur
    un chargement déjà complet : chaque étape ne fait que ce qui manque.
    """
    debut = time.perf_counter()
    rapport = Rapport(
        titre=TITRE_FINALISATION,
        id_traitement=id_referentiel,
        libelle_identifiant="Référentiel",
    )
    rejets = _Rejets(id_referentiel) if ignorer_erreurs else None
    logger.info(
        "Début finalisation chargement %s",
        ctx(
            id_referentiel=id_referentiel,
            skip_errors=ignorer_erreurs or None,
        ),
    )
    try:
        if not await db_write.fetch_one(REFERENTIEL_CHARGE_SQL, (id_referentiel,)):
            raise TraitementImpossible(
                f"Aucune ligne du référentiel {id_referentiel} en base : il n'y a rien à "
                "finaliser, relancer le chargement."
            )
        rapport.ok(f"Lignes du référentiel {id_referentiel} présentes en base")
        await _finaliser(
            rapport,
            id_referentiel,
            rejets=rejets,
            index_retires=True,
        )
    except TraitementImpossible as erreur:
        logger.warning(
            "Rejet finalisation chargement %s",
            ctx(id_referentiel=id_referentiel, motif=str(erreur)),
        )
        rapport.ko(str(erreur))
        rapport.statut = ECHEC
        _reporter_rejets(rapport, rejets)
        return rapport
    except Exception as erreur:  # noqa: BLE001 - la CLI ne doit jamais rendre de stacktrace
        logger.exception(
            "Erreur finalisation chargement %s", ctx(id_referentiel=id_referentiel)
        )
        rapport.erreur = _message_erreur(erreur)
        rapport.statut = ECHEC
        rapport.avertissements.append(
            "Les données restent en base. Relancer, une fois la cause corrigée : "
            + commande_finalisation(id_referentiel, ignorer_erreurs)
        )
        _reporter_rejets(rapport, rejets)
        return rapport

    await _controles_finaux(rapport, id_referentiel, None, None)
    _reporter_rejets(rapport, rejets)
    rapport.statut = SUCCES if rapport.reussi else ECHEC
    logger.info(
        "Fin finalisation chargement %s",
        ctx(
            id_referentiel=id_referentiel,
            verdict=rapport.statut,
            duration_ms=round((time.perf_counter() - debut) * 1000, 1),
        ),
    )
    return rapport


def _reporter_rejets(rapport: Rapport, rejets: _Rejets | None) -> None:
    """Porte les lignes écartées dans le rapport — y compris sur un chargement interrompu."""
    if rejets is None:
        return
    rapport.etats["LIGNES_IGNOREES"] = rejets.total
    rapport.avertissements.extend(rejets.avertissements())


async def _charger(
    ouvrir, cle: str, id_referentiel: int, debut: float, rejets: _Rejets | None = None
) -> tuple[int, int]:
    """Lit le CSV en streaming et insère par lots. Retourne (lignes, lots).

    `ouvrir` est `s3.ouvrir_objet` ou `fichier_local.ouvrir` : même signature, un
    gestionnaire de contexte qui rend un flux texte. `rejets`, s'il est fourni, reçoit les
    lignes non conformes au lieu de faire échouer le chargement.
    """
    lignes_inserees = 0
    lots = 0
    date_debut_min: date | None = None
    prochain_jalon = CHARGEMENT_LOG_TOUTES_LES

    with ouvrir(cle, encodage=CSV_ENCODAGE) as flux:
        lecteur = csv.DictReader(flux, delimiter=CSV_DELIMITEUR)
        verifier_entete(lecteur.fieldnames)

        def preparer():
            return asyncio.ensure_future(
                asyncio.to_thread(_preparer_lot, lecteur, id_referentiel, rejets)
            )

        # Le lot suivant est lu et converti (thread) pendant que MySQL insère le courant :
        # CPU et base travaillent en même temps. Deux lots au plus en mémoire.
        suivant = preparer()
        try:
            while True:
                lot = await suivant
                if lot is None:
                    break
                suivant = preparer()
                valeurs, numeros, premiere, min_du_lot = lot
                if not valeurs:
                    continue
                if date_debut_min is None or min_du_lot < date_debut_min:
                    date_debut_min = min_du_lot
                lignes_inserees += await _inserer_lot(valeurs, numeros, premiere, rejets)
                lots += 1

                if lignes_inserees >= prochain_jalon:
                    _journaliser_avancement(id_referentiel, lignes_inserees, lots, debut)
                    prochain_jalon += CHARGEMENT_LOG_TOUTES_LES
        finally:
            # Un thread ne s'annule pas : on attend la préparation en cours avant de fermer
            # le flux qu'elle lit, et on absorbe son éventuelle erreur (déjà remontée, ou
            # sans objet puisqu'on sort).
            await asyncio.gather(suivant, return_exceptions=True)

    return lignes_inserees, lots, date_debut_min


def _preparer_lot(
    lecteur: Iterator[dict[str, str]], id_referentiel: int, rejets: _Rejets | None
) -> tuple[list[tuple[Any, ...]], list[int], int, date | None] | None:
    """Lit et convertit un lot (appelé dans un thread). None en fin de fichier.

    Rend aussi la plus petite `date_debut_validite` du lot : la calculer ici, sur des valeurs
    déjà en mémoire, évite une relecture complète de la table en fin de chargement.
    """
    brutes = _lire_lot(lecteur, CHARGEMENT_TAILLE_LOT)
    if not brutes:
        return None
    # `lecteur.line_num` porte le numéro de la dernière ligne lue, en-tête compris : on
    # remonte au premier enregistrement du lot pour numéroter chaque ligne.
    premiere = lecteur.line_num - len(brutes) + 1
    valeurs: list[tuple[Any, ...]] = []
    numeros: list[int] = []
    for decalage, ligne in enumerate(brutes):
        numero = premiere + decalage
        try:
            valeurs.append(convertir(ligne, numero, id_referentiel))
        except TraitementImpossible as erreur:
            if rejets is None:
                raise
            rejets.ajouter(numero, str(erreur))
            continue
        numeros.append(numero)
    min_du_lot = min((v[16] for v in valeurs), default=None)  # date_debut_validite
    return valeurs, numeros, premiere, min_du_lot


async def _inserer_lot(
    valeurs: list[tuple[Any, ...]], numeros: list[int], premiere: int, rejets: _Rejets | None
) -> int:
    """Insère un lot en une requête multi-lignes, commitée seule. Retourne les insérées."""
    try:
        async with db_write.transaction() as tx:
            await tx.execute_many(INSERT_SQL, valeurs)
        return len(valeurs)
    except Exception as erreur:  # noqa: BLE001 - retraduit puis relancé
        if rejets is None or _code_mysql(erreur) not in CODES_ERREUR_LIGNE:
            raise _traduire_erreur_insertion(erreur, premiere) from erreur
        # Le lot a été annulé en bloc : on le rejoue ligne à ligne pour n'écarter que la ou
        # les lignes fautives. Sans index unique (cas nominal), un doublon ne fait plus
        # échouer de lot : ce chemin ne sert qu'aux valeurs refusées par MySQL, ou quand
        # les index n'ont pas pu être retirés.
        return await _inserer_ligne_a_ligne(valeurs, numeros, rejets)


async def _inserer_ligne_a_ligne(
    valeurs: list[tuple[Any, ...]], numeros: list[int], rejets: _Rejets
) -> int:
    """Rejoue un lot refusé ligne à ligne, dans une seule transaction. Retourne les insérées.

    InnoDB n'annule que l'instruction fautive, pas la transaction : les lignes valides du
    lot sont commitées ensemble à la fin. Ce chemin n'est pris que pour un lot en erreur,
    le coût du ligne à ligne reste marginal.
    """
    inserees = 0
    async with db_write.transaction() as tx:
        for valeur, numero in zip(valeurs, numeros):
            try:
                await tx.execute(INSERT_SQL, valeur)
            except Exception as erreur:  # noqa: BLE001 - trié ci-dessous
                if _code_mysql(erreur) not in CODES_ERREUR_LIGNE:
                    raise
                rejets.ajouter(numero, _motif_insertion(erreur, numero))
                continue
            inserees += 1
    return inserees


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
    rapport: Rapport,
    id_referentiel: int,
    lignes_inserees: int | None,
    date_debut_min: date | None,
) -> None:
    """Relit la table pour confirmer ce qui a été écrit.

    Sur l'instance d'**écriture** : une réplique aurait du retard sur 22 M d'insertions et
    deux ALTER — elle attendrait d'avoir rejoué l'ALTER, ou compterait une table incomplète
    et conclurait à tort à une volumétrie incohérente.

    Les doublons `(id_pdi, id_referentiel)` ne sont pas recomptés : `uk_pdi_ref` est en place
    (recréé en fin de chargement, ou conservé), il les rend impossibles.
    """
    debut = time.perf_counter()
    logger.info("Début contrôles finaux chargement %s", ctx(id_referentiel=id_referentiel))
    controle = await db_write.fetch_one(CONTROLE_FINAL_SQL, (id_referentiel,))
    logger.info(
        "Fin contrôles finaux chargement %s",
        ctx(
            id_referentiel=id_referentiel,
            duration_ms=round((time.perf_counter() - debut) * 1000, 1),
        ),
    )
    if not controle:
        rapport.ko("Contrôle final impossible : aucune ligne relue.")
        return

    nb_lignes = int(controle["nb_lignes"] or 0)
    nb_actives = int(controle["nb_actives"] or 0)

    if lignes_inserees is None:
        # Reprise : le nombre de lignes lues n'est plus connu, la base fait foi.
        rapport.ok(f"Volumétrie en base : {nb_lignes} ligne(s)")
        rapport.etats["LIGNES_CHARGEES"] = nb_lignes
    else:
        rapport.ajouter(
            nb_lignes == lignes_inserees,
            f"Volumétrie en base : {nb_lignes} ligne(s)",
            f"Volumétrie incohérente : {nb_lignes} ligne(s) en base pour "
            f"{lignes_inserees} insérée(s).",
        )
    rapport.ok("Unicité (PDI, référentiel) garantie par l'index uk_pdi_ref")
    rapport.ok(f"Lignes actives (date_fin_validite NULL) : {nb_actives}")
    # Conservé, et pas seulement affiché : c'est le nombre de clés que DSR-699 devra produire.
    # La commande `init` s'en sert pour vérifier son CA1 sans recompter 24 M de lignes.
    rapport.etats["LIGNES_ACTIVES"] = nb_actives
    if date_debut_min is not None:
        rapport.etats["DATE_DEBUT_VALIDITE_MIN"] = date_debut_min
