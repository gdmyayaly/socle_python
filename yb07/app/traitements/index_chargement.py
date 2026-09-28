"""Index secondaires de `trppu_cles_repartition` pendant un chargement de masse.

Pourquoi les retirer. Insérer 22 M de lignes dans une table indexée oblige MySQL à maintenir
chaque index ligne à ligne. Pour l'index **unique** `uk_pdi_ref`, c'est ruineux : l'unicité
se vérifie immédiatement, sans tampon de modifications, et les PDI arrivent dans le désordre.
Dès que l'index dépasse le cache InnoDB (`innodb_buffer_pool_size`, 128 Mo par défaut),
chaque insertion lit une page sur disque — le débit tombe vers 1 000 lignes/s et continue de
baisser à mesure que l'index grossit.

Charger sans index secondaires puis les reconstruire en une passe triée (« sorted index
build » d'InnoDB) coûte quelques minutes au lieu de plusieurs heures. C'est possible parce
que la table est vidée par TRUNCATE et ne porte qu'un référentiel : pendant le chargement,
elle n'a de toute façon rien d'utilisable à servir.

Les doublons, que `uk_pdi_ref` rejetait à l'insertion, sont traités **avant** de le recréer :
un index temporaire sur `id_pdi` permet de les trouver par un parcours ordonné, page par page
— la mémoire du batch reste bornée quel que soit leur nombre.

Tout le DDL passe par une connexion dédiée avec `lock_wait_timeout` borné : un ALTER ou un
TRUNCATE qui attend un verrou bloque derrière lui toutes les requêtes des autres sessions
(l'API), il vaut mieux qu'il échoue vite.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from app.config import CHARGEMENT_LOCK_WAIT_TIMEOUT, CHARGEMENT_MAX_REJETS_DETAILLES
from app.db.mysql import db_write
from app.db.sql_script import SqlScriptError
from app.erreurs import TraitementImpossible
from app.log_utils import ctx

logger = logging.getLogger(__name__)

TABLE = "trppu_cles_repartition"

#: Index secondaires retirés pendant le chargement, recréés à l'identique à la fin.
#: Source : `yb05/db/database.sql` et `db/DSR-696-699_migration.sql`.
INDEX_SECONDAIRES = {
    "uk_pdi_ref": "UNIQUE KEY `uk_pdi_ref` (`id_pdi`, `id_referentiel`)",
    "idx_cr_ref_actif": (
        "KEY `idx_cr_ref_actif` (`id_referentiel`, `date_fin_validite`, `co_regate_site`)"
    ),
}
#: Index temporaire de recherche des doublons, supprimé avant la fin.
INDEX_DOUBLONS = "idx_cr_pdi_doublons"

#: Groupes de doublons traités par page : borne la mémoire du batch.
TAILLE_PAGE_DOUBLONS = 1000

INDEX_PRESENTS_SQL = """
SELECT DISTINCT INDEX_NAME AS nom
  FROM information_schema.STATISTICS
 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'trppu_cles_repartition'
"""
# Parcours ordonné de l'index temporaire : pas de table temporaire, et la page suivante
# reprend là où la précédente s'est arrêtée.
PREMIERE_PAGE_DOUBLONS_SQL = """
SELECT id_pdi, COUNT(*) AS nb_occurrences FROM trppu_cles_repartition
 GROUP BY id_pdi HAVING COUNT(*) > 1 ORDER BY id_pdi LIMIT %s
"""
PAGE_SUIVANTE_DOUBLONS_SQL = """
SELECT id_pdi, COUNT(*) AS nb_occurrences FROM trppu_cles_repartition
 WHERE id_pdi > %s GROUP BY id_pdi HAVING COUNT(*) > 1 ORDER BY id_pdi LIMIT %s
"""
COLONNES_DONNEES = (
    "id_pdi, pdi_rattache, trafic_colis, trafic_oo, trafic_3s, nature, co_regate_site, "
    "type_site, lb_regate, co_regate_etablissement, lb_etablissement, co_regate_dex, lb_dex, "
    "nb_pre, potentielip, id_referentiel, date_debut_validite, date_fin_validite"
)


class DroitManquant(TraitementImpossible):
    """Le compte d'écriture n'a pas le droit exigé par l'instruction (erreur 1142)."""


# ---------------------------------------------------------------------------
# DDL sur connexion dédiée
# ---------------------------------------------------------------------------


def _code_mysql(erreur: BaseException) -> int | None:
    args = getattr(erreur, "args", ())
    return args[0] if args and isinstance(args[0], int) else None


async def executer_ddl(*instructions: str, etape: str) -> None:
    """Joue du DDL sur une connexion dédiée, avec une attente de verrou bornée.

    Connexion dédiée (runner de scripts du socle) : le `SET SESSION` disparaît avec elle et
    ne contamine pas le pool.
    """
    script = f"SET SESSION lock_wait_timeout = {CHARGEMENT_LOCK_WAIT_TIMEOUT};\n"
    script += "\n".join(f"{instruction};" for instruction in instructions)
    try:
        await db_write.execute_sql_script(
            script, label=f"chargement/{etape}", transactional=False
        )
    except Exception as erreur:  # noqa: BLE001 - retraduit ou relancé
        origine = erreur.original if isinstance(erreur, SqlScriptError) else erreur
        code = _code_mysql(origine)
        if code in (1142, 1227):
            raise DroitManquant(
                f"MySQL refuse '{instructions[-1][:60]}' : droit manquant pour le compte "
                f"d'écriture (TRUNCATE exige DROP, les index ALTER et INDEX). {origine}"
            ) from erreur
        if code == 1205:
            raise TraitementImpossible(
                f"Table {TABLE} utilisée par une autre session : verrou non obtenu en "
                f"{CHARGEMENT_LOCK_WAIT_TIMEOUT} s (CHARGEMENT_LOCK_WAIT_TIMEOUT). Relancer "
                "quand l'API et les autres traitements ne la lisent plus."
            ) from erreur
        raise


async def index_presents() -> set[str]:
    """Index de la table, lus sur l'instance d'écriture — un réplica pourrait être en retard
    sur le DDL qui vient d'être joué."""
    lignes = await db_write.fetch_all(INDEX_PRESENTS_SQL)
    return {ligne["nom"] for ligne in lignes}


# ---------------------------------------------------------------------------
# Avant le chargement
# ---------------------------------------------------------------------------


async def vider_table() -> None:
    await executer_ddl(f"TRUNCATE TABLE {TABLE}", etape="purge")


async def retirer_index() -> bool:
    """Retire les index secondaires connus. Rend False s'ils doivent rester en place.

    Sans le droit ALTER, le chargement se fait quand même, index en place : plus lent, mais
    correct. Un index inconnu est laissé tel quel — il sera maintenu pendant le chargement.
    """
    presents = await index_presents()
    a_retirer = [nom for nom in (*INDEX_SECONDAIRES, INDEX_DOUBLONS) if nom in presents]
    inconnus = presents - {"PRIMARY", *INDEX_SECONDAIRES, INDEX_DOUBLONS}
    if inconnus:
        logger.warning(
            "Rejet retrait index inconnus %s",
            ctx(index=",".join(sorted(inconnus)), motif="maintenus pendant le chargement"),
        )
    if a_retirer:
        try:
            await executer_ddl(
                f"ALTER TABLE {TABLE} " + ", ".join(f"DROP INDEX `{n}`" for n in a_retirer),
                etape="index-retrait",
            )
        except DroitManquant as erreur:
            logger.warning("Rejet retrait index %s", ctx(motif=str(erreur)))
            return False
    logger.info("Fin retrait index %s", ctx(index=",".join(a_retirer) or None))
    return True


# ---------------------------------------------------------------------------
# Après le chargement
# ---------------------------------------------------------------------------


@dataclass
class Doublons:
    """Doublons de PDI trouvés au moment de recréer `uk_pdi_ref`."""

    pdi: int = 0
    lignes_en_trop: int = 0
    identiques: int = 0
    conflits: int = 0
    details: list[str] = field(default_factory=list)


async def reconstruire_index(
    *, ecarter: bool, signaler: Callable[[str], None] | None = None
) -> Doublons:
    """Recrée les index secondaires ; traite d'abord les doublons de PDI.

    `ecarter=True` (--skip-errors) : la première occurrence chargée (plus petit `id`, donc
    première du fichier) est conservée, les autres sont supprimées et signalées. Sinon les
    doublons sont seulement comptés, l'index unique n'est **pas** recréé, et c'est à
    l'appelant d'échouer (puis de remettre la table en état).
    """
    debut = time.perf_counter()
    logger.info("Début reconstruction index %s", ctx(table=TABLE, ecarter=ecarter))

    await executer_ddl(
        f"ALTER TABLE {TABLE} ADD {INDEX_SECONDAIRES['idx_cr_ref_actif']}, "
        f"ADD KEY `{INDEX_DOUBLONS}` (`id_pdi`)",
        etape="index-construction",
    )

    doublons = Doublons()
    dernier: int | None = None
    while True:
        if dernier is None:
            page = await db_write.fetch_all(PREMIERE_PAGE_DOUBLONS_SQL, (TAILLE_PAGE_DOUBLONS,))
        else:
            page = await db_write.fetch_all(
                PAGE_SUIVANTE_DOUBLONS_SQL, (dernier, TAILLE_PAGE_DOUBLONS)
            )
        if not page:
            break
        await _traiter_page(page, doublons, ecarter=ecarter, signaler=signaler)
        dernier = page[-1]["id_pdi"]
        if len(page) < TAILLE_PAGE_DOUBLONS:
            break

    if doublons.pdi and not ecarter:
        logger.warning(
            "Rejet reconstruction index %s",
            ctx(pdi_en_doublon=doublons.pdi, lignes_en_trop=doublons.lignes_en_trop),
        )
        return doublons

    await executer_ddl(
        f"ALTER TABLE {TABLE} ADD {INDEX_SECONDAIRES['uk_pdi_ref']}, "
        f"DROP INDEX `{INDEX_DOUBLONS}`",
        etape="index-unique",
    )
    logger.info(
        "Fin reconstruction index %s",
        ctx(
            table=TABLE,
            pdi_en_doublon=doublons.pdi or None,
            lignes_ecartees=doublons.lignes_en_trop or None,
            duration_ms=round((time.perf_counter() - debut) * 1000, 1),
        ),
    )
    return doublons


async def _traiter_page(
    page: list[dict[str, Any]],
    doublons: Doublons,
    *,
    ecarter: bool,
    signaler: Callable[[str], None] | None,
) -> None:
    pdis = [ligne["id_pdi"] for ligne in page]
    marqueurs = ", ".join(["%s"] * len(pdis))
    lignes = await db_write.fetch_all(
        f"SELECT id, {COLONNES_DONNEES} FROM {TABLE} WHERE id_pdi IN ({marqueurs}) "
        "ORDER BY id_pdi, id",
        tuple(pdis),
    )

    a_supprimer: list[int] = []
    conservee: dict[str, Any] | None = None
    for ligne in lignes:
        donnees = {k: v for k, v in ligne.items() if k != "id"}
        if conservee is None or conservee["id_pdi"] != ligne["id_pdi"]:
            conservee = donnees
            doublons.pdi += 1
            continue
        identique = donnees == conservee
        doublons.lignes_en_trop += 1
        if identique:
            doublons.identiques += 1
        else:
            doublons.conflits += 1
        a_supprimer.append(ligne["id"])
        nature = "identique" if identique else "en CONFLIT (données différentes)"
        suite = " — la première occurrence du fichier est conservée." if ecarter else "."
        motif = f"PDI {ligne['id_pdi']} : doublon {nature}{suite}"
        if signaler:
            signaler(motif)
        elif len(doublons.details) < CHARGEMENT_MAX_REJETS_DETAILLES:
            doublons.details.append(motif)

    if ecarter:
        for debut in range(0, len(a_supprimer), 1000):
            tranche = a_supprimer[debut : debut + 1000]
            await db_write.execute(
                f"DELETE FROM {TABLE} WHERE id IN ({', '.join(['%s'] * len(tranche))})",
                tuple(tranche),
            )


# ---------------------------------------------------------------------------
# Après un échec
# ---------------------------------------------------------------------------


async def remettre_table_vide() -> bool:
    """Table vidée et index canoniques en place — un état propre, obtenu instantanément.

    Joué quand un chargement échoue alors que les index étaient retirés : reconstruire les
    index sur des millions de lignes partielles prendrait des minutes pour un contenu de
    toute façon à recharger. Ne lève jamais : un échec ici est journalisé, et le chargement
    suivant retire puis recrée les index de lui-même.
    """
    try:
        await vider_table()
        presents = await index_presents()
        operations = [f"DROP INDEX `{INDEX_DOUBLONS}`"] if INDEX_DOUBLONS in presents else []
        operations += [
            f"ADD {definition}"
            for nom, definition in INDEX_SECONDAIRES.items()
            if nom not in presents
        ]
        if operations:
            await executer_ddl(
                f"ALTER TABLE {TABLE} " + ", ".join(operations), etape="remise-en-etat"
            )
    except Exception:  # noqa: BLE001 - dernier filet, l'échec d'origine prime
        logger.exception("Erreur remise en état table %s", ctx(table=TABLE))
        return False
    logger.info("Fin remise en état table %s", ctx(table=TABLE))
    return True
