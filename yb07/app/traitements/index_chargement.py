"""Purge et index de `trppu_cles_repartition` avant un chargement.

Les index secondaires restent en place pendant le chargement : un ALTER final de plusieurs
minutes, muet côté réseau, était coupé par le délai d'inactivité (300 s). Le DDL passe par
une connexion dédiée à `lock_wait_timeout` borné : en attente de verrou de métadonnées, il
bloquerait toutes les autres sessions (l'API).
"""

from __future__ import annotations

import logging

from app.config import CHARGEMENT_LOCK_WAIT_TIMEOUT
from app.db.mysql import db_write
from app.db.sql_script import SqlScriptError
from app.erreurs import TraitementImpossible
from app.log_utils import ctx

logger = logging.getLogger(__name__)

TABLE = "trppu_cles_repartition"

#: Index secondaires attendus (`yb05/db/database.sql`, `db/DSR-696-699_migration.sql`).
INDEX_SECONDAIRES = {
    "uk_pdi_ref": "UNIQUE KEY `uk_pdi_ref` (`id_pdi`, `id_referentiel`)",
    "idx_cr_ref_actif": (
        "KEY `idx_cr_ref_actif` (`id_referentiel`, `date_fin_validite`, `co_regate_site`)"
    ),
}
#: Index temporaire obsolète : retiré s'il est encore là.
INDEX_DOUBLONS = "idx_cr_pdi_doublons"

INDEX_PRESENTS_SQL = """
SELECT DISTINCT INDEX_NAME AS nom
  FROM information_schema.STATISTICS
 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'trppu_cles_repartition'
"""


class DroitManquant(TraitementImpossible):
    """Le compte d'écriture n'a pas le droit exigé par l'instruction (erreur 1142)."""


def _code_mysql(erreur: BaseException) -> int | None:
    args = getattr(erreur, "args", ())
    return args[0] if args and isinstance(args[0], int) else None


async def executer_ddl(
    *instructions: str, etape: str, prefixe: str = "chargement", table: str = TABLE, db=None
) -> None:
    """Joue du DDL avec une attente de verrou bornée, sur une connexion dédiée.

    Le `SET SESSION` disparaît avec la connexion et ne contamine pas le pool.
    """
    db = db or db_write
    script = f"SET SESSION lock_wait_timeout = {CHARGEMENT_LOCK_WAIT_TIMEOUT};\n"
    script += "\n".join(f"{instruction};" for instruction in instructions)
    try:
        await db.execute_sql_script(script, label=f"{prefixe}/{etape}", transactional=False)
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
                f"Table {table} utilisée par une autre session : verrou non obtenu en "
                f"{CHARGEMENT_LOCK_WAIT_TIMEOUT} s (CHARGEMENT_LOCK_WAIT_TIMEOUT). Relancer "
                "quand l'API et les autres traitements ne la lisent plus."
            ) from erreur
        raise


async def index_presents() -> set[str]:
    """Index de la table, lus sur l'écriture (un réplica peut être en retard sur le DDL)."""
    lignes = await db_write.fetch_all(INDEX_PRESENTS_SQL)
    return {ligne["nom"] for ligne in lignes}


async def vider_table() -> None:
    await executer_ddl(f"TRUNCATE TABLE {TABLE}", etape="purge")


async def completer_index(*, etape: str = "index-completion") -> list[str]:
    """Recrée les index absents et retire l'index temporaire ; rend les opérations jouées.

    Joué sur la table vide après le TRUNCATE (ALTER instantané) : le chargement ne tourne
    jamais sans `uk_pdi_ref`.
    """
    presents = await index_presents()
    operations = [f"DROP INDEX `{INDEX_DOUBLONS}`"] if INDEX_DOUBLONS in presents else []
    operations += [
        f"ADD {definition}"
        for nom, definition in INDEX_SECONDAIRES.items()
        if nom not in presents
    ]
    if operations:
        await executer_ddl(f"ALTER TABLE {TABLE} " + ", ".join(operations), etape=etape)
        logger.info(
            "Fin remise en place des index %s",
            ctx(table=TABLE, operations=", ".join(operations)),
        )
    return operations
