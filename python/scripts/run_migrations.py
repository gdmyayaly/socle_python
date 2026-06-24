"""Reconstruit la base TRPPU à partir de `db.sql` (source de vérité unique).

Rejoue `db.sql` (schéma complet : 20 tables, FK, index, contraintes) en
réutilisant la config .env de l'app.

Le nom de base figé dans `db.sql` (`dsr_mercure_aa`, fidèle au dump prod) est
ignoré : le script crée/utilise le nom lu depuis .env (`SGBD_DB_NAME`, `trppu`
en local) via `app.config.MYSQL_DATABASE`. Aucun client `mysql` requis.

Connexion : SGBD_SERVER_WRITE / SGBD_PORT / SGBD_APP_USER_WRITE / SGBD_APP_PWD_*
(lus depuis app.config, donc depuis ton .env).

⚠️ DESTRUCTIF : `db.sql` fait `DROP TABLE IF EXISTS` sur toutes les tables.

⚠️ FK : db.sql impose des clés étrangères (co_produit -> trppu_produit ;
id_pic_version -> trppu_pic_version). Après reconstruction, la base est VIDE :
les écritures tmh / pic_coefficients / variations échoueront (erreur 1452) tant
que `trppu_produit` (et au moins une `trppu_pic_version`, id=1, est_par_defaut=1)
ne sont pas alimentés. Seeder ces référentiels avant d'utiliser les routes.

Usage :
    python scripts/run_migrations.py            # exécute
    python scripts/run_migrations.py --dry-run  # liste sans exécuter
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import aiomysql  # noqa: E402

from app.config import (  # noqa: E402
    MYSQL_DATABASE,
    MYSQL_HOST_WRITE,
    MYSQL_PASSWORD_WRITE,
    MYSQL_PORT,
    MYSQL_USER_WRITE,
)

SCHEMA_FILE = ROOT / "db.sql"


def split_statements(sql: str) -> list[str]:
    """Découpe un script SQL en instructions (split sur ';'), sans DELIMITER custom.

    - Ignore les lignes de commentaires en tête de chaque instruction.
    - Retire les instructions `CREATE DATABASE` / `USE` (le nom de base est
      surchargé depuis la config .env, cf. run()).
    - Conserve les commentaires en ligne (gérés par MySQL).
    """
    statements: list[str] = []
    for chunk in sql.split(";"):
        lines = chunk.splitlines()
        while lines and (not lines[0].strip() or lines[0].strip().startswith("--")):
            lines.pop(0)
        stmt = "\n".join(lines).strip()
        if not stmt:
            continue
        head = stmt.upper()
        if head.startswith("CREATE DATABASE") or head.startswith("USE "):
            continue
        statements.append(stmt)
    return statements


async def run(dry_run: bool) -> None:
    print(
        f"Cible : {MYSQL_USER_WRITE}@{MYSQL_HOST_WRITE}:{MYSQL_PORT} "
        f"(base {MYSQL_DATABASE})\n"
    )

    # On préfixe par CREATE DATABASE / USE basés sur le nom configuré (.env),
    # puis les instructions de db.sql (CREATE DATABASE/USE d'origine retirés).
    bootstrap = [
        f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` "
        "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci",
        f"USE `{MYSQL_DATABASE}`",
    ]
    statements = bootstrap + split_statements(SCHEMA_FILE.read_text(encoding="utf-8"))

    if dry_run:
        print(
            f"[dry-run] {SCHEMA_FILE.relative_to(ROOT)} : "
            f"{len(statements)} instructions (dont {len(bootstrap)} bootstrap base)"
        )
        return

    # db=None : la base n'existe peut-être pas encore (bootstrap fait CREATE + USE).
    conn = await aiomysql.connect(
        host=MYSQL_HOST_WRITE,
        port=MYSQL_PORT,
        user=MYSQL_USER_WRITE,
        password=MYSQL_PASSWORD_WRITE,
        autocommit=True,
    )
    try:
        async with conn.cursor() as cur:
            print(f"-> {SCHEMA_FILE.relative_to(ROOT)} : {len(statements)} instructions")
            for i, stmt in enumerate(statements, 1):
                try:
                    await cur.execute(stmt)
                except Exception as e:
                    print(f"   ÉCHEC instruction #{i} : {e}")
                    print("   SQL : " + stmt[:200].replace("\n", " "))
                    raise
        print(f"\nOK — base {MYSQL_DATABASE} reconstruite depuis db.sql.")
        print("Rappel : définir ID_RH_CRYPTO_KEY dans .env pour les écritures id_rh.")
        print(
            "Rappel : base VIDE — seeder trppu_produit et trppu_pic_version "
            "(id=1, est_par_defaut=1) avant d'utiliser les routes (FK)."
        )
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(run("--dry-run" in sys.argv))
