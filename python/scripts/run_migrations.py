"""Reconstruit la base TRPPU dans le bon ordre, en réutilisant la config .env de l'app.

Exécute, dans l'ordre :
  1. db_analyse/schema_trppu.sql            (CREATE DATABASE + 19 tables)
  2. db_migrations/001_widen_id_rh_columns.sql
  3. db_migrations/002_add_param_columns.sql
  4. db_migrations/003_widen_nb_jours.sql

Connexion : SGBD_SERVER_WRITE / SGBD_PORT / SGBD_APP_USER_WRITE / SGBD_APP_PWD_*
(lus depuis app.config, donc depuis ton .env). Aucun client `mysql` requis.

⚠️ DESTRUCTIF : l'étape 1 fait `DROP TABLE IF EXISTS` sur toutes les tables trppu.

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
    MYSQL_HOST_WRITE,
    MYSQL_PASSWORD_WRITE,
    MYSQL_PORT,
    MYSQL_USER_WRITE,
)

FILES = [
    ROOT / "db_analyse" / "schema_trppu.sql",
    ROOT / "db_migrations" / "001_widen_id_rh_columns.sql",
    ROOT / "db_migrations" / "002_add_param_columns.sql",
    ROOT / "db_migrations" / "003_widen_nb_jours.sql",
    ROOT / "db_migrations" / "004_add_variations_tracabilite.sql",
]


def split_statements(sql: str) -> list[str]:
    """Découpe un script SQL en instructions (split sur ';'), sans DELIMITER custom.

    - Ignore les lignes de commentaires en tête de chaque instruction.
    - Conserve les commentaires en ligne (gérés par MySQL).
    """
    statements: list[str] = []
    for chunk in sql.split(";"):
        lines = chunk.splitlines()
        while lines and (not lines[0].strip() or lines[0].strip().startswith("--")):
            lines.pop(0)
        stmt = "\n".join(lines).strip()
        if stmt:
            statements.append(stmt)
    return statements


async def run(dry_run: bool) -> None:
    print(
        f"Cible : {MYSQL_USER_WRITE}@{MYSQL_HOST_WRITE}:{MYSQL_PORT} (base trppu)\n"
    )
    plan = [(f, split_statements(f.read_text(encoding="utf-8"))) for f in FILES]

    if dry_run:
        for f, stmts in plan:
            print(f"[dry-run] {f.relative_to(ROOT)} : {len(stmts)} instructions")
        return

    # db=None : la base n'existe peut-être pas encore (étape 1 fait CREATE DATABASE + USE).
    conn = await aiomysql.connect(
        host=MYSQL_HOST_WRITE,
        port=MYSQL_PORT,
        user=MYSQL_USER_WRITE,
        password=MYSQL_PASSWORD_WRITE,
        autocommit=True,
    )
    try:
        async with conn.cursor() as cur:
            for f, stmts in plan:
                print(f"-> {f.relative_to(ROOT)} : {len(stmts)} instructions")
                for i, stmt in enumerate(stmts, 1):
                    try:
                        await cur.execute(stmt)
                    except Exception as e:
                        print(f"   ÉCHEC instruction #{i} ({f.name}) : {e}")
                        print("   SQL : " + stmt[:200].replace("\n", " "))
                        raise
        print("\nOK — base trppu reconstruite (schéma + migrations).")
        print("Rappel : définir ID_RH_CRYPTO_KEY dans .env pour les écritures id_rh.")
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(run("--dry-run" in sys.argv))
