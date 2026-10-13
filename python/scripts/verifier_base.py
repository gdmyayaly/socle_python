"""Vérification de la base MySQL avant une campagne de test, en ligne de commande.

À lancer après une purge ou un DROP/CREATE des tables du périmètre TRPPU. Contrôle :

1. la connexion aux deux instances (lecture et écriture), sur la base attendue ;
2. la présence de chaque table du périmètre, et ses colonnes comparées à
   `db/01_structure.sql` (une colonne manquante casse l'API en 500) ;
3. les clés étrangères déclarées dans `db/03_contraintes.sql` ;
4. les prérequis de l'API : un référentiel produits non vide (le TMH ne crée plus de
   produit), une version PIC candidate pour `POST /scenarios`, des sites.

Les contrôles de structure et de données passent par l'instance d'écriture : l'instance de
lecture est un secondaire de groupe qui peut être en retard juste après un DROP/CREATE.

Usage (depuis `python/`) :
    python scripts/verifier_base.py

Code retour : 0 = tout est OK (avertissements compris), 1 = au moins un KO.
"""

from __future__ import annotations

import asyncio
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.config import HEALTH_CHECK_QUERY, MYSQL_DATABASE  # noqa: E402
from app.db.mysql import db_read, db_write  # noqa: E402

STRUCTURE_SQL = ROOT / "db" / "01_structure.sql"
CONTRAINTES_SQL = ROOT / "db" / "03_contraintes.sql"

# Tables écrites par l'API (cf. db/purge_test.sql) : celles qu'une campagne de test recrée.
TABLES_PERIMETRE = (
    "trppu_scenario",
    "trppu_tmh",
    "trppu_neutralisations",
    "trppu_scenario_comptages_manuels",
    "trppu_scenario_exclusions",
    "trppu_scenario_pic_coeffs",
    "trppu_scenario_variations_prev",
    "trppu_trafic_agrebal",
    "trppu_trafic_pdi",
    "trppu_pic_version",
    "trppu_pic_coefficients",
    "trppu_api_log",
    "trppu_recalcul_log",
    "trppu_produit",
    "trppu_site",
)

_RE_TABLE = re.compile(r"CREATE TABLE IF NOT EXISTS `(\w+)` \((.*?)\n\)", re.S)
_RE_COLONNE = re.compile(r"^\s*`(\w+)`", re.M)
_RE_FK = re.compile(
    r"_trppu_add_constraint_if_missing`\('(\w+)',\s*'(\w+)',\s*'ADD CONSTRAINT `\w+` FOREIGN KEY"
)


def colonnes_attendues(sql: str) -> dict[str, set[str]]:
    """`{table: {colonnes}}` lu dans les CREATE TABLE de `01_structure.sql`."""
    return {t: set(_RE_COLONNE.findall(corps)) for t, corps in _RE_TABLE.findall(sql)}


def fk_attendues(sql: str) -> list[tuple[str, str]]:
    """`[(table, nom_fk)]` lu dans les ajouts de FOREIGN KEY de `03_contraintes.sql`."""
    return _RE_FK.findall(sql)


class Rapport:
    def __init__(self) -> None:
        self.nb_ko = 0
        self.nb_avert = 0

    def ok(self, msg: str) -> None:
        print(f"  [OK]    {msg}")

    def ko(self, msg: str) -> None:
        self.nb_ko += 1
        print(f"  [KO]    {msg}")

    def avert(self, msg: str) -> None:
        self.nb_avert += 1
        print(f"  [AVERT] {msg}")


async def verifier_connexions(r: Rapport) -> bool:
    print("1. Connexions MySQL")
    ecriture_ok = False
    for nom, db in (("lecture", db_read), ("écriture", db_write)):
        try:
            await db.fetch_one(HEALTH_CHECK_QUERY)
            info = await db.fetch_one("SELECT DATABASE() AS base, @@hostname AS hote")
        except Exception as e:
            r.ko(f"instance {nom} injoignable : {e}")
            continue
        if info["base"] != MYSQL_DATABASE:
            r.ko(f"instance {nom} : base {info['base']} au lieu de {MYSQL_DATABASE}")
            continue
        r.ok(f"instance {nom} : {info['hote']} / {info['base']}")
        if db is db_write:
            ecriture_ok = True
    return ecriture_ok


async def verifier_tables(r: Rapport, attendues: dict[str, set[str]]) -> set[str]:
    print("2. Tables et colonnes")
    rows = await db_write.fetch_all(
        "SELECT TABLE_NAME AS t, COLUMN_NAME AS c FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE()"
    )
    en_base: dict[str, set[str]] = {}
    for row in rows:
        en_base.setdefault(row["t"], set()).add(row["c"])

    presentes = set()
    for table in TABLES_PERIMETRE:
        if table not in en_base:
            r.ko(f"{table} : table absente")
            continue
        presentes.add(table)
        nb = (await db_write.fetch_one(f"SELECT COUNT(*) AS n FROM `{table}`"))["n"]
        manquantes = sorted(attendues.get(table, set()) - en_base[table])
        if manquantes:
            r.ko(f"{table} : colonnes manquantes {', '.join(manquantes)}")
        else:
            r.ok(f"{table} : {nb} ligne(s)")
    return presentes


async def verifier_fk(r: Rapport, attendues: list[tuple[str, str]], presentes: set[str]) -> None:
    print("3. Clés étrangères")
    rows = await db_write.fetch_all(
        "SELECT TABLE_NAME AS t, CONSTRAINT_NAME AS c "
        "FROM information_schema.REFERENTIAL_CONSTRAINTS "
        "WHERE CONSTRAINT_SCHEMA = DATABASE()"
    )
    en_base = {(row["t"], row["c"]) for row in rows}
    manquantes = [
        f"{t}.{c}" for t, c in attendues if t in presentes and (t, c) not in en_base
    ]
    if manquantes:
        r.ko(f"FK manquantes (rejouer db/03_contraintes.sql) : {', '.join(manquantes)}")
    else:
        r.ok(f"{len(attendues)} FK attendues présentes")


async def verifier_prerequis(r: Rapport, presentes: set[str]) -> None:
    print("4. Prérequis de l'API")
    if "trppu_produit" in presentes:
        nb = (await db_write.fetch_one("SELECT COUNT(*) AS n FROM trppu_produit"))["n"]
        if nb == 0:
            r.ko("trppu_produit vide : toute écriture TMH sera rejetée en 422")
        else:
            r.ok(f"trppu_produit : {nb} produit(s)")
        auto = await db_write.fetch_all(
            "SELECT co_produit FROM trppu_produit WHERE lb_produit = co_produit"
        )
        if auto:
            codes = ", ".join(row["co_produit"] for row in auto)
            r.avert(f"produits à libellé = code (ancienne création auto TMH) : {codes}")

    if "trppu_pic_version" in presentes:
        # Même règle que resolve_default_pic_version (trppu_scenario/helpers.py).
        row = await db_write.fetch_one(
            "SELECT id_pic_version FROM trppu_pic_version "
            "WHERE est_par_defaut = 1 OR id_pic_version = 1 "
            "ORDER BY est_par_defaut DESC, id_pic_version LIMIT 1"
        )
        if row:
            r.ok(f"version PIC par défaut : id_pic_version = {row['id_pic_version']}")
        else:
            r.ko(
                "aucune version PIC candidate (est_par_defaut = 1 ou id 1) : "
                "POST /scenarios sans id_pic_version renverra 422"
            )

    if "trppu_site" in presentes:
        nb = (await db_write.fetch_one("SELECT COUNT(*) AS n FROM trppu_site"))["n"]
        if nb == 0:
            r.avert("trppu_site vide : les sites seront créés au premier POST /scenarios")
        else:
            r.ok(f"trppu_site : {nb} site(s)")


async def verifier() -> int:
    r = Rapport()
    try:
        if await verifier_connexions(r):
            presentes = await verifier_tables(
                r, colonnes_attendues(STRUCTURE_SQL.read_text(encoding="utf-8"))
            )
            await verifier_fk(
                r, fk_attendues(CONTRAINTES_SQL.read_text(encoding="utf-8")), presentes
            )
            await verifier_prerequis(r, presentes)
    finally:
        await db_read.disconnect()
        await db_write.disconnect()

    print()
    print(f"Résultat : {r.nb_ko} KO, {r.nb_avert} avertissement(s)")
    return 1 if r.nb_ko else 0


def main() -> int:
    return asyncio.run(verifier())


if __name__ == "__main__":
    sys.exit(main())
