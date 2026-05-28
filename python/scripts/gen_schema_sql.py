"""Génère un script SQL (CREATE DATABASE + CREATE TABLE) à partir des dumps JSON
de db_analyse/ (sortie de /mysql/columns).

Fidélité « strict JSON » : colonnes + types + nullabilité + défauts + PRIMARY KEY,
avec AUTO_INCREMENT inféré pour les clés primaires entières simples. Pas de clés
étrangères ni d'index secondaires (absents des dumps).

Usage :
    python scripts/gen_schema_sql.py
"""

from __future__ import annotations

import json
import re
from pathlib import Path

DB_NAME = "trppu"
ROOT = Path(__file__).resolve().parent.parent
JSON_DIR = ROOT / "db_analyse"
OUTPUT = JSON_DIR / "schema_trppu.sql"

_INT_TYPE_RE = re.compile(r"^(tinyint|smallint|mediumint|int|bigint)\b", re.IGNORECASE)


def _is_integer_type(col_type: str) -> bool:
    return bool(_INT_TYPE_RE.match(col_type.strip()))


def _is_current_timestamp(default: str) -> bool:
    return default.strip().upper().rstrip("()") == "CURRENT_TIMESTAMP"


def _is_numeric(value: str) -> bool:
    return bool(re.fullmatch(r"-?\d+(\.\d+)?", value.strip()))


def _sql_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "''")


def _render_default(default: str) -> str:
    if _is_current_timestamp(default):
        return "DEFAULT CURRENT_TIMESTAMP"
    if _is_numeric(default):
        return f"DEFAULT {default.strip()}"
    return f"DEFAULT '{_sql_escape(default)}'"


def _render_column(col: dict, auto_increment_name: str | None) -> str:
    name = col["COLUMN_NAME"]
    col_type = col["COLUMN_TYPE"]
    parts = [f"  `{name}` {col_type}"]

    parts.append("NOT NULL" if col["IS_NULLABLE"] == "NO" else "NULL")

    default = col.get("COLUMN_DEFAULT")
    if default is not None:
        parts.append(_render_default(str(default)))

    if name == auto_increment_name:
        parts.append("AUTO_INCREMENT")

    comment = col.get("COLUMN_COMMENT") or ""
    if comment.strip():
        parts.append(f"COMMENT '{_sql_escape(comment)}'")

    return " ".join(parts)


def _build_create_table(table: str, columns: list[dict]) -> str:
    pk_cols = [c["COLUMN_NAME"] for c in columns if c.get("COLUMN_KEY") == "PRI"]

    # AUTO_INCREMENT inféré : un seul PK, entier, sans valeur par défaut.
    auto_increment_name: str | None = None
    if len(pk_cols) == 1:
        pk = next(c for c in columns if c["COLUMN_NAME"] == pk_cols[0])
        if _is_integer_type(pk["COLUMN_TYPE"]) and pk.get("COLUMN_DEFAULT") is None:
            auto_increment_name = pk["COLUMN_NAME"]

    lines = [_render_column(c, auto_increment_name) for c in columns]
    if pk_cols:
        pk_list = ", ".join(f"`{c}`" for c in pk_cols)
        lines.append(f"  PRIMARY KEY ({pk_list})")

    body = ",\n".join(lines)
    return (
        f"DROP TABLE IF EXISTS `{table}`;\n"
        f"CREATE TABLE `{table}` (\n{body}\n"
        f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"
    )


def main() -> None:
    json_files = sorted(p for p in JSON_DIR.glob("*.json"))
    if not json_files:
        raise SystemExit(f"Aucun fichier JSON trouvé dans {JSON_DIR}")

    blocks: list[str] = []
    for path in json_files:
        data = json.loads(path.read_text(encoding="utf-8"))
        table = data.get("table")
        columns = data.get("columns")
        if not table or not columns:
            print(f"  [ignoré] {path.name} (pas de 'table'/'columns')")
            continue
        blocks.append(_build_create_table(table, columns))
        print(f"  [ok] {table} ({len(columns)} colonnes)")

    header = (
        "-- Schéma reconstruit depuis les dumps JSON de db_analyse/\n"
        "-- (introspection de la base dsr_mercure_aa via /mysql/columns).\n"
        "-- Fidélité strict JSON : pas de clés étrangères ni d'index secondaires.\n"
        f"-- Tables : {len(blocks)}\n\n"
        f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` "
        "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;\n"
        f"USE `{DB_NAME}`;\n\n"
        "SET FOREIGN_KEY_CHECKS = 0;\n\n"
    )
    footer = "\nSET FOREIGN_KEY_CHECKS = 1;\n"

    OUTPUT.write_text(header + "\n".join(blocks) + footer, encoding="utf-8")
    print(f"\n=> {len(blocks)} tables ecrites dans {OUTPUT}")


if __name__ == "__main__":
    main()
