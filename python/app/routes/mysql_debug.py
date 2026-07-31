"""Routes de debug MySQL pour explorer le schéma et exécuter des requêtes de diagnostic."""

import base64
import logging
import time
from datetime import date, datetime, time as dtime, timedelta
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from app.config import MYSQL_DATABASE
from app.db.mysql import db_read, db_write

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/mysql", tags=["MySQL Debug"])


def _lc(row: dict | None) -> dict:
    """Normalise les clés d'une ligne en minuscules.

    `information_schema` renvoie ses colonnes en MAJUSCULES sous MySQL 8.0
    (vues sur le dictionnaire de données) et en minuscules sous d'autres
    versions / MariaDB. De même `SHOW CREATE` renvoie 'Create Table' / 'Create
    View'. On normalise pour un accès stable quelle que soit la plateforme
    (évite les `KeyError: 'table_name'` constatés en prod).
    """
    return {str(k).lower(): v for k, v in (row or {}).items()}


# Marqueur pour transporter des valeurs binaires en JSON (round-trip export -> import).
_B64_KEY = "__b64__"


def _serialize_value(v: Any) -> Any:
    """Convertit une valeur SQL en valeur JSON-safe et réinjectable à l'identique.

    - datetime/date/time -> chaînes au format MySQL (réacceptées tel quel à l'insert)
    - Decimal            -> str (préserve la précision)
    - bytes              -> dict {"__b64__": ...} (réhydraté à l'import)
    Les autres types (int, float, str, bool, None, str JSON) passent inchangés.
    """
    if isinstance(v, (bytes, bytearray)):
        return {_B64_KEY: base64.b64encode(bytes(v)).decode("ascii")}
    if isinstance(v, datetime):
        s = v.strftime("%Y-%m-%d %H:%M:%S")
        return f"{s}.{v.microsecond:06d}" if v.microsecond else s
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, dtime):
        return v.isoformat()
    if isinstance(v, timedelta):
        return str(v)
    if isinstance(v, Decimal):
        return str(v)
    return v


def _deserialize_value(v: Any) -> Any:
    """Inverse de `_serialize_value` pour les valeurs reçues à l'import."""
    if isinstance(v, dict) and _B64_KEY in v:
        return base64.b64decode(v[_B64_KEY])
    return v


def _sql_literal(v: Any) -> str:
    """Représente une valeur sous forme de littéral SQL pour un INSERT copiable."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float, Decimal)):
        return str(v)
    if isinstance(v, (bytes, bytearray)):
        return "0x" + bytes(v).hex()
    if isinstance(v, datetime):
        return "'" + v.strftime("%Y-%m-%d %H:%M:%S") + "'"
    if isinstance(v, (date, dtime)):
        return "'" + v.isoformat() + "'"
    s = (
        str(v)
        .replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )
    return "'" + s + "'"


async def _table_columns(table: str) -> list[str]:
    """Retourne la liste ordonnée des colonnes réelles d'une table, ou [] si introuvable."""
    rows = await db_read.fetch_all(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s "
        "ORDER BY ordinal_position",
        (MYSQL_DATABASE, table),
    )
    return [_lc(r)["column_name"] for r in rows]


@router.get("/test")
async def mysql_test():
    """Requête de test sur la base MySQL (lecture)."""
    start = time.perf_counter()
    try:
        result = await db_read.fetch_one("SELECT 1 AS ok")
    except Exception as e:
        logger.error("Erreur lors du test MySQL : %s", e)
        raise HTTPException(
            status_code=500,
            detail="Erreur lors du test de connexion à MySQL.",
        ) from e
    duration_s = round(time.perf_counter() - start, 3)
    return {"test": "ok", "execution_time_s": duration_s, "result": result}


@router.get("/tables")
async def list_tables():
    """Liste toutes les tables de la base de données."""
    start = time.perf_counter()
    try:
        rows = await db_read.fetch_all(
            "SELECT table_name, table_type, table_rows, table_comment "
            "FROM information_schema.tables "
            "WHERE table_schema = %s "
            "ORDER BY table_name",
            (MYSQL_DATABASE,),
        )
    except Exception as e:
        logger.error("Erreur listing tables : %s", e)
        raise HTTPException(status_code=500, detail="Erreur listing tables.") from e
    duration_s = round(time.perf_counter() - start, 3)
    return {
        "execution_time_s": duration_s,
        "database": MYSQL_DATABASE,
        "count": len(rows),
        "tables": rows,
    }


@router.get("/columns")
async def list_columns(
    table: str = Query(..., description="Nom de la table"),
):
    """Liste les colonnes d'une table avec leur type et commentaire."""
    start = time.perf_counter()
    try:
        rows = await db_read.fetch_all(
            "SELECT column_name, column_type, is_nullable, column_key, "
            "column_default, column_comment "
            "FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "ORDER BY ordinal_position",
            (MYSQL_DATABASE, table),
        )
    except Exception as e:
        logger.error("Erreur listing colonnes de %s : %s", table, e)
        raise HTTPException(status_code=500, detail=f"Erreur listing colonnes de {table}.") from e
    if not rows:
        raise HTTPException(status_code=404, detail=f"Table '{table}' introuvable dans {MYSQL_DATABASE}.")
    duration_s = round(time.perf_counter() - start, 3)
    return {
        "execution_time_s": duration_s,
        "database": MYSQL_DATABASE,
        "table": table,
        "count": len(rows),
        "columns": rows,
    }


@router.get("/indexes")
async def list_indexes(
    table: str = Query(..., description="Nom de la table"),
):
    """Liste les index d'une table."""
    start = time.perf_counter()
    try:
        rows = await db_read.fetch_all(
            "SHOW INDEX FROM " + f"`{MYSQL_DATABASE}`.`{table}`"
        )
    except Exception as e:
        logger.error("Erreur listing index de %s : %s", table, e)
        raise HTTPException(status_code=500, detail=f"Erreur listing index de {table}.") from e
    duration_s = round(time.perf_counter() - start, 3)
    return {
        "execution_time_s": duration_s,
        "table": table,
        "count": len(rows),
        "indexes": rows,
    }


@router.get("/sample")
async def sample_rows(
    table: str = Query(..., description="Nom de la table"),
    limit: int = Query(10, ge=1, le=100, description="Nombre de lignes (max 100)"),
):
    """Retourne un échantillon de lignes d'une table."""
    start = time.perf_counter()
    try:
        # Vérifier que la table existe dans le bon schéma
        check = await db_read.fetch_one(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            (MYSQL_DATABASE, table),
        )
        if not check:
            raise HTTPException(status_code=404, detail=f"Table '{table}' introuvable dans {MYSQL_DATABASE}.")
        rows = await db_read.fetch_all(
            f"SELECT * FROM `{MYSQL_DATABASE}`.`{table}` LIMIT %s",
            (limit,),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Erreur sample de %s : %s", table, e)
        raise HTTPException(status_code=500, detail=f"Erreur sample de {table}.") from e
    duration_s = round(time.perf_counter() - start, 3)
    return {
        "execution_time_s": duration_s,
        "table": table,
        "count": len(rows),
        "data": rows,
    }


@router.get("/schema")
async def full_schema():
    """Retourne le schéma complet : chaque table avec ses colonnes."""
    start = time.perf_counter()
    try:
        rows = await db_read.fetch_all(
            "SELECT table_name, table_rows, table_comment "
            "FROM information_schema.tables "
            "WHERE table_schema = %s ORDER BY table_name",
            (MYSQL_DATABASE,),
        )
        schema = []
        for t in (_lc(r) for r in rows):
            cols = await db_read.fetch_all(
                "SELECT column_name, column_type, is_nullable, column_key, column_comment "
                "FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "ORDER BY ordinal_position",
                (MYSQL_DATABASE, t["table_name"]),
            )
            schema.append({**t, "columns": [_lc(c) for c in cols]})
    except Exception as e:
        logger.error("Erreur schema complet : %s", e)
        raise HTTPException(status_code=500, detail="Erreur récupération schema.") from e
    duration_s = round(time.perf_counter() - start, 3)
    return {
        "execution_time_s": duration_s,
        "database": MYSQL_DATABASE,
        "table_count": len(schema),
        "schema": schema,
    }


@router.get("/dump")
async def dump_create_sql(
    fmt: str = Query(
        "sql",
        pattern="^(sql|json)$",
        description="Format de sortie : 'sql' (texte brut copiable) ou 'json'.",
    ),
    drop: bool = Query(
        True,
        description="Inclure les DROP TABLE/VIEW IF EXISTS avant chaque CREATE.",
    ),
    data: bool = Query(
        False,
        description="Inclure aussi les données (INSERT) de chaque table après son CREATE.",
    ),
):
    """Retourne le DDL complet (CREATE) de la base, copiable pour la recréer.

    Reconstruit le SQL via `SHOW CREATE TABLE` / `SHOW CREATE VIEW` pour chaque
    objet de la base. Par défaut la sortie n'inclut PAS les données (schéma
    uniquement) ; passer `data=true` ajoute les `INSERT` de chaque table.

    - `fmt=sql`  : réponse text/plain prête à copier-coller dans un client SQL.
    - `fmt=json` : réponse structurée (un objet par table/vue + le SQL complet).
    """
    start = time.perf_counter()

    # 1) Liste des objets (tables d'abord, puis vues qui peuvent en dépendre).
    try:
        rows = await db_read.fetch_all(
            "SELECT table_name, table_type "
            "FROM information_schema.tables "
            "WHERE table_schema = %s "
            "ORDER BY (table_type = 'VIEW'), table_name",
            (MYSQL_DATABASE,),
        )
        objects = [_lc(r) for r in rows]
    except Exception as e:
        logger.error("Erreur génération dump SQL (listing objets) : %s", e)
        raise HTTPException(status_code=500, detail="Erreur génération dump SQL.") from e

    # 2) DDL objet par objet : un échec isolé est annoté mais n'interrompt PAS le
    #    dump (on récupère ainsi toutes les tables même si une vue casse).
    items: list[dict] = []
    for obj in objects:
        name = obj.get("table_name")
        is_view = str(obj.get("table_type") or "").upper() == "VIEW"
        kind = "VIEW" if is_view else "TABLE"
        # SHOW CREATE TABLE -> "Create Table" ; SHOW CREATE VIEW -> "Create View"
        create_key = "create view" if is_view else "create table"
        error: str | None = None
        create_sql = ""
        try:
            row = _lc(await db_read.fetch_one(f"SHOW CREATE {kind} `{MYSQL_DATABASE}`.`{name}`"))
            create_sql = row.get(create_key, "") or ""
            if not create_sql:
                error = "DDL vide renvoyé par SHOW CREATE"
        except Exception as e:
            logger.error("Erreur SHOW CREATE %s `%s` : %s", kind, name, e)
            error = str(e)
        item = {"name": name, "type": kind, "create_sql": create_sql}
        if error:
            item["error"] = error
        # Données : uniquement pour les tables (les vues n'en stockent pas).
        if data and not is_view and not error:
            try:
                cols = await _table_columns(name)
                raw_rows = await db_read.fetch_all(
                    f"SELECT * FROM `{MYSQL_DATABASE}`.`{name}`"
                )
                item["columns"] = cols
                item["rows"] = [
                    {c: _serialize_value(r.get(c)) for c in cols} for r in raw_rows
                ]
                item["_raw_rows"] = raw_rows  # interne : sert à générer les INSERT SQL
            except Exception as e:
                logger.error("Erreur export données de `%s` : %s", name, e)
                item["data_error"] = str(e)
        items.append(item)

    failed = [it for it in items if it.get("error")]

    # 3) Assemble le script SQL complet
    parts: list[str] = [
        f"-- Dump du schéma de la base `{MYSQL_DATABASE}`",
        f"-- {len(items)} objet(s) — "
        + ("schéma + données" if data else "schéma uniquement (sans données)"),
    ]
    if failed:
        parts.append(
            f"-- ATTENTION : {len(failed)} objet(s) en échec "
            "(voir les commentaires '-- !! ERREUR' ci-dessous)"
        )
    parts += [
        "",
        f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}`;",
        f"USE `{MYSQL_DATABASE}`;",
        "",
        "SET FOREIGN_KEY_CHECKS = 0;",
        "",
    ]
    for item in items:
        parts.append(f"-- ----- {item['type']} `{item['name']}` -----")
        if item.get("error"):
            parts.append(
                f"-- !! ERREUR génération DDL pour {item['type']} "
                f"`{item['name']}` : {item['error']}"
            )
            parts.append("")
            continue
        if drop:
            parts.append(f"DROP {item['type']} IF EXISTS `{item['name']}`;")
        parts.append(f"{item['create_sql']};")
        parts.append("")
        # INSERT des données si demandées (tables uniquement).
        if data and item.get("_raw_rows") is not None:
            cols = item["columns"]
            raw_rows = item["_raw_rows"]
            if raw_rows:
                col_list = ", ".join(f"`{c}`" for c in cols)
                parts.append(f"-- Données de `{item['name']}` — {len(raw_rows)} ligne(s)")
                for row in raw_rows:
                    values = ", ".join(_sql_literal(row.get(c)) for c in cols)
                    parts.append(
                        f"INSERT INTO `{item['name']}` ({col_list}) VALUES ({values});"
                    )
                parts.append("")
        elif data and item.get("data_error"):
            parts.append(
                f"-- !! ERREUR export données pour `{item['name']}` : {item['data_error']}"
            )
            parts.append("")
    parts.append("SET FOREIGN_KEY_CHECKS = 1;")
    parts.append("")
    sql = "\n".join(parts)

    duration_s = round(time.perf_counter() - start, 3)

    if fmt == "json":
        # Retire la clé interne `_raw_rows` (valeurs SQL brutes non sérialisables) ;
        # `rows` (déjà sérialisé via `_serialize_value`) reste exposé.
        json_objects = [
            {k: v for k, v in item.items() if k != "_raw_rows"} for item in items
        ]
        return {
            "execution_time_s": duration_s,
            "database": MYSQL_DATABASE,
            "object_count": len(items),
            "failed_count": len(failed),
            "objects": json_objects,
            "sql": sql,
        }
    return PlainTextResponse(content=sql, media_type="text/plain; charset=utf-8")


@router.get("/export")
async def export_table(
    table: str = Query(..., description="Nom de la table à exporter"),
    fmt: str = Query(
        "json",
        pattern="^(json|sql)$",
        description="Format : 'json' (réinjectable via POST /mysql/import) ou 'sql' (INSERT copiables).",
    ),
    truncate: bool = Query(
        True,
        description="fmt=sql uniquement : ajoute un TRUNCATE TABLE avant les INSERT.",
    ),
):
    """Exporte TOUTES les données d'une table.

    - `fmt=json` : payload structuré directement réinjectable via `POST /mysql/import`
      (on peut renvoyer ce corps tel quel pour recharger la table en dev).
    - `fmt=sql`  : script `INSERT` text/plain copiable dans un client SQL.

    Les types non sérialisables (datetime, Decimal, bytes) sont normalisés pour un
    aller-retour fidèle (cf. `_serialize_value`).
    """
    start = time.perf_counter()

    columns = await _table_columns(table)
    if not columns:
        raise HTTPException(
            status_code=404, detail=f"Table '{table}' introuvable dans {MYSQL_DATABASE}."
        )

    try:
        raw_rows = await db_read.fetch_all(f"SELECT * FROM `{MYSQL_DATABASE}`.`{table}`")
    except Exception as e:
        logger.error("Erreur export de %s : %s", table, e)
        raise HTTPException(status_code=500, detail=f"Erreur export de {table}.") from e

    duration_s = round(time.perf_counter() - start, 3)

    if fmt == "sql":
        parts: list[str] = [
            f"-- Données de la table `{MYSQL_DATABASE}`.`{table}` — {len(raw_rows)} ligne(s)",
            "",
            "SET FOREIGN_KEY_CHECKS = 0;",
        ]
        if truncate:
            parts.append(f"TRUNCATE TABLE `{table}`;")
        parts.append("")
        col_list = ", ".join(f"`{c}`" for c in columns)
        for row in raw_rows:
            values = ", ".join(_sql_literal(row.get(c)) for c in columns)
            parts.append(f"INSERT INTO `{table}` ({col_list}) VALUES ({values});")
        parts += ["", "SET FOREIGN_KEY_CHECKS = 1;", ""]
        return PlainTextResponse(
            content="\n".join(parts), media_type="text/plain; charset=utf-8"
        )

    rows = [{c: _serialize_value(row.get(c)) for c in columns} for row in raw_rows]
    return {
        "execution_time_s": duration_s,
        "database": MYSQL_DATABASE,
        "table": table,
        "columns": columns,
        "count": len(rows),
        "rows": rows,
    }


class ImportPayload(BaseModel):
    """Corps de `POST /mysql/import` — compatible avec la sortie de `GET /mysql/export?fmt=json`."""

    table: str
    rows: list[dict[str, Any]]
    columns: list[str] | None = None
    truncate: bool = True


@router.post("/import")
async def import_table(payload: ImportPayload = Body(...)):
    """Recharge les données d'une table à partir d'un export JSON.

    Workflow type : `GET /mysql/export?table=X&fmt=json` en prod -> renvoyer le corps
    obtenu à `POST /mysql/import` en dev pour repeupler la table.

    - `truncate=True` (défaut) : vide la table avant insertion. Le vidage se fait par
      `DELETE FROM` et non `TRUNCATE` : ce dernier est du DDL, donc auto-commité par
      MySQL — un échec d'insertion laissait la table définitivement vide malgré le
      rollback. Avec `DELETE`, l'opération est réellement atomique : si un lot échoue,
      les données d'origine sont restaurées. Contrepartie : l'AUTO_INCREMENT n'est pas
      remis à zéro, sans incidence pour un rechargement d'export où les identifiants
      sont fournis explicitement.
    - Les contrôles FK sont désactivés le temps de l'opération et systématiquement
      rétablis (y compris en cas d'erreur) : la connexion étant rendue au pool sans
      reset de session, les laisser à 0 contaminerait les requêtes suivantes.
    - Seules les colonnes réellement présentes dans la table sont insérées (les clés
      inconnues du payload sont ignorées, pas d'injection d'identifiant arbitraire).
    """
    start = time.perf_counter()
    table = payload.table

    real_columns = await _table_columns(table)
    if not real_columns:
        raise HTTPException(
            status_code=404, detail=f"Table '{table}' introuvable dans {MYSQL_DATABASE}."
        )

    # Colonnes à insérer : intersection (en préservant l'ordre réel de la table) entre
    # les colonnes demandées / présentes dans les lignes et les colonnes réelles.
    requested = payload.columns or (list(payload.rows[0].keys()) if payload.rows else [])
    columns = [c for c in real_columns if c in set(requested)]
    if payload.rows and not columns:
        raise HTTPException(
            status_code=400,
            detail=(
                "Aucune colonne du payload ne correspond aux colonnes de la table "
                f"`{table}`. Colonnes attendues : {real_columns}"
            ),
        )

    col_list = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO `{table}` ({col_list}) VALUES ({placeholders})"
    params_seq = [
        tuple(_deserialize_value(row.get(c)) for c in columns) for row in payload.rows
    ]

    inserted = 0
    try:
        async with db_write.transaction() as tx:
            await tx.execute("SET FOREIGN_KEY_CHECKS = 0")
            try:
                if payload.truncate:
                    # DELETE et non TRUNCATE : transactionnel, donc annulable.
                    await tx.execute(f"DELETE FROM `{MYSQL_DATABASE}`.`{table}`")
                # Insertion par lots pour éviter un paquet réseau trop volumineux.
                chunk = 500
                for i in range(0, len(params_seq), chunk):
                    batch = params_seq[i : i + chunk]
                    if batch:
                        await tx.execute_many(insert_sql, batch)
                        inserted += len(batch)
            finally:
                # Impératif sur tous les chemins : la connexion retourne au pool
                # sans reset de session, et FOREIGN_KEY_CHECKS survit au rollback.
                # Le rétablissement ne doit jamais masquer l'erreur d'origine.
                try:
                    await tx.execute("SET FOREIGN_KEY_CHECKS = 1")
                except Exception:
                    logger.warning(
                        "Impossible de rétablir FOREIGN_KEY_CHECKS sur la connexion "
                        "(import de %s) — connexion probablement rompue.",
                        table,
                    )
    except Exception as e:
        logger.error("Erreur import de %s : %s", table, e)
        raise HTTPException(status_code=500, detail=f"Erreur import de {table} : {e}") from e

    duration_s = round(time.perf_counter() - start, 3)
    return {
        "execution_time_s": duration_s,
        "database": MYSQL_DATABASE,
        "table": table,
        "truncated": payload.truncate,
        "columns": columns,
        "inserted": inserted,
    }
