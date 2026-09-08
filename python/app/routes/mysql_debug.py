"""Routes de debug MySQL pour explorer le schéma et exécuter des requêtes de diagnostic.

**Volumétrie.** Les exports (`GET /mysql/export`, `GET /mysql/dump`) doivent tenir sur des
tables atteignant plusieurs dizaines de millions de lignes. Aucun export ne matérialise
donc son résultat : les lignes sont lues par curseur serveur (`db_read.iter_rows`) et la
réponse est émise au fil de l'eau (`StreamingResponse`), à mémoire constante quelle que
soit la taille de la table. Le coût devient celui du réseau et du disque du client, plus
celui de la connexion mobilisée côté pool.

Trois conséquences assumées :

- **Une erreur survenant en cours de flux ne peut plus donner un 500** : le statut et les
  en-têtes sont déjà partis. Elle est journalisée puis injectée dans le flux (commentaire
  `-- !! ERREUR` en SQL, clé `error` en JSON). Chaque export se termine par un marqueur
  explicite (`-- FIN DE L'EXPORT` / `"complete": true`) : son absence signale une sortie
  tronquée, c'est le seul contrôle d'intégrité fiable ici.
- **Une connexion du pool reste prise pendant tout l'export.** Ce sont des outils de
  diagnostic, pas des services de production : ne pas les lancer en parallèle sur un pool
  dimensionné pour l'IHM.
- **Les contrôles bloquants sont faits avant le premier octet** (existence de la table,
  cohérence des paramètres), pour que les cas d'erreur courants restent des vrais 404/400.
"""

import base64
import json
import logging
import time
from collections.abc import AsyncIterator
from datetime import date, datetime, time as dtime, timedelta
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel

from app.config import MYSQL_DATABASE
from app.db.mysql import db_read, db_write
from app.log_utils import ctx

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

# --- Réglages de streaming ------------------------------------------------------
# Lignes récupérées par aller-retour réseau sur le curseur serveur. Plus haut =
# moins d'allers-retours mais un pic mémoire proportionnel à la largeur des lignes.
TAILLE_LOT_LECTURE = 2000

# Lignes regroupées par INSERT généré. Un INSERT multi-lignes se recharge d'un ordre
# de grandeur plus vite que des INSERT unitaires, mais chaque ordre doit rester sous
# `max_allowed_packet` du serveur cible (4 Mo par défaut sous MySQL 8) : 200 lignes
# laissent de la marge même sur des tables larges.
LIGNES_PAR_INSERT = 200

# Les fragments de texte sont regroupés avant d'être poussés dans la réponse : sans
# ça, une table de 30 M de lignes produirait des dizaines de millions de micro-chunks
# HTTP, chacun avec son coût de framing.
TAILLE_TAMPON_SORTIE = 64 * 1024


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
        return _time_mysql(v)
    if isinstance(v, Decimal):
        return str(v)
    return v


def _deserialize_value(v: Any) -> Any:
    """Inverse de `_serialize_value` pour les valeurs reçues à l'import."""
    if isinstance(v, dict) and _B64_KEY in v:
        return base64.b64decode(v[_B64_KEY])
    return v


def _time_mysql(v: timedelta) -> str:
    """Formate une colonne TIME (rendue en `timedelta` par le driver) en 'HH:MM:SS'.

    `str(timedelta)` produit '1 day, 0:00:00' au-delà de 24 h et '-1 day, 23:00:00'
    pour les valeurs négatives : deux formes que MySQL refuse. Le type TIME acceptant
    -838:59:59 à 838:59:59, on reconstruit la notation horaire cumulée.
    """
    total = int(v.total_seconds())
    signe = "-" if total < 0 else ""
    total = abs(total)
    heures, reste = divmod(total, 3600)
    minutes, secondes = divmod(reste, 60)
    return f"{signe}{heures:02d}:{minutes:02d}:{secondes:02d}"


def _sql_literal(v: Any) -> str:
    """Représente une valeur sous forme de littéral SQL pour un INSERT copiable."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float, Decimal)):
        return str(v)
    if isinstance(v, (bytes, bytearray)):
        return "0x" + bytes(v).hex() if v else "''"
    if isinstance(v, datetime):
        return "'" + v.strftime("%Y-%m-%d %H:%M:%S") + "'"
    if isinstance(v, timedelta):
        return "'" + _time_mysql(v) + "'"
    if isinstance(v, (date, dtime)):
        return "'" + v.isoformat() + "'"
    # NUL et Ctrl-Z doivent être échappés au même titre que le quote et le backslash :
    # bruts dans un script, ils tronquent la valeur (Ctrl-Z est un EOF sous Windows).
    s = (
        str(v)
        .replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\0", "\\0")
        .replace("\x1a", "\\Z")
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


async def _create_sql(name: str, is_view: bool) -> tuple[str, str | None]:
    """DDL d'un objet via `SHOW CREATE`. Retourne `(ddl, erreur)`.

    L'erreur est renvoyée plutôt que levée : un dump complet doit continuer même si
    une vue casse (droits manquants, table sous-jacente supprimée…).
    """
    kind = "VIEW" if is_view else "TABLE"
    cle = "create view" if is_view else "create table"
    try:
        row = _lc(await db_read.fetch_one(f"SHOW CREATE {kind} `{MYSQL_DATABASE}`.`{name}`"))
    except Exception as e:
        logger.exception("Erreur SHOW CREATE %s", ctx(kind=kind, name=name))
        return "", str(e)
    ddl = row.get(cle, "") or ""
    return ddl, None if ddl else "DDL vide renvoyé par SHOW CREATE"


async def _primary_key_columns(table: str) -> list[str]:
    """Colonnes de la clé primaire, dans l'ordre de la clé (vide si la table n'en a pas)."""
    rows = await db_read.fetch_all(
        "SELECT column_name FROM information_schema.key_column_usage "
        "WHERE table_schema = %s AND table_name = %s AND constraint_name = 'PRIMARY' "
        "ORDER BY ordinal_position",
        (MYSQL_DATABASE, table),
    )
    return [_lc(r)["column_name"] for r in rows]


def _select_rows_sql(
    table: str,
    *,
    limit: int | None = None,
    offset: int = 0,
    pk_cols: list[str] | None = None,
    after: str | None = None,
) -> tuple[str, tuple]:
    """SELECT * d'une table, avec tranche optionnelle.

    Deux points qui décident de la tenue à grande volumétrie :

    - **L'ORDER BY n'est posé que si l'on découpe.** Sans découpage, on laisse MySQL
      parcourir la table dans l'ordre naturel (l'index clusterisé InnoDB) : trier
      30 M de lignes pour un export intégral n'apporterait rien et coûterait un tri.
      Dès qu'on découpe en revanche il devient obligatoire : `LIMIT`/`OFFSET` sans
      `ORDER BY` n'a **aucun ordre garanti**, deux pages successives peuvent donc se
      recouvrir ou sauter des lignes.
    - **`after` (pagination par curseur) est préférable à `offset`.** `OFFSET n` fait
      lire puis jeter n lignes à chaque page — le découpage d'une table de 30 M de
      lignes devient quadratique. `WHERE pk > <dernier vu>` attaque directement
      l'index : chaque page coûte le même prix, quelle que soit sa position.
    """
    pk_cols = pk_cols or []
    clauses: list[str] = []
    params: list[Any] = []

    if after is not None and pk_cols:
        clauses.append(f"`{pk_cols[0]}` > %s")
        params.append(after)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    decoupe = limit is not None or offset or after is not None
    ordre = ""
    if decoupe and pk_cols:
        ordre = " ORDER BY " + ", ".join(f"`{c}`" for c in pk_cols)

    sql = f"SELECT * FROM `{MYSQL_DATABASE}`.`{table}`{where}{ordre}"
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
        if offset:
            sql += " OFFSET %s"
            params.append(offset)
    elif offset:
        # MySQL n'accepte OFFSET qu'accompagné d'un LIMIT ; 2^64-1 = « tout le reste ».
        sql += " LIMIT 18446744073709551615 OFFSET %s"
        params.append(offset)
    return sql, tuple(params)


async def _tamponner(flux: AsyncIterator[str]) -> AsyncIterator[bytes]:
    """Regroupe les fragments produits en blocs d'au moins `TAILLE_TAMPON_SORTIE`."""
    tampon: list[str] = []
    taille = 0
    async for morceau in flux:
        tampon.append(morceau)
        taille += len(morceau)
        if taille >= TAILLE_TAMPON_SORTIE:
            yield "".join(tampon).encode("utf-8")
            tampon = []
            taille = 0
    if tampon:
        yield "".join(tampon).encode("utf-8")


def _suivre_curseur(row: dict, pk_cols: list[str] | None, stats: dict[str, Any]) -> None:
    """Mémorise la dernière PK lue, pour proposer la page suivante en fin d'export.

    Uniquement sur clé primaire mono-colonne : c'est le seul cas où `after` sait
    reprendre sans ambiguïté.
    """
    if pk_cols and len(pk_cols) == 1:
        stats["last_pk"] = row.get(pk_cols[0])


def _reprise(stats: dict[str, Any], limit: int | None) -> str | None:
    """Valeur à repasser en `after` pour obtenir le lot suivant, ou None s'il n'y en a pas.

    Un lot n'est prolongé que s'il a été rempli jusqu'à `limit` : un lot incomplet
    signifie qu'on a atteint la fin de la table. Renvoie aussi None quand la reprise par
    curseur n'a pas de sens — pas de PK mono-colonne, ou PK binaire (non transportable
    telle quelle dans une query string).
    """
    if limit is None or stats.get("rows", 0) < limit:
        return None
    dernier = stats.get("last_pk")
    if dernier is None:
        return None
    valeur = _serialize_value(dernier)
    if isinstance(valeur, dict):  # PK binaire encodée en base64
        return None
    return valeur if isinstance(valeur, str) else str(valeur)


async def _flux_inserts(
    table: str,
    columns: list[str],
    *,
    limit: int | None = None,
    offset: int = 0,
    pk_cols: list[str] | None = None,
    after: str | None = None,
    lignes_par_insert: int = LIGNES_PAR_INSERT,
    stats: dict[str, Any],
) -> AsyncIterator[str]:
    """Génère les `INSERT` multi-lignes d'une table, en streaming.

    Renseigne `stats["rows"]` (et `stats["last_pk"]` si la table a une PK mono-colonne)
    en fin de parcours : le nombre exact n'est connu qu'une fois la table lue, le
    compter d'avance imposerait un scan complet supplémentaire.
    """
    col_list = ", ".join(f"`{c}`" for c in columns)
    prefixe = f"INSERT INTO `{table}` ({col_list}) VALUES\n"
    sql, params = _select_rows_sql(
        table, limit=limit, offset=offset, pk_cols=pk_cols, after=after
    )

    lot: list[str] = []
    total = 0
    async for row in db_read.iter_rows(sql, params, chunk_size=TAILLE_LOT_LECTURE):
        lot.append("(" + ",".join(_sql_literal(row.get(c)) for c in columns) + ")")
        total += 1
        # Tenu à jour à chaque ligne (et pas seulement en fin de parcours) : si le flux
        # casse, l'appelant doit lire le nombre de lignes réellement sorties.
        stats["rows"] = total
        _suivre_curseur(row, pk_cols, stats)
        if len(lot) >= lignes_par_insert:
            yield prefixe + ",\n".join(lot) + ";\n"
            lot = []
    if lot:
        yield prefixe + ",\n".join(lot) + ";\n"
    stats["rows"] = total


async def _flux_rows_json(
    table: str,
    columns: list[str],
    *,
    limit: int | None = None,
    offset: int = 0,
    pk_cols: list[str] | None = None,
    after: str | None = None,
    stats: dict[str, Any],
) -> AsyncIterator[str]:
    """Streame les lignes d'une table comme éléments d'un tableau JSON (sans les crochets)."""
    sql, params = _select_rows_sql(
        table, limit=limit, offset=offset, pk_cols=pk_cols, after=after
    )
    total = 0
    async for row in db_read.iter_rows(sql, params, chunk_size=TAILLE_LOT_LECTURE):
        ligne = {c: _serialize_value(row.get(c)) for c in columns}
        yield ("," if total else "") + json.dumps(ligne, ensure_ascii=False)
        total += 1
        stats["rows"] = total
        _suivre_curseur(row, pk_cols, stats)
    stats["rows"] = total


def _entetes(download: bool, nom_fichier: str) -> dict[str, str]:
    """En-têtes de la réponse ; `download=true` déclenche l'enregistrement fichier."""
    if not download:
        return {}
    return {"Content-Disposition": f'attachment; filename="{nom_fichier}"'}


def _horodatage() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


@router.get("/test")
async def mysql_test():
    """Requête de test sur la base MySQL (lecture)."""
    start = time.perf_counter()
    try:
        result = await db_read.fetch_one("SELECT 1 AS ok")
    except Exception as e:
        logger.exception("Erreur test MySQL %s", ctx())
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
        logger.exception("Erreur listing tables %s", ctx())
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
        logger.exception("Erreur listing colonnes %s", ctx(table=table))
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
        logger.exception("Erreur listing index %s", ctx(table=table))
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
        logger.exception("Erreur sample %s", ctx(table=table))
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
        logger.exception("Erreur schéma complet %s", ctx())
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
    limit_per_table: int | None = Query(
        None,
        ge=1,
        description=(
            "data=true : nombre maximal de lignes exportées PAR table (défaut : toutes). "
            "Permet un dump de dev allégé — ex. 10000 lignes par table."
        ),
    ),
    rows_per_insert: int = Query(
        LIGNES_PAR_INSERT,
        ge=1,
        le=5000,
        description="fmt=sql : lignes regroupées par INSERT généré.",
    ),
    download: bool = Query(
        False, description="Renvoyer la réponse en pièce jointe (téléchargement fichier)."
    ),
):
    """Retourne le DDL complet (CREATE) de la base, copiable pour la recréer.

    Reconstruit le SQL via `SHOW CREATE TABLE` / `SHOW CREATE VIEW` pour chaque
    objet de la base. Par défaut la sortie n'inclut PAS les données (schéma
    uniquement) ; passer `data=true` ajoute les `INSERT` de chaque table.

    - `fmt=sql`  : réponse text/plain prête à copier-coller dans un client SQL.
    - `fmt=json` : réponse structurée (un objet par table/vue).

    Le DDL de tous les objets est collecté avant l'envoi (quelques Ko par objet), les
    **données sont ensuite streamées table par table** : un dump complet avec données
    tient donc en mémoire constante quelle que soit la volumétrie. Avec `data=true` en
    `fmt=json`, la clé `sql` n'est pas produite (elle dupliquerait tout le contenu en
    mémoire) : utiliser `fmt=sql` pour obtenir le script rechargeable.
    """
    start = time.perf_counter()
    logger.info(
        "Début dump base %s",
        ctx(fmt=fmt, avec_donnees=data, drop=drop, limit_par_table=limit_per_table),
    )

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
        logger.exception("Erreur génération dump SQL %s", ctx(etape="listing objets"))
        raise HTTPException(status_code=500, detail="Erreur génération dump SQL.") from e

    # 2) DDL objet par objet : un échec isolé est annoté mais n'interrompt PAS le
    #    dump (on récupère ainsi toutes les tables même si une vue casse). Cette
    #    phase est bornée (le DDL d'un objet pèse quelques Ko), elle peut donc rester
    #    en mémoire et être faite avant tout envoi — c'est ce qui permet de compter
    #    les échecs dans l'en-tête et de renvoyer un vrai 500 si le listing casse.
    items: list[dict] = []
    for obj in objects:
        name = obj.get("table_name")
        is_view = str(obj.get("table_type") or "").upper() == "VIEW"
        create_sql, error = await _create_sql(name, is_view)
        item = {
            "name": name,
            "type": "VIEW" if is_view else "TABLE",
            "create_sql": create_sql,
        }
        if error:
            item["error"] = error
        items.append(item)

    failed = [it for it in items if it.get("error")]
    if failed:
        logger.warning(
            "Dump partiel %s",
            ctx(objets=len(items), en_echec=[it["name"] for it in failed]),
        )

    async def _flux_sql() -> AsyncIterator[str]:
        total_lignes = 0
        yield (
            f"-- Dump du schéma de la base `{MYSQL_DATABASE}`\n"
            f"-- {len(items)} objet(s) — "
            + ("schéma + données" if data else "schéma uniquement (sans données)")
            + (
                f" — au plus {limit_per_table} ligne(s) par table"
                if data and limit_per_table
                else ""
            )
            + "\n"
        )
        if failed:
            yield (
                f"-- ATTENTION : {len(failed)} objet(s) en échec "
                "(voir les commentaires '-- !! ERREUR' ci-dessous)\n"
            )
        yield (
            "\n"
            f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}`;\n"
            f"USE `{MYSQL_DATABASE}`;\n"
            "\n"
            "SET FOREIGN_KEY_CHECKS = 0;\n"
            "SET UNIQUE_CHECKS = 0;\n"
            "\n"
        )
        for item in items:
            yield f"-- ----- {item['type']} `{item['name']}` -----\n"
            if item.get("error"):
                yield (
                    f"-- !! ERREUR génération DDL pour {item['type']} "
                    f"`{item['name']}` : {item['error']}\n\n"
                )
                continue
            if drop:
                yield f"DROP {item['type']} IF EXISTS `{item['name']}`;\n"
            yield f"{item['create_sql']};\n\n"

            if not data or item["type"] != "TABLE":
                continue
            # Les données sont streamées ici : jamais accumulées entre deux tables.
            stats: dict[str, Any] = {}
            try:
                cols = await _table_columns(item["name"])
                # Le tri sur la PK n'a d'intérêt que si l'on tronque : sur un export
                # intégral il ferait payer un tri pour rien.
                pk = await _primary_key_columns(item["name"]) if limit_per_table else []
                yield f"-- Données de `{item['name']}`\n"
                async for morceau in _flux_inserts(
                    item["name"],
                    cols,
                    limit=limit_per_table,
                    pk_cols=pk,
                    lignes_par_insert=rows_per_insert,
                    stats=stats,
                ):
                    yield morceau
                total_lignes += stats.get("rows", 0)
                yield f"-- {stats.get('rows', 0)} ligne(s) pour `{item['name']}`\n\n"
            except Exception as e:
                logger.exception("Erreur export données %s", ctx(name=item["name"]))
                yield (
                    f"\n-- !! ERREUR export données pour `{item['name']}` : {e}\n"
                    "-- !! Le dump est INCOMPLET pour cette table.\n\n"
                )
        yield "SET UNIQUE_CHECKS = 1;\nSET FOREIGN_KEY_CHECKS = 1;\n"
        duree = round(time.perf_counter() - start, 3)
        yield f"-- FIN DE L'EXPORT — {total_lignes} ligne(s) au total, {duree}s\n"
        logger.info(
            "Fin dump base %s",
            ctx(
                fmt="sql",
                objets=len(items),
                en_echec=len(failed),
                lignes=total_lignes,
                duration_s=duree,
            ),
        )

    async def _flux_json() -> AsyncIterator[str]:
        total_lignes = 0
        yield (
            "{"
            f'"database": {json.dumps(MYSQL_DATABASE)}, '
            f'"object_count": {len(items)}, '
            f'"failed_count": {len(failed)}, '
            '"objects": ['
        )
        for idx, item in enumerate(items):
            if idx:
                yield ","
            yield (
                "{"
                f'"name": {json.dumps(item["name"])}, '
                f'"type": {json.dumps(item["type"])}, '
                f'"create_sql": {json.dumps(item["create_sql"], ensure_ascii=False)}'
            )
            if item.get("error"):
                yield f', "error": {json.dumps(item["error"])}'
            if data and item["type"] == "TABLE" and not item.get("error"):
                stats: dict[str, Any] = {}
                tableau_ouvert = False
                try:
                    cols = await _table_columns(item["name"])
                    pk = await _primary_key_columns(item["name"]) if limit_per_table else []
                    yield f', "columns": {json.dumps(cols)}, "rows": ['
                    tableau_ouvert = True
                    async for morceau in _flux_rows_json(
                        item["name"], cols, limit=limit_per_table, pk_cols=pk, stats=stats
                    ):
                        yield morceau
                    yield f'], "row_count": {stats.get("rows", 0)}'
                    total_lignes += stats.get("rows", 0)
                except Exception as e:
                    logger.exception("Erreur export données %s", ctx(name=item["name"]))
                    # La sortie doit rester du JSON analysable : on ne referme le
                    # tableau que s'il a effectivement été ouvert (un échec sur la
                    # lecture des colonnes survient avant).
                    if tableau_ouvert:
                        yield f'], "row_count": {stats.get("rows", 0)}'
                        total_lignes += stats.get("rows", 0)
                    yield f', "data_error": {json.dumps(str(e))}'
            yield "}"
        yield "]"
        if data:
            yield (
                ', "note": "Script SQL non inclus avec data=true (utiliser fmt=sql) ; '
                'les lignes sont dans objects[].rows."'
            )
        else:
            # Schéma seul : borné, on peut reconstituer le script complet en mémoire.
            script = [
                f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}`;",
                f"USE `{MYSQL_DATABASE}`;",
                "",
                "SET FOREIGN_KEY_CHECKS = 0;",
                "",
            ]
            for item in items:
                if item.get("error"):
                    continue
                if drop:
                    script.append(f"DROP {item['type']} IF EXISTS `{item['name']}`;")
                script.append(f"{item['create_sql']};")
                script.append("")
            script.append("SET FOREIGN_KEY_CHECKS = 1;")
            yield f', "sql": {json.dumps(chr(10).join(script), ensure_ascii=False)}'
        duree = round(time.perf_counter() - start, 3)
        yield f', "row_count": {total_lignes}, "execution_time_s": {duree}, "complete": true}}'
        logger.info(
            "Fin dump base %s",
            ctx(
                fmt="json",
                objets=len(items),
                en_echec=len(failed),
                lignes=total_lignes,
                duration_s=duree,
            ),
        )

    suffixe = "sql" if fmt == "sql" else "json"
    nom = f"{MYSQL_DATABASE}_dump_{_horodatage()}.{suffixe}"
    if fmt == "json":
        return StreamingResponse(
            _tamponner(_flux_json()),
            media_type="application/json; charset=utf-8",
            headers=_entetes(download, nom),
        )
    return StreamingResponse(
        _tamponner(_flux_sql()),
        media_type="text/plain; charset=utf-8",
        headers=_entetes(download, nom),
    )


@router.get("/export")
async def export_table(
    table: str = Query(..., description="Nom de la table à exporter"),
    fmt: str = Query(
        "json",
        pattern="^(json|sql)$",
        description="Format : 'json' (réinjectable via POST /mysql/import) ou 'sql' (script copiable).",
    ),
    schema: bool = Query(
        False,
        description="Inclure le DDL (CREATE TABLE) de la table, comme /mysql/dump.",
    ),
    data: bool = Query(
        True,
        description="Inclure les données. schema=true&data=false = structure seule.",
    ),
    drop: bool = Query(
        False,
        description="schema=true uniquement : ajoute un DROP TABLE IF EXISTS avant le CREATE.",
    ),
    truncate: bool = Query(
        True,
        description="fmt=sql uniquement : ajoute un TRUNCATE TABLE avant les INSERT.",
    ),
    limit: int | None = Query(
        None,
        ge=1,
        description="Nombre maximal de lignes exportées (défaut : toutes). Ex. 10000, 100000.",
    ),
    after: str | None = Query(
        None,
        description=(
            "Pagination par curseur : n'exporte que les lignes dont la clé primaire est "
            "strictement supérieure à cette valeur. À privilégier sur 'offset' pour "
            "découper une grosse table (voir 'next_after' en fin de réponse)."
        ),
    ),
    offset: int = Query(
        0,
        ge=0,
        description="Lignes à sauter avant l'export. Coûteux sur grosse table : préférer 'after'.",
    ),
    rows_per_insert: int = Query(
        LIGNES_PAR_INSERT,
        ge=1,
        le=5000,
        description="fmt=sql : lignes regroupées par INSERT généré.",
    ),
    download: bool = Query(
        False, description="Renvoyer la réponse en pièce jointe (téléchargement fichier)."
    ),
):
    """Exporte une table : structure et/ou données, dans le même esprit que `/mysql/dump`.

    Les trois combinaisons utiles, symétriques de celles du dump complet :

    - `schema=false&data=true` (défaut) : données seules — comportement historique.
    - `schema=true&data=true`           : `CREATE TABLE` puis `INSERT`, table rechargeable.
    - `schema=true&data=false`          : structure seule.

    Formats :

    - `fmt=sql`  : script text/plain, `INSERT` multi-lignes (cf. `rows_per_insert`),
      directement rejouable dans un client SQL. **C'est le format à utiliser sur les
      grosses tables.**
    - `fmt=json` : payload réinjectable via `POST /mysql/import`. Attention : cet import
      lit le corps entier en mémoire, il ne convient donc qu'aux tables modestes — au-delà,
      passer par `fmt=sql`, ou découper avec `limit` + `after`.

    **Découpage.** `limit` fixe librement la taille d'un lot (10 000, 100 000, 1 000 000 :
    aucun plafond imposé, seul le défaut « tout » change si on ne le passe pas). Pour
    enchaîner les lots, utiliser `after` et non `offset` : chaque réponse indique la
    dernière clé primaire atteinte (`next_after` en JSON, commentaire de reprise en SQL),
    à repasser en `after` pour le lot suivant. Le coût d'un lot reste alors constant, là
    où `offset` fait relire depuis le début à chaque fois. Dès qu'un découpage est demandé
    les lignes sont triées sur la clé primaire, sans quoi deux lots pourraient se
    recouvrir ou sauter des lignes.

    La réponse est streamée : une table de plusieurs dizaines de millions de lignes
    s'exporte sans pic mémoire côté API. Le compte de lignes n'est donc connu qu'à la fin
    (commentaire final en SQL, clé `count` en JSON, après `rows`).

    Les types non sérialisables (datetime, Decimal, bytes) sont normalisés pour un
    aller-retour fidèle (cf. `_serialize_value`).
    """
    start = time.perf_counter()

    if not schema and not data:
        raise HTTPException(
            status_code=400,
            detail="Rien à exporter : activer 'schema' et/ou 'data'.",
        )

    # Contrôles faits avant le premier octet : une fois le flux ouvert, plus aucun
    # code HTTP ne peut être corrigé.
    columns = await _table_columns(table)
    if not columns:
        raise HTTPException(
            status_code=404, detail=f"Table '{table}' introuvable dans {MYSQL_DATABASE}."
        )

    pk_cols = await _primary_key_columns(table)
    if after is not None and len(pk_cols) != 1:
        detail = (
            f"La table `{table}` n'a pas de clé primaire : 'after' est inutilisable, "
            "découper avec 'limit' + 'offset'."
            if not pk_cols
            else (
                f"La clé primaire de `{table}` est composite ({', '.join(pk_cols)}) : "
                "'after' ne sait pas reprendre dessus. Découper avec 'limit' + 'offset'."
            )
        )
        raise HTTPException(status_code=400, detail=detail)
    if (limit is not None or offset) and not pk_cols:
        # Pas bloquant, mais l'appelant doit savoir que ses lots ne sont pas reproductibles.
        logger.warning(
            "Découpage sans clé primaire %s",
            ctx(
                table=table,
                limit=limit,
                offset=offset,
                consequence="ordre non garanti entre deux lots",
            ),
        )

    create_sql = ""
    if schema:
        create_sql, erreur_ddl = await _create_sql(table, is_view=False)
        if erreur_ddl:
            raise HTTPException(
                status_code=500,
                detail=f"Erreur récupération du DDL de {table} : {erreur_ddl}",
            )

    logger.info(
        "Début export table %s",
        ctx(
            table=table,
            fmt=fmt,
            avec_schema=schema,
            avec_donnees=data,
            limit=limit,
            offset=offset,
        ),
    )

    def _log_fin(lignes: int, duree: float, tronque: bool = False) -> None:
        logger.info(
            "Fin export table %s",
            ctx(
                table=table,
                fmt=fmt,
                lignes=lignes,
                tronque=tronque,
                duration_s=duree,
            ),
        )

    async def _flux_sql() -> AsyncIterator[str]:
        stats: dict[str, Any] = {}
        portee = " + ".join(
            p for p, actif in (("schéma", schema), ("données", data)) if actif
        )
        yield f"-- Export de `{MYSQL_DATABASE}`.`{table}` — {portee}\n"
        if limit is not None or offset or after is not None:
            yield (
                f"-- Lot exporté : limit={limit if limit is not None else 'aucune'}"
                f" offset={offset} after={after if after is not None else 'aucun'}\n"
            )
        yield "\nSET FOREIGN_KEY_CHECKS = 0;\nSET UNIQUE_CHECKS = 0;\n\n"

        if schema:
            if drop:
                yield f"DROP TABLE IF EXISTS `{table}`;\n"
            yield f"{create_sql};\n\n"

        if data:
            # Un DROP+CREATE vient de recréer la table vide : le TRUNCATE ferait double emploi.
            if truncate and not (schema and drop):
                yield f"TRUNCATE TABLE `{table}`;\n\n"
            try:
                async for morceau in _flux_inserts(
                    table,
                    columns,
                    limit=limit,
                    offset=offset,
                    pk_cols=pk_cols,
                    after=after,
                    lignes_par_insert=rows_per_insert,
                    stats=stats,
                ):
                    yield morceau
            except Exception as e:
                logger.exception("Erreur export %s", ctx(table=table))
                duree = round(time.perf_counter() - start, 3)
                _log_fin(stats.get("rows", 0), duree, tronque=True)
                yield (
                    f"\n-- !! ERREUR export de `{table}` après "
                    f"{stats.get('rows', 0)} ligne(s) : {e}\n"
                    "-- !! EXPORT INCOMPLET — ne pas recharger en l'état.\n"
                )
                return

        yield "\nSET UNIQUE_CHECKS = 1;\nSET FOREIGN_KEY_CHECKS = 1;\n"
        duree = round(time.perf_counter() - start, 3)
        yield f"-- FIN DE L'EXPORT — {stats.get('rows', 0)} ligne(s), {duree}s\n"
        suite = _reprise(stats, limit)
        if suite is not None:
            yield f"-- Lot suivant : &after={suite}\n"
        _log_fin(stats.get("rows", 0), duree)

    async def _flux_json() -> AsyncIterator[str]:
        stats: dict[str, Any] = {}
        yield (
            "{"
            f'"database": {json.dumps(MYSQL_DATABASE)}, '
            f'"table": {json.dumps(table)}, '
            f'"columns": {json.dumps(columns)}'
        )
        if schema:
            yield f', "create_sql": {json.dumps(create_sql, ensure_ascii=False)}'
        # "rows" est toujours présent (tableau vide si data=false) pour que la sortie
        # reste directement acceptée par POST /mysql/import.
        yield ', "rows": ['
        erreur: str | None = None
        if data:
            try:
                async for morceau in _flux_rows_json(
                    table,
                    columns,
                    limit=limit,
                    offset=offset,
                    pk_cols=pk_cols,
                    after=after,
                    stats=stats,
                ):
                    yield morceau
            except Exception as e:
                logger.exception("Erreur export %s", ctx(table=table))
                erreur = str(e)
        yield "]"
        duree = round(time.perf_counter() - start, 3)
        yield f', "count": {stats.get("rows", 0)}, "execution_time_s": {duree}'
        if erreur:
            yield f', "error": {json.dumps(erreur)}, "complete": false}}'
            _log_fin(stats.get("rows", 0), duree, tronque=True)
            return
        suite = _reprise(stats, limit)
        if suite is not None:
            yield f', "next_after": {json.dumps(suite)}'
        yield ', "complete": true}'
        _log_fin(stats.get("rows", 0), duree)

    nom = f"{table}_{_horodatage()}.{'sql' if fmt == 'sql' else 'json'}"
    if fmt == "sql":
        return StreamingResponse(
            _tamponner(_flux_sql()),
            media_type="text/plain; charset=utf-8",
            headers=_entetes(download, nom),
        )
    return StreamingResponse(
        _tamponner(_flux_json()),
        media_type="application/json; charset=utf-8",
        headers=_entetes(download, nom),
    )


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

    **Limite de volumétrie** : contrairement à l'export, cette route n'est pas streamée —
    FastAPI désérialise le corps entier avant d'entrer ici. Au-delà de quelques centaines
    de milliers de lignes, rapatrier plutôt un `GET /mysql/export?fmt=sql` et le rejouer
    avec un client MySQL, ou découper l'export avec `limit`/`offset`.

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
                        "Rétablissement FOREIGN_KEY_CHECKS impossible %s",
                        ctx(
                            table=table,
                            consequence="connexion probablement rompue",
                        ),
                        exc_info=True,
                    )
    except Exception as e:
        logger.exception("Erreur import %s", ctx(table=table))
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
