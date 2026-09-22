"""Helpers pour la rétention PIC d'un scénario (tables trppu_pic_version / trppu_pic_coefficients)."""

from __future__ import annotations

import logging
from typing import Any

from app.log_utils import ctx
from app.routes.trppu_scenario.helpers import last_insert_id

# Fallback si aucune version PIC nationale par défaut n'est trouvée en base — cf. DSR-660.
DEFAULT_PIC_VERSION = 1

_COEF_COLS = "id_pic_version, co_produit, jour_semaine, densite, coef"

logger = logging.getLogger(__name__)


async def resolve_default_pic_version(db) -> int:
    """id_pic_version du paramétrage par défaut : niveau NATIONAL + est_par_defaut=1.

    Conforme à l'intention de DSR-660 (le défaut n'est pas forcément l'id 1). Fallback sur
    `DEFAULT_PIC_VERSION` si la ligne national/défaut n'existe pas encore.
    """
    row = await db.fetch_one(
        "SELECT id_pic_version FROM trppu_pic_version "
        "WHERE niveau = 'NATIONAL' AND est_par_defaut = 1 "
        "ORDER BY id_pic_version LIMIT 1"
    )
    if row:
        return int(row["id_pic_version"])
    return DEFAULT_PIC_VERSION


async def fetch_coeffs_for_version(db, id_pic_version: int) -> list[dict[str, Any]]:
    return await db.fetch_all(
        f"SELECT {_COEF_COLS} FROM trppu_pic_coefficients WHERE id_pic_version = %s",
        (id_pic_version,),
    )


async def fetch_scenario_pic_version(db, id_scenario: int) -> dict[str, Any] | None:
    """Version PIC propre au scénario (niveau SCENARIO), la plus récente active."""
    return await db.fetch_one(
        "SELECT id_pic_version, niveau FROM trppu_pic_version "
        "WHERE id_scenario = %s AND niveau = 'SCENARIO' "
        "AND (dt_desactivation IS NULL OR dt_desactivation > NOW()) "
        "ORDER BY id_pic_version DESC LIMIT 1",
        (id_scenario,),
    )


def _key(row: dict) -> tuple:
    return (row["co_produit"], row["jour_semaine"], int(row["densite"]))


def merge_coeffs(defaults: list[dict], overrides: list[dict]) -> list[dict]:
    """Fusionne défaut national + surcharge scénario sur (co_produit, jour, densite).

    La surcharge remplace le défaut et marque `modifie=True`.
    """
    merged: dict[tuple, dict] = {}
    for r in defaults:
        merged[_key(r)] = {
            "id_pic_version": int(r["id_pic_version"]),
            "co_produit": r["co_produit"],
            "jour_semaine": r["jour_semaine"],
            "densite": int(r["densite"]),
            "coef": r["coef"],
            "modifie": False,
        }
    for r in overrides:
        merged[_key(r)] = {
            "id_pic_version": int(r["id_pic_version"]),
            "co_produit": r["co_produit"],
            "jour_semaine": r["jour_semaine"],
            "densite": int(r["densite"]),
            "coef": r["coef"],
            "modifie": True,  # surchargé par le scénario (id_pic_version != défaut)
        }
    return sorted(
        merged.values(),
        key=lambda x: (x["co_produit"], x["jour_semaine"], x["densite"]),
    )


# --- Écriture (DSR-661 : une cellule, ou un lot de cellules) -----------------
#
# Les deux endpoints d'écriture partagent la même mécanique : on résout (ou on
# crée) la version PIC propre au scénario, puis on upserte chaque coefficient sur
# sa clé naturelle. Les factoriser garantit qu'un enregistrement multiple produit
# exactement les mêmes lignes qu'une suite d'enregistrements unitaires.

SELECT_VERSION_SCENARIO_SQL = (
    "SELECT id_pic_version FROM trppu_pic_version "
    "WHERE id_scenario = %s AND niveau = 'SCENARIO' "
    "ORDER BY id_pic_version DESC LIMIT 1"
)

INSERT_VERSION_SCENARIO_SQL = (
    "INSERT INTO trppu_pic_version "
    "(lb_pic_version, niveau, co_regate, id_scenario, dt_activation, "
    " id_rh_creation, id_rh_maj) "
    "VALUES (%s, 'SCENARIO', %s, %s, NOW(), %s, %s)"
)

SELECT_COEF_SQL = (
    "SELECT id_pic_coef, coef FROM trppu_pic_coefficients "
    "WHERE id_pic_version = %s AND co_produit = %s "
    "AND jour_semaine = %s AND densite = %s"
)

UPDATE_COEF_SQL = (
    "UPDATE trppu_pic_coefficients "
    "SET coef = %s, dt_maj = NOW(), id_rh = %s WHERE id_pic_coef = %s"
)

INSERT_COEF_SQL = (
    "INSERT INTO trppu_pic_coefficients "
    "(id_pic_version, co_produit, jour_semaine, dt_effet, coef, densite, id_rh) "
    "VALUES (%s, %s, %s, NOW(), %s, %s, %s)"
)


async def ensure_scenario_pic_version(
    tx, id_scenario: int, co_regate: str, id_rh_token: str
) -> tuple[int, bool]:
    """Version PIC du scénario, créée à la volée si elle n'existe pas encore.

    Retourne `(id_pic_version, creee)`. À appeler dans la transaction d'écriture :
    la version créée doit être annulée avec les coefficients si l'un d'eux échoue.
    """
    version = await tx.fetch_one(SELECT_VERSION_SCENARIO_SQL, (id_scenario,))
    if version:
        id_pic_version = int(version["id_pic_version"])
        logger.debug(
            "Version PIC scénario existante %s",
            ctx(id_scenario=id_scenario, id_pic_version=id_pic_version),
        )
        return id_pic_version, False

    await tx.execute(
        INSERT_VERSION_SCENARIO_SQL,
        (
            f"{co_regate}_{id_scenario}",
            co_regate,
            id_scenario,
            id_rh_token,
            id_rh_token,
        ),
    )
    id_pic_version = await last_insert_id(tx)
    logger.info(
        "Version PIC scénario créée %s",
        ctx(id_scenario=id_scenario, id_pic_version=id_pic_version, co_regate=co_regate),
    )
    return id_pic_version, True


async def upsert_coef(tx, id_pic_version: int, item, id_rh_token: str) -> tuple[str, Any, int]:
    """Upsert d'un coefficient sur sa clé naturelle (version, produit, jour, densité).

    `item` : objet exposant `co_produit`, `jour_semaine`, `densite`, `coef`
    (cf. `PicCoefUpsert` / `PicCoefBatchItem`).
    Retourne `("update" | "insert", coef_avant, id_pic_coef)` ; `coef_avant` n'est
    renseigné que sur la branche UPDATE (seule à connaître une valeur antérieure).
    """
    existant = await tx.fetch_one(
        SELECT_COEF_SQL,
        (id_pic_version, item.co_produit, item.jour_semaine, item.densite),
    )
    if existant:
        id_pic_coef = int(existant["id_pic_coef"])
        await tx.execute(UPDATE_COEF_SQL, (item.coef, id_rh_token, id_pic_coef))
        return "update", existant.get("coef"), id_pic_coef

    await tx.execute(
        INSERT_COEF_SQL,
        (
            id_pic_version,
            item.co_produit,
            item.jour_semaine,
            item.coef,
            item.densite,
            id_rh_token,
        ),
    )
    return "insert", None, await last_insert_id(tx)


def dedupliquer_items(items: list) -> list:
    """Une seule écriture par cellule : la dernière valeur reçue l'emporte.

    L'IHM enregistre le tableau entier en fin de saisie ; une même cellule peut y
    figurer deux fois si le front la réémet. Sans ce filtre, le lot ferait un
    INSERT suivi d'un UPDATE et fausserait les compteurs retournés.
    """
    retenus: dict[tuple, Any] = {}
    for it in items:
        retenus[(it.co_produit, it.jour_semaine, int(it.densite))] = it
    return list(retenus.values())
