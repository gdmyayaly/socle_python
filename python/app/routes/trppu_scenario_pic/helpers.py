"""Helpers pour la rétention PIC d'un scénario (tables trppu_pic_version / trppu_pic_coefficients)."""

from __future__ import annotations

from typing import Any

# Fallback si aucune version PIC nationale par défaut n'est trouvée en base — cf. DSR-660.
DEFAULT_PIC_VERSION = 1

_COEF_COLS = "id_pic_version, co_produit, jour_semaine, densite, coef"


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
