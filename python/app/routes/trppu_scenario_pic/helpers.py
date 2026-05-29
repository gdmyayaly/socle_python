"""Helpers pour la rétention PIC d'un scénario (tables trppu_pic_version / trppu_pic_coefficients)."""

from __future__ import annotations

from typing import Any

# Version PIC par défaut (national) — cf. DSR-660.
DEFAULT_PIC_VERSION = 1

_COEF_COLS = "co_produit, jour_semaine, densite, coef"


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
            "co_produit": r["co_produit"],
            "jour_semaine": r["jour_semaine"],
            "densite": int(r["densite"]),
            "coef": r["coef"],
            "modifie": False,
        }
    for r in overrides:
        merged[_key(r)] = {
            "co_produit": r["co_produit"],
            "jour_semaine": r["jour_semaine"],
            "densite": int(r["densite"]),
            "coef": r["coef"],
            "modifie": True,
        }
    return sorted(
        merged.values(),
        key=lambda x: (x["co_produit"], x["jour_semaine"], x["densite"]),
    )
