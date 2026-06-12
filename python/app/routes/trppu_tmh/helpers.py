"""Helpers pour le module TMH (table trppu_tmh)."""

from __future__ import annotations

from typing import Any

_TMH_COLS = (
    "co_produit, volume_realise, volume_previsionnel, "
    "moyenne_journaliere, moyenne_hebdo, bl_exclu, bl_manuel"
)
SELECT_TMH_SQL = f"SELECT {_TMH_COLS} FROM trppu_tmh WHERE id_scenario = %s ORDER BY co_produit"
SELECT_TMH_ONE_SQL = (
    f"SELECT {_TMH_COLS} FROM trppu_tmh WHERE id_scenario = %s AND co_produit = %s"
)


async def fetch_tmh(db_read, id_scenario: int) -> list[dict[str, Any]]:
    """Toutes les lignes TMH d'un scénario."""
    return await db_read.fetch_all(SELECT_TMH_SQL, (id_scenario,))


async def upsert_tmh_row(
    tx,
    id_scenario: int,
    *,
    co_produit: str,
    volume_realise: int | None,
    volume_previsionnel: int | None,
    moyenne_journaliere,
    moyenne_hebdo,
    bl_exclu: bool,
    bl_manuel: bool = False,
    id_rh: str | None = None,
) -> str:
    """UPDATE la ligne (id_scenario, co_produit) si elle existe, sinon INSERT.

    Retourne 'updated' ou 'inserted'. Met dt_calcul = NOW() dans les deux cas.
    Pas de dépendance à une contrainte d'unicité (compatible schéma existant).

    `bl_manuel` : ligne saisie manuellement (True) ou issue d'un calcul (False).
    `id_rh` : token déjà crypté de l'utilisateur (None = inchangé / non renseigné).
    """
    existing = await tx.fetch_one(
        "SELECT id_tmh FROM trppu_tmh WHERE id_scenario = %s AND co_produit = %s",
        (id_scenario, co_produit),
    )
    if existing:
        await tx.execute(
            "UPDATE trppu_tmh SET volume_realise = %s, volume_previsionnel = %s, "
            "moyenne_journaliere = %s, moyenne_hebdo = %s, bl_exclu = %s, "
            "bl_manuel = %s, id_rh = %s, dt_calcul = NOW() "
            "WHERE id_scenario = %s AND co_produit = %s",
            (
                volume_realise,
                volume_previsionnel,
                moyenne_journaliere,
                moyenne_hebdo,
                1 if bl_exclu else 0,
                1 if bl_manuel else 0,
                id_rh,
                id_scenario,
                co_produit,
            ),
        )
        return "updated"

    await tx.execute(
        "INSERT INTO trppu_tmh "
        "(id_scenario, co_produit, volume_realise, volume_previsionnel, "
        " moyenne_journaliere, moyenne_hebdo, dt_calcul, bl_exclu, bl_manuel, id_rh) "
        "VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s)",
        (
            id_scenario,
            co_produit,
            volume_realise,
            volume_previsionnel,
            moyenne_journaliere,
            moyenne_hebdo,
            1 if bl_exclu else 0,
            1 if bl_manuel else 0,
            id_rh,
        ),
    )
    return "inserted"


async def upsert_tmh_rows(
    tx, id_scenario: int, items: list, id_rh: str | None = None
) -> tuple[int, int]:
    """Upsert d'un lot d'items TMH. Retourne (nb_inserted, nb_updated).

    `items` : liste d'objets exposant co_produit, volume_realise, volume_previsionnel,
    moyenne_journaliere, moyenne_hebdo, exclusion et (optionnel) manuel (cf. TmhUpsert).
    `id_rh` : token déjà crypté de l'utilisateur, appliqué à toutes les lignes du lot.
    Réutilisé par la création de scénario (DSR-634) et la MAJ TMH (DSR-659).
    """
    nb_inserted = 0
    nb_updated = 0
    for it in items:
        action = await upsert_tmh_row(
            tx,
            id_scenario,
            co_produit=it.co_produit,
            volume_realise=it.volume_realise,
            volume_previsionnel=it.volume_previsionnel,
            moyenne_journaliere=it.moyenne_journaliere,
            moyenne_hebdo=it.moyenne_hebdo,
            bl_exclu=it.exclusion,
            bl_manuel=getattr(it, "manuel", False),
            id_rh=id_rh,
        )
        if action == "inserted":
            nb_inserted += 1
        else:
            nb_updated += 1
    return nb_inserted, nb_updated
