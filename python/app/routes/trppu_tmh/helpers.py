"""Helpers pour le module TMH (table trppu_tmh).

Depuis la migration du 24/06/2026, un même `co_produit` peut exister plusieurs
fois pour un scénario (uq_tmh = id_tmh, id_scenario, co_produit). La clé d'une
ligne est donc `id_tmh`, pas `co_produit`.
"""

from __future__ import annotations

from typing import Any

_TMH_COLS = (
    "id_tmh, co_produit, volume_realise, volume_previsionnel, "
    "volume_previsionnel_recalcule, "
    "moyenne_journaliere, moyenne_hebdo, bl_exclu, bl_manuel, motif"
)
SELECT_TMH_SQL = (
    f"SELECT {_TMH_COLS} FROM trppu_tmh WHERE id_scenario = %s ORDER BY co_produit, id_tmh"
)
SELECT_TMH_BY_ID_SQL = (
    f"SELECT {_TMH_COLS} FROM trppu_tmh WHERE id_tmh = %s AND id_scenario = %s"
)


async def fetch_tmh(db_read, id_scenario: int) -> list[dict[str, Any]]:
    """Toutes les lignes TMH d'un scénario (plusieurs lignes possibles par produit)."""
    return await db_read.fetch_all(SELECT_TMH_SQL, (id_scenario,))


async def insert_tmh_row(
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
    motif: str | None = None,
    id_rh: str | None = None,
    volume_previsionnel_recalcule: int | None = None,
) -> int:
    """INSERT d'une nouvelle ligne TMH. Retourne l'`id_tmh` généré.

    Met dt_calcul = NOW(). Un même produit peut être inséré plusieurs fois.
    `id_rh` : token déjà crypté de l'utilisateur.
    `volume_previsionnel_recalcule` : prévisionnel après variation % (calcul
    front). Absent (None) => réaligné sur `volume_previsionnel` (valeur de base).
    """
    vpr = (
        volume_previsionnel_recalcule
        if volume_previsionnel_recalcule is not None
        else volume_previsionnel
    )
    await tx.execute(
        "INSERT INTO trppu_tmh "
        "(id_scenario, co_produit, volume_realise, volume_previsionnel, "
        " volume_previsionnel_recalcule, "
        " moyenne_journaliere, moyenne_hebdo, dt_calcul, bl_exclu, bl_manuel, motif, id_rh) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s)",
        (
            id_scenario,
            co_produit,
            volume_realise,
            volume_previsionnel,
            vpr,
            moyenne_journaliere,
            moyenne_hebdo,
            1 if bl_exclu else 0,
            1 if bl_manuel else 0,
            motif,
            id_rh,
        ),
    )
    row = await tx.fetch_one("SELECT LAST_INSERT_ID() AS id")
    return int(row["id"])


async def update_tmh_row(
    tx,
    id_scenario: int,
    id_tmh: int,
    *,
    co_produit: str,
    volume_realise: int | None,
    volume_previsionnel: int | None,
    moyenne_journaliere,
    moyenne_hebdo,
    bl_exclu: bool,
    bl_manuel: bool = False,
    motif: str | None = None,
    id_rh: str | None = None,
    volume_previsionnel_recalcule: int | None = None,
) -> bool:
    """UPDATE la ligne (id_tmh, id_scenario) si elle existe. Retourne True si trouvée.

    La ligne est identifiée par `id_tmh` et bornée au `id_scenario` (contrôle
    d'appartenance). Met dt_calcul = NOW().
    `volume_previsionnel_recalcule` : absent (None) => réaligné sur
    `volume_previsionnel` ; fourni (pipeline variations) => stocké tel quel.
    """
    existing = await tx.fetch_one(
        "SELECT id_tmh FROM trppu_tmh WHERE id_tmh = %s AND id_scenario = %s",
        (id_tmh, id_scenario),
    )
    if not existing:
        return False
    vpr = (
        volume_previsionnel_recalcule
        if volume_previsionnel_recalcule is not None
        else volume_previsionnel
    )
    await tx.execute(
        "UPDATE trppu_tmh SET co_produit = %s, volume_realise = %s, volume_previsionnel = %s, "
        "volume_previsionnel_recalcule = %s, "
        "moyenne_journaliere = %s, moyenne_hebdo = %s, bl_exclu = %s, "
        "bl_manuel = %s, motif = %s, id_rh = %s, dt_calcul = NOW() "
        "WHERE id_tmh = %s AND id_scenario = %s",
        (
            co_produit,
            volume_realise,
            volume_previsionnel,
            vpr,
            moyenne_journaliere,
            moyenne_hebdo,
            1 if bl_exclu else 0,
            1 if bl_manuel else 0,
            motif,
            id_rh,
            id_tmh,
            id_scenario,
        ),
    )
    return True


async def upsert_tmh_row(
    tx,
    id_scenario: int,
    *,
    id_tmh: int | None = None,
    co_produit: str,
    volume_realise: int | None,
    volume_previsionnel: int | None,
    moyenne_journaliere,
    moyenne_hebdo,
    bl_exclu: bool,
    bl_manuel: bool = False,
    motif: str | None = None,
    id_rh: str | None = None,
    volume_previsionnel_recalcule: int | None = None,
) -> str:
    """MAJ la ligne `id_tmh` (si fournie et existante pour le scénario), sinon INSERT.

    Retourne 'updated' ou 'inserted'.

    `id_tmh` : présent → MAJ de cette ligne ; absent (ou inconnu pour le scénario)
    → insertion d'une nouvelle ligne. Permet plusieurs lignes par produit.
    `bl_manuel` : ligne saisie manuellement (True) ou issue d'un calcul (False).
    `motif` : justification libre d'une modif manuelle / exclusion.
    `id_rh` : token déjà crypté de l'utilisateur.
    """
    if id_tmh is not None:
        updated = await update_tmh_row(
            tx,
            id_scenario,
            id_tmh,
            co_produit=co_produit,
            volume_realise=volume_realise,
            volume_previsionnel=volume_previsionnel,
            moyenne_journaliere=moyenne_journaliere,
            moyenne_hebdo=moyenne_hebdo,
            bl_exclu=bl_exclu,
            bl_manuel=bl_manuel,
            motif=motif,
            id_rh=id_rh,
            volume_previsionnel_recalcule=volume_previsionnel_recalcule,
        )
        if updated:
            return "updated"

    await insert_tmh_row(
        tx,
        id_scenario,
        co_produit=co_produit,
        volume_realise=volume_realise,
        volume_previsionnel=volume_previsionnel,
        moyenne_journaliere=moyenne_journaliere,
        moyenne_hebdo=moyenne_hebdo,
        bl_exclu=bl_exclu,
        bl_manuel=bl_manuel,
        motif=motif,
        id_rh=id_rh,
        volume_previsionnel_recalcule=volume_previsionnel_recalcule,
    )
    return "inserted"


async def upsert_tmh_rows(
    tx, id_scenario: int, items: list, id_rh: str | None = None
) -> tuple[int, int]:
    """Upsert d'un lot d'items TMH. Retourne (nb_inserted, nb_updated).

    `items` : liste d'objets exposant co_produit, volume_realise, volume_previsionnel,
    moyenne_journaliere, moyenne_hebdo, exclusion et (optionnels) manuel, motif,
    id_tmh (cf. TmhUpsert). Chaque item portant un `id_tmh` existant est mis à jour ;
    les autres sont insérés (un produit peut donc apparaître plusieurs fois).
    `id_rh` : token déjà crypté de l'utilisateur, appliqué à toutes les lignes du lot.
    Réutilisé par la création de scénario (DSR-634) et la MAJ TMH (DSR-659).
    """
    nb_inserted = 0
    nb_updated = 0
    for it in items:
        action = await upsert_tmh_row(
            tx,
            id_scenario,
            id_tmh=getattr(it, "id_tmh", None),
            co_produit=it.co_produit,
            volume_realise=it.volume_realise,
            volume_previsionnel=it.volume_previsionnel,
            moyenne_journaliere=it.moyenne_journaliere,
            moyenne_hebdo=it.moyenne_hebdo,
            bl_exclu=it.exclusion,
            bl_manuel=getattr(it, "manuel", False),
            motif=getattr(it, "motif", None),
            id_rh=id_rh,
            volume_previsionnel_recalcule=getattr(
                it, "volume_previsionnel_recalcule", None
            ),
        )
        if action == "inserted":
            nb_inserted += 1
        else:
            nb_updated += 1
    return nb_inserted, nb_updated
