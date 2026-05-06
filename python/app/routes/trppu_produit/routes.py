"""Routes CRUD et upload Excel pour la table trppu_produit."""

import logging
import time
from datetime import date

from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status

from app.db.mysql import db_read, db_write

from .helpers import UPSERT_SQL, parse_excel_produits, produit_to_upsert_params
from .schemas import (
    BulkUploadResult,
    ProduitCreate,
    ProduitOut,
    ProduitUpdate,
    SoftDeleteResult,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/produits", tags=["Produits"])


SELECT_PRODUIT_SQL = (
    "SELECT co_produit, lb_produit, dt_creation, dt_desactivation, motif_desactivation "
    "FROM trppu_produit"
)


@router.get("", response_model=list[ProduitOut])
async def list_produits(
    actif_only: bool = Query(False, description="Si true, n'inclut pas les produits désactivés."),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Liste paginée des produits."""
    where: list[str] = []
    params: list = []
    if actif_only:
        where.append("(dt_desactivation IS NULL OR dt_desactivation > CURDATE())")

    sql = SELECT_PRODUIT_SQL
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY co_produit LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    try:
        rows = await db_read.fetch_all(sql, tuple(params))
    except Exception as e:
        logger.error("Erreur listing produits : %s", e)
        raise HTTPException(status_code=500, detail="Erreur listing produits.") from e
    return rows


@router.get("/{co_produit}", response_model=ProduitOut)
async def get_produit(co_produit: str):
    try:
        row = await db_read.fetch_one(
            SELECT_PRODUIT_SQL + " WHERE co_produit = %s", (co_produit,)
        )
    except Exception as e:
        logger.error("Erreur get produit %s : %s", co_produit, e)
        raise HTTPException(status_code=500, detail="Erreur récupération produit.") from e
    if not row:
        raise HTTPException(status_code=404, detail=f"Produit {co_produit} introuvable.")
    return row


@router.post("", response_model=ProduitOut, status_code=status.HTTP_201_CREATED)
async def create_produit(payload: ProduitCreate):
    existing = await db_read.fetch_one(
        "SELECT co_produit FROM trppu_produit WHERE co_produit = %s",
        (payload.co_produit,),
    )
    if existing:
        raise HTTPException(
            status_code=409, detail=f"Le produit {payload.co_produit} existe déjà."
        )

    try:
        await db_write.execute(
            "INSERT INTO trppu_produit "
            "(co_produit, lb_produit, dt_desactivation, motif_desactivation) "
            "VALUES (%s, %s, %s, %s)",
            (
                payload.co_produit,
                payload.lb_produit,
                payload.dt_desactivation,
                payload.motif_desactivation,
            ),
        )
    except Exception as e:
        logger.error("Erreur création produit %s : %s", payload.co_produit, e)
        raise HTTPException(status_code=500, detail="Erreur création produit.") from e

    return await db_read.fetch_one(
        SELECT_PRODUIT_SQL + " WHERE co_produit = %s", (payload.co_produit,)
    )


@router.put("/{co_produit}", response_model=ProduitOut)
async def update_produit(co_produit: str, payload: ProduitUpdate):
    fields = payload.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(status_code=400, detail="Aucun champ à mettre à jour.")

    existing = await db_read.fetch_one(
        "SELECT co_produit FROM trppu_produit WHERE co_produit = %s", (co_produit,)
    )
    if not existing:
        raise HTTPException(status_code=404, detail=f"Produit {co_produit} introuvable.")

    set_parts: list[str] = []
    params: list = []
    for key, value in fields.items():
        set_parts.append(f"{key} = %s")
        params.append(value)
    params.append(co_produit)

    try:
        await db_write.execute(
            f"UPDATE trppu_produit SET {', '.join(set_parts)} WHERE co_produit = %s",
            tuple(params),
        )
    except Exception as e:
        logger.error("Erreur update produit %s : %s", co_produit, e)
        raise HTTPException(status_code=500, detail="Erreur mise à jour produit.") from e

    return await db_read.fetch_one(
        SELECT_PRODUIT_SQL + " WHERE co_produit = %s", (co_produit,)
    )


@router.delete("/{co_produit}", response_model=SoftDeleteResult)
async def soft_delete_produit(
    co_produit: str,
    motif: str = Query("Désactivé via API", max_length=255),
):
    """Soft delete : positionne dt_desactivation = aujourd'hui + motif_desactivation."""
    existing = await db_read.fetch_one(
        "SELECT co_produit FROM trppu_produit WHERE co_produit = %s", (co_produit,)
    )
    if not existing:
        raise HTTPException(status_code=404, detail=f"Produit {co_produit} introuvable.")

    today = date.today()
    try:
        rows_affected = await db_write.execute(
            "UPDATE trppu_produit "
            "SET dt_desactivation = %s, motif_desactivation = %s "
            "WHERE co_produit = %s",
            (today, motif, co_produit),
        )
    except Exception as e:
        logger.error("Erreur soft delete produit %s : %s", co_produit, e)
        raise HTTPException(status_code=500, detail="Erreur désactivation produit.") from e

    return SoftDeleteResult(
        co_produit=co_produit,
        dt_desactivation=today,
        motif_desactivation=motif,
        rows_affected=rows_affected,
    )


@router.post("/upload-excel", response_model=BulkUploadResult)
async def upload_excel(file: UploadFile = File(..., description="Fichier .xlsx")):
    """Upload massif via Excel : upsert (INSERT ... ON DUPLICATE KEY UPDATE)."""
    start = time.perf_counter()

    if not file.filename or not file.filename.lower().endswith((".xlsx", ".xlsm")):
        raise HTTPException(
            status_code=400,
            detail="Format de fichier non supporté : attendu .xlsx ou .xlsm.",
        )

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Fichier vide.")

    try:
        produits, errors = parse_excel_produits(content)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("Erreur parsing Excel produits : %s", e)
        raise HTTPException(status_code=500, detail="Erreur lecture Excel.") from e

    nb_inserted = 0
    nb_updated = 0
    nb_unchanged = 0

    if produits:
        try:
            async with db_write.transaction() as tx:
                for p in produits:
                    rc = await tx.execute(UPSERT_SQL, produit_to_upsert_params(p))
                    if rc == 1:
                        nb_inserted += 1
                    elif rc == 2:
                        nb_updated += 1
                    else:
                        nb_unchanged += 1
        except Exception as e:
            logger.error("Erreur upsert lot trppu_produit : %s", e)
            raise HTTPException(
                status_code=500,
                detail=f"Échec de l'écriture du lot en base : {e}",
            ) from e

    duration_s = round(time.perf_counter() - start, 3)
    return BulkUploadResult(
        nb_rows_read=len(produits) + len(errors),
        nb_inserted=nb_inserted,
        nb_updated=nb_updated,
        nb_unchanged=nb_unchanged,
        nb_errors=len(errors),
        errors=errors,
        execution_time_s=duration_s,
    )
