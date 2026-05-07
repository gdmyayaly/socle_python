"""Routes CRUD et upload Excel pour la table trppu_produit."""

import logging
import time
from datetime import date

from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status

from app.db.mysql import db_read, db_write
from app.log_utils import safe_preview

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
    start = time.perf_counter()
    filters = {"actif_only": actif_only, "limit": limit, "offset": offset}
    logger.info("Début listing des produits (filtres=%s)", safe_preview(filters))

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
        logger.exception(
            "Erreur listing des produits (filtres=%s)", safe_preview(filters)
        )
        raise HTTPException(status_code=500, detail="Erreur listing produits.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Listing des produits terminé (count=%d, duration_ms=%.1f)", len(rows), duration_ms
    )
    return rows


@router.get("/{co_produit}", response_model=ProduitOut)
async def get_produit(co_produit: str):
    start = time.perf_counter()
    logger.info("Début récupération produit (co_produit=%s)", co_produit)

    try:
        row = await db_read.fetch_one(
            SELECT_PRODUIT_SQL + " WHERE co_produit = %s", (co_produit,)
        )
    except Exception as e:
        logger.exception("Erreur récupération produit (co_produit=%s)", co_produit)
        raise HTTPException(status_code=500, detail="Erreur récupération produit.") from e
    if not row:
        logger.info("Produit introuvable (co_produit=%s)", co_produit)
        raise HTTPException(status_code=404, detail=f"Produit {co_produit} introuvable.")

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Récupération produit terminée (co_produit=%s, duration_ms=%.1f)",
        co_produit,
        duration_ms,
    )
    return row


@router.post("", response_model=ProduitOut, status_code=status.HTTP_201_CREATED)
async def create_produit(payload: ProduitCreate):
    start = time.perf_counter()
    logger.info(
        "Début création produit (co_produit=%s, payload=%s)",
        payload.co_produit,
        safe_preview(payload.model_dump(mode="json")),
    )

    existing = await db_read.fetch_one(
        "SELECT co_produit FROM trppu_produit WHERE co_produit = %s",
        (payload.co_produit,),
    )
    if existing:
        logger.info("Produit déjà existant (co_produit=%s)", payload.co_produit)
        raise HTTPException(
            status_code=409, detail=f"Le produit {payload.co_produit} existe déjà."
        )
    logger.info("Vérification doublon OK (co_produit=%s)", payload.co_produit)

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
        logger.exception(
            "Erreur création produit (co_produit=%s, payload=%s)",
            payload.co_produit,
            safe_preview(payload.model_dump(mode="json")),
        )
        raise HTTPException(status_code=500, detail="Erreur création produit.") from e

    created = await db_read.fetch_one(
        SELECT_PRODUIT_SQL + " WHERE co_produit = %s", (payload.co_produit,)
    )
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Création produit terminée (co_produit=%s, duration_ms=%.1f)",
        payload.co_produit,
        duration_ms,
    )
    return created


@router.put("/{co_produit}", response_model=ProduitOut)
async def update_produit(co_produit: str, payload: ProduitUpdate):
    start = time.perf_counter()
    fields = payload.model_dump(exclude_unset=True)
    logger.info(
        "Début mise à jour produit (co_produit=%s, champs=%s)",
        co_produit,
        safe_preview(fields),
    )

    if not fields:
        raise HTTPException(status_code=400, detail="Aucun champ à mettre à jour.")

    existing = await db_read.fetch_one(
        "SELECT co_produit FROM trppu_produit WHERE co_produit = %s", (co_produit,)
    )
    if not existing:
        logger.info("Produit introuvable pour mise à jour (co_produit=%s)", co_produit)
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
        logger.exception(
            "Erreur mise à jour produit (co_produit=%s, champs=%s)",
            co_produit,
            safe_preview(fields),
        )
        raise HTTPException(status_code=500, detail="Erreur mise à jour produit.") from e

    updated = await db_read.fetch_one(
        SELECT_PRODUIT_SQL + " WHERE co_produit = %s", (co_produit,)
    )
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Mise à jour produit terminée (co_produit=%s, nb_champs=%d, duration_ms=%.1f)",
        co_produit,
        len(fields),
        duration_ms,
    )
    return updated


@router.delete("/{co_produit}", response_model=SoftDeleteResult)
async def soft_delete_produit(
    co_produit: str,
    motif: str = Query("Désactivé via API", max_length=255),
):
    """Soft delete : positionne dt_desactivation = aujourd'hui + motif_desactivation."""
    start = time.perf_counter()
    logger.info(
        "Début désactivation produit (co_produit=%s, motif=%s)",
        co_produit,
        safe_preview(motif),
    )

    existing = await db_read.fetch_one(
        "SELECT co_produit FROM trppu_produit WHERE co_produit = %s", (co_produit,)
    )
    if not existing:
        logger.info(
            "Produit introuvable pour désactivation (co_produit=%s)", co_produit
        )
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
        logger.exception(
            "Erreur désactivation produit (co_produit=%s, motif=%s)",
            co_produit,
            safe_preview(motif),
        )
        raise HTTPException(status_code=500, detail="Erreur désactivation produit.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Désactivation produit terminée (co_produit=%s, dt_desactivation=%s, duration_ms=%.1f)",
        co_produit,
        today,
        duration_ms,
    )
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
    logger.info("Début upload Excel produits (fichier=%s)", file.filename)

    if not file.filename or not file.filename.lower().endswith((".xlsx", ".xlsm")):
        raise HTTPException(
            status_code=400,
            detail="Format de fichier non supporté : attendu .xlsx ou .xlsm.",
        )

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Fichier vide.")
    logger.info(
        "Fichier Excel produits lu (fichier=%s, taille=%d octets)",
        file.filename,
        len(content),
    )

    try:
        produits, errors = parse_excel_produits(content)
    except ValueError as e:
        logger.info(
            "Excel produits invalide (fichier=%s, raison=%s)",
            file.filename,
            safe_preview(str(e), max_len=200),
        )
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception(
            "Erreur parsing Excel produits (fichier=%s, taille=%d)",
            file.filename,
            len(content),
        )
        raise HTTPException(status_code=500, detail="Erreur lecture Excel.") from e
    logger.info(
        "Excel produits parsé (fichier=%s, valides=%d, erreurs=%d)",
        file.filename,
        len(produits),
        len(errors),
    )

    nb_inserted = 0
    nb_updated = 0
    nb_unchanged = 0

    if produits:
        logger.info(
            "Ouverture transaction upsert produits (fichier=%s, taille_lot=%d)",
            file.filename,
            len(produits),
        )
        try:
            async with db_write.transaction() as tx:
                for excel_row, p in enumerate(produits, start=2):
                    try:
                        rc = await tx.execute(UPSERT_SQL, produit_to_upsert_params(p))
                    except Exception:
                        logger.exception(
                            "Échec UPSERT trppu_produit (fichier=%s, ligne_excel=%d, payload=%s)",
                            file.filename,
                            excel_row,
                            safe_preview(p.model_dump(mode="json")),
                        )
                        raise
                    if rc == 1:
                        nb_inserted += 1
                    elif rc == 2:
                        nb_updated += 1
                    else:
                        nb_unchanged += 1
        except Exception as e:
            logger.exception(
                "Erreur upsert lot trppu_produit (fichier=%s, taille_lot=%d)",
                file.filename,
                len(produits),
            )
            raise HTTPException(
                status_code=500,
                detail=f"Échec de l'écriture du lot en base : {e}",
            ) from e

    duration_s = round(time.perf_counter() - start, 3)
    logger.info(
        "Upload Excel produits terminé (fichier=%s, insérés=%d, modifiés=%d, inchangés=%d, erreurs=%d, duration_s=%.3f)",
        file.filename,
        nb_inserted,
        nb_updated,
        nb_unchanged,
        len(errors),
        duration_s,
    )
    return BulkUploadResult(
        nb_rows_read=len(produits) + len(errors),
        nb_inserted=nb_inserted,
        nb_updated=nb_updated,
        nb_unchanged=nb_unchanged,
        nb_errors=len(errors),
        errors=errors,
        execution_time_s=duration_s,
    )
