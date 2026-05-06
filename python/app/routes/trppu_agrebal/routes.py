"""Routes CRUD et upload Excel pour la table trppu_agrebal."""

import logging
import time

from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status

from app.db.mysql import db_read, db_write

from .helpers import UPSERT_SQL, agrebal_to_upsert_params, parse_excel_agrebals
from .schemas import (
    AgrebalCreate,
    AgrebalOut,
    AgrebalUpdate,
    BulkUploadError,
    BulkUploadResult,
    SoftDeleteResult,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/agrebals", tags=["Agrebals"])


SELECT_AGREBAL_SQL = (
    "SELECT id_agrebal, co_regate, lb_agrebal, est_actif, dt_maj FROM trppu_agrebal"
)


async def _site_exists(co_regate: str) -> bool:
    row = await db_read.fetch_one(
        "SELECT 1 AS ok FROM trppu_site WHERE co_regate = %s", (co_regate,)
    )
    return row is not None


@router.get("", response_model=list[AgrebalOut])
async def list_agrebals(
    co_regate: str | None = Query(None, min_length=6, max_length=6),
    est_actif: bool | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    where: list[str] = []
    params: list = []
    if co_regate is not None:
        where.append("co_regate = %s")
        params.append(co_regate)
    if est_actif is not None:
        where.append("est_actif = %s")
        params.append(1 if est_actif else 0)

    sql = SELECT_AGREBAL_SQL
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id_agrebal LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    try:
        return await db_read.fetch_all(sql, tuple(params))
    except Exception as e:
        logger.error("Erreur listing agrebals : %s", e)
        raise HTTPException(status_code=500, detail="Erreur listing agrebals.") from e


@router.get("/{id_agrebal}", response_model=AgrebalOut)
async def get_agrebal(id_agrebal: int):
    try:
        row = await db_read.fetch_one(
            SELECT_AGREBAL_SQL + " WHERE id_agrebal = %s", (id_agrebal,)
        )
    except Exception as e:
        logger.error("Erreur get agrebal %d : %s", id_agrebal, e)
        raise HTTPException(status_code=500, detail="Erreur récupération amas.") from e
    if not row:
        raise HTTPException(status_code=404, detail=f"Amas {id_agrebal} introuvable.")
    return row


@router.post("", response_model=AgrebalOut, status_code=status.HTTP_201_CREATED)
async def create_agrebal(payload: AgrebalCreate):
    if not await _site_exists(payload.co_regate):
        raise HTTPException(
            status_code=422,
            detail=f"Site parent {payload.co_regate} inexistant dans trppu_site.",
        )
    existing = await db_read.fetch_one(
        "SELECT id_agrebal FROM trppu_agrebal WHERE id_agrebal = %s", (payload.id_agrebal,)
    )
    if existing:
        raise HTTPException(
            status_code=409, detail=f"L'amas {payload.id_agrebal} existe déjà."
        )

    try:
        await db_write.execute(
            "INSERT INTO trppu_agrebal (id_agrebal, co_regate, lb_agrebal, est_actif) "
            "VALUES (%s, %s, %s, %s)",
            (payload.id_agrebal, payload.co_regate, payload.lb_agrebal, 1 if payload.est_actif else 0),
        )
    except Exception as e:
        logger.error("Erreur création agrebal %d : %s", payload.id_agrebal, e)
        raise HTTPException(status_code=500, detail="Erreur création amas.") from e

    return await db_read.fetch_one(
        SELECT_AGREBAL_SQL + " WHERE id_agrebal = %s", (payload.id_agrebal,)
    )


@router.put("/{id_agrebal}", response_model=AgrebalOut)
async def update_agrebal(id_agrebal: int, payload: AgrebalUpdate):
    fields = payload.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(status_code=400, detail="Aucun champ à mettre à jour.")

    existing = await db_read.fetch_one(
        "SELECT id_agrebal FROM trppu_agrebal WHERE id_agrebal = %s", (id_agrebal,)
    )
    if not existing:
        raise HTTPException(status_code=404, detail=f"Amas {id_agrebal} introuvable.")

    if "co_regate" in fields and not await _site_exists(fields["co_regate"]):
        raise HTTPException(
            status_code=422,
            detail=f"Site parent {fields['co_regate']} inexistant dans trppu_site.",
        )

    set_parts: list[str] = []
    params: list = []
    for key, value in fields.items():
        set_parts.append(f"{key} = %s")
        if key == "est_actif":
            params.append(1 if value else 0)
        else:
            params.append(value)
    params.append(id_agrebal)

    try:
        await db_write.execute(
            f"UPDATE trppu_agrebal SET {', '.join(set_parts)} WHERE id_agrebal = %s",
            tuple(params),
        )
    except Exception as e:
        logger.error("Erreur update agrebal %d : %s", id_agrebal, e)
        raise HTTPException(status_code=500, detail="Erreur mise à jour amas.") from e

    return await db_read.fetch_one(
        SELECT_AGREBAL_SQL + " WHERE id_agrebal = %s", (id_agrebal,)
    )


@router.delete("/{id_agrebal}", response_model=SoftDeleteResult)
async def soft_delete_agrebal(id_agrebal: int):
    """Soft delete : passe est_actif à 0."""
    existing = await db_read.fetch_one(
        "SELECT id_agrebal FROM trppu_agrebal WHERE id_agrebal = %s", (id_agrebal,)
    )
    if not existing:
        raise HTTPException(status_code=404, detail=f"Amas {id_agrebal} introuvable.")

    try:
        rows_affected = await db_write.execute(
            "UPDATE trppu_agrebal SET est_actif = 0 WHERE id_agrebal = %s", (id_agrebal,)
        )
    except Exception as e:
        logger.error("Erreur soft delete agrebal %d : %s", id_agrebal, e)
        raise HTTPException(status_code=500, detail="Erreur désactivation amas.") from e

    return SoftDeleteResult(id_agrebal=id_agrebal, est_actif=0, rows_affected=rows_affected)


@router.post("/upload-excel", response_model=BulkUploadResult)
async def upload_excel(file: UploadFile = File(..., description="Fichier .xlsx")):
    """Upload massif via Excel : upsert + pré-check FK trppu_site."""
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
        agrebals, errors = parse_excel_agrebals(content)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("Erreur parsing Excel agrebals : %s", e)
        raise HTTPException(status_code=500, detail="Erreur lecture Excel.") from e

    valid_agrebals: list = []
    if agrebals:
        wanted = {a.co_regate for a in agrebals}
        existing_rows = await db_read.fetch_all(
            f"SELECT co_regate FROM trppu_site WHERE co_regate IN ({','.join(['%s'] * len(wanted))})",
            tuple(wanted),
        )
        existing_sites = {r["co_regate"] for r in existing_rows}
        for i, a in enumerate(agrebals, start=2):
            if a.co_regate not in existing_sites:
                errors.append(
                    BulkUploadError(
                        row=i,
                        error=f"Site parent {a.co_regate} inexistant dans trppu_site",
                        raw=a.model_dump(),
                    )
                )
            else:
                valid_agrebals.append(a)

    nb_inserted = 0
    nb_updated = 0
    nb_unchanged = 0

    if valid_agrebals:
        try:
            async with db_write.transaction() as tx:
                for a in valid_agrebals:
                    rc = await tx.execute(UPSERT_SQL, agrebal_to_upsert_params(a))
                    if rc == 1:
                        nb_inserted += 1
                    elif rc == 2:
                        nb_updated += 1
                    else:
                        nb_unchanged += 1
        except Exception as e:
            logger.error("Erreur upsert lot trppu_agrebal : %s", e)
            raise HTTPException(
                status_code=500,
                detail=f"Échec de l'écriture du lot en base : {e}",
            ) from e

    duration_s = round(time.perf_counter() - start, 3)
    return BulkUploadResult(
        nb_rows_read=len(agrebals) + len([e for e in errors if e.row > 0]),
        nb_inserted=nb_inserted,
        nb_updated=nb_updated,
        nb_unchanged=nb_unchanged,
        nb_errors=len(errors),
        errors=errors,
        execution_time_s=duration_s,
    )
