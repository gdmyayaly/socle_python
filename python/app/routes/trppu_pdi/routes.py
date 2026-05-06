"""Routes CRUD et upload Excel pour la table trppu_pdi."""

import logging
import time

from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status

from app.db.mysql import db_read, db_write

from .helpers import UPSERT_SQL, parse_excel_pdis, pdi_to_upsert_params
from .schemas import (
    BulkUploadError,
    BulkUploadResult,
    PdiCreate,
    PdiOut,
    PdiUpdate,
    SoftDeleteResult,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/pdis", tags=["PDI"])


SELECT_PDI_SQL = (
    "SELECT id_pdi, co_regate, lb_pdi, est_actif, dt_maj FROM trppu_pdi"
)


async def _site_exists(co_regate: str) -> bool:
    row = await db_read.fetch_one(
        "SELECT 1 AS ok FROM trppu_site WHERE co_regate = %s", (co_regate,)
    )
    return row is not None


@router.get("", response_model=list[PdiOut])
async def list_pdis(
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

    sql = SELECT_PDI_SQL
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id_pdi LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    try:
        return await db_read.fetch_all(sql, tuple(params))
    except Exception as e:
        logger.error("Erreur listing pdis : %s", e)
        raise HTTPException(status_code=500, detail="Erreur listing pdis.") from e


@router.get("/{id_pdi}", response_model=PdiOut)
async def get_pdi(id_pdi: int):
    try:
        row = await db_read.fetch_one(
            SELECT_PDI_SQL + " WHERE id_pdi = %s", (id_pdi,)
        )
    except Exception as e:
        logger.error("Erreur get pdi %d : %s", id_pdi, e)
        raise HTTPException(status_code=500, detail="Erreur récupération PDI.") from e
    if not row:
        raise HTTPException(status_code=404, detail=f"PDI {id_pdi} introuvable.")
    return row


@router.post("", response_model=PdiOut, status_code=status.HTTP_201_CREATED)
async def create_pdi(payload: PdiCreate):
    if not await _site_exists(payload.co_regate):
        raise HTTPException(
            status_code=422,
            detail=f"Site parent {payload.co_regate} inexistant dans trppu_site.",
        )
    existing = await db_read.fetch_one(
        "SELECT id_pdi FROM trppu_pdi WHERE id_pdi = %s", (payload.id_pdi,)
    )
    if existing:
        raise HTTPException(
            status_code=409, detail=f"Le PDI {payload.id_pdi} existe déjà."
        )

    try:
        await db_write.execute(
            "INSERT INTO trppu_pdi (id_pdi, co_regate, lb_pdi, est_actif) "
            "VALUES (%s, %s, %s, %s)",
            (payload.id_pdi, payload.co_regate, payload.lb_pdi, 1 if payload.est_actif else 0),
        )
    except Exception as e:
        logger.error("Erreur création pdi %d : %s", payload.id_pdi, e)
        raise HTTPException(status_code=500, detail="Erreur création PDI.") from e

    return await db_read.fetch_one(
        SELECT_PDI_SQL + " WHERE id_pdi = %s", (payload.id_pdi,)
    )


@router.put("/{id_pdi}", response_model=PdiOut)
async def update_pdi(id_pdi: int, payload: PdiUpdate):
    fields = payload.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(status_code=400, detail="Aucun champ à mettre à jour.")

    existing = await db_read.fetch_one(
        "SELECT id_pdi FROM trppu_pdi WHERE id_pdi = %s", (id_pdi,)
    )
    if not existing:
        raise HTTPException(status_code=404, detail=f"PDI {id_pdi} introuvable.")

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
    params.append(id_pdi)

    try:
        await db_write.execute(
            f"UPDATE trppu_pdi SET {', '.join(set_parts)} WHERE id_pdi = %s",
            tuple(params),
        )
    except Exception as e:
        logger.error("Erreur update pdi %d : %s", id_pdi, e)
        raise HTTPException(status_code=500, detail="Erreur mise à jour PDI.") from e

    return await db_read.fetch_one(SELECT_PDI_SQL + " WHERE id_pdi = %s", (id_pdi,))


@router.delete("/{id_pdi}", response_model=SoftDeleteResult)
async def soft_delete_pdi(id_pdi: int):
    """Soft delete : passe est_actif à 0."""
    existing = await db_read.fetch_one(
        "SELECT id_pdi FROM trppu_pdi WHERE id_pdi = %s", (id_pdi,)
    )
    if not existing:
        raise HTTPException(status_code=404, detail=f"PDI {id_pdi} introuvable.")

    try:
        rows_affected = await db_write.execute(
            "UPDATE trppu_pdi SET est_actif = 0 WHERE id_pdi = %s", (id_pdi,)
        )
    except Exception as e:
        logger.error("Erreur soft delete pdi %d : %s", id_pdi, e)
        raise HTTPException(status_code=500, detail="Erreur désactivation PDI.") from e

    return SoftDeleteResult(id_pdi=id_pdi, est_actif=0, rows_affected=rows_affected)


@router.post("/upload-excel", response_model=BulkUploadResult)
async def upload_excel(file: UploadFile = File(..., description="Fichier .xlsx")):
    """Upload massif via Excel : upsert (INSERT ... ON DUPLICATE KEY UPDATE).

    Pré-vérifie que chaque `co_regate` existe dans trppu_site — les lignes
    référençant un site absent partent en erreur sans bloquer le lot.
    """
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
        pdis, errors = parse_excel_pdis(content)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("Erreur parsing Excel pdis : %s", e)
        raise HTTPException(status_code=500, detail="Erreur lecture Excel.") from e

    valid_pdis: list = []
    if pdis:
        wanted = {p.co_regate for p in pdis}
        existing_rows = await db_read.fetch_all(
            f"SELECT co_regate FROM trppu_site WHERE co_regate IN ({','.join(['%s'] * len(wanted))})",
            tuple(wanted),
        )
        existing_sites = {r["co_regate"] for r in existing_rows}
        for i, p in enumerate(pdis, start=2):
            if p.co_regate not in existing_sites:
                errors.append(
                    BulkUploadError(
                        row=i,
                        error=f"Site parent {p.co_regate} inexistant dans trppu_site",
                        raw=p.model_dump(),
                    )
                )
            else:
                valid_pdis.append(p)

    nb_inserted = 0
    nb_updated = 0
    nb_unchanged = 0

    if valid_pdis:
        try:
            async with db_write.transaction() as tx:
                for p in valid_pdis:
                    rc = await tx.execute(UPSERT_SQL, pdi_to_upsert_params(p))
                    if rc == 1:
                        nb_inserted += 1
                    elif rc == 2:
                        nb_updated += 1
                    else:
                        nb_unchanged += 1
        except Exception as e:
            logger.error("Erreur upsert lot trppu_pdi : %s", e)
            raise HTTPException(
                status_code=500,
                detail=f"Échec de l'écriture du lot en base : {e}",
            ) from e

    duration_s = round(time.perf_counter() - start, 3)
    return BulkUploadResult(
        nb_rows_read=len(pdis) + len([e for e in errors if e.row > 0]),
        nb_inserted=nb_inserted,
        nb_updated=nb_updated,
        nb_unchanged=nb_unchanged,
        nb_errors=len(errors),
        errors=errors,
        execution_time_s=duration_s,
    )
