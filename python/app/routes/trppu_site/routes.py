"""Routes CRUD et upload Excel pour la table trppu_site."""

import logging
import time

from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status

from app.db.mysql import db_read, db_write

from .helpers import UPSERT_SQL, parse_excel_sites, site_to_upsert_params
from .schemas import (
    BulkUploadResult,
    SiteCreate,
    SiteOut,
    SiteUpdate,
    SoftDeleteResult,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/sites", tags=["Sites"])


SELECT_SITE_SQL = (
    "SELECT co_regate, lb_regate, lb_site, type_site, co_roc, est_actif, dt_maj "
    "FROM trppu_site"
)


@router.get("", response_model=list[SiteOut])
async def list_sites(
    type_site: str | None = Query(None, min_length=1, max_length=5, description="Filtre type_site"),
    est_actif: bool | None = Query(None, description="Filtre actif/inactif"),
    co_roc: str | None = Query(None, min_length=6, max_length=6, description="Filtre co_roc"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Liste des sites avec filtres optionnels et pagination."""
    where: list[str] = []
    params: list = []
    if type_site is not None:
        where.append("type_site = %s")
        params.append(type_site)
    if est_actif is not None:
        where.append("est_actif = %s")
        params.append(1 if est_actif else 0)
    if co_roc is not None:
        where.append("co_roc = %s")
        params.append(co_roc)

    sql = SELECT_SITE_SQL
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY co_regate LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    try:
        rows = await db_read.fetch_all(sql, tuple(params))
    except Exception as e:
        logger.error("Erreur listing sites : %s", e)
        raise HTTPException(status_code=500, detail="Erreur listing sites.") from e
    return rows


@router.get("/{co_regate}", response_model=SiteOut)
async def get_site(co_regate: str):
    """Récupération d'un site par sa clé primaire."""
    try:
        row = await db_read.fetch_one(
            SELECT_SITE_SQL + " WHERE co_regate = %s", (co_regate,)
        )
    except Exception as e:
        logger.error("Erreur get site %s : %s", co_regate, e)
        raise HTTPException(status_code=500, detail="Erreur récupération site.") from e
    if not row:
        raise HTTPException(status_code=404, detail=f"Site {co_regate} introuvable.")
    return row


@router.post("", response_model=SiteOut, status_code=status.HTTP_201_CREATED)
async def create_site(payload: SiteCreate):
    """Création d'un site. Échoue (409) si co_regate déjà présent."""
    existing = await db_read.fetch_one(
        "SELECT co_regate FROM trppu_site WHERE co_regate = %s",
        (payload.co_regate,),
    )
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"Le site {payload.co_regate} existe déjà.",
        )

    try:
        await db_write.execute(
            "INSERT INTO trppu_site (co_regate, lb_regate, lb_site, type_site, co_roc, est_actif) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (
                payload.co_regate,
                payload.lb_regate,
                payload.lb_site,
                payload.type_site,
                payload.co_roc,
                1 if payload.est_actif else 0,
            ),
        )
    except Exception as e:
        logger.error("Erreur création site %s : %s", payload.co_regate, e)
        raise HTTPException(status_code=500, detail="Erreur création site.") from e

    created = await db_read.fetch_one(
        SELECT_SITE_SQL + " WHERE co_regate = %s", (payload.co_regate,)
    )
    return created


@router.put("/{co_regate}", response_model=SiteOut)
async def update_site(co_regate: str, payload: SiteUpdate):
    """Mise à jour partielle d'un site."""
    fields = payload.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(status_code=400, detail="Aucun champ à mettre à jour.")

    existing = await db_read.fetch_one(
        "SELECT co_regate FROM trppu_site WHERE co_regate = %s", (co_regate,)
    )
    if not existing:
        raise HTTPException(status_code=404, detail=f"Site {co_regate} introuvable.")

    set_parts: list[str] = []
    params: list = []
    for key, value in fields.items():
        set_parts.append(f"{key} = %s")
        if key == "est_actif":
            params.append(1 if value else 0)
        else:
            params.append(value)
    params.append(co_regate)

    try:
        await db_write.execute(
            f"UPDATE trppu_site SET {', '.join(set_parts)} WHERE co_regate = %s",
            tuple(params),
        )
    except Exception as e:
        logger.error("Erreur update site %s : %s", co_regate, e)
        raise HTTPException(status_code=500, detail="Erreur mise à jour site.") from e

    updated = await db_read.fetch_one(
        SELECT_SITE_SQL + " WHERE co_regate = %s", (co_regate,)
    )
    return updated


@router.delete("/{co_regate}", response_model=SoftDeleteResult)
async def soft_delete_site(co_regate: str):
    """Soft delete : passe est_actif à 0 (les FK RESTRICT bloqueraient un DELETE physique)."""
    existing = await db_read.fetch_one(
        "SELECT co_regate FROM trppu_site WHERE co_regate = %s", (co_regate,)
    )
    if not existing:
        raise HTTPException(status_code=404, detail=f"Site {co_regate} introuvable.")

    try:
        rows_affected = await db_write.execute(
            "UPDATE trppu_site SET est_actif = 0 WHERE co_regate = %s",
            (co_regate,),
        )
    except Exception as e:
        logger.error("Erreur soft delete site %s : %s", co_regate, e)
        raise HTTPException(status_code=500, detail="Erreur désactivation site.") from e

    return SoftDeleteResult(co_regate=co_regate, est_actif=0, rows_affected=rows_affected)


@router.post("/upload-excel", response_model=BulkUploadResult)
async def upload_excel(file: UploadFile = File(..., description="Fichier .xlsx")):
    """Upload massif via Excel : upsert (INSERT ... ON DUPLICATE KEY UPDATE).

    - Les lignes invalides sont collectées dans `errors` (n° de ligne + message).
    - Les lignes valides sont écrites dans une transaction unique.
    - Compteurs MySQL : rowcount=1 → insert, rowcount=2 → update, rowcount=0 → unchanged.
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
        sites, errors = parse_excel_sites(content)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("Erreur parsing Excel : %s", e)
        raise HTTPException(status_code=500, detail="Erreur lecture Excel.") from e

    nb_inserted = 0
    nb_updated = 0
    nb_unchanged = 0

    if sites:
        try:
            async with db_write.transaction() as tx:
                for site in sites:
                    rc = await tx.execute(UPSERT_SQL, site_to_upsert_params(site))
                    if rc == 1:
                        nb_inserted += 1
                    elif rc == 2:
                        nb_updated += 1
                    else:
                        nb_unchanged += 1
        except Exception as e:
            logger.error("Erreur upsert lot trppu_site : %s", e)
            raise HTTPException(
                status_code=500,
                detail=f"Échec de l'écriture du lot en base : {e}",
            ) from e

    duration_s = round(time.perf_counter() - start, 3)
    return BulkUploadResult(
        nb_rows_read=len(sites) + len(errors),
        nb_inserted=nb_inserted,
        nb_updated=nb_updated,
        nb_unchanged=nb_unchanged,
        nb_errors=len(errors),
        errors=errors,
        execution_time_s=duration_s,
    )
