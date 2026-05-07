"""Routes CRUD et upload Excel pour la table trppu_site."""

import logging
import time

from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status

from app.db.mysql import db_read, db_write
from app.log_utils import safe_preview

from .helpers import UPSERT_SQL, parse_excel_sites, site_to_upsert_params
from .schemas import BulkUploadResult, SiteCreate, SiteOut, SiteUpdate

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/sites", tags=["Sites"])


SELECT_SITE_SQL = (
    "SELECT co_regate, lb_regate, type_site, co_roc, dt_maj FROM trppu_site"
)


@router.get("", response_model=list[SiteOut])
async def list_sites(
    type_site: str | None = Query(None, min_length=1, max_length=5, description="Filtre type_site"),
    co_roc: str | None = Query(None, min_length=6, max_length=6, description="Filtre co_roc"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Liste des sites avec filtres optionnels et pagination."""
    start = time.perf_counter()
    filters = {
        "type_site": type_site,
        "co_roc": co_roc,
        "limit": limit,
        "offset": offset,
    }
    logger.info("Début listing des sites (filtres=%s)", safe_preview(filters))

    where: list[str] = []
    params: list = []
    if type_site is not None:
        where.append("type_site = %s")
        params.append(type_site)
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
        logger.exception("Erreur listing des sites (filtres=%s)", safe_preview(filters))
        raise HTTPException(status_code=500, detail="Erreur listing sites.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info("Listing des sites terminé (count=%d, duration_ms=%.1f)", len(rows), duration_ms)
    return rows


@router.get("/{co_regate}", response_model=SiteOut)
async def get_site(co_regate: str):
    """Récupération d'un site par sa clé primaire."""
    start = time.perf_counter()
    logger.info("Début récupération site (co_regate=%s)", co_regate)

    try:
        row = await db_read.fetch_one(
            SELECT_SITE_SQL + " WHERE co_regate = %s", (co_regate,)
        )
    except Exception as e:
        logger.exception("Erreur récupération site (co_regate=%s)", co_regate)
        raise HTTPException(status_code=500, detail="Erreur récupération site.") from e
    if not row:
        logger.info("Site introuvable (co_regate=%s)", co_regate)
        raise HTTPException(status_code=404, detail=f"Site {co_regate} introuvable.")

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Récupération site terminée (co_regate=%s, duration_ms=%.1f)",
        co_regate,
        duration_ms,
    )
    return row


@router.post("", response_model=SiteOut, status_code=status.HTTP_201_CREATED)
async def create_site(payload: SiteCreate):
    """Création d'un site. Échoue (409) si co_regate déjà présent."""
    start = time.perf_counter()
    logger.info(
        "Début création site (co_regate=%s, payload=%s)",
        payload.co_regate,
        safe_preview(payload.model_dump(mode="json")),
    )

    existing = await db_read.fetch_one(
        "SELECT co_regate FROM trppu_site WHERE co_regate = %s",
        (payload.co_regate,),
    )
    if existing:
        logger.info("Site déjà existant (co_regate=%s)", payload.co_regate)
        raise HTTPException(
            status_code=409,
            detail=f"Le site {payload.co_regate} existe déjà.",
        )
    logger.info("Vérification doublon OK (co_regate=%s)", payload.co_regate)

    try:
        await db_write.execute(
            "INSERT INTO trppu_site (co_regate, lb_regate, type_site, co_roc) "
            "VALUES (%s, %s, %s, %s)",
            (
                payload.co_regate,
                payload.lb_regate,
                payload.type_site,
                payload.co_roc,
            ),
        )
    except Exception as e:
        logger.exception(
            "Erreur création site (co_regate=%s, payload=%s)",
            payload.co_regate,
            safe_preview(payload.model_dump(mode="json")),
        )
        raise HTTPException(status_code=500, detail="Erreur création site.") from e

    created = await db_read.fetch_one(
        SELECT_SITE_SQL + " WHERE co_regate = %s", (payload.co_regate,)
    )
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Création site terminée (co_regate=%s, duration_ms=%.1f)",
        payload.co_regate,
        duration_ms,
    )
    return created


@router.put("/{co_regate}", response_model=SiteOut)
async def update_site(co_regate: str, payload: SiteUpdate):
    """Mise à jour partielle d'un site."""
    start = time.perf_counter()
    fields = payload.model_dump(exclude_unset=True)
    logger.info(
        "Début mise à jour site (co_regate=%s, champs=%s)",
        co_regate,
        safe_preview(fields),
    )

    if not fields:
        raise HTTPException(status_code=400, detail="Aucun champ à mettre à jour.")

    existing = await db_read.fetch_one(
        "SELECT co_regate FROM trppu_site WHERE co_regate = %s", (co_regate,)
    )
    if not existing:
        logger.info("Site introuvable pour mise à jour (co_regate=%s)", co_regate)
        raise HTTPException(status_code=404, detail=f"Site {co_regate} introuvable.")

    set_parts = [f"{k} = %s" for k in fields]
    params = list(fields.values()) + [co_regate]

    try:
        await db_write.execute(
            f"UPDATE trppu_site SET {', '.join(set_parts)} WHERE co_regate = %s",
            tuple(params),
        )
    except Exception as e:
        logger.exception(
            "Erreur mise à jour site (co_regate=%s, champs=%s)",
            co_regate,
            safe_preview(fields),
        )
        raise HTTPException(status_code=500, detail="Erreur mise à jour site.") from e

    updated = await db_read.fetch_one(
        SELECT_SITE_SQL + " WHERE co_regate = %s", (co_regate,)
    )
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Mise à jour site terminée (co_regate=%s, nb_champs=%d, duration_ms=%.1f)",
        co_regate,
        len(fields),
        duration_ms,
    )
    return updated


@router.post("/upload-excel", response_model=BulkUploadResult)
async def upload_excel(file: UploadFile = File(..., description="Fichier .xlsx")):
    """Upload massif via Excel : upsert (INSERT ... ON DUPLICATE KEY UPDATE)."""
    start = time.perf_counter()
    logger.info("Début upload Excel sites (fichier=%s)", file.filename)

    if not file.filename or not file.filename.lower().endswith((".xlsx", ".xlsm")):
        raise HTTPException(
            status_code=400,
            detail="Format de fichier non supporté : attendu .xlsx ou .xlsm.",
        )

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Fichier vide.")
    logger.info(
        "Fichier Excel sites lu (fichier=%s, taille=%d octets)",
        file.filename,
        len(content),
    )

    try:
        sites, errors = parse_excel_sites(content)
    except ValueError as e:
        logger.info(
            "Excel sites invalide (fichier=%s, raison=%s)",
            file.filename,
            safe_preview(str(e), max_len=200),
        )
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception(
            "Erreur parsing Excel sites (fichier=%s, taille=%d)",
            file.filename,
            len(content),
        )
        raise HTTPException(status_code=500, detail="Erreur lecture Excel.") from e
    logger.info(
        "Excel sites parsé (fichier=%s, valides=%d, erreurs=%d)",
        file.filename,
        len(sites),
        len(errors),
    )

    nb_inserted = 0
    nb_updated = 0
    nb_unchanged = 0

    if sites:
        logger.info(
            "Ouverture transaction upsert sites (fichier=%s, taille_lot=%d)",
            file.filename,
            len(sites),
        )
        try:
            async with db_write.transaction() as tx:
                for excel_row, site in enumerate(sites, start=2):
                    try:
                        rc = await tx.execute(UPSERT_SQL, site_to_upsert_params(site))
                    except Exception:
                        logger.exception(
                            "Échec UPSERT trppu_site (fichier=%s, ligne_excel=%d, payload=%s)",
                            file.filename,
                            excel_row,
                            safe_preview(site.model_dump(mode="json")),
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
                "Erreur upsert lot trppu_site (fichier=%s, taille_lot=%d)",
                file.filename,
                len(sites),
            )
            raise HTTPException(
                status_code=500,
                detail=f"Échec de l'écriture du lot en base : {e}",
            ) from e

    duration_s = round(time.perf_counter() - start, 3)
    logger.info(
        "Upload Excel sites terminé (fichier=%s, insérés=%d, modifiés=%d, inchangés=%d, erreurs=%d, duration_s=%.3f)",
        file.filename,
        nb_inserted,
        nb_updated,
        nb_unchanged,
        len(errors),
        duration_s,
    )
    return BulkUploadResult(
        nb_rows_read=len(sites) + len(errors),
        nb_inserted=nb_inserted,
        nb_updated=nb_updated,
        nb_unchanged=nb_unchanged,
        nb_errors=len(errors),
        errors=errors,
        execution_time_s=duration_s,
    )
