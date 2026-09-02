"""Routes CRUD et upload Excel pour la table trppu_produit."""

import logging
import time
from datetime import date

from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status

from app.db.mysql import db_read, db_write
from app.log_utils import ctx, diff_champs, params_loggables, safe_preview
from app.services.api_log import (
    ACTION_ECRITURE_PRODUIT,
    ACTION_IMPORT_EXCEL,
    enregistrer_appel,
)

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
    logger.info("Début listing produits %s", ctx(filtres=filters))

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
        logger.exception("Erreur listing produits %s", ctx(filtres=filters))
        raise HTTPException(status_code=500, detail="Erreur listing produits.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin listing produits %s", ctx(count=len(rows), duration_ms=duration_ms)
    )
    return rows


@router.get("/{co_produit}", response_model=ProduitOut)
async def get_produit(co_produit: str):
    start = time.perf_counter()
    logger.info("Début lecture produit %s", ctx(co_produit=co_produit))

    try:
        row = await db_read.fetch_one(
            SELECT_PRODUIT_SQL + " WHERE co_produit = %s", (co_produit,)
        )
    except Exception as e:
        logger.exception("Erreur lecture produit %s", ctx(co_produit=co_produit))
        raise HTTPException(status_code=500, detail="Erreur récupération produit.") from e
    if not row:
        logger.warning(
            "Rejet lecture produit %s",
            ctx(co_produit=co_produit, http=404, motif="produit introuvable"),
        )
        raise HTTPException(status_code=404, detail=f"Produit {co_produit} introuvable.")

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin lecture produit %s", ctx(co_produit=co_produit, duration_ms=duration_ms)
    )
    return row


@router.post("", response_model=ProduitOut, status_code=status.HTTP_201_CREATED)
async def create_produit(payload: ProduitCreate):
    start = time.perf_counter()
    logged = params_loggables(payload)
    logger.info(
        "Début création produit %s", ctx(co_produit=payload.co_produit, params=logged)
    )

    existing = await db_read.fetch_one(
        "SELECT co_produit FROM trppu_produit WHERE co_produit = %s",
        (payload.co_produit,),
    )
    if existing:
        logger.warning(
            "Rejet création produit %s",
            ctx(co_produit=payload.co_produit, http=409, motif="produit déjà existant"),
        )
        raise HTTPException(
            status_code=409, detail=f"Le produit {payload.co_produit} existe déjà."
        )
    logger.debug("Vérification doublon OK %s", ctx(co_produit=payload.co_produit))

    try:
        rows_inseres = await db_write.execute(
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
            "Erreur création produit %s",
            ctx(co_produit=payload.co_produit, params=logged),
        )
        raise HTTPException(status_code=500, detail="Erreur création produit.") from e

    created = await db_read.fetch_one(
        SELECT_PRODUIT_SQL + " WHERE co_produit = %s", (payload.co_produit,)
    )
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_ECRITURE_PRODUIT,
        params={"operation": "creation", "co_produit": payload.co_produit, "params": logged},
    )
    logger.info(
        "Fin création produit %s",
        ctx(
            co_produit=payload.co_produit,
            rows_affected=rows_inseres,
            duration_ms=duration_ms,
        ),
    )
    return created


@router.put("/{co_produit}", response_model=ProduitOut)
async def update_produit(co_produit: str, payload: ProduitUpdate):
    start = time.perf_counter()
    fields = payload.model_dump(exclude_unset=True)
    logged = params_loggables(fields)
    logger.info("Début MAJ produit %s", ctx(co_produit=co_produit, params=logged))

    if not fields:
        logger.warning(
            "Rejet MAJ produit %s",
            ctx(co_produit=co_produit, http=400, motif="aucun champ fourni"),
        )
        raise HTTPException(status_code=400, detail="Aucun champ à mettre à jour.")

    # État avant : sert au delta journalisé en fin de MAJ.
    existing = await db_read.fetch_one(
        SELECT_PRODUIT_SQL + " WHERE co_produit = %s", (co_produit,)
    )
    if not existing:
        logger.warning(
            "Rejet MAJ produit %s",
            ctx(co_produit=co_produit, http=404, motif="produit introuvable"),
        )
        raise HTTPException(status_code=404, detail=f"Produit {co_produit} introuvable.")

    set_parts: list[str] = []
    params: list = []
    for key, value in fields.items():
        set_parts.append(f"{key} = %s")
        params.append(value)
    params.append(co_produit)

    try:
        rows_maj = await db_write.execute(
            f"UPDATE trppu_produit SET {', '.join(set_parts)} WHERE co_produit = %s",
            tuple(params),
        )
    except Exception as e:
        logger.exception(
            "Erreur MAJ produit %s", ctx(co_produit=co_produit, params=logged)
        )
        raise HTTPException(status_code=500, detail="Erreur mise à jour produit.") from e

    updated = await db_read.fetch_one(
        SELECT_PRODUIT_SQL + " WHERE co_produit = %s", (co_produit,)
    )
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    delta = diff_champs(existing, updated or {})
    await enregistrer_appel(
        api_name=ACTION_ECRITURE_PRODUIT,
        params={
            "operation": "maj",
            "co_produit": co_produit,
            "params": logged,
            "delta": delta,
        },
    )
    logger.info(
        "Fin MAJ produit %s",
        ctx(
            co_produit=co_produit,
            nb_champs=len(fields),
            rows_affected=rows_maj,
            delta=delta,
            duration_ms=duration_ms,
        ),
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
        "Début désactivation produit %s", ctx(co_produit=co_produit, motif=motif)
    )

    existing = await db_read.fetch_one(
        SELECT_PRODUIT_SQL + " WHERE co_produit = %s", (co_produit,)
    )
    if not existing:
        logger.warning(
            "Rejet désactivation produit %s",
            ctx(co_produit=co_produit, http=404, motif="produit introuvable"),
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
            "Erreur désactivation produit %s",
            ctx(co_produit=co_produit, motif=motif),
        )
        raise HTTPException(status_code=500, detail="Erreur désactivation produit.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_ECRITURE_PRODUIT,
        params={
            "operation": "desactivation",
            "co_produit": co_produit,
            "etat_avant": params_loggables(dict(existing)),
            "dt_desactivation": str(today),
            "motif": motif,
        },
    )
    logger.info(
        "Fin désactivation produit %s",
        ctx(
            co_produit=co_produit,
            dt_desactivation=today,
            motif=motif,
            rows_affected=rows_affected,
            duration_ms=duration_ms,
        ),
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
    logger.info("Début upload Excel produits %s", ctx(fichier=file.filename))

    if not file.filename or not file.filename.lower().endswith((".xlsx", ".xlsm")):
        logger.warning(
            "Rejet upload Excel produits %s",
            ctx(fichier=file.filename, http=400, motif="extension non supportée"),
        )
        raise HTTPException(
            status_code=400,
            detail="Format de fichier non supporté : attendu .xlsx ou .xlsm.",
        )

    content = await file.read()
    if not content:
        logger.warning(
            "Rejet upload Excel produits %s",
            ctx(fichier=file.filename, http=400, motif="fichier vide"),
        )
        raise HTTPException(status_code=400, detail="Fichier vide.")
    logger.debug(
        "Fichier Excel produits lu %s",
        ctx(fichier=file.filename, taille_octets=len(content)),
    )

    try:
        produits, errors = parse_excel_produits(content)
    except ValueError as e:
        logger.warning(
            "Rejet upload Excel produits %s",
            ctx(
                fichier=file.filename,
                http=400,
                motif=safe_preview(str(e), max_len=200),
            ),
        )
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception(
            "Erreur parsing Excel produits %s",
            ctx(fichier=file.filename, taille_octets=len(content)),
        )
        raise HTTPException(status_code=500, detail="Erreur lecture Excel.") from e
    logger.info(
        "Excel produits parsé %s",
        ctx(fichier=file.filename, valides=len(produits), erreurs=len(errors)),
    )

    nb_inserted = 0
    nb_updated = 0
    nb_unchanged = 0

    if produits:
        logger.debug(
            "Ouverture transaction upsert produits %s",
            ctx(fichier=file.filename, taille_lot=len(produits)),
        )
        try:
            async with db_write.transaction() as tx:
                for excel_row, p in enumerate(produits, start=2):
                    try:
                        rc = await tx.execute(UPSERT_SQL, produit_to_upsert_params(p))
                    except Exception:
                        logger.exception(
                            "Échec UPSERT trppu_produit %s",
                            ctx(
                                fichier=file.filename,
                                ligne_excel=excel_row,
                                params=params_loggables(p),
                            ),
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
                "Erreur upsert lot trppu_produit %s",
                ctx(fichier=file.filename, taille_lot=len(produits)),
            )
            raise HTTPException(
                status_code=500,
                detail=f"Échec de l'écriture du lot en base : {e}",
            ) from e

    # `duration_s` alimente `execution_time_s` de la réponse (contrat d'API,
    # inchangé) ; le log utilise `duration_ms` comme partout ailleurs.
    duration_s = round(time.perf_counter() - start, 3)
    duration_ms = round(duration_s * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_IMPORT_EXCEL,
        params={
            "cible": "trppu_produit",
            "fichier": file.filename,
            "inseres": nb_inserted,
            "modifies": nb_updated,
            "inchanges": nb_unchanged,
            "erreurs": len(errors),
        },
    )
    logger.info(
        "Fin upload Excel produits %s",
        ctx(
            fichier=file.filename,
            inseres=nb_inserted,
            modifies=nb_updated,
            inchanges=nb_unchanged,
            erreurs=len(errors),
            duration_ms=duration_ms,
        ),
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
