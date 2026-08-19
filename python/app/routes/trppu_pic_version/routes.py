"""Routes CRUD et upload Excel pour la table trppu_pic_version."""

import logging
import time
from datetime import datetime

from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status

from app.db.mysql import db_read, db_write
from app.log_utils import safe_preview

from .helpers import (
    INSERT_SQL,
    parse_excel_pic_versions,
    pic_version_to_insert_params,
)
from .schemas import (
    BulkUploadResult,
    NiveauCreationEnum,
    NiveauEnum,
    PicVersionCreate,
    PicVersionOut,
    PicVersionUpdate,
    SoftDeleteResult,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/pic-versions", tags=["PIC Versions"])


SELECT_PICV_SQL = (
    "SELECT id_pic_version, lb_pic_version, niveau, co_regate, "
    "dt_activation, dt_desactivation, motif_desactivation, commentaire, "
    "est_par_defaut, dt_creation, dt_maj, id_rh_creation, id_rh_maj "
    "FROM trppu_pic_version"
)


@router.get("", response_model=list[PicVersionOut])
async def list_pic_versions(
    co_regate: str | None = Query(None, min_length=6, max_length=6),
    niveau: NiveauEnum | None = Query(None),
    actif_only: bool = Query(False, description="N'inclut pas les versions désactivées."),
    est_par_defaut: bool | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    start = time.perf_counter()
    filters = {
        "co_regate": co_regate,
        "niveau": niveau,
        "actif_only": actif_only,
        "est_par_defaut": est_par_defaut,
        "limit": limit,
        "offset": offset,
    }
    logger.info("Début listing des versions PIC (filtres=%s)", safe_preview(filters))

    where: list[str] = []
    params: list = []
    if co_regate is not None:
        where.append("co_regate = %s")
        params.append(co_regate)
    if niveau is not None:
        where.append("niveau = %s")
        params.append(niveau.value)
    if actif_only:
        where.append("(dt_desactivation IS NULL OR dt_desactivation > NOW())")
    if est_par_defaut is not None:
        where.append("est_par_defaut = %s")
        params.append(1 if est_par_defaut else 0)

    sql = SELECT_PICV_SQL
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id_pic_version LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    try:
        rows = await db_read.fetch_all(sql, tuple(params))
    except Exception as e:
        logger.exception(
            "Erreur listing des versions PIC (filtres=%s)", safe_preview(filters)
        )
        raise HTTPException(status_code=500, detail="Erreur listing pic_versions.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Listing des versions PIC terminé (count=%d, duration_ms=%.1f)",
        len(rows),
        duration_ms,
    )
    return rows


@router.get("/enums")
async def list_enums():
    """Valeurs autorisées pour les colonnes ENUM de trppu_pic_version."""
    logger.info("Récupération des enums versions PIC")
    # "niveau" : valeurs lisibles (enum base, SCENARIO compris).
    # "niveau_creation" : sous-ensemble accepté par POST / PUT.
    return {
        "niveau": [e.value for e in NiveauEnum],
        "niveau_creation": [e.value for e in NiveauCreationEnum],
    }


@router.get("/{id_pic_version}", response_model=PicVersionOut)
async def get_pic_version(id_pic_version: int):
    start = time.perf_counter()
    logger.info("Début récupération version PIC (id_pic_version=%d)", id_pic_version)

    try:
        row = await db_read.fetch_one(
            SELECT_PICV_SQL + " WHERE id_pic_version = %s", (id_pic_version,)
        )
    except Exception as e:
        logger.exception(
            "Erreur récupération version PIC (id_pic_version=%d)", id_pic_version
        )
        raise HTTPException(status_code=500, detail="Erreur récupération PIC version.") from e
    if not row:
        logger.info("Version PIC introuvable (id_pic_version=%d)", id_pic_version)
        raise HTTPException(
            status_code=404, detail=f"PIC version {id_pic_version} introuvable."
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Récupération version PIC terminée (id_pic_version=%d, duration_ms=%.1f)",
        id_pic_version,
        duration_ms,
    )
    return row


@router.post("", response_model=PicVersionOut, status_code=status.HTTP_201_CREATED)
async def create_pic_version(payload: PicVersionCreate):
    start = time.perf_counter()
    logger.info(
        "Début création version PIC (co_regate=%s, niveau=%s, payload=%s)",
        payload.co_regate,
        payload.niveau.value,
        safe_preview(payload.model_dump(mode="json")),
    )

    try:
        async with db_write.transaction() as tx:
            await tx.execute(INSERT_SQL, pic_version_to_insert_params(payload))
            new_row = await tx.fetch_one(
                SELECT_PICV_SQL + " WHERE id_pic_version = LAST_INSERT_ID()"
            )
    except Exception as e:
        logger.exception(
            "Erreur création version PIC (payload=%s)",
            safe_preview(payload.model_dump(mode="json")),
        )
        raise HTTPException(status_code=500, detail="Erreur création PIC version.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Création version PIC terminée (id_pic_version=%d, co_regate=%s, duration_ms=%.1f)",
        new_row["id_pic_version"],
        new_row["co_regate"],
        duration_ms,
    )
    return new_row


@router.put("/{id_pic_version}", response_model=PicVersionOut)
async def update_pic_version(id_pic_version: int, payload: PicVersionUpdate):
    start = time.perf_counter()
    fields = payload.model_dump(exclude_unset=True)
    logger.info(
        "Début mise à jour version PIC (id_pic_version=%d, champs=%s)",
        id_pic_version,
        safe_preview(fields),
    )

    if not fields:
        raise HTTPException(status_code=400, detail="Aucun champ à mettre à jour.")

    existing = await db_read.fetch_one(
        "SELECT id_pic_version, dt_activation FROM trppu_pic_version WHERE id_pic_version = %s",
        (id_pic_version,),
    )
    if not existing:
        logger.info(
            "Version PIC introuvable pour mise à jour (id_pic_version=%d)", id_pic_version
        )
        raise HTTPException(
            status_code=404, detail=f"PIC version {id_pic_version} introuvable."
        )

    new_dt_activation = fields.get("dt_activation", existing["dt_activation"])
    if "dt_desactivation" in fields and fields["dt_desactivation"] is not None:
        if fields["dt_desactivation"] <= new_dt_activation:
            raise HTTPException(
                status_code=422,
                detail="dt_desactivation doit être strictement supérieure à dt_activation.",
            )

    set_parts: list[str] = []
    params: list = []
    for key, value in fields.items():
        set_parts.append(f"{key} = %s")
        if key == "niveau":
            params.append(value.value if hasattr(value, "value") else value)
        elif key == "est_par_defaut":
            params.append(1 if value else 0)
        else:
            params.append(value)
    params.append(id_pic_version)

    try:
        await db_write.execute(
            f"UPDATE trppu_pic_version SET {', '.join(set_parts)} WHERE id_pic_version = %s",
            tuple(params),
        )
    except Exception as e:
        logger.exception(
            "Erreur mise à jour version PIC (id_pic_version=%d, champs=%s)",
            id_pic_version,
            safe_preview(fields),
        )
        raise HTTPException(
            status_code=500, detail="Erreur mise à jour PIC version."
        ) from e

    updated = await db_read.fetch_one(
        SELECT_PICV_SQL + " WHERE id_pic_version = %s", (id_pic_version,)
    )
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Mise à jour version PIC terminée (id_pic_version=%d, nb_champs=%d, duration_ms=%.1f)",
        id_pic_version,
        len(fields),
        duration_ms,
    )
    return updated


@router.delete("/{id_pic_version}", response_model=SoftDeleteResult)
async def soft_delete_pic_version(
    id_pic_version: int,
    motif: str = Query("Désactivé via API", max_length=255),
):
    start = time.perf_counter()
    logger.info(
        "Début désactivation version PIC (id_pic_version=%d, motif=%s)",
        id_pic_version,
        safe_preview(motif),
    )

    existing = await db_read.fetch_one(
        "SELECT id_pic_version, dt_activation FROM trppu_pic_version WHERE id_pic_version = %s",
        (id_pic_version,),
    )
    if not existing:
        logger.info(
            "Version PIC introuvable pour désactivation (id_pic_version=%d)",
            id_pic_version,
        )
        raise HTTPException(
            status_code=404, detail=f"PIC version {id_pic_version} introuvable."
        )

    now = datetime.now()
    if now <= existing["dt_activation"]:
        logger.info(
            "Désactivation refusée : dt_activation dans le futur (id_pic_version=%d, dt_activation=%s)",
            id_pic_version,
            existing["dt_activation"],
        )
        raise HTTPException(
            status_code=422,
            detail="dt_activation est dans le futur — impossible de désactiver maintenant. "
            "Utilisez PUT pour fixer dt_desactivation explicitement.",
        )

    try:
        rows_affected = await db_write.execute(
            "UPDATE trppu_pic_version "
            "SET dt_desactivation = %s, motif_desactivation = %s "
            "WHERE id_pic_version = %s",
            (now, motif, id_pic_version),
        )
    except Exception as e:
        logger.exception(
            "Erreur désactivation version PIC (id_pic_version=%d, motif=%s)",
            id_pic_version,
            safe_preview(motif),
        )
        raise HTTPException(
            status_code=500, detail="Erreur désactivation PIC version."
        ) from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Désactivation version PIC terminée (id_pic_version=%d, dt_desactivation=%s, duration_ms=%.1f)",
        id_pic_version,
        now,
        duration_ms,
    )
    return SoftDeleteResult(
        id_pic_version=id_pic_version,
        dt_desactivation=now,
        motif_desactivation=motif,
        rows_affected=rows_affected,
    )


@router.post("/upload-excel", response_model=BulkUploadResult)
async def upload_excel(file: UploadFile = File(..., description="Fichier .xlsx")):
    """Upload massif via Excel : INSERT-only (PK auto-incrément)."""
    start = time.perf_counter()
    logger.info("Début upload Excel versions PIC (fichier=%s)", file.filename)

    if not file.filename or not file.filename.lower().endswith((".xlsx", ".xlsm")):
        raise HTTPException(
            status_code=400,
            detail="Format de fichier non supporté : attendu .xlsx ou .xlsm.",
        )

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Fichier vide.")
    logger.info(
        "Fichier Excel versions PIC lu (fichier=%s, taille=%d octets)",
        file.filename,
        len(content),
    )

    try:
        pic_versions, errors = parse_excel_pic_versions(content)
    except ValueError as e:
        logger.info(
            "Excel versions PIC invalide (fichier=%s, raison=%s)",
            file.filename,
            safe_preview(str(e), max_len=200),
        )
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception(
            "Erreur parsing Excel versions PIC (fichier=%s, taille=%d)",
            file.filename,
            len(content),
        )
        raise HTTPException(status_code=500, detail="Erreur lecture Excel.") from e
    logger.info(
        "Excel versions PIC parsé (fichier=%s, valides=%d, erreurs=%d)",
        file.filename,
        len(pic_versions),
        len(errors),
    )

    nb_inserted = 0
    if pic_versions:
        logger.info(
            "Ouverture transaction insertion versions PIC (fichier=%s, taille_lot=%d)",
            file.filename,
            len(pic_versions),
        )
        try:
            async with db_write.transaction() as tx:
                for excel_row, p in enumerate(pic_versions, start=2):
                    try:
                        await tx.execute(INSERT_SQL, pic_version_to_insert_params(p))
                        nb_inserted += 1
                    except Exception:
                        logger.exception(
                            "Échec INSERT trppu_pic_version (fichier=%s, ligne_excel=%d, payload=%s)",
                            file.filename,
                            excel_row,
                            safe_preview(p.model_dump(mode="json")),
                        )
                        raise
        except Exception as e:
            logger.exception(
                "Erreur insertion lot trppu_pic_version (fichier=%s, taille_lot=%d)",
                file.filename,
                len(pic_versions),
            )
            raise HTTPException(
                status_code=500,
                detail=f"Échec de l'écriture du lot en base : {e}",
            ) from e

    duration_s = round(time.perf_counter() - start, 3)
    logger.info(
        "Upload Excel versions PIC terminé (fichier=%s, insérés=%d, erreurs=%d, duration_s=%.3f)",
        file.filename,
        nb_inserted,
        len(errors),
        duration_s,
    )
    return BulkUploadResult(
        nb_rows_read=len(pic_versions) + len([e for e in errors if e.row > 0]),
        nb_inserted=nb_inserted,
        nb_errors=len(errors),
        errors=errors,
        execution_time_s=duration_s,
    )
