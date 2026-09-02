"""Routes CRUD et upload Excel pour la table trppu_pic_version."""

import logging
import time
from datetime import datetime

from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status

from app.db.mysql import db_read, db_write
from app.log_utils import ctx, diff_champs, params_loggables, safe_preview
from app.services.api_log import (
    ACTION_CREATION_PIC_VERSION,
    ACTION_IMPORT_EXCEL,
    ACTION_MAJ_PIC_VERSION,
    ACTION_SUPPRESSION_PIC_VERSION,
    enregistrer_appel,
)

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
    logger.info("Début listing versions PIC %s", ctx(filtres=filters))

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
        logger.exception("Erreur listing versions PIC %s", ctx(filtres=filters))
        raise HTTPException(status_code=500, detail="Erreur listing pic_versions.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin listing versions PIC %s", ctx(count=len(rows), duration_ms=duration_ms)
    )
    return rows


@router.get("/enums")
async def list_enums():
    """Valeurs autorisées pour les colonnes ENUM de trppu_pic_version."""
    logger.debug("Lecture enums versions PIC %s", ctx(count=len(NiveauEnum)))
    # "niveau" : valeurs lisibles (enum base, SCENARIO compris).
    # "niveau_creation" : sous-ensemble accepté par POST / PUT.
    return {
        "niveau": [e.value for e in NiveauEnum],
        "niveau_creation": [e.value for e in NiveauCreationEnum],
    }


@router.get("/{id_pic_version}", response_model=PicVersionOut)
async def get_pic_version(id_pic_version: int):
    start = time.perf_counter()
    logger.info("Début lecture version PIC %s", ctx(id_pic_version=id_pic_version))

    try:
        row = await db_read.fetch_one(
            SELECT_PICV_SQL + " WHERE id_pic_version = %s", (id_pic_version,)
        )
    except Exception as e:
        logger.exception(
            "Erreur lecture version PIC %s", ctx(id_pic_version=id_pic_version)
        )
        raise HTTPException(status_code=500, detail="Erreur récupération PIC version.") from e
    if not row:
        logger.warning(
            "Rejet lecture version PIC %s",
            ctx(id_pic_version=id_pic_version, http=404, motif="version introuvable"),
        )
        raise HTTPException(
            status_code=404, detail=f"PIC version {id_pic_version} introuvable."
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin lecture version PIC %s",
        ctx(id_pic_version=id_pic_version, duration_ms=duration_ms),
    )
    return row


@router.post("", response_model=PicVersionOut, status_code=status.HTTP_201_CREATED)
async def create_pic_version(payload: PicVersionCreate):
    start = time.perf_counter()
    logged = params_loggables(payload)
    logger.info(
        "Début création version PIC %s",
        ctx(co_regate=payload.co_regate, niveau=payload.niveau.value, params=logged),
    )

    try:
        async with db_write.transaction() as tx:
            rows_inseres = await tx.execute(
                INSERT_SQL, pic_version_to_insert_params(payload)
            )
            new_row = await tx.fetch_one(
                SELECT_PICV_SQL + " WHERE id_pic_version = LAST_INSERT_ID()"
            )
    except Exception as e:
        logger.exception("Erreur création version PIC %s", ctx(params=logged))
        raise HTTPException(status_code=500, detail="Erreur création PIC version.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_CREATION_PIC_VERSION,
        regate=new_row["co_regate"],
        params={
            "operation": "creation",
            "id_pic_version": new_row["id_pic_version"],
            "params": logged,
        },
    )
    logger.info(
        "Fin création version PIC %s",
        ctx(
            id_pic_version=new_row["id_pic_version"],
            co_regate=new_row["co_regate"],
            niveau=payload.niveau.value,
            rows_affected=rows_inseres,
            duration_ms=duration_ms,
        ),
    )
    return new_row


@router.put("/{id_pic_version}", response_model=PicVersionOut)
async def update_pic_version(id_pic_version: int, payload: PicVersionUpdate):
    start = time.perf_counter()
    fields = payload.model_dump(exclude_unset=True)
    logged = params_loggables(fields)
    logger.info(
        "Début MAJ version PIC %s", ctx(id_pic_version=id_pic_version, params=logged)
    )

    if not fields:
        logger.warning(
            "Rejet MAJ version PIC %s",
            ctx(id_pic_version=id_pic_version, http=400, motif="aucun champ fourni"),
        )
        raise HTTPException(status_code=400, detail="Aucun champ à mettre à jour.")

    # État avant : sert au delta journalisé en fin de MAJ.
    existing = await db_read.fetch_one(
        SELECT_PICV_SQL + " WHERE id_pic_version = %s", (id_pic_version,)
    )
    if not existing:
        logger.warning(
            "Rejet MAJ version PIC %s",
            ctx(id_pic_version=id_pic_version, http=404, motif="version introuvable"),
        )
        raise HTTPException(
            status_code=404, detail=f"PIC version {id_pic_version} introuvable."
        )

    new_dt_activation = fields.get("dt_activation", existing["dt_activation"])
    if "dt_desactivation" in fields and fields["dt_desactivation"] is not None:
        if fields["dt_desactivation"] <= new_dt_activation:
            logger.warning(
                "Rejet MAJ version PIC %s",
                ctx(
                    id_pic_version=id_pic_version,
                    dt_activation=new_dt_activation,
                    dt_desactivation=fields["dt_desactivation"],
                    http=422,
                    motif="dt_desactivation <= dt_activation",
                ),
            )
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
        rows_maj = await db_write.execute(
            f"UPDATE trppu_pic_version SET {', '.join(set_parts)} WHERE id_pic_version = %s",
            tuple(params),
        )
    except Exception as e:
        logger.exception(
            "Erreur MAJ version PIC %s",
            ctx(id_pic_version=id_pic_version, params=logged),
        )
        raise HTTPException(
            status_code=500, detail="Erreur mise à jour PIC version."
        ) from e

    updated = await db_read.fetch_one(
        SELECT_PICV_SQL + " WHERE id_pic_version = %s", (id_pic_version,)
    )
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    delta = diff_champs(existing, updated or {})
    await enregistrer_appel(
        api_name=ACTION_MAJ_PIC_VERSION,
        regate=existing.get("co_regate"),
        params={
            "operation": "maj",
            "id_pic_version": id_pic_version,
            "params": logged,
            "delta": delta,
        },
    )
    logger.info(
        "Fin MAJ version PIC %s",
        ctx(
            id_pic_version=id_pic_version,
            nb_champs=len(fields),
            rows_affected=rows_maj,
            delta=delta,
            duration_ms=duration_ms,
        ),
    )
    return updated


@router.delete("/{id_pic_version}", response_model=SoftDeleteResult)
async def soft_delete_pic_version(
    id_pic_version: int,
    motif: str = Query("Désactivé via API", max_length=255),
):
    start = time.perf_counter()
    logger.info(
        "Début désactivation version PIC %s",
        ctx(id_pic_version=id_pic_version, motif=motif),
    )

    existing = await db_read.fetch_one(
        SELECT_PICV_SQL + " WHERE id_pic_version = %s", (id_pic_version,)
    )
    if not existing:
        logger.warning(
            "Rejet désactivation version PIC %s",
            ctx(id_pic_version=id_pic_version, http=404, motif="version introuvable"),
        )
        raise HTTPException(
            status_code=404, detail=f"PIC version {id_pic_version} introuvable."
        )

    now = datetime.now()
    if now <= existing["dt_activation"]:
        logger.warning(
            "Rejet désactivation version PIC %s",
            ctx(
                id_pic_version=id_pic_version,
                dt_activation=existing["dt_activation"],
                http=422,
                motif="dt_activation dans le futur",
            ),
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
            "Erreur désactivation version PIC %s",
            ctx(id_pic_version=id_pic_version, motif=motif),
        )
        raise HTTPException(
            status_code=500, detail="Erreur désactivation PIC version."
        ) from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_SUPPRESSION_PIC_VERSION,
        regate=existing.get("co_regate"),
        params={
            "operation": "desactivation",
            "id_pic_version": id_pic_version,
            "etat_avant": params_loggables(dict(existing)),
            "dt_desactivation": str(now),
            "motif": motif,
        },
    )
    logger.info(
        "Fin désactivation version PIC %s",
        ctx(
            id_pic_version=id_pic_version,
            dt_desactivation=now,
            motif=motif,
            rows_affected=rows_affected,
            duration_ms=duration_ms,
        ),
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
    logger.info("Début upload Excel versions PIC %s", ctx(fichier=file.filename))

    if not file.filename or not file.filename.lower().endswith((".xlsx", ".xlsm")):
        logger.warning(
            "Rejet upload Excel versions PIC %s",
            ctx(fichier=file.filename, http=400, motif="extension non supportée"),
        )
        raise HTTPException(
            status_code=400,
            detail="Format de fichier non supporté : attendu .xlsx ou .xlsm.",
        )

    content = await file.read()
    if not content:
        logger.warning(
            "Rejet upload Excel versions PIC %s",
            ctx(fichier=file.filename, http=400, motif="fichier vide"),
        )
        raise HTTPException(status_code=400, detail="Fichier vide.")
    logger.debug(
        "Fichier Excel versions PIC lu %s",
        ctx(fichier=file.filename, taille_octets=len(content)),
    )

    try:
        pic_versions, errors = parse_excel_pic_versions(content)
    except ValueError as e:
        logger.warning(
            "Rejet upload Excel versions PIC %s",
            ctx(
                fichier=file.filename,
                http=400,
                motif=safe_preview(str(e), max_len=200),
            ),
        )
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception(
            "Erreur parsing Excel versions PIC %s",
            ctx(fichier=file.filename, taille_octets=len(content)),
        )
        raise HTTPException(status_code=500, detail="Erreur lecture Excel.") from e
    logger.info(
        "Excel versions PIC parsé %s",
        ctx(fichier=file.filename, valides=len(pic_versions), erreurs=len(errors)),
    )

    nb_inserted = 0
    if pic_versions:
        logger.debug(
            "Ouverture transaction insertion versions PIC %s",
            ctx(fichier=file.filename, taille_lot=len(pic_versions)),
        )
        try:
            async with db_write.transaction() as tx:
                for excel_row, p in enumerate(pic_versions, start=2):
                    try:
                        await tx.execute(INSERT_SQL, pic_version_to_insert_params(p))
                        nb_inserted += 1
                    except Exception:
                        logger.exception(
                            "Échec INSERT trppu_pic_version %s",
                            ctx(
                                fichier=file.filename,
                                ligne_excel=excel_row,
                                params=params_loggables(p),
                            ),
                        )
                        raise
        except Exception as e:
            logger.exception(
                "Erreur insertion lot trppu_pic_version %s",
                ctx(fichier=file.filename, taille_lot=len(pic_versions)),
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
            "cible": "trppu_pic_version",
            "fichier": file.filename,
            "inseres": nb_inserted,
            "erreurs": len(errors),
        },
    )
    logger.info(
        "Fin upload Excel versions PIC %s",
        ctx(
            fichier=file.filename,
            inseres=nb_inserted,
            erreurs=len(errors),
            duration_ms=duration_ms,
        ),
    )
    return BulkUploadResult(
        nb_rows_read=len(pic_versions) + len([e for e in errors if e.row > 0]),
        nb_inserted=nb_inserted,
        nb_errors=len(errors),
        errors=errors,
        execution_time_s=duration_s,
    )
