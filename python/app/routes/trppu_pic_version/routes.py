"""Routes CRUD et upload Excel pour la table trppu_pic_version."""

import logging
import time
from datetime import datetime

from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status

from app.db.mysql import db_read, db_write

from .helpers import (
    INSERT_SQL,
    parse_excel_pic_versions,
    pic_version_to_insert_params,
)
from .schemas import (
    BulkUploadError,
    BulkUploadResult,
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


async def _site_exists(co_regate: str) -> bool:
    row = await db_read.fetch_one(
        "SELECT 1 AS ok FROM trppu_site WHERE co_regate = %s", (co_regate,)
    )
    return row is not None


@router.get("", response_model=list[PicVersionOut])
async def list_pic_versions(
    co_regate: str | None = Query(None, min_length=6, max_length=6),
    niveau: NiveauEnum | None = Query(None),
    actif_only: bool = Query(False, description="N'inclut pas les versions désactivées."),
    est_par_defaut: bool | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
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
        return await db_read.fetch_all(sql, tuple(params))
    except Exception as e:
        logger.error("Erreur listing pic_versions : %s", e)
        raise HTTPException(status_code=500, detail="Erreur listing pic_versions.") from e


@router.get("/enums")
async def list_enums():
    """Valeurs autorisées pour les colonnes ENUM de trppu_pic_version."""
    return {"niveau": [e.value for e in NiveauEnum]}


@router.get("/{id_pic_version}", response_model=PicVersionOut)
async def get_pic_version(id_pic_version: int):
    try:
        row = await db_read.fetch_one(
            SELECT_PICV_SQL + " WHERE id_pic_version = %s", (id_pic_version,)
        )
    except Exception as e:
        logger.error("Erreur get pic_version %d : %s", id_pic_version, e)
        raise HTTPException(status_code=500, detail="Erreur récupération PIC version.") from e
    if not row:
        raise HTTPException(
            status_code=404, detail=f"PIC version {id_pic_version} introuvable."
        )
    return row


@router.post("", response_model=PicVersionOut, status_code=status.HTTP_201_CREATED)
async def create_pic_version(payload: PicVersionCreate):
    if not await _site_exists(payload.co_regate):
        raise HTTPException(
            status_code=422,
            detail=f"Site parent {payload.co_regate} inexistant dans trppu_site.",
        )

    try:
        async with db_write.transaction() as tx:
            await tx.execute(INSERT_SQL, pic_version_to_insert_params(payload))
            new_row = await tx.fetch_one(
                SELECT_PICV_SQL + " WHERE id_pic_version = LAST_INSERT_ID()"
            )
    except Exception as e:
        logger.error("Erreur création pic_version : %s", e)
        raise HTTPException(status_code=500, detail="Erreur création PIC version.") from e

    return new_row


@router.put("/{id_pic_version}", response_model=PicVersionOut)
async def update_pic_version(id_pic_version: int, payload: PicVersionUpdate):
    fields = payload.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(status_code=400, detail="Aucun champ à mettre à jour.")

    existing = await db_read.fetch_one(
        "SELECT id_pic_version, dt_activation FROM trppu_pic_version WHERE id_pic_version = %s",
        (id_pic_version,),
    )
    if not existing:
        raise HTTPException(
            status_code=404, detail=f"PIC version {id_pic_version} introuvable."
        )

    if "co_regate" in fields and not await _site_exists(fields["co_regate"]):
        raise HTTPException(
            status_code=422,
            detail=f"Site parent {fields['co_regate']} inexistant dans trppu_site.",
        )

    # Cohérence dt_activation/dt_desactivation après MAJ partielle
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
        logger.error("Erreur update pic_version %d : %s", id_pic_version, e)
        raise HTTPException(
            status_code=500, detail="Erreur mise à jour PIC version."
        ) from e

    return await db_read.fetch_one(
        SELECT_PICV_SQL + " WHERE id_pic_version = %s", (id_pic_version,)
    )


@router.delete("/{id_pic_version}", response_model=SoftDeleteResult)
async def soft_delete_pic_version(
    id_pic_version: int,
    motif: str = Query("Désactivé via API", max_length=255),
):
    """Soft delete : positionne dt_desactivation = NOW() + motif_desactivation.

    Échoue si dt_activation est dans le futur (CHECK strict dt_desactivation > dt_activation).
    """
    existing = await db_read.fetch_one(
        "SELECT id_pic_version, dt_activation FROM trppu_pic_version WHERE id_pic_version = %s",
        (id_pic_version,),
    )
    if not existing:
        raise HTTPException(
            status_code=404, detail=f"PIC version {id_pic_version} introuvable."
        )

    now = datetime.now()
    if now <= existing["dt_activation"]:
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
        logger.error("Erreur soft delete pic_version %d : %s", id_pic_version, e)
        raise HTTPException(
            status_code=500, detail="Erreur désactivation PIC version."
        ) from e

    return SoftDeleteResult(
        id_pic_version=id_pic_version,
        dt_desactivation=now,
        motif_desactivation=motif,
        rows_affected=rows_affected,
    )


@router.post("/upload-excel", response_model=BulkUploadResult)
async def upload_excel(file: UploadFile = File(..., description="Fichier .xlsx")):
    """Upload massif via Excel : INSERT-only (PK auto-incrément).

    - Pré-vérifie que chaque `co_regate` existe dans trppu_site.
    - Les lignes invalides sont collectées dans `errors`.
    - Les nouvelles versions reçoivent un `id_pic_version` auto-généré côté base.
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
        pic_versions, errors = parse_excel_pic_versions(content)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("Erreur parsing Excel pic_versions : %s", e)
        raise HTTPException(status_code=500, detail="Erreur lecture Excel.") from e

    valid_pvs: list = []
    if pic_versions:
        wanted = {p.co_regate for p in pic_versions}
        existing_rows = await db_read.fetch_all(
            f"SELECT co_regate FROM trppu_site WHERE co_regate IN "
            f"({','.join(['%s'] * len(wanted))})",
            tuple(wanted),
        )
        existing_sites = {r["co_regate"] for r in existing_rows}
        for i, p in enumerate(pic_versions, start=2):
            if p.co_regate not in existing_sites:
                errors.append(
                    BulkUploadError(
                        row=i,
                        error=f"Site parent {p.co_regate} inexistant dans trppu_site",
                        raw=p.model_dump(mode="json"),
                    )
                )
            else:
                valid_pvs.append(p)

    nb_inserted = 0
    if valid_pvs:
        try:
            async with db_write.transaction() as tx:
                for p in valid_pvs:
                    await tx.execute(INSERT_SQL, pic_version_to_insert_params(p))
                    nb_inserted += 1
        except Exception as e:
            logger.error("Erreur insert lot trppu_pic_version : %s", e)
            raise HTTPException(
                status_code=500,
                detail=f"Échec de l'écriture du lot en base : {e}",
            ) from e

    duration_s = round(time.perf_counter() - start, 3)
    return BulkUploadResult(
        nb_rows_read=len(pic_versions) + len([e for e in errors if e.row > 0]),
        nb_inserted=nb_inserted,
        nb_errors=len(errors),
        errors=errors,
        execution_time_s=duration_s,
    )
