"""Routes CRUD et upload Excel pour la table trppu_pic_coefficients."""

import logging
import time
from datetime import date, datetime

from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status

from app.db.mysql import db_read, db_write
from app.log_utils import safe_preview

from .helpers import (
    UPSERT_SQL,
    parse_excel_pic_coefs,
    pic_coef_to_upsert_params,
)
from .schemas import (
    BulkUploadResult,
    JourSemaineEnum,
    PicCoefCreate,
    PicCoefOut,
    PicCoefUpdate,
    SoftDeleteResult,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/pic-coefficients", tags=["PIC Coefficients"])


SELECT_PICC_SQL = (
    "SELECT id_pic_coef, id_pic_version, co_produit, jour_semaine, densite, "
    "dt_effet, dt_fin, coef, "
    "dt_creation, dt_maj, id_rh "
    "FROM trppu_pic_coefficients"
)


@router.get("", response_model=list[PicCoefOut])
async def list_pic_coefs(
    id_pic_version: int | None = Query(None, gt=0),
    co_produit: str | None = Query(None, min_length=2, max_length=2),
    jour_semaine: JourSemaineEnum | None = Query(None),
    actif_only: bool = Query(False, description="Si true, exclut les coefficients clos."),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    start = time.perf_counter()
    filters = {
        "id_pic_version": id_pic_version,
        "co_produit": co_produit,
        "jour_semaine": jour_semaine,
        "actif_only": actif_only,
        "limit": limit,
        "offset": offset,
    }
    logger.info("Début listing des coefficients PIC (filtres=%s)", safe_preview(filters))

    where: list[str] = []
    params: list = []
    if id_pic_version is not None:
        where.append("id_pic_version = %s")
        params.append(id_pic_version)
    if co_produit is not None:
        where.append("co_produit = %s")
        params.append(co_produit)
    if jour_semaine is not None:
        where.append("jour_semaine = %s")
        params.append(jour_semaine.value)
    if actif_only:
        where.append("(dt_fin IS NULL OR dt_fin > NOW())")

    sql = SELECT_PICC_SQL
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id_pic_version, co_produit, jour_semaine, dt_effet LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    try:
        rows = await db_read.fetch_all(sql, tuple(params))
    except Exception as e:
        logger.exception(
            "Erreur listing des coefficients PIC (filtres=%s)", safe_preview(filters)
        )
        raise HTTPException(
            status_code=500, detail="Erreur listing pic_coefficients."
        ) from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Listing des coefficients PIC terminé (count=%d, duration_ms=%.1f)",
        len(rows),
        duration_ms,
    )
    return rows


@router.get("/enums")
async def list_enums():
    """Valeurs autorisées pour les colonnes ENUM de trppu_pic_coefficients."""
    logger.info("Récupération des enums coefficients PIC")
    return {"jour_semaine": [e.value for e in JourSemaineEnum]}


@router.get("/{id_pic_coef}", response_model=PicCoefOut)
async def get_pic_coef(id_pic_coef: int):
    start = time.perf_counter()
    logger.info("Début récupération coefficient PIC (id_pic_coef=%d)", id_pic_coef)

    try:
        row = await db_read.fetch_one(
            SELECT_PICC_SQL + " WHERE id_pic_coef = %s", (id_pic_coef,)
        )
    except Exception as e:
        logger.exception("Erreur récupération coefficient PIC (id_pic_coef=%d)", id_pic_coef)
        raise HTTPException(
            status_code=500, detail="Erreur récupération coefficient."
        ) from e
    if not row:
        logger.info("Coefficient PIC introuvable (id_pic_coef=%d)", id_pic_coef)
        raise HTTPException(
            status_code=404, detail=f"Coefficient {id_pic_coef} introuvable."
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Récupération coefficient PIC terminée (id_pic_coef=%d, duration_ms=%.1f)",
        id_pic_coef,
        duration_ms,
    )
    return row


@router.post("", response_model=PicCoefOut, status_code=status.HTTP_201_CREATED)
async def create_pic_coef(payload: PicCoefCreate):
    start = time.perf_counter()
    logger.info(
        "Début création coefficient PIC (id_pic_version=%d, co_produit=%s, jour_semaine=%s, densite=%s)",
        payload.id_pic_version,
        payload.co_produit,
        payload.jour_semaine.value,
        payload.densite,
    )

    duplicate = await db_read.fetch_one(
        "SELECT id_pic_coef FROM trppu_pic_coefficients "
        "WHERE id_pic_version = %s AND co_produit = %s "
        "AND jour_semaine = %s AND densite = %s",
        (
            payload.id_pic_version,
            payload.co_produit,
            payload.jour_semaine.value,
            payload.densite,
        ),
    )
    if duplicate:
        logger.info(
            "Coefficient PIC déjà existant pour la clé naturelle (id_pic_coef existant=%d)",
            duplicate["id_pic_coef"],
        )
        raise HTTPException(
            status_code=409,
            detail=(
                f"Un coefficient existe déjà pour (id_pic_version={payload.id_pic_version}, "
                f"co_produit={payload.co_produit}, jour_semaine={payload.jour_semaine.value}, "
                f"densite={payload.densite}) — id_pic_coef={duplicate['id_pic_coef']}."
            ),
        )
    logger.info("Vérification clé naturelle OK")

    try:
        async with db_write.transaction() as tx:
            await tx.execute(
                "INSERT INTO trppu_pic_coefficients "
                "(id_pic_version, co_produit, jour_semaine, densite, dt_effet, dt_fin, coef) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                pic_coef_to_upsert_params(payload),
            )
            new_row = await tx.fetch_one(
                SELECT_PICC_SQL + " WHERE id_pic_coef = LAST_INSERT_ID()"
            )
    except Exception as e:
        logger.exception(
            "Erreur création coefficient PIC (payload=%s)",
            safe_preview(payload.model_dump(mode="json")),
        )
        raise HTTPException(status_code=500, detail="Erreur création coefficient.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Création coefficient PIC terminée (id_pic_coef=%d, duration_ms=%.1f)",
        new_row["id_pic_coef"],
        duration_ms,
    )
    return new_row


@router.put("/{id_pic_coef}", response_model=PicCoefOut)
async def update_pic_coef(id_pic_coef: int, payload: PicCoefUpdate):
    start = time.perf_counter()
    fields = payload.model_dump(exclude_unset=True)
    logger.info(
        "Début mise à jour coefficient PIC (id_pic_coef=%d, champs=%s)",
        id_pic_coef,
        safe_preview(fields),
    )

    if not fields:
        raise HTTPException(status_code=400, detail="Aucun champ à mettre à jour.")

    existing = await db_read.fetch_one(
        "SELECT id_pic_coef, id_pic_version, co_produit, jour_semaine, densite, dt_effet "
        "FROM trppu_pic_coefficients WHERE id_pic_coef = %s",
        (id_pic_coef,),
    )
    if not existing:
        logger.info(
            "Coefficient PIC introuvable pour mise à jour (id_pic_coef=%d)", id_pic_coef
        )
        raise HTTPException(
            status_code=404, detail=f"Coefficient {id_pic_coef} introuvable."
        )

    new_dt_effet = fields.get("dt_effet", existing["dt_effet"])
    # dt_effet en base est un datetime ; on compare sur la partie date.
    new_dt_effet_date = (
        new_dt_effet.date() if isinstance(new_dt_effet, datetime) else new_dt_effet
    )
    if "dt_fin" in fields and fields["dt_fin"] is not None:
        if fields["dt_fin"] <= new_dt_effet_date:
            raise HTTPException(
                status_code=422,
                detail="dt_fin doit être strictement supérieure à dt_effet.",
            )

    nk_changed = any(
        k in fields for k in ("id_pic_version", "co_produit", "jour_semaine", "densite")
    )
    if nk_changed:
        nk = {
            "id_pic_version": fields.get("id_pic_version", existing["id_pic_version"]),
            "co_produit": fields.get("co_produit", existing["co_produit"]),
            "jour_semaine": (
                fields["jour_semaine"].value
                if isinstance(fields.get("jour_semaine"), JourSemaineEnum)
                else fields.get("jour_semaine", existing["jour_semaine"])
            ),
            "densite": fields.get("densite", existing["densite"]),
        }
        collision = await db_read.fetch_one(
            "SELECT id_pic_coef FROM trppu_pic_coefficients "
            "WHERE id_pic_version = %s AND co_produit = %s "
            "AND jour_semaine = %s AND densite = %s "
            "AND id_pic_coef <> %s",
            (nk["id_pic_version"], nk["co_produit"], nk["jour_semaine"], nk["densite"], id_pic_coef),
        )
        if collision:
            logger.info(
                "Collision clé naturelle (id_pic_coef=%d collisionne avec id_pic_coef=%d)",
                id_pic_coef,
                collision["id_pic_coef"],
            )
            raise HTTPException(
                status_code=409,
                detail=f"La nouvelle clé naturelle entre en collision avec id_pic_coef={collision['id_pic_coef']}.",
            )
        logger.info("Vérification collision clé naturelle OK")

    set_parts: list[str] = []
    params: list = []
    for key, value in fields.items():
        set_parts.append(f"{key} = %s")
        if key == "jour_semaine":
            params.append(value.value if hasattr(value, "value") else value)
        else:
            params.append(value)
    params.append(id_pic_coef)

    try:
        await db_write.execute(
            f"UPDATE trppu_pic_coefficients SET {', '.join(set_parts)} WHERE id_pic_coef = %s",
            tuple(params),
        )
    except Exception as e:
        logger.exception(
            "Erreur mise à jour coefficient PIC (id_pic_coef=%d, champs=%s)",
            id_pic_coef,
            safe_preview(fields),
        )
        raise HTTPException(
            status_code=500, detail="Erreur mise à jour coefficient."
        ) from e

    updated = await db_read.fetch_one(
        SELECT_PICC_SQL + " WHERE id_pic_coef = %s", (id_pic_coef,)
    )
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Mise à jour coefficient PIC terminée (id_pic_coef=%d, nb_champs=%d, duration_ms=%.1f)",
        id_pic_coef,
        len(fields),
        duration_ms,
    )
    return updated


@router.delete("/{id_pic_coef}", response_model=SoftDeleteResult)
async def soft_delete_pic_coef(id_pic_coef: int):
    """Soft delete : positionne dt_fin = aujourd'hui (clôt la période)."""
    start = time.perf_counter()
    logger.info("Début clôture coefficient PIC (id_pic_coef=%d)", id_pic_coef)

    existing = await db_read.fetch_one(
        "SELECT id_pic_coef, dt_effet FROM trppu_pic_coefficients WHERE id_pic_coef = %s",
        (id_pic_coef,),
    )
    if not existing:
        logger.info("Coefficient PIC introuvable pour clôture (id_pic_coef=%d)", id_pic_coef)
        raise HTTPException(
            status_code=404, detail=f"Coefficient {id_pic_coef} introuvable."
        )

    today = date.today()
    dt_effet_val = existing["dt_effet"]
    dt_effet_date = dt_effet_val.date() if isinstance(dt_effet_val, datetime) else dt_effet_val
    if today <= dt_effet_date:
        logger.info(
            "Clôture refusée : dt_effet >= aujourd'hui (id_pic_coef=%d, dt_effet=%s)",
            id_pic_coef,
            dt_effet_val,
        )
        raise HTTPException(
            status_code=422,
            detail="dt_effet est aujourd'hui ou dans le futur — impossible de clore par DELETE. "
            "Utilisez PUT pour fixer dt_fin explicitement.",
        )

    try:
        rows_affected = await db_write.execute(
            "UPDATE trppu_pic_coefficients SET dt_fin = %s WHERE id_pic_coef = %s",
            (today, id_pic_coef),
        )
    except Exception as e:
        logger.exception("Erreur clôture coefficient PIC (id_pic_coef=%d)", id_pic_coef)
        raise HTTPException(
            status_code=500, detail="Erreur clôture coefficient."
        ) from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Clôture coefficient PIC terminée (id_pic_coef=%d, dt_fin=%s, duration_ms=%.1f)",
        id_pic_coef,
        today,
        duration_ms,
    )
    return SoftDeleteResult(
        id_pic_coef=id_pic_coef, dt_fin=today, rows_affected=rows_affected
    )


@router.post("/upload-excel", response_model=BulkUploadResult)
async def upload_excel(file: UploadFile = File(..., description="Fichier .xlsx")):
    """Upload massif via Excel : upsert sur la natural key uq_picc."""
    start = time.perf_counter()
    logger.info("Début upload Excel coefficients PIC (fichier=%s)", file.filename)

    if not file.filename or not file.filename.lower().endswith((".xlsx", ".xlsm")):
        raise HTTPException(
            status_code=400,
            detail="Format de fichier non supporté : attendu .xlsx ou .xlsm.",
        )

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Fichier vide.")
    logger.info(
        "Fichier Excel coefficients PIC lu (fichier=%s, taille=%d octets)",
        file.filename,
        len(content),
    )

    try:
        coefs, errors = parse_excel_pic_coefs(content)
    except ValueError as e:
        logger.info(
            "Excel coefficients PIC invalide (fichier=%s, raison=%s)",
            file.filename,
            safe_preview(str(e), max_len=200),
        )
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception(
            "Erreur parsing Excel coefficients PIC (fichier=%s, taille=%d)",
            file.filename,
            len(content),
        )
        raise HTTPException(status_code=500, detail="Erreur lecture Excel.") from e
    logger.info(
        "Excel coefficients PIC parsé (fichier=%s, valides=%d, erreurs=%d)",
        file.filename,
        len(coefs),
        len(errors),
    )

    nb_inserted = 0
    nb_updated = 0
    nb_unchanged = 0

    if coefs:
        logger.info(
            "Ouverture transaction upsert coefficients PIC (fichier=%s, taille_lot=%d)",
            file.filename,
            len(coefs),
        )
        try:
            async with db_write.transaction() as tx:
                for excel_row, c in enumerate(coefs, start=2):
                    try:
                        rc = await tx.execute(UPSERT_SQL, pic_coef_to_upsert_params(c))
                    except Exception:
                        logger.exception(
                            "Échec UPSERT trppu_pic_coefficients (fichier=%s, ligne_excel=%d, payload=%s)",
                            file.filename,
                            excel_row,
                            safe_preview(c.model_dump(mode="json")),
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
                "Erreur upsert lot trppu_pic_coefficients (fichier=%s, taille_lot=%d)",
                file.filename,
                len(coefs),
            )
            raise HTTPException(
                status_code=500,
                detail=f"Échec de l'écriture du lot en base : {e}",
            ) from e

    duration_s = round(time.perf_counter() - start, 3)
    logger.info(
        "Upload Excel coefficients PIC terminé (fichier=%s, insérés=%d, modifiés=%d, inchangés=%d, erreurs=%d, duration_s=%.3f)",
        file.filename,
        nb_inserted,
        nb_updated,
        nb_unchanged,
        len(errors),
        duration_s,
    )
    return BulkUploadResult(
        nb_rows_read=len(coefs) + len([e for e in errors if e.row > 0]),
        nb_inserted=nb_inserted,
        nb_updated=nb_updated,
        nb_unchanged=nb_unchanged,
        nb_errors=len(errors),
        errors=errors,
        execution_time_s=duration_s,
    )
