"""Routes CRUD et upload Excel pour la table trppu_pic_coefficients."""

import logging
import time
from datetime import date, datetime

from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status

from app.db.mysql import db_read, db_write
from app.log_utils import ctx, diff_champs, params_loggables, safe_preview
from app.services.api_log import (
    ACTION_ECRITURE_PIC_COEFFICIENT,
    ACTION_IMPORT_EXCEL,
    ACTION_SUPPRESSION_PIC_COEFFICIENT,
    enregistrer_appel,
)

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
    logger.info("Début listing coefficients PIC %s", ctx(filtres=filters))

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
        logger.exception("Erreur listing coefficients PIC %s", ctx(filtres=filters))
        raise HTTPException(
            status_code=500, detail="Erreur listing pic_coefficients."
        ) from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin listing coefficients PIC %s",
        ctx(count=len(rows), duration_ms=duration_ms),
    )
    return rows


@router.get("/enums")
async def list_enums():
    """Valeurs autorisées pour les colonnes ENUM de trppu_pic_coefficients."""
    logger.debug(
        "Lecture enums coefficients PIC %s", ctx(count=len(JourSemaineEnum))
    )
    return {"jour_semaine": [e.value for e in JourSemaineEnum]}


@router.get("/{id_pic_coef}", response_model=PicCoefOut)
async def get_pic_coef(id_pic_coef: int):
    start = time.perf_counter()
    logger.info("Début lecture coefficient PIC %s", ctx(id_pic_coef=id_pic_coef))

    try:
        row = await db_read.fetch_one(
            SELECT_PICC_SQL + " WHERE id_pic_coef = %s", (id_pic_coef,)
        )
    except Exception as e:
        logger.exception(
            "Erreur lecture coefficient PIC %s", ctx(id_pic_coef=id_pic_coef)
        )
        raise HTTPException(
            status_code=500, detail="Erreur récupération coefficient."
        ) from e
    if not row:
        logger.warning(
            "Rejet lecture coefficient PIC %s",
            ctx(id_pic_coef=id_pic_coef, http=404, motif="coefficient introuvable"),
        )
        raise HTTPException(
            status_code=404, detail=f"Coefficient {id_pic_coef} introuvable."
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin lecture coefficient PIC %s",
        ctx(id_pic_coef=id_pic_coef, duration_ms=duration_ms),
    )
    return row


@router.post("", response_model=PicCoefOut, status_code=status.HTTP_201_CREATED)
async def create_pic_coef(payload: PicCoefCreate):
    start = time.perf_counter()
    logged = params_loggables(payload)
    logger.info(
        "Début création coefficient PIC %s",
        ctx(
            id_pic_version=payload.id_pic_version,
            co_produit=payload.co_produit,
            jour_semaine=payload.jour_semaine.value,
            densite=payload.densite,
            params=logged,
        ),
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
        logger.warning(
            "Rejet création coefficient PIC %s",
            ctx(
                id_pic_coef_existant=duplicate["id_pic_coef"],
                id_pic_version=payload.id_pic_version,
                co_produit=payload.co_produit,
                http=409,
                motif="clé naturelle déjà utilisée",
            ),
        )
        raise HTTPException(
            status_code=409,
            detail=(
                f"Un coefficient existe déjà pour (id_pic_version={payload.id_pic_version}, "
                f"co_produit={payload.co_produit}, jour_semaine={payload.jour_semaine.value}, "
                f"densite={payload.densite}) — id_pic_coef={duplicate['id_pic_coef']}."
            ),
        )
    logger.debug(
        "Vérification clé naturelle OK %s",
        ctx(id_pic_version=payload.id_pic_version, co_produit=payload.co_produit),
    )

    try:
        async with db_write.transaction() as tx:
            rows_inseres = await tx.execute(
                "INSERT INTO trppu_pic_coefficients "
                "(id_pic_version, co_produit, jour_semaine, densite, dt_effet, dt_fin, coef) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                pic_coef_to_upsert_params(payload),
            )
            new_row = await tx.fetch_one(
                SELECT_PICC_SQL + " WHERE id_pic_coef = LAST_INSERT_ID()"
            )
    except Exception as e:
        logger.exception("Erreur création coefficient PIC %s", ctx(params=logged))
        raise HTTPException(status_code=500, detail="Erreur création coefficient.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_ECRITURE_PIC_COEFFICIENT,
        params={
            "operation": "creation",
            "id_pic_coef": new_row["id_pic_coef"],
            "params": logged,
        },
    )
    logger.info(
        "Fin création coefficient PIC %s",
        ctx(
            id_pic_coef=new_row["id_pic_coef"],
            id_pic_version=payload.id_pic_version,
            co_produit=payload.co_produit,
            rows_affected=rows_inseres,
            duration_ms=duration_ms,
        ),
    )
    return new_row


@router.put("/{id_pic_coef}", response_model=PicCoefOut)
async def update_pic_coef(id_pic_coef: int, payload: PicCoefUpdate):
    start = time.perf_counter()
    fields = payload.model_dump(exclude_unset=True)
    logged = params_loggables(fields)
    logger.info(
        "Début MAJ coefficient PIC %s",
        ctx(id_pic_coef=id_pic_coef, params=logged),
    )

    if not fields:
        logger.warning(
            "Rejet MAJ coefficient PIC %s",
            ctx(id_pic_coef=id_pic_coef, http=400, motif="aucun champ fourni"),
        )
        raise HTTPException(status_code=400, detail="Aucun champ à mettre à jour.")

    existing = await db_read.fetch_one(
        "SELECT id_pic_coef, id_pic_version, co_produit, jour_semaine, densite, dt_effet "
        "FROM trppu_pic_coefficients WHERE id_pic_coef = %s",
        (id_pic_coef,),
    )
    if not existing:
        logger.warning(
            "Rejet MAJ coefficient PIC %s",
            ctx(id_pic_coef=id_pic_coef, http=404, motif="coefficient introuvable"),
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
            logger.warning(
                "Rejet MAJ coefficient PIC %s",
                ctx(
                    id_pic_coef=id_pic_coef,
                    dt_effet=new_dt_effet_date,
                    dt_fin=fields["dt_fin"],
                    http=422,
                    motif="dt_fin <= dt_effet",
                ),
            )
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
            logger.warning(
                "Rejet MAJ coefficient PIC %s",
                ctx(
                    id_pic_coef=id_pic_coef,
                    id_pic_coef_collision=collision["id_pic_coef"],
                    http=409,
                    motif="collision de clé naturelle",
                ),
            )
            raise HTTPException(
                status_code=409,
                detail=f"La nouvelle clé naturelle entre en collision avec id_pic_coef={collision['id_pic_coef']}.",
            )
        logger.debug(
            "Vérification collision clé naturelle OK %s", ctx(id_pic_coef=id_pic_coef)
        )

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
        rows_maj = await db_write.execute(
            f"UPDATE trppu_pic_coefficients SET {', '.join(set_parts)} WHERE id_pic_coef = %s",
            tuple(params),
        )
    except Exception as e:
        logger.exception(
            "Erreur MAJ coefficient PIC %s",
            ctx(id_pic_coef=id_pic_coef, params=logged),
        )
        raise HTTPException(
            status_code=500, detail="Erreur mise à jour coefficient."
        ) from e

    updated = await db_read.fetch_one(
        SELECT_PICC_SQL + " WHERE id_pic_coef = %s", (id_pic_coef,)
    )
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    delta = diff_champs(existing, updated or {})
    await enregistrer_appel(
        api_name=ACTION_ECRITURE_PIC_COEFFICIENT,
        params={
            "operation": "maj",
            "id_pic_coef": id_pic_coef,
            "params": logged,
            "delta": delta,
        },
    )
    logger.info(
        "Fin MAJ coefficient PIC %s",
        ctx(
            id_pic_coef=id_pic_coef,
            nb_champs=len(fields),
            rows_affected=rows_maj,
            delta=delta,
            duration_ms=duration_ms,
        ),
    )
    return updated


@router.delete("/{id_pic_coef}", response_model=SoftDeleteResult)
async def soft_delete_pic_coef(id_pic_coef: int):
    """Soft delete : positionne dt_fin = aujourd'hui (clôt la période)."""
    start = time.perf_counter()
    logger.info("Début clôture coefficient PIC %s", ctx(id_pic_coef=id_pic_coef))

    existing = await db_read.fetch_one(
        "SELECT id_pic_coef, dt_effet FROM trppu_pic_coefficients WHERE id_pic_coef = %s",
        (id_pic_coef,),
    )
    if not existing:
        logger.warning(
            "Rejet clôture coefficient PIC %s",
            ctx(id_pic_coef=id_pic_coef, http=404, motif="coefficient introuvable"),
        )
        raise HTTPException(
            status_code=404, detail=f"Coefficient {id_pic_coef} introuvable."
        )

    today = date.today()
    dt_effet_val = existing["dt_effet"]
    dt_effet_date = dt_effet_val.date() if isinstance(dt_effet_val, datetime) else dt_effet_val
    if today <= dt_effet_date:
        logger.warning(
            "Rejet clôture coefficient PIC %s",
            ctx(
                id_pic_coef=id_pic_coef,
                dt_effet=dt_effet_val,
                http=422,
                motif="dt_effet aujourd'hui ou dans le futur",
            ),
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
        logger.exception(
            "Erreur clôture coefficient PIC %s", ctx(id_pic_coef=id_pic_coef)
        )
        raise HTTPException(
            status_code=500, detail="Erreur clôture coefficient."
        ) from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_SUPPRESSION_PIC_COEFFICIENT,
        params={
            "operation": "cloture",
            "id_pic_coef": id_pic_coef,
            "dt_effet": str(dt_effet_val),
            "dt_fin": str(today),
        },
    )
    logger.info(
        "Fin clôture coefficient PIC %s",
        ctx(
            id_pic_coef=id_pic_coef,
            dt_fin=today,
            rows_affected=rows_affected,
            duration_ms=duration_ms,
        ),
    )
    return SoftDeleteResult(
        id_pic_coef=id_pic_coef, dt_fin=today, rows_affected=rows_affected
    )


@router.post("/upload-excel", response_model=BulkUploadResult)
async def upload_excel(file: UploadFile = File(..., description="Fichier .xlsx")):
    """Upload massif via Excel : upsert sur la natural key uq_picc."""
    start = time.perf_counter()
    logger.info(
        "Début upload Excel coefficients PIC %s", ctx(fichier=file.filename)
    )

    if not file.filename or not file.filename.lower().endswith((".xlsx", ".xlsm")):
        logger.warning(
            "Rejet upload Excel coefficients PIC %s",
            ctx(fichier=file.filename, http=400, motif="extension non supportée"),
        )
        raise HTTPException(
            status_code=400,
            detail="Format de fichier non supporté : attendu .xlsx ou .xlsm.",
        )

    content = await file.read()
    if not content:
        logger.warning(
            "Rejet upload Excel coefficients PIC %s",
            ctx(fichier=file.filename, http=400, motif="fichier vide"),
        )
        raise HTTPException(status_code=400, detail="Fichier vide.")
    logger.debug(
        "Fichier Excel coefficients PIC lu %s",
        ctx(fichier=file.filename, taille_octets=len(content)),
    )

    try:
        coefs, errors = parse_excel_pic_coefs(content)
    except ValueError as e:
        logger.warning(
            "Rejet upload Excel coefficients PIC %s",
            ctx(
                fichier=file.filename,
                http=400,
                motif=safe_preview(str(e), max_len=200),
            ),
        )
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception(
            "Erreur parsing Excel coefficients PIC %s",
            ctx(fichier=file.filename, taille_octets=len(content)),
        )
        raise HTTPException(status_code=500, detail="Erreur lecture Excel.") from e
    logger.info(
        "Excel coefficients PIC parsé %s",
        ctx(fichier=file.filename, valides=len(coefs), erreurs=len(errors)),
    )

    nb_inserted = 0
    nb_updated = 0
    nb_unchanged = 0

    if coefs:
        logger.debug(
            "Ouverture transaction upsert coefficients PIC %s",
            ctx(fichier=file.filename, taille_lot=len(coefs)),
        )
        try:
            async with db_write.transaction() as tx:
                for excel_row, c in enumerate(coefs, start=2):
                    try:
                        rc = await tx.execute(UPSERT_SQL, pic_coef_to_upsert_params(c))
                    except Exception:
                        logger.exception(
                            "Échec UPSERT trppu_pic_coefficients %s",
                            ctx(
                                fichier=file.filename,
                                ligne_excel=excel_row,
                                params=params_loggables(c),
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
                "Erreur upsert lot trppu_pic_coefficients %s",
                ctx(fichier=file.filename, taille_lot=len(coefs)),
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
            "cible": "trppu_pic_coefficients",
            "fichier": file.filename,
            "inseres": nb_inserted,
            "modifies": nb_updated,
            "inchanges": nb_unchanged,
            "erreurs": len(errors),
        },
    )
    logger.info(
        "Fin upload Excel coefficients PIC %s",
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
        nb_rows_read=len(coefs) + len([e for e in errors if e.row > 0]),
        nb_inserted=nb_inserted,
        nb_updated=nb_updated,
        nb_unchanged=nb_unchanged,
        nb_errors=len(errors),
        errors=errors,
        execution_time_s=duration_s,
    )
