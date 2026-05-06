"""Routes CRUD et upload Excel pour la table trppu_pic_coefficients."""

import logging
import time
from datetime import date

from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status

from app.db.mysql import db_read, db_write

from .helpers import (
    UPSERT_SQL,
    parse_excel_pic_coefs,
    pic_coef_to_upsert_params,
)
from .schemas import (
    BulkUploadError,
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
    "SELECT id_pic_coef, id_pic_version, co_produit, jour_semaine, "
    "dt_effet, dt_fin_effet, coef_dense, coef_faible1, coef_faible2, "
    "dt_creation, dt_maj, id_rh_creation "
    "FROM trppu_pic_coefficients"
)


async def _pic_version_exists(id_pic_version: int) -> bool:
    row = await db_read.fetch_one(
        "SELECT 1 AS ok FROM trppu_pic_version WHERE id_pic_version = %s",
        (id_pic_version,),
    )
    return row is not None


async def _produit_exists(co_produit: str) -> bool:
    row = await db_read.fetch_one(
        "SELECT 1 AS ok FROM trppu_produit WHERE co_produit = %s", (co_produit,)
    )
    return row is not None


@router.get("", response_model=list[PicCoefOut])
async def list_pic_coefs(
    id_pic_version: int | None = Query(None, gt=0),
    co_produit: str | None = Query(None, min_length=2, max_length=2),
    jour_semaine: JourSemaineEnum | None = Query(None),
    actif_only: bool = Query(False, description="Si true, exclut les coefficients clos."),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
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
        where.append("(dt_fin_effet IS NULL OR dt_fin_effet > CURDATE())")

    sql = SELECT_PICC_SQL
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id_pic_version, co_produit, jour_semaine, dt_effet LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    try:
        return await db_read.fetch_all(sql, tuple(params))
    except Exception as e:
        logger.error("Erreur listing pic_coefficients : %s", e)
        raise HTTPException(
            status_code=500, detail="Erreur listing pic_coefficients."
        ) from e


@router.get("/enums")
async def list_enums():
    """Valeurs autorisées pour les colonnes ENUM de trppu_pic_coefficients."""
    return {"jour_semaine": [e.value for e in JourSemaineEnum]}


@router.get("/{id_pic_coef}", response_model=PicCoefOut)
async def get_pic_coef(id_pic_coef: int):
    try:
        row = await db_read.fetch_one(
            SELECT_PICC_SQL + " WHERE id_pic_coef = %s", (id_pic_coef,)
        )
    except Exception as e:
        logger.error("Erreur get pic_coef %d : %s", id_pic_coef, e)
        raise HTTPException(
            status_code=500, detail="Erreur récupération coefficient."
        ) from e
    if not row:
        raise HTTPException(
            status_code=404, detail=f"Coefficient {id_pic_coef} introuvable."
        )
    return row


@router.post("", response_model=PicCoefOut, status_code=status.HTTP_201_CREATED)
async def create_pic_coef(payload: PicCoefCreate):
    if not await _pic_version_exists(payload.id_pic_version):
        raise HTTPException(
            status_code=422,
            detail=f"PIC version {payload.id_pic_version} inexistante dans trppu_pic_version.",
        )
    if not await _produit_exists(payload.co_produit):
        raise HTTPException(
            status_code=422,
            detail=f"Produit {payload.co_produit} inexistant dans trppu_produit.",
        )

    # Pré-check de la natural key uq_picc pour distinguer 409 / 500.
    duplicate = await db_read.fetch_one(
        "SELECT id_pic_coef FROM trppu_pic_coefficients "
        "WHERE id_pic_version = %s AND co_produit = %s "
        "AND jour_semaine = %s AND dt_effet = %s",
        (
            payload.id_pic_version,
            payload.co_produit,
            payload.jour_semaine.value,
            payload.dt_effet,
        ),
    )
    if duplicate:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Un coefficient existe déjà pour (id_pic_version={payload.id_pic_version}, "
                f"co_produit={payload.co_produit}, jour_semaine={payload.jour_semaine.value}, "
                f"dt_effet={payload.dt_effet.isoformat()}) — id_pic_coef={duplicate['id_pic_coef']}."
            ),
        )

    try:
        async with db_write.transaction() as tx:
            await tx.execute(
                "INSERT INTO trppu_pic_coefficients "
                "(id_pic_version, co_produit, jour_semaine, dt_effet, dt_fin_effet, "
                " coef_dense, coef_faible1, coef_faible2) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                pic_coef_to_upsert_params(payload),
            )
            new_row = await tx.fetch_one(
                SELECT_PICC_SQL + " WHERE id_pic_coef = LAST_INSERT_ID()"
            )
    except Exception as e:
        logger.error("Erreur création pic_coef : %s", e)
        raise HTTPException(status_code=500, detail="Erreur création coefficient.") from e

    return new_row


@router.put("/{id_pic_coef}", response_model=PicCoefOut)
async def update_pic_coef(id_pic_coef: int, payload: PicCoefUpdate):
    fields = payload.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(status_code=400, detail="Aucun champ à mettre à jour.")

    existing = await db_read.fetch_one(
        "SELECT id_pic_coef, id_pic_version, co_produit, jour_semaine, dt_effet "
        "FROM trppu_pic_coefficients WHERE id_pic_coef = %s",
        (id_pic_coef,),
    )
    if not existing:
        raise HTTPException(
            status_code=404, detail=f"Coefficient {id_pic_coef} introuvable."
        )

    if "id_pic_version" in fields and not await _pic_version_exists(fields["id_pic_version"]):
        raise HTTPException(
            status_code=422,
            detail=f"PIC version {fields['id_pic_version']} inexistante dans trppu_pic_version.",
        )
    if "co_produit" in fields and not await _produit_exists(fields["co_produit"]):
        raise HTTPException(
            status_code=422,
            detail=f"Produit {fields['co_produit']} inexistant dans trppu_produit.",
        )

    # Cohérence dt_effet/dt_fin_effet après MAJ partielle.
    new_dt_effet = fields.get("dt_effet", existing["dt_effet"])
    if "dt_fin_effet" in fields and fields["dt_fin_effet"] is not None:
        if fields["dt_fin_effet"] <= new_dt_effet:
            raise HTTPException(
                status_code=422,
                detail="dt_fin_effet doit être strictement supérieure à dt_effet.",
            )

    # Détection collision natural key uq_picc.
    nk_changed = any(k in fields for k in ("id_pic_version", "co_produit", "jour_semaine", "dt_effet"))
    if nk_changed:
        nk = {
            "id_pic_version": fields.get("id_pic_version", existing["id_pic_version"]),
            "co_produit": fields.get("co_produit", existing["co_produit"]),
            "jour_semaine": (
                fields["jour_semaine"].value
                if isinstance(fields.get("jour_semaine"), JourSemaineEnum)
                else fields.get("jour_semaine", existing["jour_semaine"])
            ),
            "dt_effet": new_dt_effet,
        }
        collision = await db_read.fetch_one(
            "SELECT id_pic_coef FROM trppu_pic_coefficients "
            "WHERE id_pic_version = %s AND co_produit = %s "
            "AND jour_semaine = %s AND dt_effet = %s "
            "AND id_pic_coef <> %s",
            (nk["id_pic_version"], nk["co_produit"], nk["jour_semaine"], nk["dt_effet"], id_pic_coef),
        )
        if collision:
            raise HTTPException(
                status_code=409,
                detail=f"La nouvelle clé naturelle entre en collision avec id_pic_coef={collision['id_pic_coef']}.",
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
        await db_write.execute(
            f"UPDATE trppu_pic_coefficients SET {', '.join(set_parts)} WHERE id_pic_coef = %s",
            tuple(params),
        )
    except Exception as e:
        logger.error("Erreur update pic_coef %d : %s", id_pic_coef, e)
        raise HTTPException(
            status_code=500, detail="Erreur mise à jour coefficient."
        ) from e

    return await db_read.fetch_one(
        SELECT_PICC_SQL + " WHERE id_pic_coef = %s", (id_pic_coef,)
    )


@router.delete("/{id_pic_coef}", response_model=SoftDeleteResult)
async def soft_delete_pic_coef(id_pic_coef: int):
    """Soft delete : positionne dt_fin_effet = aujourd'hui (clôt la période).

    Échoue en 422 si dt_effet >= aujourd'hui (CHECK strict dt_fin_effet > dt_effet).
    Dans ce cas, utiliser PUT pour fixer une dt_fin_effet postérieure à dt_effet.
    """
    existing = await db_read.fetch_one(
        "SELECT id_pic_coef, dt_effet FROM trppu_pic_coefficients WHERE id_pic_coef = %s",
        (id_pic_coef,),
    )
    if not existing:
        raise HTTPException(
            status_code=404, detail=f"Coefficient {id_pic_coef} introuvable."
        )

    today = date.today()
    if today <= existing["dt_effet"]:
        raise HTTPException(
            status_code=422,
            detail="dt_effet est aujourd'hui ou dans le futur — impossible de clore par DELETE. "
            "Utilisez PUT pour fixer dt_fin_effet explicitement.",
        )

    try:
        rows_affected = await db_write.execute(
            "UPDATE trppu_pic_coefficients SET dt_fin_effet = %s WHERE id_pic_coef = %s",
            (today, id_pic_coef),
        )
    except Exception as e:
        logger.error("Erreur soft delete pic_coef %d : %s", id_pic_coef, e)
        raise HTTPException(
            status_code=500, detail="Erreur clôture coefficient."
        ) from e

    return SoftDeleteResult(
        id_pic_coef=id_pic_coef, dt_fin_effet=today, rows_affected=rows_affected
    )


@router.post("/upload-excel", response_model=BulkUploadResult)
async def upload_excel(file: UploadFile = File(..., description="Fichier .xlsx")):
    """Upload massif via Excel : upsert sur la natural key uq_picc.

    - Pré-vérifie que chaque `id_pic_version` existe dans trppu_pic_version
      et chaque `co_produit` dans trppu_produit.
    - Upsert via INSERT … ON DUPLICATE KEY UPDATE sur
      (id_pic_version, co_produit, jour_semaine, dt_effet).
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
        coefs, errors = parse_excel_pic_coefs(content)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("Erreur parsing Excel pic_coefs : %s", e)
        raise HTTPException(status_code=500, detail="Erreur lecture Excel.") from e

    valid_coefs: list = []
    if coefs:
        wanted_versions = {c.id_pic_version for c in coefs}
        wanted_produits = {c.co_produit for c in coefs}

        existing_v = await db_read.fetch_all(
            f"SELECT id_pic_version FROM trppu_pic_version "
            f"WHERE id_pic_version IN ({','.join(['%s'] * len(wanted_versions))})",
            tuple(wanted_versions),
        )
        existing_p = await db_read.fetch_all(
            f"SELECT co_produit FROM trppu_produit "
            f"WHERE co_produit IN ({','.join(['%s'] * len(wanted_produits))})",
            tuple(wanted_produits),
        )
        valid_versions = {r["id_pic_version"] for r in existing_v}
        valid_produits = {r["co_produit"] for r in existing_p}

        for i, c in enumerate(coefs, start=2):
            problems = []
            if c.id_pic_version not in valid_versions:
                problems.append(f"id_pic_version {c.id_pic_version} inexistante")
            if c.co_produit not in valid_produits:
                problems.append(f"co_produit {c.co_produit} inexistant")
            if problems:
                errors.append(
                    BulkUploadError(
                        row=i, error=" / ".join(problems), raw=c.model_dump(mode="json")
                    )
                )
            else:
                valid_coefs.append(c)

    nb_inserted = 0
    nb_updated = 0
    nb_unchanged = 0

    if valid_coefs:
        try:
            async with db_write.transaction() as tx:
                for c in valid_coefs:
                    rc = await tx.execute(UPSERT_SQL, pic_coef_to_upsert_params(c))
                    if rc == 1:
                        nb_inserted += 1
                    elif rc == 2:
                        nb_updated += 1
                    else:
                        nb_unchanged += 1
        except Exception as e:
            logger.error("Erreur upsert lot trppu_pic_coefficients : %s", e)
            raise HTTPException(
                status_code=500,
                detail=f"Échec de l'écriture du lot en base : {e}",
            ) from e

    duration_s = round(time.perf_counter() - start, 3)
    return BulkUploadResult(
        nb_rows_read=len(coefs) + len([e for e in errors if e.row > 0]),
        nb_inserted=nb_inserted,
        nb_updated=nb_updated,
        nb_unchanged=nb_unchanged,
        nb_errors=len(errors),
        errors=errors,
        execution_time_s=duration_s,
    )
