"""Utilitaires pour trppu_produit : parsing Excel, normalisation, requêtes SQL."""

from __future__ import annotations

import logging
from datetime import date, datetime
from io import BytesIO
from typing import Any

from fastapi import HTTPException
from openpyxl import load_workbook
from pydantic import ValidationError

from app.log_utils import ctx

from .schemas import BulkUploadError, ProduitCreate

logger = logging.getLogger(__name__)

EXPECTED_HEADERS = [
    "co_produit",
    "lb_produit",
    "dt_desactivation",
    "motif_desactivation",
]
REQUIRED_HEADERS = ["co_produit", "lb_produit"]


UPSERT_SQL = (
    "INSERT INTO trppu_produit "
    "(co_produit, lb_produit, dt_desactivation, motif_desactivation) "
    "VALUES (%s, %s, %s, %s) "
    "ON DUPLICATE KEY UPDATE "
    "lb_produit = VALUES(lb_produit), "
    "dt_desactivation = VALUES(dt_desactivation), "
    "motif_desactivation = VALUES(motif_desactivation)"
)


SELECT_CODES_PRODUITS_SQL = "SELECT co_produit FROM trppu_produit WHERE co_produit IN ({})"


async def assert_produits_exist(tx, codes) -> None:
    """Rejette en 422 un lot TMH qui référence des produits absents de `trppu_produit`.

    Les produits sont alimentés hors des écritures TMH : le TMH ne les crée pas. Ce contrôle,
    fait avant tout INSERT sur `trppu_tmh`, remplace la violation de la FK `fk_tmh_produit`
    (500 opaque) par une erreur qui nomme les codes manquants.
    """
    demandes = list(dict.fromkeys(str(c).strip().upper() for c in codes if c is not None))
    demandes = [c for c in demandes if c]
    if not demandes:
        return
    placeholders = ", ".join(["%s"] * len(demandes))
    rows = await tx.fetch_all(SELECT_CODES_PRODUITS_SQL.format(placeholders), tuple(demandes))
    connus = {str(r["co_produit"]).upper() for r in rows}
    manquants = [c for c in demandes if c not in connus]
    if manquants:
        logger.warning(
            "Rejet écriture TMH %s",
            ctx(http=422, motif="produits inconnus", codes=manquants),
        )
        raise HTTPException(
            status_code=422,
            detail=f"Produit(s) inconnu(s) dans trppu_produit : {', '.join(manquants)}.",
        )


def _normalize_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        s = value.strip()
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
    raise ValueError(f"Date invalide : {value!r}")


def _normalize_co_produit(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(int(value)).zfill(2)
    return str(value).strip().upper().zfill(2)


def parse_excel_produits(content: bytes) -> tuple[list[ProduitCreate], list[BulkUploadError]]:
    wb = load_workbook(BytesIO(content), data_only=True, read_only=True)
    ws = wb.worksheets[0]

    rows_iter = ws.iter_rows(values_only=True)
    try:
        header_row = next(rows_iter)
    except StopIteration as e:
        raise ValueError("Fichier Excel vide.") from e

    headers = [
        (str(h).strip().lower() if h is not None else "") for h in header_row
    ]
    missing = [h for h in REQUIRED_HEADERS if h not in headers]
    if missing:
        raise ValueError(
            f"Colonnes obligatoires manquantes : {missing}. Colonnes attendues : {EXPECTED_HEADERS}"
        )

    idx = {h: headers.index(h) for h in EXPECTED_HEADERS if h in headers}

    valid: list[ProduitCreate] = []
    errors: list[BulkUploadError] = []

    for excel_row_num, row in enumerate(rows_iter, start=2):
        if row is None or all(cell is None or cell == "" for cell in row):
            continue

        raw = {h: row[i] if i < len(row) else None for h, i in idx.items()}

        try:
            payload = {
                "co_produit": _normalize_co_produit(raw.get("co_produit")),
                "lb_produit": (str(raw["lb_produit"]).strip() if raw.get("lb_produit") is not None else ""),
                "dt_desactivation": _normalize_date(raw.get("dt_desactivation")),
                "motif_desactivation": (
                    str(raw["motif_desactivation"]).strip()
                    if raw.get("motif_desactivation") not in (None, "")
                    else None
                ),
            }
            produit = ProduitCreate.model_validate(payload)
            valid.append(produit)
        except ValidationError as e:
            errors.append(BulkUploadError(row=excel_row_num, error=str(e), raw=raw))
        except (ValueError, TypeError) as e:
            errors.append(BulkUploadError(row=excel_row_num, error=str(e), raw=raw))

    return valid, errors


def produit_to_upsert_params(p: ProduitCreate) -> tuple:
    return (
        p.co_produit,
        p.lb_produit,
        p.dt_desactivation,
        p.motif_desactivation,
    )
