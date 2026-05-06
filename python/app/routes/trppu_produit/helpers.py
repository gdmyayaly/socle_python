"""Utilitaires pour trppu_produit : parsing Excel, normalisation, requêtes SQL."""

from __future__ import annotations

from datetime import date, datetime
from io import BytesIO
from typing import Any

from openpyxl import load_workbook
from pydantic import ValidationError

from .schemas import BulkUploadError, ProduitCreate

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
