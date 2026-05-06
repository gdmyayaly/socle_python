"""Utilitaires pour la table trppu_site : parsing Excel, normalisation, requêtes SQL."""

from __future__ import annotations

from io import BytesIO
from typing import Any

from openpyxl import load_workbook
from pydantic import ValidationError

from .schemas import BulkUploadError, SiteCreate

EXPECTED_HEADERS = ["co_regate", "lb_regate", "lb_site", "type_site", "co_roc", "est_actif"]
REQUIRED_HEADERS = ["co_regate", "lb_site", "type_site", "co_roc"]


UPSERT_SQL = (
    "INSERT INTO trppu_site (co_regate, lb_regate, lb_site, type_site, co_roc, est_actif) "
    "VALUES (%s, %s, %s, %s, %s, %s) "
    "ON DUPLICATE KEY UPDATE "
    "lb_regate = VALUES(lb_regate), "
    "lb_site = VALUES(lb_site), "
    "type_site = VALUES(type_site), "
    "co_roc = VALUES(co_roc), "
    "est_actif = VALUES(est_actif)"
)


def _normalize_est_actif(value: Any) -> bool:
    """Accepte 0/1, True/False, '0'/'1', 'oui'/'non', 'true'/'false', vide → True."""
    if value is None or value == "":
        return True
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) == 1
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"1", "true", "vrai", "oui", "yes", "y", "o", "actif"}:
            return True
        if v in {"0", "false", "faux", "non", "no", "n", "inactif"}:
            return False
    raise ValueError(f"Valeur est_actif invalide : {value!r}")


def _normalize_co(value: Any) -> str:
    """co_regate / co_roc : padding gauche avec '0' jusqu'à 6 caractères (Excel rogne les zéros)."""
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(int(value)).zfill(6)
    return str(value).strip().zfill(6)


def parse_excel_sites(content: bytes) -> tuple[list[SiteCreate], list[BulkUploadError]]:
    """Lit un fichier Excel et retourne (sites valides, erreurs par ligne).

    Format attendu : première feuille, ligne 1 = en-têtes, ligne 2+ = données.
    Les en-têtes doivent contenir au moins : co_regate, lb_site, type_site, co_roc.
    La colonne est_actif est optionnelle (défaut True).
    """
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
            f"Colonnes obligatoires manquantes dans l'Excel : {missing}. "
            f"Colonnes attendues : {EXPECTED_HEADERS}"
        )

    idx = {h: headers.index(h) for h in EXPECTED_HEADERS if h in headers}

    valid: list[SiteCreate] = []
    errors: list[BulkUploadError] = []

    for excel_row_num, row in enumerate(rows_iter, start=2):
        if row is None or all(cell is None or cell == "" for cell in row):
            continue

        raw = {h: row[i] if i < len(row) else None for h, i in idx.items()}

        try:
            payload = {
                "co_regate": _normalize_co(raw.get("co_regate")),
                "lb_regate": (
                    str(raw["lb_regate"]).strip()
                    if raw.get("lb_regate") not in (None, "")
                    else None
                ),
                "lb_site": (str(raw["lb_site"]).strip() if raw.get("lb_site") is not None else ""),
                "type_site": (str(raw["type_site"]).strip() if raw.get("type_site") is not None else None),
                "co_roc": _normalize_co(raw.get("co_roc")),
                "est_actif": _normalize_est_actif(raw.get("est_actif")),
            }
            site = SiteCreate.model_validate(payload)
            valid.append(site)
        except ValidationError as e:
            errors.append(
                BulkUploadError(row=excel_row_num, error=str(e), raw=raw)
            )
        except (ValueError, TypeError) as e:
            errors.append(
                BulkUploadError(row=excel_row_num, error=str(e), raw=raw)
            )

    return valid, errors


def site_to_upsert_params(site: SiteCreate) -> tuple:
    """Convertit un SiteCreate en tuple de paramètres pour UPSERT_SQL."""
    return (
        site.co_regate,
        site.lb_regate,
        site.lb_site,
        site.type_site,
        site.co_roc,
        1 if site.est_actif else 0,
    )
