"""Routes pour télécharger et nettoyer les fichiers de log applicatifs."""

import logging
import time
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

from app.json_formatter import _get_logs_dir

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/logs", tags=["Logs"])


def _list_log_files(logs_dir: Path) -> list[Path]:
    """Retourne la liste des fichiers .log dans logs_dir, triés du plus récent au plus ancien."""
    return sorted(
        logs_dir.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True
    )


def _today_log_filename() -> str:
    return f"{datetime.now().strftime('%Y-%m-%d')}.log"


@router.get("/latest")
async def download_latest_log():
    """Télécharge le fichier .log le plus récent du dossier de logs.

    Le « plus récent » est déterminé par le `mtime` (date de modification) — donc
    le fichier en cours d'écriture si l'application logge dans un fichier.
    """
    start = time.perf_counter()
    logger.info("→ download_latest_log")

    logs_dir = _get_logs_dir()
    if not logs_dir.exists():
        logger.info("← download_latest_log 404 (logs_dir absent: %s)", logs_dir)
        raise HTTPException(
            status_code=404,
            detail=f"Dossier de logs introuvable : {logs_dir}",
        )

    log_files = _list_log_files(logs_dir)
    if not log_files:
        logger.info("← download_latest_log 404 (aucun .log dans %s)", logs_dir)
        raise HTTPException(
            status_code=404,
            detail=f"Aucun fichier .log dans {logs_dir}",
        )

    latest = log_files[0]
    size = latest.stat().st_size
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "← download_latest_log OK (file=%s, size=%d bytes, duration_ms=%.1f)",
        latest.name,
        size,
        duration_ms,
    )
    return FileResponse(
        path=latest,
        filename=latest.name,
        media_type="text/plain; charset=utf-8",
    )


@router.delete("")
async def clean_logs(
    keep_today: bool = Query(
        False,
        description="Si true, conserve le fichier du jour intact (utile sous Windows si "
        "le handler tient le fichier ouvert).",
    ),
):
    """Nettoie tous les fichiers .log du dossier de logs.

    - Fichier du jour : tronqué à 0 octet (les futurs logs continueront d'y être
      écrits via le handler existant).
    - Autres fichiers : supprimés.

    Avec `keep_today=true`, le fichier du jour est ignoré (ni tronqué ni supprimé).
    Toute erreur fichier-par-fichier est listée dans la réponse sans bloquer le
    reste — utile sous Windows où un handler ouvert peut empêcher la suppression.
    """
    start = time.perf_counter()
    logger.info("→ clean_logs (keep_today=%s)", keep_today)

    logs_dir = _get_logs_dir()
    if not logs_dir.exists():
        logger.info("← clean_logs 404 (logs_dir absent: %s)", logs_dir)
        raise HTTPException(
            status_code=404,
            detail=f"Dossier de logs introuvable : {logs_dir}",
        )

    today_filename = _today_log_filename()
    deleted: list[str] = []
    truncated: list[str] = []
    errors: list[dict] = []

    for log_file in logs_dir.glob("*.log"):
        if log_file.name == today_filename:
            if keep_today:
                continue
            try:
                with log_file.open("w", encoding="utf-8") as f:
                    f.truncate(0)
                truncated.append(log_file.name)
            except OSError as e:
                logger.exception(
                    "Échec truncate du log courant (file=%s)", log_file.name
                )
                errors.append({"file": log_file.name, "error": str(e)})
        else:
            try:
                log_file.unlink()
                deleted.append(log_file.name)
            except OSError as e:
                logger.exception(
                    "Échec suppression du log (file=%s)", log_file.name
                )
                errors.append({"file": log_file.name, "error": str(e)})

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "← clean_logs OK (deleted=%d, truncated=%d, errors=%d, duration_ms=%.1f)",
        len(deleted),
        len(truncated),
        len(errors),
        duration_ms,
    )
    return {
        "logs_dir": str(logs_dir),
        "deleted": deleted,
        "truncated": truncated,
        "errors": errors,
    }
