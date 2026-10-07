"""Logs au format JSON DSR et ``setup_logging`` (console, fichier en local)."""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from app.config import (
    APP,
    APP_ENV,
    APP_VERSION,
    LOGS_DIR as CONFIG_LOGS_DIR,
    MODULE,
)
from app.log_utils import get_id_scenario, get_site

DEFAULT_LOGS_DIR = os.path.join(os.getcwd(), "logs")


class JsonFormatter(logging.Formatter):
    """Formateur JSON avec métadonnées applicatives et contexte scénario/site."""

    def format(self, record):
        """Une ligne JSON ; id_scenario, co_regate, co_roc suivent un scénario dans Kibana."""
        app_run_mode = 'run' if APP_ENV == 'prod' else 'build'
        # Clés toujours posées (null hors traitement) : mapping Kibana stable ; ne jamais lever.
        try:
            id_scenario = get_id_scenario()
        except Exception:
            id_scenario = None
        try:
            co_regate, co_roc = get_site()
        except Exception:
            co_regate, co_roc = None, None
        log_record = {
            'app_datetime': datetime.fromtimestamp(record.created, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            'app_ccx': APP,
            'app_env': APP_ENV,
            'app_ptf': app_run_mode,
            'app_tm': MODULE,
            'app_version': APP_VERSION,
            'severity_label': record.levelname,
            'app_message': record.getMessage(),
            'id_scenario': id_scenario,
            'co_regate': co_regate,
            'co_roc': co_roc,
            'name': record.name,
            'filename': record.filename,
            'lineno': record.lineno
        }
        if record.exc_info:
            log_record['exc_info'] = self.formatException(record.exc_info)
        if record.stack_info:
            log_record['stack_info'] = self.formatStack(record.stack_info)
        return json.dumps(log_record, ensure_ascii=False)


def _get_logs_dir(logs_dir=None):
    if logs_dir is not None:
        return Path(logs_dir)
    if CONFIG_LOGS_DIR:
        return Path(CONFIG_LOGS_DIR)
    return Path(DEFAULT_LOGS_DIR)


def _should_enable_file_logging(logs_dir=None):
    if logs_dir is not None:
        return True

    return APP_ENV == 'local'


def _build_handler(formatter, level, logs_dir):
    today = datetime.now().strftime("%Y-%m-%d")
    file_handler = logging.FileHandler(
        filename=logs_dir / f"{today}.log",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)
    file_handler._json_formatter_managed = True
    return file_handler


def _build_console_handler(formatter, level):
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)
    console_handler._json_formatter_managed = True
    return console_handler


def setup_logging(level=logging.INFO, logs_dir=None):
    """Logger racine en JSON : console toujours, fichier en local ou si `logs_dir` est donné."""
    formatter = JsonFormatter()

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    for handler in tuple(root_logger.handlers):
        if getattr(handler, '_json_formatter_managed', False):
            root_logger.removeHandler(handler)
            handler.close()

    if _should_enable_file_logging(logs_dir):
        target_logs_dir = _get_logs_dir(logs_dir)
        target_logs_dir.mkdir(parents=True, exist_ok=True)
        root_logger.addHandler(_build_handler(formatter, level, target_logs_dir))

    root_logger.addHandler(_build_console_handler(formatter, level))


__all__ = [
    'JsonFormatter',
    'DEFAULT_LOGS_DIR',
    'setup_logging',
]
