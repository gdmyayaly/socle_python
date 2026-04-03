import logging
import os
from logging.handlers import TimedRotatingFileHandler

from pythonjsonlogger.json import JsonFormatter

LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(level: int = logging.INFO) -> None:
    """Configure le logging de l'application en format JSON.

    Les fichiers sont créés dans le dossier logs/ avec rotation quotidienne :
        logs/app.log          (jour courant)
        logs/app.log.2026-03-27  (jours précédents)
    """
    os.makedirs(LOGS_DIR, exist_ok=True)

    formatter = JsonFormatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT, json_ensure_ascii=False)

    # Handler fichier avec rotation quotidienne à minuit,  30 jours durée de concervation
    file_handler = TimedRotatingFileHandler(
        filename=os.path.join(LOGS_DIR, "app.log"),
        when="midnight",
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.suffix = "%Y-%m-%d"
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)

    # Handler console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)

    # Configuration du logger racine
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
