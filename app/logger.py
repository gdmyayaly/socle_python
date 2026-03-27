import logging
import os
from datetime import datetime

LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(level: int = logging.INFO) -> None:
    """Configure le logging de l'application.

    Les fichiers sont créés dans le dossier logs/ avec le format :
        logs/2026-03-27.log
        logs/2026-03-28.log
    """
    os.makedirs(LOGS_DIR, exist_ok=True)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    # Handler fichier nommé par la date du jour
    today = datetime.now().strftime("%Y-%m-%d")
    file_handler = logging.FileHandler(
        filename=os.path.join(LOGS_DIR, f"{today}.log"),
        encoding="utf-8",
    )
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
