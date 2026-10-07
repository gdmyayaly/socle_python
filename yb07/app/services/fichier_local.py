"""Accès à un fichier du disque local, symétrique de `app/services/s3.py`.

Mêmes garanties : localisé avant toute écriture en base, lu en streaming (`.gz` à la volée).
"""

from __future__ import annotations

import gzip
import io
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, TextIO

from app.erreurs import TraitementImpossible
from app.log_utils import ctx

logger = logging.getLogger(__name__)

# Même taille de tampon que pour S3 : 1 Mo.
TAILLE_BUFFER = 1024 * 1024


def verifier_presence(chemin: str) -> int:
    """Taille du fichier en octets ; `TraitementImpossible` s'il est inaccessible.

    Appelée avant toute écriture en base : l'absence découverte après la purge coûterait
    un rechargement complet.
    """
    fichier = Path(chemin)
    if not fichier.exists():
        raise TraitementImpossible(f"Fichier local '{fichier.resolve()}' introuvable.")
    if not fichier.is_file():
        raise TraitementImpossible(f"'{fichier.resolve()}' n'est pas un fichier.")
    try:
        taille = fichier.stat().st_size
        # Ouvrir sans lire : un fichier illisible (droits) doit échouer avant la purge.
        with fichier.open("rb"):
            pass
    except OSError as erreur:
        raise TraitementImpossible(
            f"Fichier local '{fichier.resolve()}' illisible : {erreur}"
        ) from erreur

    logger.info(
        "Fichier local localisé %s",
        ctx(chemin=str(fichier.resolve()), taille_octets=taille),
    )
    return taille


@contextmanager
def ouvrir(chemin: str, *, encodage: str) -> Iterator[TextIO]:
    """Ouvre un fichier local en lecture texte, décompressé à la volée si `.gz`."""
    try:
        brut = open(chemin, "rb", buffering=TAILLE_BUFFER)  # noqa: SIM115 - fermé ci-dessous
    except OSError as erreur:
        raise TraitementImpossible(f"Fichier local '{chemin}' illisible : {erreur}") from erreur

    binaire = gzip.GzipFile(fileobj=brut) if chemin.endswith(".gz") else brut
    # newline="" : le module csv gère les fins de ligne (cf. `s3.ouvrir_objet`).
    flux = io.TextIOWrapper(binaire, encoding=encodage, newline="")
    try:
        yield flux
    finally:
        try:
            flux.close()
        finally:
            brut.close()
