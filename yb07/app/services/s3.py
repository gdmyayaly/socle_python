"""Accès au stockage objet S3 : client boto3 et lecture d'un objet en streaming.

Le fichier (plusieurs Go) est lu ligne à ligne, à mémoire constante. boto3 étant
synchrone, l'appelant enveloppe les appels dans `asyncio.to_thread`.
"""

from __future__ import annotations

import gzip
import io
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, TextIO

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError, SSLError

from app.config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_BUCKET,
    S3_CA_BUNDLE,
    S3_ENDPOINT_URL,
    S3_PREFIXE,
    S3_TIMEOUT,
    S3_VERIFY_SSL,
)
from app.erreurs import TraitementImpossible
from app.log_utils import ctx

logger = logging.getLogger(__name__)

# Taille des morceaux lus sur le réseau (1 Mo) : empreinte mémoire plate.
TAILLE_BUFFER = 1024 * 1024


class _FluxBrut(io.RawIOBase):
    """Ajoute `readinto` au `StreamingBody` botocore pour `io.BufferedReader`."""

    def __init__(self, source) -> None:
        self._source = source

    def readable(self) -> bool:
        return True

    def readinto(self, tampon) -> int:
        donnees = self._source.read(len(tampon))
        if not donnees:
            return 0
        tampon[: len(donnees)] = donnees
        return len(donnees)


def chemin_objet(fichier: str) -> str:
    """Clé complète de l'objet : préfixe de configuration + nom du fichier."""
    prefixe = S3_PREFIXE.strip("/")
    return f"{prefixe}/{fichier}" if prefixe else fichier


def construire_client():
    """Client S3 boto3.

    Identifiants du `.env` s'ils sont renseignés, sinon chaîne de résolution boto3.
    Un endpoint explicite (S3 interne) force l'adressage *path-style*.
    """
    options = {
        "config": Config(
            signature_version="s3v4",
            s3={"addressing_style": "path" if S3_ENDPOINT_URL else "auto"},
            connect_timeout=S3_TIMEOUT,
            read_timeout=S3_TIMEOUT,
        ),
    }
    if S3_ENDPOINT_URL:
        options["endpoint_url"] = S3_ENDPOINT_URL
    verify = _verification_tls()
    if verify is not None:
        options["verify"] = verify
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        options["aws_access_key_id"] = AWS_ACCESS_KEY_ID
        options["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY

    logger.debug(
        "Client S3 construit %s",
        ctx(
            endpoint=S3_ENDPOINT_URL or "par défaut",
            bucket=S3_BUCKET,
            identifiants="explicites" if (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY) else "chaîne boto3",
        ),
    )
    return boto3.client("s3", **options)


def _verification_tls() -> bool | str | None:
    """Paramètre `verify` de boto3 (None = défaut) : désactivée > bundle CA > standard.

    Un bundle introuvable échoue tout de suite avec son chemin, que l'erreur SSL boto3 tait.
    """
    if not S3_VERIFY_SSL:
        return False
    if S3_CA_BUNDLE:
        if not Path(S3_CA_BUNDLE).is_file():
            raise TraitementImpossible(
                f"Bundle CA introuvable : '{S3_CA_BUNDLE}' (S3_CA_BUNDLE)."
            )
        return S3_CA_BUNDLE
    return None


def _libelle_tls() -> str:
    if not S3_VERIFY_SSL:
        return "DÉSACTIVÉE (S3_VERIFY_SSL=false)"
    if S3_CA_BUNDLE:
        return f"bundle {S3_CA_BUNDLE}"
    return "standard"


def _traduire(erreur: Exception, cle: str) -> TraitementImpossible:
    """Traduit une erreur boto3 en message actionnable pour l'exploitant."""
    if isinstance(erreur, ClientError):
        code = erreur.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound"):
            return TraitementImpossible(
                f"Fichier '{cle}' absent du bucket '{S3_BUCKET}'."
            )
        if code in ("NoSuchBucket",):
            return TraitementImpossible(f"Bucket '{S3_BUCKET}' introuvable.")
        if code in ("AccessDenied", "403", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
            return TraitementImpossible(
                f"Accès refusé à '{cle}' — vérifier AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY."
            )
        return TraitementImpossible(f"Erreur S3 sur '{cle}' : {code or erreur}.")
    if isinstance(erreur, SSLError):
        return TraitementImpossible(
            f"Certificat TLS du stockage S3 refusé ({S3_ENDPOINT_URL or 'endpoint par défaut'}) "
            "— renseigner S3_CA_BUNDLE avec le bundle CA de l'entreprise (ex. "
            f"certif/cacert.pem). Détail : {erreur}"
        )
    return TraitementImpossible(
        f"Stockage S3 injoignable ({S3_ENDPOINT_URL or 'endpoint par défaut'}) : {erreur}"
    )


def _masquer(valeur: str) -> str:
    """Rend une clé d'accès identifiable sans la divulguer."""
    if not valeur:
        return ""
    if len(valeur) <= 4:
        return "*" * len(valeur)
    return f"{valeur[:2]}{'*' * (len(valeur) - 4)}{valeur[-2:]}"


def decrire_configuration() -> dict:
    """Configuration S3 telle qu'elle sera utilisée. Le secret n'est jamais restitué."""
    explicites = bool(AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)
    return {
        "endpoint": S3_ENDPOINT_URL or "(par défaut AWS)",
        "bucket": S3_BUCKET or "(non renseigné)",
        "prefixe": S3_PREFIXE or "(racine)",
        "adressage": "path" if S3_ENDPOINT_URL else "auto",
        "timeout_s": S3_TIMEOUT,
        "verification_tls": _libelle_tls(),
        "identifiants": "explicites" if explicites else "chaîne boto3 par défaut",
        "access_key": _masquer(AWS_ACCESS_KEY_ID) if explicites else "",
        # Une clé sans secret est ignorée par `construire_client` : on le signale.
        "avertissement": (
            "AWS_ACCESS_KEY_ID renseignée sans AWS_SECRET_ACCESS_KEY (ou l'inverse) : les "
            "deux sont "
            "ignorées."
            if bool(AWS_ACCESS_KEY_ID) != bool(AWS_SECRET_ACCESS_KEY)
            else ""
        ),
    }


def verifier_acces() -> dict:
    """Teste l'accès au stockage et au bucket configuré ; ne lève jamais (diagnostic)."""
    resultat: dict = {"status": "ok", "bucket": S3_BUCKET, "buckets_visibles": [], "error": None}
    try:
        client = construire_client()
        # `list_buckets` peut échouer si le compte n'a de droits que sur son bucket :
        # non bloquant, c'est `head_bucket` qui fait foi.
        try:
            reponse = client.list_buckets()
            resultat["buckets_visibles"] = [
                seau["Name"] for seau in reponse.get("Buckets", [])
            ]
        except (ClientError, BotoCoreError):
            logger.debug("Listing des buckets indisponible %s", ctx(motif="droits restreints"))

        if S3_BUCKET:
            client.head_bucket(Bucket=S3_BUCKET)
        elif not resultat["buckets_visibles"]:
            resultat["status"] = "error"
            resultat["error"] = (
                "S3_BUCKET n'est pas renseigné et aucun bucket n'est visible."
            )
    except (ClientError, BotoCoreError, TraitementImpossible) as erreur:
        resultat["status"] = "error"
        resultat["error"] = (
            str(erreur)
            if isinstance(erreur, TraitementImpossible)
            else str(_traduire(erreur, S3_BUCKET or "(bucket)"))
        )
        logger.warning(
            "Rejet accès S3 %s",
            ctx(endpoint=S3_ENDPOINT_URL or "par défaut", bucket=S3_BUCKET, motif=resultat["error"]),
        )
    return resultat


def lister(prefixe: str = "", *, recursif: bool = False, limite: int = 200) -> dict:
    """Sous-dossiers et objets sous `prefixe` (niveau courant, ou arbre aplati si `recursif`).

    `limite` borne le nombre d'objets rendus.
    """
    if not S3_BUCKET:
        raise TraitementImpossible("S3_BUCKET n'est pas renseigné dans la configuration.")

    client = construire_client()
    parametres = {"Bucket": S3_BUCKET, "Prefix": prefixe}
    if not recursif:
        parametres["Delimiter"] = "/"

    dossiers: list[str] = []
    objets: list[dict] = []
    tronque = False
    try:
        for page in client.get_paginator("list_objects_v2").paginate(**parametres):
            dossiers += [p["Prefix"] for p in page.get("CommonPrefixes", [])]
            for objet in page.get("Contents", []):
                # Le préfixe lui-même (« dossier » explicite) remonte comme objet vide.
                if objet["Key"] == prefixe:
                    continue
                if len(objets) >= limite:
                    tronque = True
                    break
                objets.append(
                    {
                        "cle": objet["Key"],
                        "nom": objet["Key"][len(prefixe):] if prefixe else objet["Key"],
                        "taille": int(objet.get("Size", 0)),
                        "modifie_le": objet.get("LastModified"),
                    }
                )
            if tronque:
                break
    except (ClientError, BotoCoreError) as erreur:
        raise _traduire(erreur, prefixe or "(racine)") from erreur

    logger.info(
        "Listing S3 effectué %s",
        ctx(
            bucket=S3_BUCKET,
            prefixe=prefixe or "(racine)",
            dossiers=len(dossiers),
            objets=len(objets),
            tronque=tronque or None,
        ),
    )
    return {
        "bucket": S3_BUCKET,
        "prefixe": prefixe,
        "recursif": recursif,
        "dossiers": dossiers,
        "objets": objets,
        "tronque": tronque,
    }


def taille_lisible(octets: int) -> str:
    """Taille en unité lisible (o, Ko, Mo, Go, To)."""
    valeur = float(octets)
    for unite in ("o", "Ko", "Mo", "Go", "To"):
        if valeur < 1024 or unite == "To":
            return f"{valeur:.0f} {unite}" if unite == "o" else f"{valeur:.1f} {unite}"
        valeur /= 1024
    return f"{valeur:.1f} To"


def verifier_presence(cle: str) -> int:
    """Taille de l'objet en octets ; `TraitementImpossible` s'il est inaccessible.

    Appelée avant toute écriture en base : l'absence découverte après la purge coûterait
    un rechargement complet.
    """
    if not S3_BUCKET:
        raise TraitementImpossible("S3_BUCKET n'est pas renseigné dans la configuration.")
    client = construire_client()
    try:
        reponse = client.head_object(Bucket=S3_BUCKET, Key=cle)
    except (ClientError, BotoCoreError) as erreur:
        raise _traduire(erreur, cle) from erreur
    taille = int(reponse.get("ContentLength", 0))
    logger.info(
        "Fichier S3 localisé %s",
        ctx(bucket=S3_BUCKET, cle=cle, taille_octets=taille),
    )
    return taille


@contextmanager
def ouvrir_objet(cle: str, *, encodage: str) -> Iterator[TextIO]:
    """Ouvre un objet S3 en lecture texte, en streaming (`.gz` décompressé à la volée).

    Tout est refermé en sortie, même sur exception, sinon la connexion resterait ouverte.
    """
    if not S3_BUCKET:
        raise TraitementImpossible("S3_BUCKET n'est pas renseigné dans la configuration.")
    client = construire_client()
    try:
        reponse = client.get_object(Bucket=S3_BUCKET, Key=cle)
    except (ClientError, BotoCoreError) as erreur:
        raise _traduire(erreur, cle) from erreur

    corps = reponse["Body"]
    brut = io.BufferedReader(_FluxBrut(corps), buffer_size=TAILLE_BUFFER)
    binaire = gzip.GzipFile(fileobj=brut) if cle.endswith(".gz") else brut
    # newline="" : le module csv gère les fins de ligne (sauts de ligne entre guillemets,
    # \r des fichiers Windows).
    flux = io.TextIOWrapper(binaire, encoding=encodage, newline="")
    try:
        yield flux
    finally:
        try:
            flux.close()
        finally:
            corps.close()
