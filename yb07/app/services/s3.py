"""Accès au stockage objet S3 : client boto3 et lecture d'un objet en streaming.

Un fichier métier pèse plusieurs gigaoctets — 22 M de lignes pour les clés de répartition.
Il n'est ni chargé en mémoire ni déposé sur disque : `ouvrir_objet` rend un flux texte que
l'appelant consomme ligne à ligne, à mémoire constante.

boto3 est **synchrone**. Les appels réseau ne doivent donc jamais être faits directement
depuis la boucle asyncio : l'appelant les enveloppe dans `asyncio.to_thread` (cf.
`app/traitements/cles_repartition.py`).
"""

from __future__ import annotations

import gzip
import io
import logging
from contextlib import contextmanager
from typing import Iterator, TextIO

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

from app.config import (
    S3_ACCESS_KEY,
    S3_BUCKET,
    S3_ENDPOINT_URL,
    S3_PREFIXE,
    S3_REGION,
    S3_SECRET_KEY,
    S3_TIMEOUT,
)
from app.erreurs import TraitementImpossible
from app.log_utils import ctx

logger = logging.getLogger(__name__)

# Taille des morceaux lus sur le réseau. 1 Mo : assez grand pour que le coût par appel soit
# négligeable, assez petit pour que l'empreinte mémoire reste plate quel que soit le fichier.
TAILLE_BUFFER = 1024 * 1024


class _FluxBrut(io.RawIOBase):
    """Adapte un corps de réponse S3 à l'interface attendue par `io.BufferedReader`.

    Le `StreamingBody` de botocore n'expose que `read(n)` ; il lui manque `readinto`, que
    la pile `BufferedReader` / `TextIOWrapper` exige. Cet adaptateur ne fait que combler
    ce manque — il ne met rien en tampon lui-même.
    """

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

    Les identifiants du `.env` priment s'ils sont renseignés ; sinon ils sont omis et boto3
    applique sa chaîne de résolution habituelle (rôle de la machine, profil `~/.aws`,
    variables `AWS_*`). Les deux configurations sont donc valides, et c'est le `.env` qui
    tranche.

    Un endpoint explicite désigne un S3 interne (MinIO, Ceph…) : l'adressage est forcé en
    *path-style*, ces serveurs ne servant pas le style « bucket dans le nom d'hôte ».
    """
    options = {
        "region_name": S3_REGION,
        "config": Config(
            signature_version="s3v4",
            s3={"addressing_style": "path" if S3_ENDPOINT_URL else "auto"},
            connect_timeout=S3_TIMEOUT,
            read_timeout=S3_TIMEOUT,
        ),
    }
    if S3_ENDPOINT_URL:
        options["endpoint_url"] = S3_ENDPOINT_URL
    if S3_ACCESS_KEY and S3_SECRET_KEY:
        options["aws_access_key_id"] = S3_ACCESS_KEY
        options["aws_secret_access_key"] = S3_SECRET_KEY

    logger.debug(
        "Client S3 construit %s",
        ctx(
            endpoint=S3_ENDPOINT_URL or "par défaut",
            region=S3_REGION,
            bucket=S3_BUCKET,
            identifiants="explicites" if (S3_ACCESS_KEY and S3_SECRET_KEY) else "chaîne boto3",
        ),
    )
    return boto3.client("s3", **options)


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
                f"Accès refusé à '{cle}' — vérifier S3_ACCESS_KEY / S3_SECRET_KEY."
            )
        return TraitementImpossible(f"Erreur S3 sur '{cle}' : {code or erreur}.")
    return TraitementImpossible(
        f"Stockage S3 injoignable ({S3_ENDPOINT_URL or 'endpoint par défaut'}) : {erreur}"
    )


def _masquer(valeur: str) -> str:
    """Rend une clé d'accès identifiable sans la divulguer.

    Un diagnostic doit permettre de dire « ce n'est pas la bonne clé » sans imprimer un
    secret dans une console ou un fichier de log.
    """
    if not valeur:
        return ""
    if len(valeur) <= 4:
        return "*" * len(valeur)
    return f"{valeur[:2]}{'*' * (len(valeur) - 4)}{valeur[-2:]}"


def decrire_configuration() -> dict:
    """Configuration S3 telle qu'elle sera utilisée. Le secret n'est jamais restitué."""
    explicites = bool(S3_ACCESS_KEY and S3_SECRET_KEY)
    return {
        "endpoint": S3_ENDPOINT_URL or "(par défaut AWS)",
        "region": S3_REGION,
        "bucket": S3_BUCKET or "(non renseigné)",
        "prefixe": S3_PREFIXE or "(racine)",
        "adressage": "path" if S3_ENDPOINT_URL else "auto",
        "timeout_s": S3_TIMEOUT,
        "identifiants": "explicites" if explicites else "chaîne boto3 par défaut",
        "access_key": _masquer(S3_ACCESS_KEY) if explicites else "",
        # Une clé sans secret est ignorée par `construire_client` : le signaler évite de
        # chercher longtemps pourquoi ce sont les identifiants de la machine qui servent.
        "avertissement": (
            "S3_ACCESS_KEY renseignée sans S3_SECRET_KEY (ou l'inverse) : les deux sont "
            "ignorées."
            if bool(S3_ACCESS_KEY) != bool(S3_SECRET_KEY)
            else ""
        ),
    }


def verifier_acces() -> dict:
    """Teste l'accès au stockage, et au bucket configuré s'il y en a un.

    Ne lève jamais : le diagnostic doit rendre un état, pas s'interrompre — c'est
    précisément quand l'accès échoue qu'on l'exécute.
    """
    resultat: dict = {"status": "ok", "bucket": S3_BUCKET, "buckets_visibles": [], "error": None}
    try:
        client = construire_client()
        # `list_buckets` sert de test de connexion et d'authentification. Il échoue sur
        # certains stockages où le compte n'a de droits que sur son bucket : ce n'est pas
        # bloquant, d'où le `head_bucket` qui suit et qui, lui, fait foi.
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
    except (ClientError, BotoCoreError) as erreur:
        resultat["status"] = "error"
        resultat["error"] = str(_traduire(erreur, S3_BUCKET or "(bucket)"))
        logger.warning(
            "Rejet accès S3 %s",
            ctx(endpoint=S3_ENDPOINT_URL or "par défaut", bucket=S3_BUCKET, motif=resultat["error"]),
        )
    return resultat


def lister(prefixe: str = "", *, recursif: bool = False, limite: int = 200) -> dict:
    """Contenu du bucket sous `prefixe` : sous-dossiers et objets.

    Sans `recursif`, le listing s'arrête au niveau courant — `Delimiter="/"` fait remonter
    les sous-dossiers sous forme de préfixes communs, et on navigue de niveau en niveau
    comme avec `ls`. Avec, tout l'arbre est aplati.

    `limite` borne le nombre d'objets rendus : un bucket de référentiels peut en contenir
    des milliers, et un diagnostic n'a pas à les dérouler tous.
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
                # Le préfixe lui-même remonte comme un objet de taille nulle quand il a été
                # créé explicitement comme « dossier » : il n'apporte rien au listing.
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
    """Taille en unité lisible — un fichier de 1,2 Go ne se lit pas en octets."""
    valeur = float(octets)
    for unite in ("o", "Ko", "Mo", "Go", "To"):
        if valeur < 1024 or unite == "To":
            return f"{valeur:.0f} {unite}" if unite == "o" else f"{valeur:.1f} {unite}"
        valeur /= 1024
    return f"{valeur:.1f} To"


def verifier_presence(cle: str) -> int:
    """Taille de l'objet en octets. Lève `TraitementImpossible` s'il est inaccessible.

    Appelée **avant** toute écriture en base : découvrir que le fichier n'existe pas après
    avoir purgé un référentiel de 22 M de lignes coûterait un rechargement complet.
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
    """Ouvre un objet S3 en lecture texte, sans le télécharger.

    Le corps de la réponse est un flux réseau : il est enveloppé dans un `TextIOWrapper`
    (et dans un `GzipFile` si la clé se termine par `.gz`, la décompression se faisant
    alors à la volée). Tout est refermé à la sortie du contexte, y compris en cas
    d'exception — sans quoi la connexion resterait ouverte jusqu'au timeout.
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
    # newline="" : c'est le module csv qui doit gérer les fins de ligne, sinon un champ
    # contenant un saut de ligne entre guillemets serait coupé en deux. Cela laisse aussi
    # passer les fichiers en fins de ligne Windows sans que le \r ne colle au dernier champ.
    flux = io.TextIOWrapper(binaire, encoding=encodage, newline="")
    try:
        yield flux
    finally:
        try:
            flux.close()
        finally:
            corps.close()
