"""Tests de l'accès S3 (`app/services/s3.py`).

Aucun appel réseau : `boto3.client` est remplacé par une doublure qui enregistre les
options reçues et rend des corps de réponse en mémoire. Ce qui est vérifié, c'est la
**résolution de la configuration** — quels identifiants sont utilisés, quel adressage —
et la traduction des erreurs boto3 en messages actionnables.
"""

from __future__ import annotations

import gzip
import io

import pytest
from botocore.exceptions import ClientError

from app.services import s3
from app.erreurs import TraitementImpossible


class FauxCorps(io.BytesIO):
    """Substitut de `StreamingBody` : il n'expose que `read()` et `close()`."""

    def __init__(self, donnees: bytes) -> None:
        super().__init__(donnees)
        self.ferme = False

    def close(self) -> None:
        self.ferme = True
        super().close()


class FauxPaginateur:
    def __init__(self, pages: list[dict]) -> None:
        self._pages = pages
        self.parametres: dict = {}

    def paginate(self, **kwargs):
        self.parametres = kwargs
        return iter(self._pages)


class FauxClient:
    def __init__(
        self,
        corps: bytes = b"",
        erreur: Exception | None = None,
        *,
        pages: list[dict] | None = None,
        buckets: list[str] | None = None,
    ) -> None:
        self._corps = corps
        self._erreur = erreur
        self.paginateur = FauxPaginateur(pages or [])
        self._buckets = buckets if buckets is not None else ["trppu"]
        self.appels: list[tuple[str, dict]] = []

    def head_object(self, **kwargs):
        self.appels.append(("head_object", kwargs))
        if self._erreur:
            raise self._erreur
        return {"ContentLength": len(self._corps)}

    def get_object(self, **kwargs):
        self.appels.append(("get_object", kwargs))
        if self._erreur:
            raise self._erreur
        return {"Body": FauxCorps(self._corps)}

    def list_buckets(self, **kwargs):
        self.appels.append(("list_buckets", kwargs))
        if self._erreur:
            raise self._erreur
        return {"Buckets": [{"Name": nom} for nom in self._buckets]}

    def head_bucket(self, **kwargs):
        self.appels.append(("head_bucket", kwargs))
        if self._erreur:
            raise self._erreur
        return {}

    def get_paginator(self, operation):
        self.appels.append(("get_paginator", {"operation": operation}))
        return self.paginateur


@pytest.fixture
def bouchonner_boto3(monkeypatch):
    """Remplace `boto3.client` ; retourne la liste des options reçues."""
    options_recues: list[dict] = []

    def poser(client: FauxClient) -> list[dict]:
        def _client(service, **options):
            assert service == "s3"
            options_recues.append(options)
            return client

        monkeypatch.setattr(s3.boto3, "client", _client)
        return options_recues

    return poser


@pytest.fixture(autouse=True)
def _bucket_par_defaut(monkeypatch):
    monkeypatch.setattr(s3, "S3_BUCKET", "trppu")


def erreur_client(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "GetObject")


# --- Résolution de la configuration ----------------------------------------


def test_identifiants_du_env_utilises_quand_ils_sont_renseignes(
    monkeypatch, bouchonner_boto3
):
    monkeypatch.setattr(s3, "AWS_ACCESS_KEY_ID", "utilisateur")
    monkeypatch.setattr(s3, "AWS_SECRET_ACCESS_KEY", "motdepasse")
    options = bouchonner_boto3(FauxClient())

    s3.construire_client()

    assert options[0]["aws_access_key_id"] == "utilisateur"
    assert options[0]["aws_secret_access_key"] == "motdepasse"


def test_sans_identifiants_on_laisse_boto3_resoudre(monkeypatch, bouchonner_boto3):
    """Rôle IAM, profil ~/.aws : boto3 s'en charge seul."""
    monkeypatch.setattr(s3, "AWS_ACCESS_KEY_ID", "")
    monkeypatch.setattr(s3, "AWS_SECRET_ACCESS_KEY", "")
    options = bouchonner_boto3(FauxClient())

    s3.construire_client()

    assert "aws_access_key_id" not in options[0]
    assert "aws_secret_access_key" not in options[0]


def test_identifiants_incomplets_ignores(monkeypatch, bouchonner_boto3):
    """Une clé sans secret ne doit pas produire une configuration à moitié posée."""
    monkeypatch.setattr(s3, "AWS_ACCESS_KEY_ID", "utilisateur")
    monkeypatch.setattr(s3, "AWS_SECRET_ACCESS_KEY", "")
    options = bouchonner_boto3(FauxClient())

    s3.construire_client()

    assert "aws_access_key_id" not in options[0]


def test_aucune_region_imposee(bouchonner_boto3):
    """La région est laissée à boto3 : us-east-1 par défaut, accepté par les S3 internes."""
    options = bouchonner_boto3(FauxClient())

    s3.construire_client()

    assert "region_name" not in options[0]
    assert "region" not in s3.decrire_configuration()


def test_endpoint_force_l_adressage_path_style(monkeypatch, bouchonner_boto3):
    """Un S3 interne (MinIO, Ceph) ne sert pas le style « bucket dans le nom d'hôte »."""
    monkeypatch.setattr(s3, "S3_ENDPOINT_URL", "https://s3.interne.example")
    options = bouchonner_boto3(FauxClient())

    s3.construire_client()

    assert options[0]["endpoint_url"] == "https://s3.interne.example"
    assert options[0]["config"].s3["addressing_style"] == "path"


def test_sans_endpoint_adressage_automatique(monkeypatch, bouchonner_boto3):
    monkeypatch.setattr(s3, "S3_ENDPOINT_URL", "")
    options = bouchonner_boto3(FauxClient())

    s3.construire_client()

    assert "endpoint_url" not in options[0]
    assert options[0]["config"].s3["addressing_style"] == "auto"


def test_chemin_objet_applique_le_prefixe(monkeypatch):
    monkeypatch.setattr(s3, "S3_PREFIXE", "referentiels/")
    assert s3.chemin_objet("f.csv") == "referentiels/f.csv"

    monkeypatch.setattr(s3, "S3_PREFIXE", "")
    assert s3.chemin_objet("f.csv") == "f.csv"


# --- Lecture ----------------------------------------------------------------


def test_lecture_en_streaming(bouchonner_boto3):
    bouchonner_boto3(FauxClient(b"a;b\n1;2\n"))

    with s3.ouvrir_objet("f.csv", encodage="utf-8-sig") as flux:
        assert flux.read() == "a;b\n1;2\n"


def test_bom_absorbe(bouchonner_boto3):
    bouchonner_boto3(FauxClient("﻿id;pdi\n".encode("utf-8")))

    with s3.ouvrir_objet("f.csv", encodage="utf-8-sig") as flux:
        assert flux.read() == "id;pdi\n"


def test_gzip_decompresse_a_la_volee(bouchonner_boto3):
    bouchonner_boto3(FauxClient(gzip.compress(b"a;b\n1;2\n")))

    with s3.ouvrir_objet("f.csv.gz", encodage="utf-8-sig") as flux:
        assert flux.read() == "a;b\n1;2\n"


def test_corps_ferme_meme_sur_exception(bouchonner_boto3):
    """Un corps non fermé laisserait la connexion ouverte jusqu'au timeout."""
    client = FauxClient(b"a;b\n")
    bouchonner_boto3(client)

    corps: list[FauxCorps] = []
    original = client.get_object

    def espion(**kwargs):
        reponse = original(**kwargs)
        corps.append(reponse["Body"])
        return reponse

    client.get_object = espion

    with pytest.raises(RuntimeError):
        with s3.ouvrir_objet("f.csv", encodage="utf-8-sig"):
            raise RuntimeError("boom")

    assert corps[0].ferme


def test_taille_rendue_par_verifier_presence(bouchonner_boto3):
    bouchonner_boto3(FauxClient(b"12345"))
    assert s3.verifier_presence("f.csv") == 5


# --- Traduction des erreurs -------------------------------------------------


@pytest.mark.parametrize(
    ("code", "attendu"),
    [
        ("NoSuchKey", "absent du bucket"),
        ("NoSuchBucket", "introuvable"),
        ("AccessDenied", "Accès refusé"),
        ("SignatureDoesNotMatch", "Accès refusé"),
    ],
)
def test_erreurs_traduites_en_message_actionnable(bouchonner_boto3, code, attendu):
    bouchonner_boto3(FauxClient(erreur=erreur_client(code)))

    with pytest.raises(TraitementImpossible) as erreur:
        s3.verifier_presence("f.csv")

    assert attendu in str(erreur.value)


def test_bucket_non_configure_refuse_avant_tout_appel(monkeypatch):
    monkeypatch.setattr(s3, "S3_BUCKET", "")
    with pytest.raises(TraitementImpossible) as erreur:
        s3.verifier_presence("f.csv")
    assert "S3_BUCKET" in str(erreur.value)


# --- Diagnostic : configuration, accès, listing -----------------------------


def test_la_cle_est_masquee_et_le_secret_jamais_restitue(monkeypatch):
    monkeypatch.setattr(s3, "AWS_ACCESS_KEY_ID", "AKIA1234567890")
    monkeypatch.setattr(s3, "AWS_SECRET_ACCESS_KEY", "secret-tres-confidentiel")

    config = s3.decrire_configuration()

    assert config["access_key"] == "AK**********90"
    assert "secret-tres-confidentiel" not in str(config)


def test_identifiants_incomplets_signales(monkeypatch):
    """Sans cet avertissement, on cherche longtemps pourquoi la clé n'est pas utilisée."""
    monkeypatch.setattr(s3, "AWS_ACCESS_KEY_ID", "AKIA123")
    monkeypatch.setattr(s3, "AWS_SECRET_ACCESS_KEY", "")

    config = s3.decrire_configuration()

    assert "ignorées" in config["avertissement"]
    assert config["identifiants"] == "chaîne boto3 par défaut"


def test_acces_ok_liste_les_buckets_visibles(bouchonner_boto3):
    bouchonner_boto3(FauxClient(buckets=["trppu", "archives"]))

    acces = s3.verifier_acces()

    assert acces["status"] == "ok"
    assert acces["buckets_visibles"] == ["trppu", "archives"]


def test_acces_en_erreur_ne_leve_pas(bouchonner_boto3):
    """Le diagnostic doit rendre un état : c'est quand l'accès échoue qu'on le lance."""
    bouchonner_boto3(FauxClient(erreur=erreur_client("AccessDenied")))

    acces = s3.verifier_acces()

    assert acces["status"] == "error"
    assert "Accès refusé" in acces["error"]


def test_listing_separe_dossiers_et_objets(bouchonner_boto3):
    client = FauxClient(
        pages=[
            {
                "CommonPrefixes": [{"Prefix": "referentiels/2026/"}],
                "Contents": [
                    {"Key": "referentiels/cles.csv", "Size": 2048, "LastModified": None}
                ],
            }
        ]
    )
    bouchonner_boto3(client)

    contenu = s3.lister("referentiels/")

    assert contenu["dossiers"] == ["referentiels/2026/"]
    assert contenu["objets"][0]["nom"] == "cles.csv"
    assert contenu["objets"][0]["taille"] == 2048
    assert contenu["tronque"] is False
    # Delimiter posé : on reste au niveau courant, comme un `ls`.
    assert client.paginateur.parametres["Delimiter"] == "/"


def test_listing_recursif_sans_delimiteur(bouchonner_boto3):
    client = FauxClient(pages=[{"Contents": []}])
    bouchonner_boto3(client)

    s3.lister("referentiels/", recursif=True)

    assert "Delimiter" not in client.paginateur.parametres


def test_le_prefixe_lui_meme_n_est_pas_liste_comme_un_objet(bouchonner_boto3):
    """Un « dossier » créé explicitement remonte comme un objet de taille nulle."""
    bouchonner_boto3(
        FauxClient(
            pages=[
                {
                    "Contents": [
                        {"Key": "referentiels/", "Size": 0, "LastModified": None},
                        {"Key": "referentiels/cles.csv", "Size": 10, "LastModified": None},
                    ]
                }
            ]
        )
    )

    contenu = s3.lister("referentiels/")

    assert [objet["nom"] for objet in contenu["objets"]] == ["cles.csv"]


def test_listing_tronque_a_la_limite(bouchonner_boto3):
    bouchonner_boto3(
        FauxClient(
            pages=[
                {
                    "Contents": [
                        {"Key": f"f{i}.csv", "Size": 1, "LastModified": None}
                        for i in range(10)
                    ]
                }
            ]
        )
    )

    contenu = s3.lister("", limite=3)

    assert len(contenu["objets"]) == 3
    assert contenu["tronque"] is True


@pytest.mark.parametrize(
    ("octets", "attendu"),
    [(0, "0 o"), (512, "512 o"), (2048, "2.0 Ko"), (5 * 1024**3, "5.0 Go")],
)
def test_taille_lisible(octets, attendu):
    assert s3.taille_lisible(octets) == attendu
