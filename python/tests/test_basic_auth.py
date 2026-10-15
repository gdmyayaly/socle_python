"""Authentification HTTP Basic des routes métier (TRPPU_USER / TRPPU_SECRET)."""

import base64
import logging

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.security import basic_auth

client = TestClient(app)

# Route métier protégée, appelée sans ses query params obligatoires : une fois
# l'authentification passée elle répond 422, sans toucher ni base ni API externe.
URL_PROTEGEE = "/trppu-api/calcl_nbr_jours/get_nb_jours"
AUTH_OK = 422


def _basic(user: str, secret: str) -> dict[str, str]:
    jeton = base64.b64encode(f"{user}:{secret}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {jeton}"}


@pytest.fixture
def auth_active(monkeypatch):
    monkeypatch.setattr(basic_auth, "TRPPU_USER", "trppu")
    monkeypatch.setattr(basic_auth, "TRPPU_SECRET", "s3cr3t")


def test_auth_desactivee_si_identifiants_vides():
    assert basic_auth.auth_desactivee() is True
    assert client.get(URL_PROTEGEE).status_code == AUTH_OK


def test_identifiants_corrects_acceptes(auth_active):
    resp = client.get(URL_PROTEGEE, headers=_basic("trppu", "s3cr3t"))
    assert resp.status_code == AUTH_OK


def test_sans_identifiants_401(auth_active, caplog):
    with caplog.at_level(logging.WARNING, logger=basic_auth.__name__):
        resp = client.get(URL_PROTEGEE)
    assert resp.status_code == 401
    assert resp.headers["WWW-Authenticate"] == "Basic"
    assert resp.json() == {"detail": "Authentification requise"}
    (rejet,) = [r.getMessage() for r in caplog.records]
    assert rejet.startswith("Rejet authentification")
    assert "http=401" in rejet and "identifiants absents" in rejet


@pytest.mark.parametrize(
    "user, secret",
    [("trppu", "faux"), ("autre", "s3cr3t"), ("", "")],
)
def test_identifiants_faux_401(auth_active, caplog, user, secret):
    with caplog.at_level(logging.WARNING, logger=basic_auth.__name__):
        resp = client.get(URL_PROTEGEE, headers=_basic(user, secret))
    assert resp.status_code == 401
    (rejet,) = [r.getMessage() for r in caplog.records]
    assert "identifiants invalides" in rejet
    # Le secret soumis ne doit jamais apparaître dans le log.
    assert secret == "" or secret not in rejet


def test_identifiants_non_ascii_refuses_par_fastapi(auth_active):
    """`HTTPBasic` décode en ASCII et lève lui-même 401 : TRPPU_USER/SECRET doivent
    rester ASCII (documenté dans `basic_auth.py`)."""
    resp = client.get(URL_PROTEGEE, headers=_basic("trppué", "s3cr3t"))
    assert resp.status_code == 401


def test_schema_autre_que_basic_401(auth_active):
    resp = client.get(URL_PROTEGEE, headers={"Authorization": "Bearer abc"})
    assert resp.status_code == 401


@pytest.mark.parametrize("user, secret", [("trppu", ""), ("", "s3cr3t")])
def test_configuration_incomplete_refuse_tout(monkeypatch, user, secret):
    """Un seul identifiant configuré : on refuse plutôt qu'accepter un champ vide."""
    monkeypatch.setattr(basic_auth, "TRPPU_USER", user)
    monkeypatch.setattr(basic_auth, "TRPPU_SECRET", secret)
    assert basic_auth.auth_desactivee() is False
    resp = client.get(URL_PROTEGEE, headers=_basic(user, secret))
    assert resp.status_code == 401


@pytest.mark.parametrize("url", ["/", "/health", "/trppu-api/health", "/docs", "/openapi.json"])
def test_routes_ouvertes_sans_auth(auth_active, url):
    assert client.get(url).status_code == 200


def _protegee(route) -> bool:
    return any(d.call is basic_auth.verifier_basic_auth for d in route.dependant.dependencies)


def test_toutes_les_routes_metier_protegees():
    """Garde-fou : un router ajouté dans main.py sans `dependencies=_auth` est détecté."""
    ouvertes = {"/", "/health", "/health/resources", "/trppu-api/health", "/trppu-api/health/resources"}
    non_protegees = [
        route.path
        for route in app.routes
        if getattr(route, "dependant", None) is not None
        and route.include_in_schema
        and route.path not in ouvertes
        and not _protegee(route)
    ]
    assert non_protegees == []


def test_openapi_declare_le_schema_basic():
    schema = client.get("/openapi.json").json()
    assert schema["components"]["securitySchemes"]["HTTPBasic"] == {"type": "http", "scheme": "basic"}
