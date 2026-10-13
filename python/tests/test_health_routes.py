"""Tests des routes de santé : accessibles à la racine et sous le préfixe `/trppu-api`."""

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routes import health

client = TestClient(app)


class FakeDb:
    def __init__(self, result=None, erreur=None):
        self.result = result
        self.erreur = erreur

    async def fetch_one(self, sql, params=None):
        if self.erreur:
            raise self.erreur
        return self.result


class FakeDatabricks:
    def __init__(self, result=None, erreur=None):
        self.result = result
        self.erreur = erreur

    def fetch_one(self, sql, params=None):
        if self.erreur:
            raise self.erreur
        return self.result


@pytest.mark.parametrize("prefixe", ["", "/trppu-api"])
def test_health_config_accessible_avec_et_sans_prefixe(prefixe):
    resp = client.get(f"{prefixe}/health")
    assert resp.status_code == 200
    assert set(resp.json()) == {"status", "mysql_config", "databricks_config"}


@pytest.mark.parametrize("prefixe", ["", "/trppu-api"])
def test_health_resources_toutes_connectees(monkeypatch, prefixe):
    monkeypatch.setattr(health, "db_read", FakeDb({"ok": 1}))
    monkeypatch.setattr(health, "db_write", FakeDb({"ok": 1}))
    monkeypatch.setattr(health, "databricks", FakeDatabricks({"ok": 1}))
    resp = client.get(f"{prefixe}/health/resources")
    assert resp.status_code == 200
    assert resp.json() == {
        "status": "ok",
        "mysql_read": "connected",
        "mysql_write": "connected",
        "databricks": "connected",
    }


def test_health_resources_signale_les_ressources_en_echec(monkeypatch):
    monkeypatch.setattr(health, "db_read", FakeDb(erreur=RuntimeError("down")))
    monkeypatch.setattr(health, "db_write", FakeDb(None))
    monkeypatch.setattr(health, "databricks", FakeDatabricks(erreur=RuntimeError("down")))
    resp = client.get("/trppu-api/health/resources")
    assert resp.status_code == 200
    body = resp.json()
    assert body["mysql_read"] == "disconnected"
    assert body["mysql_write"] == "error"
    assert body["databricks"] == "disconnected"


def test_racine_reste_hors_prefixe():
    assert client.get("/").status_code == 200
    assert client.get("/trppu-api/").status_code == 404
