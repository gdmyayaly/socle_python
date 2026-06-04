"""Tests du client API jours fermés (parsing, filtrage, cache, erreurs).

Les appels HTTP sont stubés (pas de réseau). On exécute le code async via
``asyncio.run`` pour ne pas dépendre de pytest-asyncio.
"""

import asyncio

import httpx
import pytest

from app.services import jours_fermes_client as jfc
from app.services.jours_fermes_client import (
    JoursFermesAPIError,
    fetch_feries_annee,
    fetch_feries_between,
)
from datetime import date

# Réponse type de l'API (le champ `action` et les autres sont ignorés).
PAYLOAD_2025 = [
    {
        "date": "01-01-2025",
        "ymd_date": "2025-01-01",
        "jour": "Mercredi",
        "libelle": "1er janvier",
        "timestamp": 1735689600,
        "action": "delete",
    },
    {
        "date": "21-04-2025",
        "ymd_date": "2025-04-21",
        "jour": "Lundi",
        "libelle": "Lundi de Pâques",
        "timestamp": 1745193600,
        "action": "delete",
    },
]


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                "error", request=None, response=None
            )

    def json(self):
        return self._payload


class _FakeAsyncClient:
    """Stub de httpx.AsyncClient : compte les requêtes et renvoie un payload fixe."""

    calls = 0
    payload = PAYLOAD_2025
    status_code = 200

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, params=None):
        type(self).calls += 1
        return _FakeResponse(self.payload, self.status_code)


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    """Configure une base URL, stub httpx et vide le cache avant chaque test."""
    monkeypatch.setattr(jfc, "JOURS_FERMES_API_BASE_URL", "http://host/tournees/jours-fermes/v1")
    monkeypatch.setattr(jfc, "JOURS_FERMES_CACHE_TTL", 86400)
    monkeypatch.setattr(jfc.httpx, "AsyncClient", _FakeAsyncClient)
    jfc._cache.clear()
    _FakeAsyncClient.calls = 0
    _FakeAsyncClient.payload = PAYLOAD_2025
    _FakeAsyncClient.status_code = 200
    yield
    jfc._cache.clear()


def test_parse_ymd_date_et_ignore_action():
    feries = asyncio.run(fetch_feries_annee(2025))
    assert feries == {date(2025, 1, 1), date(2025, 4, 21)}


def test_cache_un_seul_appel_http_par_annee():
    asyncio.run(fetch_feries_annee(2025))
    asyncio.run(fetch_feries_annee(2025))
    assert _FakeAsyncClient.calls == 1  # second appel servi par le cache


def test_fetch_between_filtre_la_plage():
    feries = asyncio.run(fetch_feries_between(date(2025, 2, 1), date(2025, 3, 31)))
    # 01/01 hors plage, 21/04 hors plage -> aucun férié dans [01/02, 31/03]
    assert feries == set()
    feries = asyncio.run(fetch_feries_between(date(2025, 1, 1), date(2025, 12, 31)))
    assert feries == {date(2025, 1, 1), date(2025, 4, 21)}


def test_erreur_http_leve_jours_fermes_api_error():
    _FakeAsyncClient.status_code = 503
    with pytest.raises(JoursFermesAPIError):
        asyncio.run(fetch_feries_annee(2025))


def test_payload_invalide_leve_jours_fermes_api_error():
    _FakeAsyncClient.payload = [{"libelle": "sans ymd_date"}]
    with pytest.raises(JoursFermesAPIError):
        asyncio.run(fetch_feries_annee(2025))


def test_base_url_absente_leve_jours_fermes_api_error(monkeypatch):
    monkeypatch.setattr(jfc, "JOURS_FERMES_API_BASE_URL", "")
    with pytest.raises(JoursFermesAPIError):
        asyncio.run(fetch_feries_annee(2025))
