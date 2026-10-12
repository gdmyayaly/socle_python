"""Tests du cryptage réversible de l'id_rh (Fernet)."""

import importlib

import pytest


@pytest.fixture()
def crypto(monkeypatch):
    """Recharge le module crypto avec une clé de test (cache lru_cache réinitialisé)."""
    import app.config as config

    monkeypatch.setattr(config, "ID_RH_CRYPTO_KEY", "cle-de-test-trppu", raising=False)
    import app.security.crypto as crypto_mod

    importlib.reload(crypto_mod)
    crypto_mod._get_fernet.cache_clear()
    return crypto_mod


def test_round_trip(crypto):
    token = crypto.encrypt_id_rh("A123456")
    assert token != "A123456"
    assert crypto.decrypt_id_rh(token) == "A123456"


def test_token_non_deterministe_mais_dechiffrable(crypto):
    t1 = crypto.encrypt_id_rh("A123456")
    t2 = crypto.encrypt_id_rh("A123456")
    # Fernet inclut un timestamp/IV : deux tokens diffèrent mais se déchiffrent pareil.
    assert crypto.decrypt_id_rh(t1) == crypto.decrypt_id_rh(t2) == "A123456"


def test_entree_vide_rejetee(crypto):
    with pytest.raises(ValueError):
        crypto.encrypt_id_rh("")


def test_token_invalide_rejete(crypto):
    with pytest.raises(ValueError):
        crypto.decrypt_id_rh("pas-un-token-fernet")


def test_cle_vide_id_rh_non_enregistre(monkeypatch):
    """Clé vide => l'id_rh n'est pas enregistré : encrypt renvoie None (jamais le clair).

    decrypt reste un pass-through pour relire d'éventuelles valeurs historiques en clair.
    """
    import app.config as config

    monkeypatch.setattr(config, "ID_RH_CRYPTO_KEY", "", raising=False)
    import app.security.crypto as crypto_mod

    importlib.reload(crypto_mod)
    crypto_mod._get_fernet.cache_clear()

    assert crypto_mod.encrypt_id_rh("A123456") is None
    assert crypto_mod.decrypt_id_rh("A123456") == "A123456"
    # Entrée vide toujours rejetée même cryptage désactivé.
    with pytest.raises(ValueError):
        crypto_mod.encrypt_id_rh("")


def _upsert_variation_params(monkeypatch, cle: str) -> tuple:
    """Joue PUT variation (création) avec la clé donnée et renvoie les params de l'INSERT."""
    import asyncio
    from contextlib import asynccontextmanager
    from decimal import Decimal

    import app.security.crypto as crypto_mod
    from app.routes.trppu_variations import routes
    from app.routes.trppu_variations.schemas import VariationUpsert

    monkeypatch.setattr(crypto_mod, "ID_RH_CRYPTO_KEY", cle)
    crypto_mod._get_fernet.cache_clear()

    calls: list = []

    class FakeTx:
        async def fetch_one(self, sql, params=None):
            return None  # aucune variation existante => INSERT

        async def execute(self, sql, params=None):
            calls.append((sql, params))
            return 1

    @asynccontextmanager
    async def fake_transaction():
        yield FakeTx()

    async def fake_fetch_scenario(id_scenario):
        return {"id_scenario": id_scenario, "co_regate": "R1"}

    async def fake_enregistrer_appel(**kwargs):
        return None

    monkeypatch.setattr(routes, "fetch_scenario_or_404", fake_fetch_scenario)
    monkeypatch.setattr(routes, "assert_editable", lambda scenario: None)
    monkeypatch.setattr(routes, "enregistrer_appel", fake_enregistrer_appel)
    monkeypatch.setattr(routes.db_write, "transaction", fake_transaction)

    payload = VariationUpsert(variation_pct=Decimal("5"), id_rh="A123456")
    try:
        result = asyncio.run(routes.upsert_variation(1, "P1", payload))
    finally:
        crypto_mod._get_fernet.cache_clear()
    assert result.action == "created"
    (sql, params), = calls
    assert sql.startswith("INSERT INTO trppu_scenario_variations_prev")
    return params


def test_route_sans_cle_enregistre_id_rh_null(monkeypatch):
    """Sans ID_RH_CRYPTO_KEY, la route écrit NULL dans id_rh (pas l'id_rh en clair)."""
    params = _upsert_variation_params(monkeypatch, "")
    assert params[-1] is None
    assert "A123456" not in params


def test_route_avec_cle_enregistre_id_rh_chiffre(monkeypatch):
    """Avec ID_RH_CRYPTO_KEY, la route écrit le token chiffré (déchiffrable) dans id_rh."""
    import app.security.crypto as crypto_mod

    params = _upsert_variation_params(monkeypatch, "cle-de-test-trppu")
    token = params[-1]
    assert token and token != "A123456"
    monkeypatch.setattr(crypto_mod, "ID_RH_CRYPTO_KEY", "cle-de-test-trppu")
    crypto_mod._get_fernet.cache_clear()
    assert crypto_mod.decrypt_id_rh(token) == "A123456"
    crypto_mod._get_fernet.cache_clear()
