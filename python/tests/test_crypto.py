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
