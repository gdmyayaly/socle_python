"""Fixtures communes à la suite de tests."""

import pytest

from app.security import basic_auth


@pytest.fixture(autouse=True)
def _basic_auth_desactivee(monkeypatch):
    """Les tests passent sans identifiants, quel que soit le `.env` local.

    `tests/test_basic_auth.py` réactive l'authentification test par test.
    """
    monkeypatch.setattr(basic_auth, "TRPPU_USER", "")
    monkeypatch.setattr(basic_auth, "TRPPU_SECRET", "")
