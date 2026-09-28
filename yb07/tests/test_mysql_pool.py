"""Réglages du pool MySQL (`app/db/mysql.py`)."""

import asyncio

import aiomysql

from app.config import MYSQL_POOL_RECYCLE
from app.db.mysql import Database


def test_le_pool_renouvelle_les_connexions_inactives(monkeypatch):
    """Sans `pool_recycle`, une connexion inactive pendant une étape longue serait coupée
    par MySQL (`wait_timeout`) et la requête suivante échouerait."""
    recu = {}

    async def faux_pool(**options):
        recu.update(options)
        return object()

    monkeypatch.setattr(aiomysql, "create_pool", faux_pool)

    asyncio.run(Database(host="h", max_connections=2).connect())

    assert recu["pool_recycle"] == MYSQL_POOL_RECYCLE
    assert 0 < MYSQL_POOL_RECYCLE
