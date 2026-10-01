"""Réglages du pool MySQL (`app/db/mysql.py`) : renouvellement, classement, keepalive."""

import asyncio
import socket

import aiomysql

from app.config import MYSQL_POOL_RECYCLE
from app.db import mysql
from app.db.mysql import Database


def test_le_pool_renouvelle_ses_connexions_et_aligne_leur_classement(monkeypatch):
    recu = {}

    async def faux_pool(**options):
        recu.update(options)
        return object()

    monkeypatch.setattr(aiomysql, "create_pool", faux_pool)

    asyncio.run(Database(host="h").connect())

    assert recu["pool_recycle"] == MYSQL_POOL_RECYCLE
    assert recu["charset"] == "utf8mb4"
    assert recu["init_command"] == "SET collation_connection = @@collation_database"


def test_classement_impose_par_la_configuration(monkeypatch):
    monkeypatch.setattr(mysql, "MYSQL_COLLATION", "utf8mb4_0900_ai_ci")
    assert mysql._init_command() == "SET collation_connection = 'utf8mb4_0900_ai_ci'"


class _Socket:
    def __init__(self):
        self.options = []

    def setsockopt(self, niveau, option, valeur):
        self.options.append((niveau, option, valeur))


class _Connexion:
    def __init__(self):
        self.socket = _Socket()

        class Transport:
            def get_extra_info(inner, nom):
                return self.socket if nom == "socket" else None

        self._writer = Transport()


def test_keepalive_tcp_active_une_seule_fois():
    conn = _Connexion()
    mysql._activer_keepalive(conn)
    mysql._activer_keepalive(conn)

    assert conn.socket.options.count((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)) == 1


def test_keepalive_ignore_une_connexion_sans_socket():
    class SansTransport:
        _writer = None

    mysql._activer_keepalive(SansTransport())
