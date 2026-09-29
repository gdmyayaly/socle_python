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


def test_les_connexions_alignent_leur_classement_sur_la_base(monkeypatch):
    """Sans cela, `co_regate = @co_regate` lève l'erreur 1267 (Illegal mix of collations) :
    pymysql ouvre en utf8mb4_general_ci, les tables sont en utf8mb4_0900_ai_ci."""
    recu = {}

    async def faux_pool(**options):
        recu.update(options)
        return object()

    monkeypatch.setattr(aiomysql, "create_pool", faux_pool)

    asyncio.run(Database(host="h").connect())

    assert recu["charset"] == "utf8mb4"
    assert recu["init_command"] == "SET collation_connection = @@collation_database"


def test_la_connexion_des_scripts_aligne_aussi_son_classement(monkeypatch):
    """Les scripts (dont celui des versions) tournent sur une connexion hors pool."""
    recu = {}

    class FausseConnexion:
        async def ensure_closed(self):
            return None

    async def faux_connect(**options):
        recu.update(options)
        return FausseConnexion()

    monkeypatch.setattr(aiomysql, "connect", faux_connect)

    async def ouvrir():
        async with Database(host="h")._script_connection("base", autocommit=True):
            pass

    asyncio.run(ouvrir())

    assert recu["init_command"] == "SET collation_connection = @@collation_database"


def test_classement_impose_par_la_configuration(monkeypatch):
    from app.db import mysql

    monkeypatch.setattr(mysql, "MYSQL_COLLATION", "utf8mb4_0900_ai_ci")
    assert mysql._init_command() == "SET collation_connection = 'utf8mb4_0900_ai_ci'"


class _FausseSocket:
    def __init__(self):
        self.options = []

    def setsockopt(self, niveau, option, valeur):
        self.options.append((niveau, option, valeur))


class _FausseConnexionReseau:
    def __init__(self):
        self.socket = _FausseSocket()

        class Transport:
            def get_extra_info(inner, nom):
                return self.socket if nom == "socket" else None

        self._writer = Transport()


def test_keepalive_tcp_active_une_seule_fois():
    """Sans sondes, un ALTER de plusieurs minutes laisse la connexion muette : un équipement
    réseau à délai d'inactivité la coupe (erreur 2013), MySQL travaillant encore."""
    import socket

    from app.db import mysql

    conn = _FausseConnexionReseau()
    mysql._activer_keepalive(conn)
    mysql._activer_keepalive(conn)  # idempotent

    options = conn.socket.options
    assert (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) in options
    assert options.count((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)) == 1
    if hasattr(socket, "TCP_KEEPIDLE"):
        assert (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, mysql.MYSQL_TCP_KEEPALIVE) in options


def test_keepalive_ignore_une_connexion_sans_socket():
    from app.db import mysql

    class SansTransport:
        _writer = None

    mysql._activer_keepalive(SansTransport())  # ne lève pas
