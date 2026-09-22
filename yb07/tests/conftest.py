"""Doublures partagées par les tests.

Aucune base MySQL n'est nécessaire : `FausseBase` remplace `Database` en rendant des réponses
indexées par fragment de requête, et journalise tout ce qui est exécuté. Les tests peuvent donc
vérifier non seulement le résultat, mais **l'ordre** des écritures — c'est ce qui compte dès
qu'un traitement pose un verrou ou purge avant de réécrire.

Deux partis pris :

* une requête sans réponse déclarée lève `KeyError`. Un test qui interroge une table à laquelle
  il n'a pas pensé échoue bruyamment, au lieu de recevoir un `None` qui ressemble à « pas de
  données » et fait passer le test pour la mauvaise raison ;
* `FausseBase(lecture_seule=True)` lève sur toute écriture : c'est ainsi qu'on prouve qu'un
  traitement de contrôle n'a rien modifié, plutôt que de le relire.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any


def _normaliser(sql: str) -> str:
    """Requête sur une seule ligne, espaces multiples réduits — pour la comparaison."""
    return " ".join(sql.split())


class EcritureInterdite(AssertionError):
    """Lève quand un traitement censé être en lecture seule tente d'écrire."""


class FauxCurseur:
    """Curseur de transaction : journalise et rend le nombre de lignes déclaré."""

    def __init__(self, base: "FausseBase") -> None:
        self._base = base

    async def execute(self, query: str, params: tuple | None = None) -> int:
        return self._base._enregistrer("execute", query, params)

    async def execute_many(self, query: str, params_seq) -> int:
        lignes = list(params_seq)
        self._base._enregistrer("execute_many", query, lignes)
        return len(lignes)

    async def fetch_one(self, query: str, params: tuple | None = None):
        return await self._base.fetch_one(query, params)

    async def fetch_all(self, query: str, params: tuple | None = None):
        return await self._base.fetch_all(query, params)


class FausseBase:
    """Substitut de `app.db.mysql.Database` pour les tests."""

    def __init__(
        self,
        reponses: dict[str, Any] | None = None,
        *,
        rowcounts: dict[str, int] | None = None,
        lecture_seule: bool = False,
    ) -> None:
        self.reponses = {_normaliser(k): v for k, v in (reponses or {}).items()}
        self.rowcounts = {_normaliser(k): v for k, v in (rowcounts or {}).items()}
        self.lecture_seule = lecture_seule
        self.journal: list[tuple[str, str, Any]] = []
        self.transactions_commitees = 0
        self.transactions_annulees = 0

    # -- lectures ---------------------------------------------------------

    async def fetch_all(self, query: str, params: tuple | None = None) -> list[dict]:
        self.journal.append(("fetch", _normaliser(query), params))
        reponse = self._reponse(query)
        if reponse is None:
            return []
        return list(reponse) if isinstance(reponse, list) else [reponse]

    async def fetch_one(self, query: str, params: tuple | None = None) -> dict | None:
        lignes = await self.fetch_all(query, params)
        return lignes[0] if lignes else None

    # -- écritures --------------------------------------------------------

    async def execute(self, query: str, params: tuple | None = None) -> int:
        return self._enregistrer("execute", query, params)

    @asynccontextmanager
    async def transaction(self):
        curseur = FauxCurseur(self)
        try:
            yield curseur
        except Exception:
            self.transactions_annulees += 1
            raise
        self.transactions_commitees += 1

    async def disconnect(self) -> None:  # pragma: no cover - symétrie avec Database
        return None

    # -- utilitaires de test ---------------------------------------------

    def _enregistrer(self, genre: str, query: str, params: Any) -> int:
        if self.lecture_seule:
            raise EcritureInterdite(f"écriture interdite : {_normaliser(query)[:80]}")
        normalisee = _normaliser(query)
        self.journal.append((genre, normalisee, params))
        for fragment, lignes in self.rowcounts.items():
            if fragment in normalisee:
                return lignes
        return 1

    def _reponse(self, query: str) -> Any:
        normalisee = _normaliser(query)
        for fragment, reponse in self.reponses.items():
            if fragment in normalisee:
                return reponse
        raise KeyError(f"aucune réponse déclarée pour : {normalisee[:120]}")

    def ecritures(self) -> list[str]:
        """Requêtes d'écriture, dans l'ordre — pour vérifier un enchaînement."""
        return [sql for genre, sql, _ in self.journal if genre != "fetch"]

    def a_ecrit(self, fragment: str) -> bool:
        return any(_normaliser(fragment) in sql for sql in self.ecritures())

    def parametres_de(self, fragment: str) -> list[Any]:
        """Paramètres passés à la première écriture contenant `fragment`."""
        cible = _normaliser(fragment)
        for genre, sql, params in self.journal:
            if genre != "fetch" and cible in sql:
                return params
        raise AssertionError(f"aucune écriture ne contient : {fragment}")

