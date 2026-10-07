"""Journal `trppu_recalcul_log` et verrou de calcul.

`journaliser` est best-effort (ne masque pas l'erreur métier) ; `liberer_verrou` propage
(un verrou non libéré bloque le scénario pour tous).
"""

import asyncio
import logging

import pytest

from app.traitements import scenario as scn


class BaseEnPanne:
    """Base dont toute écriture échoue."""

    def __init__(self, exception: Exception | None = None):
        self.appels: list[tuple[str, tuple]] = []
        self._exception = exception or RuntimeError("base injoignable")

    async def execute(self, sql, params=None, retries=None):
        self.appels.append((sql, params))
        self.retries = retries
        raise self._exception


class BaseOk:
    """Base qui accepte les écritures et rend un rowcount."""

    def __init__(self, rowcount: int = 1):
        self.appels: list[tuple[str, tuple]] = []
        self._rowcount = rowcount

    async def execute(self, sql, params=None, retries=None):
        self.appels.append((sql, params))
        self.retries = retries
        return self._rowcount


# --- journaliser : best-effort ---------------------------------------------


def test_journaliser_ecrit_la_ligne_attendue():
    db = BaseOk()
    asyncio.run(scn.journaliser(db, 12345, "INITIAL", "Premier calcul"))

    sql, params = db.appels[0]
    assert "INSERT INTO trppu_recalcul_log" in sql
    assert params == (12345, "INITIAL", "Premier calcul")


def test_commentaire_tronque_a_255_caracteres():
    """La colonne est un varchar(255) : la trace ne doit pas faire échouer l'INSERT."""
    db = BaseOk()
    asyncio.run(scn.journaliser(db, 12345, "INITIAL", "x" * 400))
    assert len(db.appels[0][1][2]) == 255


def test_echec_d_ecriture_ne_remonte_pas(caplog):
    """Une base en erreur ne doit pas masquer l'erreur métier d'origine."""
    db = BaseEnPanne()
    with caplog.at_level(logging.WARNING, logger="app.traitements.scenario"):
        asyncio.run(scn.journaliser(db, 12345, "INITIAL", "Recalcul interrompu - boom"))

    assert "trppu_recalcul_log" in caplog.text


def test_erreur_metier_preservee_malgre_un_echec_de_journalisation():
    """L'appelant voit la cause métier, pas l'échec de l'audit."""
    db = BaseEnPanne(RuntimeError("audit KO"))

    async def chemin_d_echec() -> str:
        try:
            raise ValueError("cause métier réelle")
        except ValueError as erreur:
            await scn.journaliser(db, 12345, "INITIAL", f"interrompu - {erreur}")
            return str(erreur)

    assert asyncio.run(chemin_d_echec()) == "cause métier réelle"


# --- liberer_verrou : volontairement propageante ---------------------------


def test_liberer_verrou_propage_l_echec():
    """Un verrou non libéré bloque le scénario pour tous : ça reste un échec visible."""
    db = BaseEnPanne()
    with pytest.raises(RuntimeError):
        asyncio.run(scn.liberer_verrou(db, 12345))


def test_liberer_verrou_journalise_la_volumetrie(caplog):
    db = BaseOk(rowcount=1)
    with caplog.at_level(logging.INFO, logger="app.traitements.scenario"):
        asyncio.run(scn.liberer_verrou(db, 12345))
    assert "rows_affected=1" in caplog.text


# --- prendre_verrou : collision entre workers ------------------------------


def test_verrou_obtenu():
    db = BaseOk(rowcount=1)
    assert asyncio.run(scn.prendre_verrou(db, 12345)) is True


def test_verrou_non_obtenu_est_trace(caplog):
    """0 ligne affectée = un autre worker détient le scénario."""
    db = BaseOk(rowcount=0)
    with caplog.at_level(logging.WARNING, logger="app.traitements.scenario"):
        obtenu = asyncio.run(scn.prendre_verrou(db, 12345))

    assert obtenu is False
    assert "Verrou de calcul non obtenu" in caplog.text
    assert "id_scenario=12345" in caplog.text
