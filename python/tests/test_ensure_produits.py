"""Tests de la création automatique des produits référencés par un TMH (sans base).

Le référentiel produits est piloté par Databricks : un objet remonté par les trafics peut ne pas
exister dans `trppu_produit`, ce qui casserait la FK `fk_tmh_produit` à l'insert du TMH.
"""

import asyncio

from app.routes.trppu_produit.helpers import (
    ENSURE_PRODUIT_SQL,
    ensure_produits_exist,
    normalize_co_produit_ref,
)


class FakeTx:
    """Curseur de transaction minimal : journalise les execute et simule le rowcount MySQL.

    Convention MySQL sur `INSERT ... ON DUPLICATE KEY UPDATE` : 1 = insertion, 0 = no-op.
    """

    def __init__(self, existants=()):
        self.existants = set(existants)
        self.executes: list[tuple[str, tuple]] = []

    async def execute(self, sql, params):
        self.executes.append((sql, params))
        code = params[0]
        if code in self.existants:
            return 0
        self.existants.add(code)
        return 1


def test_cree_uniquement_les_produits_absents():
    tx = FakeTx(existants={"OO"})
    crees = asyncio.run(
        ensure_produits_exist(tx, ["OO", "PPI"], {"OO": "OBJETS ORDINAIRES"})
    )
    assert crees == ["PPI"]
    assert all(sql == ENSURE_PRODUIT_SQL for sql, _ in tx.executes)


def test_libelle_issu_du_mapping_databricks():
    tx = FakeTx()
    asyncio.run(ensure_produits_exist(tx, ["CO"], {"CO": "COLIS"}))
    assert tx.executes[0][1] == ("CO", "COLIS")


def test_repli_sur_le_code_quand_le_mapping_ignore_l_objet():
    """`lb_produit` est NOT NULL : PR/PPI, absents du mapping, prennent leur code."""
    tx = FakeTx()
    asyncio.run(ensure_produits_exist(tx, ["PR", "PPI"], {"OO": "OBJETS ORDINAIRES"}))
    assert [params for _, params in tx.executes] == [("PR", "PR"), ("PPI", "PPI")]


def test_dedoublonne_et_ignore_les_codes_vides():
    tx = FakeTx()
    crees = asyncio.run(ensure_produits_exist(tx, ["oo", "OO", " ", None], None))
    assert crees == ["OO"]
    assert len(tx.executes) == 1


def test_normalisation_sans_zfill():
    """Contrairement à l'import Excel, pas de `zfill(2)` : le code doit rester celui du TMH."""
    assert normalize_co_produit_ref(" pr ") == "PR"
    assert normalize_co_produit_ref("P") == "P"
    assert normalize_co_produit_ref("ppi") == "PPI"
