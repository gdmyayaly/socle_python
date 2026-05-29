"""Tests de recompute_realise_prev (bornes réalisé / prévisionnel)."""

from datetime import date

from app.routes.trppu_scenario.helpers import recompute_realise_prev

TODAY = date(2026, 5, 28)


def test_periode_entierement_passee():
    rd, rf, pd_, pf = recompute_realise_prev(date(2025, 1, 1), date(2025, 12, 31), today=TODAY)
    assert (rd, rf) == (date(2025, 1, 1), date(2025, 12, 31))
    assert (pd_, pf) == (None, None)


def test_periode_entierement_future():
    rd, rf, pd_, pf = recompute_realise_prev(date(2027, 1, 1), date(2027, 12, 31), today=TODAY)
    assert (rd, rf) == (None, None)
    assert (pd_, pf) == (date(2027, 1, 1), date(2027, 12, 31))


def test_periode_chevauchant_today():
    rd, rf, pd_, pf = recompute_realise_prev(date(2026, 1, 1), date(2026, 12, 31), today=TODAY)
    assert rd == date(2026, 1, 1)
    assert rf == TODAY
    assert pd_ == TODAY
    assert pf == date(2026, 12, 31)
