"""Tests du contrat d'erreur du domaine trafics et du bloc debug (DSR-679).

Databricks est stubé (pas de réseau). On exécute le code async via ``asyncio.run`` pour ne pas
dépendre de pytest-asyncio.

Le point couvert en priorité est la restitution de la **requête fautive** : avant la refonte,
la requête n'était tracée qu'*après* son exécution, donc celle qui plantait n'apparaissait
jamais dans le bloc debug — on ne voyait que celles qui avaient réussi.
"""

import asyncio

import pytest
from fastapi import HTTPException

from app.routes.trppu_trafics import errors, helpers, routes

MESSAGE_500 = "Erreur lors de la récupération des trafics."

# Période produisant exactement 2 requêtes (une par zone) grâce à `is_day=True` :
# zone réelle 01-14/01, zone prévisionnelle 15-31/01, table jour forcée dans les deux cas.
PARAMS_PIVOT = {
    "co_regate": "400300",
    "date_debut": "20250101",
    "date_fin": "20250131",
    "date_pivot": "20250115",
    "is_day": True,
}

LIGNES_OK = [{helpers.OBJ_ALIAS: "OO", helpers.LIBELLE_ALIAS: "OBJETS ORDINAIRES",
              helpers.SOMME_ALIAS: 10}]


@pytest.fixture(autouse=True)
def _debug_actif(monkeypatch):
    """Debug actif par défaut ; les tests qui vérifient l'inverse le repassent à False.

    `DEBUG_SHOW_QUERY` n'est lu que par `errors` : un seul point de patch pour tout le domaine.
    """
    monkeypatch.setattr(errors, "DEBUG_SHOW_QUERY", True)


@pytest.fixture(autouse=True)
def _nb_jours_neutralise(monkeypatch):
    """Neutralise l'appel à l'API jours fermés (hors périmètre de ces tests)."""
    async def _faux_compute(debut, fin):
        raise RuntimeError("jours fermés indisponibles")

    monkeypatch.setattr(routes, "compute_nb_jours", _faux_compute)


def _stub_databricks(monkeypatch, comportements):
    """Stube `fetch_all` : `comportements[i]` = lignes à renvoyer, ou exception à lever."""
    appels = {"n": 0}

    def _fetch_all(sql, params=None):
        i = appels["n"]
        appels["n"] += 1
        resultat = comportements[i] if i < len(comportements) else []
        if isinstance(resultat, Exception):
            raise resultat
        return resultat

    monkeypatch.setattr(helpers.databricks, "fetch_all", _fetch_all)
    return appels


def _appeler_pivot(**surcharges):
    params = {**PARAMS_PIVOT, **surcharges}
    return asyncio.run(routes.get_trafics_pivot(**params))


def _detail_du_500(**surcharges) -> dict:
    with pytest.raises(HTTPException) as exc:
        _appeler_pivot(**surcharges)
    assert exc.value.status_code == 500
    return exc.value.detail


# --- Bloc debug en erreur -----------------------------------------------------------------
def test_erreur_500_expose_toutes_les_requetes_et_identifie_la_fautive(monkeypatch):
    _stub_databricks(monkeypatch, [LIGNES_OK, RuntimeError("boom databricks")])

    debug = _detail_du_500()["debug"]

    assert [q["statut"] for q in debug["queries"]] == ["ok", "echec"]
    assert debug["requete_en_echec"]["index"] == 1
    # La requête fautive est restituée en entier et rejouable telle quelle.
    assert "g_trppu_trafics_jour_3" in debug["requete_en_echec"]["sql"]
    assert ":co_regate" not in debug["requete_en_echec"]["sql"]


def test_erreur_500_porte_le_message_databricks_brut(monkeypatch):
    _stub_databricks(monkeypatch, [LIGNES_OK, RuntimeError("boom databricks")])

    debug = _detail_du_500()["debug"]

    assert debug["databricks_error"] == "boom databricks"
    assert debug["requete_en_echec"]["erreur"] == "boom databricks"


def test_requete_fautive_tracee_des_le_premier_appel(monkeypatch):
    """Non-régression : l'ancien code traçait après l'exécution, `queries` sortait donc vide."""
    _stub_databricks(monkeypatch, [RuntimeError("echec immediat")])

    debug = _detail_du_500()["debug"]

    assert len(debug["queries"]) == 1
    assert debug["queries"][0]["statut"] == "echec"
    assert debug["queries"][0]["sql"]


def test_erreur_500_n_expose_rien_sans_debug(monkeypatch):
    monkeypatch.setattr(errors, "DEBUG_SHOW_QUERY", False)
    _stub_databricks(monkeypatch, [RuntimeError("boom databricks")])

    # Égalité stricte : garantit l'absence de `debug`, `queries` et `databricks_error`.
    assert _detail_du_500() == {"error": True, "message": MESSAGE_500, "code": 500}


# --- Bloc debug en succès -----------------------------------------------------------------
def test_reponse_ok_expose_les_requetes_en_debug(monkeypatch):
    _stub_databricks(monkeypatch, [LIGNES_OK, LIGNES_OK])

    debug = _appeler_pivot()["debug"]

    assert [q["statut"] for q in debug["queries"]] == ["ok", "ok"]
    assert "requete_en_echec" not in debug
    assert "databricks_error" not in debug
    # Le bloc debug ne recrache jamais les données, seulement leur volume.
    assert all("lignes" not in q and q["nb_lignes"] == 1 for q in debug["queries"])


def test_reponse_ok_sans_debug_ne_contient_pas_de_bloc_debug(monkeypatch):
    monkeypatch.setattr(errors, "DEBUG_SHOW_QUERY", False)
    _stub_databricks(monkeypatch, [LIGNES_OK, LIGNES_OK])

    reponse = _appeler_pivot()

    assert "debug" not in reponse
    assert reponse["count"] == 1
    assert reponse["trafics"][0]["co_produit"] == "OO"
    assert reponse["nb_jours"] is None  # API jours fermés stubée en échec : non bloquant


# --- Exécuteur tracé ----------------------------------------------------------------------
def test_trace_execution_capture_le_sql_rendu(monkeypatch):
    _stub_databricks(monkeypatch, [LIGNES_OK])

    trace = helpers.executer_requete("SELECT 1 WHERE co_regate = :co_regate", {"co_regate": "400300"})

    assert trace["statut"] == "ok"
    assert trace["sql"] == "SELECT 1 WHERE co_regate = '400300'"
    assert trace["nb_lignes"] == len(LIGNES_OK)


def test_trace_execution_conserve_le_sql_en_cas_d_echec(monkeypatch):
    _stub_databricks(monkeypatch, [RuntimeError("table introuvable")])

    trace = helpers.executer_requete("SELECT 1 WHERE co_regate = :co_regate", {"co_regate": "400300"})

    assert trace["statut"] == "echec"
    assert trace["erreur"] == "table introuvable"
    assert trace["sql"] == "SELECT 1 WHERE co_regate = '400300'"
    assert "lignes" not in trace


# --- Format des 400 -----------------------------------------------------------------------
def test_erreur_400_format_uniforme():
    with pytest.raises(HTTPException) as exc:
        helpers.validate_params_pivot(None, "20250101", "20250131", "20250115")

    detail = exc.value.detail
    assert exc.value.status_code == 400
    assert detail["error"] is True and detail["code"] == 400
    assert "co_regate" in detail["message"]
    # Un 400 métier n'expose jamais de SQL, même avec le debug actif.
    assert "debug" not in detail


@pytest.mark.parametrize(
    "dates, attendu",
    [
        (("20250201", "20250101", "20250115"), "antérieure ou égale"),
        (("20200101", "20250101", "20220101"), "dépasse les 2 ans"),
        (("pas-une-date", "20250131", "20250115"), "Format de date_debut invalide"),
    ],
)
def test_400_meme_format_sur_tous_les_chemins(dates, attendu):
    """Les 4 `raise` factorisés dans `erreur_400` gardent leur message d'origine."""
    with pytest.raises(HTTPException) as exc:
        helpers.validate_params_pivot("400300", *dates)

    detail = exc.value.detail
    assert exc.value.status_code == 400
    assert set(detail) == {"error", "message", "code"}
    assert attendu in detail["message"]
