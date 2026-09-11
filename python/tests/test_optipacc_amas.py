"""Tests des trafics Agrébal exposés à OPTIPACC (DSR-705).

Quatre familles, dans l'esprit de `test_optipacc.py` :

1. **Pivot des densités (SQLite en mémoire)** — le point le plus subtil du ticket :
   en base la densité est un *discriminant de ligne* (`couleur_pic`), alors que le
   JSON attendu porte `fort` / `faible1` / `faible2` sur la même ligne produit. On
   rejoue donc réellement la constante SQL, ainsi que le LEFT JOIN qui va chercher
   `nom_amas` dans une autre table.
2. **Garde de visibilité C2/C3** — matrice statut × flags.
3. **Pagination et filtre d'amas** — RG-API-007/008 et Cas 6 à 8.
4. **Non-régression schéma** — colonnes et valeurs d'ENUM citées par les constantes.
"""

import asyncio
import re
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.routes.trppu_optipacc import helpers, routes
from app.routes.trppu_optipacc.schemas import TraficAmasRequest


# --- Outils communs -------------------------------------------------------------


def _scenario(
    statut="VALIDE",
    co_regate="123456",
    id_scenario=125,
    est_fige=1,
    pdi=1,
    agrebal=1,
    en_cours=0,
):
    return {
        "id_scenario": id_scenario,
        "co_regate": co_regate,
        "statut": statut,
        "est_fige": est_fige,
        "trafic_pdi_calcule": pdi,
        "trafic_agrebal_calcule": agrebal,
        "calcul_trafic_en_cours": en_cours,
    }


def _ligne(uuid, jour, produit, fort, faible1, faible2, nom="PLUVENCE_2449"):
    """Une ligne telle que la renvoie la requête pivotée."""
    return {
        "agrebal_uuid": uuid,
        "nom_amas": nom,
        "jour_semaine": jour,
        "co_produit": produit,
        "fort": fort,
        "faible1": faible1,
        "faible2": faible2,
    }


class FakeRead:
    """`db_read` scripté : `fetch_one` pour la garde, `fetch_all` par appel successif."""

    def __init__(self, *, scenario=None, nb_amas=0, pages=None, trafics=None, boom=False):
        self._scenario = scenario
        self._nb_amas = nb_amas
        self._pages = pages if pages is not None else []
        self._trafics = trafics or []
        self._boom = boom
        self.executed: list[tuple[str, tuple]] = []

    async def fetch_one(self, sql, params=None):
        self.executed.append((sql, params))
        if "COUNT(DISTINCT agrebal_uuid)" in sql:
            return {"nb": self._nb_amas}
        return self._scenario

    async def fetch_all(self, sql, params=None):
        self.executed.append((sql, params))
        if self._boom:
            raise RuntimeError("MySQL indisponible")
        if "SELECT DISTINCT agrebal_uuid" in sql:
            return [{"agrebal_uuid": u} for u in self._pages]
        return self._trafics


@pytest.fixture
def site_existant(monkeypatch):
    async def _ok(co_regate):
        return {"co_regate": co_regate}

    monkeypatch.setattr(routes, "fetch_site_or_404", _ok)


def _appeler(db, *, monkeypatch, amas=None, page=1, taille_page=None):
    monkeypatch.setattr(routes, "db_read", db)
    if taille_page is not None:
        monkeypatch.setattr(routes, "NB_AMAS_PAR_PAGE", taille_page)
    payload = TraficAmasRequest(
        code_regate="123456", scenario_id=125, amas=amas, page=page
    )
    return asyncio.run(routes.trafic_amas(payload, id_session_ihm=None))


# --- 1. Pivot des densités (SQLite en mémoire) ----------------------------------

_DDL = """
CREATE TABLE trppu_trafic_agrebal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_scenario INTEGER NOT NULL,
    co_regate TEXT NOT NULL,
    id_agrebal INTEGER NOT NULL,
    agrebal_uuid TEXT NOT NULL,
    co_produit TEXT NOT NULL,
    jour_semaine TEXT NOT NULL,
    couleur_pic TEXT NOT NULL,
    volume NUMERIC NOT NULL
);
CREATE TABLE trppu_agrebal_pdi (
    agrebal_id_pdi INTEGER PRIMARY KEY AUTOINCREMENT,
    agrebal_id INTEGER NOT NULL,
    agrebal_uuid TEXT NOT NULL,
    agrebal_nom TEXT,
    agrebal_code_regate TEXT NOT NULL
);
"""


def _rejouer(lignes, amas_pdi, uuids, id_scenario=125, co_regate="123456"):
    """Joue la requête pivotée sur SQLite et renvoie les lignes résultat.

    `FIELD()` étant propre à MySQL, le module l'isole dans ORDER_BY_TRAFICS_AMAS :
    on rejoue donc le SELECT sans son ORDER BY.
    """
    cx = sqlite3.connect(":memory:")
    cx.row_factory = sqlite3.Row
    cx.executescript(_DDL)
    cx.executemany(
        "INSERT INTO trppu_trafic_agrebal (id_scenario, co_regate, id_agrebal, "
        "agrebal_uuid, co_produit, jour_semaine, couleur_pic, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        lignes,
    )
    cx.executemany(
        "INSERT INTO trppu_agrebal_pdi (agrebal_id, agrebal_uuid, agrebal_nom, "
        "agrebal_code_regate) VALUES (?, ?, ?, ?)",
        amas_pdi,
    )
    sql = helpers.select_trafics_amas_sql(len(uuids))
    sql = sql[: sql.index(" ORDER BY")].replace("%s", "?")
    rows = cx.execute(sql, (id_scenario, co_regate, *uuids)).fetchall()
    cx.close()
    return [dict(r) for r in rows]


def test_pivot_des_trois_densites_sur_une_seule_ligne_produit():
    """DENSE -> fort, FAIBLE1 -> faible1, FAIBLE2 -> faible2 (mapping du ticket)."""
    lignes = [
        (125, "123456", 7, "uuidA", "OO", "LUNDI", "DENSE", 12),
        (125, "123456", 7, "uuidA", "OO", "LUNDI", "FAIBLE1", 2),
        (125, "123456", 7, "uuidA", "OO", "LUNDI", "FAIBLE2", 1),
    ]
    pdi = [(7, "uuidA", "PLUVENCE_2449", "123456")]
    rows = _rejouer(lignes, pdi, ["uuidA"])
    assert len(rows) == 1
    assert (rows[0]["fort"], rows[0]["faible1"], rows[0]["faible2"]) == (12, 2, 1)
    assert rows[0]["nom_amas"] == "PLUVENCE_2449"


def test_une_densite_absente_ressort_a_null():
    """Toutes les densités ne sont pas forcément produites par YB05 pour un couple."""
    lignes = [(125, "123456", 7, "uuidA", "PPI", "MARDI", "DENSE", 8)]
    rows = _rejouer(lignes, [(7, "uuidA", "A", "123456")], ["uuidA"])
    assert rows[0]["fort"] == 8
    assert rows[0]["faible1"] is None and rows[0]["faible2"] is None


def test_les_jours_et_produits_restent_separes():
    lignes = [
        (125, "123456", 7, "uuidA", "OO", "LUNDI", "DENSE", 12),
        (125, "123456", 7, "uuidA", "PPI", "LUNDI", "DENSE", 8),
        (125, "123456", 7, "uuidA", "OO", "MARDI", "DENSE", 10),
    ]
    rows = _rejouer(lignes, [(7, "uuidA", "A", "123456")], ["uuidA"])
    assert len(rows) == 3


def test_un_amas_sans_ligne_referentiel_garde_ses_trafics():
    """LEFT JOIN volontaire : `nom_amas` est nullable et la ligne peut avoir disparu."""
    lignes = [(125, "123456", 7, "uuidA", "OO", "LUNDI", "DENSE", 12)]
    rows = _rejouer(lignes, [], ["uuidA"])  # aucun trppu_agrebal_pdi
    assert len(rows) == 1
    assert rows[0]["nom_amas"] is None


def test_le_scenario_et_le_site_sont_isoles():
    lignes = [
        (125, "123456", 7, "uuidA", "OO", "LUNDI", "DENSE", 12),
        (999, "123456", 7, "uuidA", "OO", "LUNDI", "DENSE", 500),  # autre scénario
        (125, "999999", 7, "uuidA", "OO", "LUNDI", "DENSE", 500),  # autre site
    ]
    rows = _rejouer(lignes, [(7, "uuidA", "A", "123456")], ["uuidA"])
    assert len(rows) == 1 and rows[0]["fort"] == 12


def test_les_doublons_techniques_sont_replies():
    """La table n'a aucune clé unique : deux lignes identiques ne doivent pas
    produire deux entrées produit. Repli technique, pas une règle métier (RG-API-006).
    """
    lignes = [
        (125, "123456", 7, "uuidA", "OO", "LUNDI", "DENSE", 12),
        (125, "123456", 7, "uuidA", "OO", "LUNDI", "DENSE", 3),
    ]
    rows = _rejouer(lignes, [(7, "uuidA", "A", "123456")], ["uuidA"])
    assert len(rows) == 1 and rows[0]["fort"] == 15


def test_la_jointure_passe_par_la_cle_unique_et_non_par_luuid():
    """`agrebal_uuid` n'est pas indexé dans trppu_agrebal_pdi : joindre dessus
    serait un balayage de table. uq_agrpdi_courant = (agrebal_id, agrebal_code_regate).
    """
    sql = helpers.select_trafics_amas_sql(1)
    assert "a.agrebal_id = t.id_agrebal AND a.agrebal_code_regate = t.co_regate" in sql
    assert "LEFT JOIN trppu_agrebal_pdi" in sql


def test_aucun_coefficient_ni_cle_de_repartition_dans_la_requete():
    """RG-API-006 : restitution brute, aucune transformation métier."""
    sql = helpers.select_trafics_amas_sql(1).lower()
    for interdit in ("coef", "trppu_scenario_pic_coeffs", "cle_repartition"):
        assert interdit not in sql


def test_les_uuid_restent_des_parametres_lies():
    """La clause IN est construite depuis le seul *nombre* de valeurs."""
    sql = helpers.select_amas_existants_sql(3)
    assert "IN (%s, %s, %s)" in sql


# --- 2. Regroupement Python -----------------------------------------------------


def test_regroupement_par_amas_puis_jour_puis_produit():
    rows = [
        _ligne("uuidA", "LUNDI", "OO", 12, 2, 1),
        _ligne("uuidA", "LUNDI", "PPI", 8, 1, 1),
        _ligne("uuidA", "MARDI", "OO", 10, 3, 2),
        _ligne("uuidB", "LUNDI", "OO", 5, 0, 0, nom="AUTRE"),
    ]
    amas = routes._regrouper_amas(rows)
    assert [a.agrebal_uuid for a in amas] == ["uuidA", "uuidB"]
    assert list(amas[0].jours) == ["lundi", "mardi"]
    assert len(amas[0].jours["lundi"]) == 2
    assert amas[0].jours["lundi"][0].produit == "OO"
    assert amas[1].nom_amas == "AUTRE"


def test_les_volumes_decimal_sont_restitues_en_entier():
    """SUM sur decimal(12,4) renvoie un Decimal ; les volumes sont entiers par
    construction (YB05 somme des smallint de trppu_trafic_pdi)."""
    rows = [_ligne("uuidA", "LUNDI", "OO", Decimal("12.0000"), None, Decimal("1.0000"))]
    produit = routes._regrouper_amas(rows)[0].jours["lundi"][0]
    assert (produit.fort, produit.faible1, produit.faible2) == (12, 0, 1)


def test_les_jours_sont_mis_en_minuscules():
    """Mapping du ticket : LUNDI -> lundi, …, SAMEDI -> samedi."""
    jours = ["LUNDI", "MARDI", "MERCREDI", "JEUDI", "VENDREDI", "SAMEDI"]
    rows = [_ligne("uuidA", j, "OO", 1, 0, 0) for j in jours]
    amas = routes._regrouper_amas(rows)[0]
    assert list(amas.jours) == [j.lower() for j in jours]


# --- 3. Gardes C1 à C3 ----------------------------------------------------------


def test_route_exposee_en_post():
    methodes = {
        m
        for r in routes.router.routes
        if r.path == "/trppu-api/optipacc/trafic-amas"
        for m in r.methods
    }
    assert methodes == {"POST"}


def test_c1_404_si_scenario_inexistant(monkeypatch, site_existant):
    with pytest.raises(HTTPException) as exc:
        _appeler(FakeRead(scenario=None), monkeypatch=monkeypatch)
    assert exc.value.status_code == 404


def test_c2_400_si_scenario_dun_autre_site(monkeypatch, site_existant):
    with pytest.raises(HTTPException) as exc:
        _appeler(FakeRead(scenario=_scenario(co_regate="999999")), monkeypatch=monkeypatch)
    assert exc.value.status_code == 400


@pytest.mark.parametrize(
    "flags",
    [
        {"statut": "EN COURS"},
        {"statut": "SIMULATION"},
        {"statut": "ARCHIVE"},
        {"est_fige": 0},
        {"en_cours": 1},  # Cas 3 du ticket
        {"agrebal": 0},  # Cas 4 du ticket
        {"pdi": 0},
    ],
)
def test_c3_409_scenario_non_disponible(flags, monkeypatch, site_existant):
    with pytest.raises(HTTPException) as exc:
        _appeler(FakeRead(scenario=_scenario(**flags)), monkeypatch=monkeypatch)
    assert exc.value.status_code == 409
    assert exc.value.detail == "Scenario non disponible"


@pytest.mark.parametrize("statut", ["VALIDE", "EN PRODUCTION"])
def test_c3_accepte_aussi_un_scenario_en_production(statut, monkeypatch, site_existant):
    """DSR-707 met EN PRODUCTION le scénario retenu par OPTIPACC : une lecture
    stricte de C3 rendrait ses propres trafics illisibles juste après.
    """
    db = FakeRead(
        scenario=_scenario(statut=statut),
        nb_amas=1,
        pages=["uuidA"],
        trafics=[_ligne("uuidA", "LUNDI", "OO", 12, 2, 1)],
    )
    reponse = _appeler(db, monkeypatch=monkeypatch)
    assert reponse.amas[0].agrebal_uuid == "uuidA"


def test_erreur_sgbd_donne_un_500(monkeypatch, site_existant):
    db = FakeRead(scenario=_scenario(), nb_amas=1, boom=True)
    with pytest.raises(HTTPException) as exc:
        _appeler(db, monkeypatch=monkeypatch)
    assert exc.value.status_code == 500


# --- 4. Pagination et filtre d'amas ---------------------------------------------


def test_cas_1_tous_les_amas_du_scenario(monkeypatch, site_existant):
    db = FakeRead(
        scenario=_scenario(),
        nb_amas=2,
        pages=["uuidA", "uuidB"],
        trafics=[
            _ligne("uuidA", "LUNDI", "OO", 12, 2, 1),
            _ligne("uuidB", "LUNDI", "OO", 5, 0, 0),
        ],
    )
    reponse = _appeler(db, monkeypatch=monkeypatch)
    assert reponse.site == "123456" and reponse.scenario == 125
    assert len(reponse.amas) == 2
    assert reponse.pagination.nb_amas_total == 2
    assert reponse.pagination.page_suivante is None


def test_cas_6_premiere_page_sur_742_amas(monkeypatch, site_existant):
    """742 Agrébals, NB_AMAS_PAR_PAGE = 200 -> 4 pages, page suivante = 2."""
    db = FakeRead(scenario=_scenario(), nb_amas=742, pages=["uuidA"], trafics=[])
    reponse = _appeler(db, monkeypatch=monkeypatch, page=1, taille_page=200)
    assert reponse.pagination.page == 1
    assert reponse.pagination.taille_page == 200
    assert reponse.pagination.nb_pages == 4
    assert reponse.pagination.page_suivante == 2


def test_cas_7_derniere_page(monkeypatch, site_existant):
    db = FakeRead(scenario=_scenario(), nb_amas=742, pages=["uuidZ"], trafics=[])
    reponse = _appeler(db, monkeypatch=monkeypatch, page=4, taille_page=200)
    assert reponse.pagination.page == 4
    assert reponse.pagination.page_suivante is None


def test_loffset_suit_la_page_demandee(monkeypatch, site_existant):
    db = FakeRead(scenario=_scenario(), nb_amas=742, pages=["uuidZ"], trafics=[])
    _appeler(db, monkeypatch=monkeypatch, page=3, taille_page=200)
    sql, params = next(
        (s, p) for s, p in db.executed if "LIMIT %s OFFSET %s" in s
    )
    assert params == (125, "123456", 200, 400)


def test_une_page_au_dela_de_la_derniere_nest_pas_une_erreur(monkeypatch, site_existant):
    """200 avec une liste vide : l'appelant sait qu'il a dépassé, pas une erreur."""
    db = FakeRead(scenario=_scenario(), nb_amas=10, pages=[], trafics=[])
    reponse = _appeler(db, monkeypatch=monkeypatch, page=99, taille_page=200)
    assert reponse.amas == []
    assert reponse.pagination.page_suivante is None


def test_un_scenario_sans_amas_renvoie_une_page_vide(monkeypatch, site_existant):
    db = FakeRead(scenario=_scenario(), nb_amas=0, pages=[], trafics=[])
    reponse = _appeler(db, monkeypatch=monkeypatch)
    assert reponse.amas == []
    assert reponse.pagination.nb_amas_total == 0
    assert reponse.pagination.nb_pages == 1


def test_rg008_le_tri_precede_le_decoupage():
    """Sans ORDER BY avant LIMIT, un amas pourrait changer de page entre deux appels."""
    sql = helpers.SELECT_AMAS_PAGE_SQL
    assert sql.index("ORDER BY agrebal_uuid") < sql.index("LIMIT")


def test_cas_2_selection_damas(monkeypatch, site_existant):
    db = FakeRead(
        scenario=_scenario(),
        pages=["uuidA", "uuidB"],
        trafics=[
            _ligne("uuidA", "LUNDI", "OO", 12, 2, 1),
            _ligne("uuidB", "LUNDI", "OO", 5, 0, 0),
        ],
    )
    reponse = _appeler(db, monkeypatch=monkeypatch, amas=["uuidA", "uuidB"])
    assert [a.agrebal_uuid for a in reponse.amas] == ["uuidA", "uuidB"]
    assert reponse.amas_non_trouves == []


def test_cas_8_le_filtre_desactive_la_pagination(monkeypatch, site_existant):
    db = FakeRead(
        scenario=_scenario(),
        nb_amas=742,
        pages=["uuidA"],
        trafics=[_ligne("uuidA", "LUNDI", "OO", 12, 2, 1)],
    )
    reponse = _appeler(db, monkeypatch=monkeypatch, amas=["uuidA"], page=3, taille_page=200)
    assert reponse.pagination.page == 1
    assert reponse.pagination.nb_pages == 1
    assert reponse.pagination.page_suivante is None
    # Ni COUNT ni LIMIT : le périmètre est déjà borné par l'appelant.
    assert not any("LIMIT %s OFFSET %s" in s for s, _ in db.executed)
    assert not any("COUNT(DISTINCT" in s for s, _ in db.executed)


def test_c4_les_uuid_inconnus_sont_ignores_mais_traces(monkeypatch, site_existant):
    db = FakeRead(
        scenario=_scenario(),
        pages=["uuidA"],  # seul uuidA existe
        trafics=[_ligne("uuidA", "LUNDI", "OO", 12, 2, 1)],
    )
    reponse = _appeler(db, monkeypatch=monkeypatch, amas=["uuidA", "uuidInconnu"])
    assert [a.agrebal_uuid for a in reponse.amas] == ["uuidA"]
    assert reponse.amas_non_trouves == ["uuidInconnu"]


def test_c4_404_si_aucun_amas_valide(monkeypatch, site_existant):
    db = FakeRead(scenario=_scenario(), pages=[], trafics=[])
    with pytest.raises(HTTPException) as exc:
        _appeler(db, monkeypatch=monkeypatch, amas=["uuidInconnu"])
    assert exc.value.status_code == 404


def test_les_doublons_de_la_demande_sont_dedoublonnes(monkeypatch, site_existant):
    db = FakeRead(
        scenario=_scenario(),
        pages=["uuidA"],
        trafics=[_ligne("uuidA", "LUNDI", "OO", 12, 2, 1)],
    )
    _appeler(db, monkeypatch=monkeypatch, amas=["uuidA", "uuidA", "uuidA"])
    _, params = next((s, p) for s, p in db.executed if "SELECT DISTINCT" in s)
    assert params == (125, "123456", "uuidA")


def test_une_liste_damas_vide_nest_pas_traitee_comme_absente(monkeypatch, site_existant):
    """`amas: []` est une demande explicite de rien, pas un « tous les amas »."""
    db = FakeRead(scenario=_scenario(), nb_amas=742, pages=[], trafics=[])
    with pytest.raises(HTTPException) as exc:
        _appeler(db, monkeypatch=monkeypatch, amas=[])
    assert exc.value.status_code == 404
    # Et surtout : aucune requête ne part avec un `IN ()`, erreur de syntaxe MySQL.
    assert not any("IN ()" in sql for sql, _ in db.executed)


def test_la_liste_damas_est_bornee():
    """Borne la clause IN générée."""
    with pytest.raises(ValidationError):
        TraficAmasRequest(
            code_regate="123456",
            scenario_id=125,
            amas=[f"uuid{i}" for i in range(2000)],
        )


def test_page_zero_refusee():
    with pytest.raises(ValidationError):
        TraficAmasRequest(code_regate="123456", scenario_id=125, page=0)


def test_aucune_ecriture_dans_le_service(monkeypatch, site_existant):
    """RG-API-003 : l'API est purement consultative, aucun recalcul déclenché."""
    db = FakeRead(scenario=_scenario(), nb_amas=1, pages=["uuidA"], trafics=[])
    _appeler(db, monkeypatch=monkeypatch)
    for sql, _ in db.executed:
        assert sql.lstrip().upper().startswith("SELECT")


# --- 5. Non-régression schéma ---------------------------------------------------

_DB_SQL = Path(__file__).resolve().parents[1] / "db" / "db_new.sql"


def _bloc_de(table: str) -> str:
    contenu = _DB_SQL.read_text(encoding="utf-8")
    bloc = re.search(rf"CREATE TABLE `{table}` \((.*?)\n\) ENGINE", contenu, re.S)
    assert bloc, f"Table {table} introuvable dans {_DB_SQL.name}"
    return bloc.group(1)


def _colonnes_de(table: str) -> set[str]:
    return set(re.findall(r"^\s*`(\w+)`", _bloc_de(table), re.M))


@pytest.mark.parametrize(
    "table, attendues",
    [
        (
            "trppu_trafic_agrebal",
            {
                "id_scenario",
                "co_regate",
                "id_agrebal",
                "agrebal_uuid",
                "co_produit",
                "jour_semaine",
                "couleur_pic",
                "volume",
            },
        ),
        (
            "trppu_agrebal_pdi",
            {"agrebal_id", "agrebal_nom", "agrebal_code_regate"},
        ),
    ],
)
def test_les_colonnes_citees_existent_en_base(table, attendues):
    requete = helpers.select_trafics_amas_sql(1)
    citees = {c for c in attendues if c in requete}
    assert citees == attendues, f"la requête ne cite plus : {attendues - citees}"
    assert attendues <= _colonnes_de(table)


def test_les_densites_correspondent_a_lenum_de_la_base():
    """Le pivot code en dur DENSE / FAIBLE1 / FAIBLE2 : ils doivent rester l'ENUM."""
    enum = re.search(r"`couleur_pic`\s+enum\(([^)]*)\)", _bloc_de("trppu_trafic_agrebal"))
    assert enum, "ENUM couleur_pic introuvable"
    valeurs = set(re.findall(r"'([^']+)'", enum.group(1)))
    assert valeurs == {"DENSE", "FAIBLE1", "FAIBLE2"}
    requete = helpers.select_trafics_amas_sql(1)
    for valeur in valeurs:
        assert f"'{valeur}'" in requete


def test_les_jours_correspondent_a_lenum_de_la_base():
    """L'ORDER BY code en dur l'ordre de la semaine ; la réponse en dérive les clés."""
    enum = re.search(r"`jour_semaine`\s+enum\(([^)]*)\)", _bloc_de("trppu_trafic_agrebal"))
    assert enum, "ENUM jour_semaine introuvable"
    valeurs = re.findall(r"'([^']+)'", enum.group(1))
    assert valeurs == ["LUNDI", "MARDI", "MERCREDI", "JEUDI", "VENDREDI", "SAMEDI"]
    for valeur in valeurs:
        assert f"'{valeur}'" in helpers.ORDER_BY_TRAFICS_AMAS
