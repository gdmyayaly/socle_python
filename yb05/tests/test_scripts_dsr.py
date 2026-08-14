"""Tests des scripts SQL métier de `db/` (DSR-696, DSR-698).

Aucune base MySQL n'est nécessaire : les scripts sont analysés par le découpeur du socle, et
leur exécution n'est vérifiée qu'en `dry_run`, mode qui n'ouvre aucune connexion. Le code
async passe par ``asyncio.run`` pour ne pas dépendre de pytest-asyncio.

L'enjeu principal est le contrôle des noms de colonnes : DSR-696 spécifie
``INSERT INTO trppu_site_trafic (id_site, …)`` alors que cette colonne n'existe pas — la
table porte ``co_regate_site``. Recopié tel quel, le script échouerait en base. Le schéma de
référence est donc rappelé ici et confronté aux listes d'insertion.
"""

import asyncio
import re
from pathlib import Path

import pytest

from app.db import mysql
from app.db.sql_script import first_keyword, is_ddl, split_sql_script

DB_DIR = Path(__file__).resolve().parent.parent / "db"

MIGRATION = DB_DIR / "DSR-696-698_migration.sql"
SITE_TRAFIC = DB_DIR / "DSR-696_site_trafic.sql"
VERSION_CLE = DB_DIR / "DSR-698_version_cle.sql"

SCRIPTS_METIER = (SITE_TRAFIC, VERSION_CLE)
TOUS_LES_SCRIPTS = (MIGRATION, *SCRIPTS_METIER)


# ---------------------------------------------------------------------------
# Schéma de référence
# ---------------------------------------------------------------------------

# Extrait de python/db/db_new.sql, limité aux tables écrites par ces scripts. Recopié plutôt
# que lu depuis le projet voisin : yb05 ne doit pas dépendre de l'arborescence de python/.
# À resynchroniser si le schéma évolue.
SCHEMA_REFERENCE = """\
CREATE TABLE `trppu_site_trafic` (
  `id_site_trafic` bigint NOT NULL AUTO_INCREMENT,
  `id_referentiel` int NOT NULL,
  `co_regate_site` varchar(10) NOT NULL,
  `trafic_colis_total` decimal(24,18) NOT NULL,
  `trafic_oo_total` decimal(24,18) NOT NULL,
  `trafic_3s_total` decimal(24,18) NOT NULL,
  `potentielip_total` bigint NOT NULL,
  `date_debut_validite` date NOT NULL,
  `date_fin_validite` date DEFAULT NULL,
  `date_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_site_trafic`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `trppu_version_cle` (
  `id_version_cle` int NOT NULL AUTO_INCREMENT,
  `id_referentiel` int NOT NULL,
  `libelle` varchar(100) DEFAULT NULL,
  `co_regate` char(6) NOT NULL,
  `actif` char(1) NOT NULL DEFAULT 'O',
  `date_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `commentaire` varchar(500) DEFAULT NULL,
  PRIMARY KEY (`id_version_cle`),
  KEY `idx_ref` (`id_referentiel`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def _colonnes_du_schema(table: str) -> set[str]:
    """Colonnes déclarées pour `table` dans SCHEMA_REFERENCE."""
    corps = re.search(
        rf"CREATE TABLE `{table}` \((.*?)\n\) ENGINE", SCHEMA_REFERENCE, re.S
    )
    assert corps is not None, f"table absente du schéma de référence : {table}"
    return set(re.findall(r"^\s{2}`([a-z_0-9]+)` [a-z]", corps.group(1), re.M))


def _colonnes_insérées(script: Path, table: str) -> list[str]:
    """Liste de colonnes de l'`INSERT INTO <table> (...)` du script."""
    liste = re.search(
        rf"INSERT INTO\s+{table}\s*\(([^)]*)\)", script.read_text(encoding="utf-8")
    )
    assert liste is not None, f"aucun INSERT INTO {table} dans {script.name}"
    # Les commentaires en fin de ligne ne peuvent pas apparaître dans une liste de colonnes,
    # un simple découpage sur les virgules suffit.
    return [c.strip() for c in liste.group(1).split(",") if c.strip()]


def _instructions(script: Path) -> list[str]:
    return split_sql_script(script.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Doublure : interdit toute connexion
# ---------------------------------------------------------------------------


def _patch_connect_interdit(monkeypatch):
    """Fait échouer tout appel à aiomysql.connect (vérifie l'absence de connexion)."""

    async def fake_connect(**kwargs):
        raise AssertionError("aiomysql.connect ne devait pas être appelé")

    monkeypatch.setattr(mysql.aiomysql, "connect", fake_connect)


def _db() -> mysql.Database:
    return mysql.Database(host="h", user="u", password="p", database="ma_base")


# ---------------------------------------------------------------------------
# Présence et découpage
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("script", TOUS_LES_SCRIPTS, ids=lambda p: p.name)
def test_le_script_existe_et_est_decoupable(script):
    assert script.is_file(), f"script manquant : {script}"
    assert _instructions(script), "aucune instruction exécutable"


@pytest.mark.parametrize(
    "script, attendu",
    [(MIGRATION, 13), (SITE_TRAFIC, 9), (VERSION_CLE, 10)],
    ids=lambda v: v.name if isinstance(v, Path) else str(v),
)
def test_nombre_d_instructions(script, attendu):
    """Verrouille le découpage : une instruction perdue passerait sinon inaperçue."""
    assert len(_instructions(script)) == attendu


def test_dsr696_enchaine_bien_delete_puis_insert():
    """L'ordre porte le CA4 : purger avant de recalculer, pour que les sites sans PDI
    actif disparaissent au lieu de conserver un total périmé."""
    verbes = [first_keyword(s) for s in _instructions(SITE_TRAFIC)]

    assert verbes.index("DELETE") < verbes.index("INSERT")
    assert verbes[:2] == ["SET", "SET"], "les paramètres doivent précéder tout accès"


def test_dsr698_calcule_deja_avant_toute_ecriture():
    """`@deja` doit être figé AVANT l'UPDATE : c'est ce qui rend le script rejouable.
    Calculé après, il vaudrait toujours 0 (l'UPDATE ayant désactivé la version) et chaque
    exécution créerait une version de plus."""
    verbes = [first_keyword(s) for s in _instructions(VERSION_CLE)]
    instructions = _instructions(VERSION_CLE)

    index_deja = next(i for i, s in enumerate(instructions) if "@deja :=" in s)
    assert index_deja < verbes.index("UPDATE") < verbes.index("INSERT")


# ---------------------------------------------------------------------------
# Colonnes — le piège du ticket
# ---------------------------------------------------------------------------


def test_dsr696_n_insere_que_des_colonnes_existantes():
    """`id_site`, annoncé par le ticket, n'existe pas : la colonne est `co_regate_site`."""
    colonnes = _colonnes_insérées(SITE_TRAFIC, "trppu_site_trafic")
    schema = _colonnes_du_schema("trppu_site_trafic")

    assert set(colonnes) <= schema, f"colonnes inconnues : {set(colonnes) - schema}"
    assert "co_regate_site" in colonnes
    # AUTO_INCREMENT et DEFAULT : jamais alimentées explicitement.
    assert "id_site_trafic" not in colonnes
    assert "date_creation" not in colonnes


def test_dsr698_n_insere_que_des_colonnes_existantes():
    colonnes = _colonnes_insérées(VERSION_CLE, "trppu_version_cle")
    schema = _colonnes_du_schema("trppu_version_cle")

    assert set(colonnes) <= schema, f"colonnes inconnues : {set(colonnes) - schema}"
    assert "id_version_cle" not in colonnes
    assert "date_creation" not in colonnes


@pytest.mark.parametrize("script", TOUS_LES_SCRIPTS, ids=lambda p: p.name)
def test_aucun_script_ne_reprend_la_colonne_fantome_du_ticket(script):
    """Garde-fou de non-régression sur l'erreur de spécification de DSR-696."""
    contenu = script.read_text(encoding="utf-8")
    sql_seul = "\n".join(
        ligne for ligne in contenu.splitlines() if not ligne.lstrip().startswith("--")
    )

    assert not re.search(r"\bid_site\b", sql_seul)


def test_les_listes_insert_et_select_ont_la_meme_longueur():
    """Un décalage entre les deux listes ne se voit qu'à l'exécution, en base."""
    contenu = SITE_TRAFIC.read_text(encoding="utf-8")
    colonnes = _colonnes_insérées(SITE_TRAFIC, "trppu_site_trafic")

    corps_select = re.search(r"SELECT id_referentiel,(.*?)\n\s*FROM", contenu, re.S)
    assert corps_select is not None
    # +1 : `id_referentiel` est consommé par le motif de recherche lui-même.
    expressions = 1 + len(
        [e for e in re.split(r",\n", corps_select.group(1)) if e.strip()]
    )

    assert expressions == len(colonnes)


# ---------------------------------------------------------------------------
# DDL et transaction
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("script", SCRIPTS_METIER, ids=lambda p: p.name)
def test_les_scripts_metier_sont_purement_transactionnels(script):
    """Aucun DDL : ces deux scripts sont intégralement annulables par un ROLLBACK."""
    assert not any(is_ddl(s) for s in _instructions(script))


def test_le_ddl_de_la_migration_echappe_a_la_detection():
    """Comportement contre-intuitif, verrouillé ici parce qu'il est piégeux.

    Les ALTER de la migration voyagent dans une chaîne exécutée par PREPARE/EXECUTE, ce qui
    les rend invisibles à `is_ddl` : l'avertissement « DDL en mode transactionnel » du socle
    ne se déclenchera pas, alors que le commit implicite a bien lieu. D'où la consigne de
    jouer ce fichier avec `transactional=False`.
    """
    instructions = _instructions(MIGRATION)

    assert not any(is_ddl(s) for s in instructions)
    assert {first_keyword(s) for s in instructions} == {
        "SET", "PREPARE", "EXECUTE", "DEALLOCATE", "SELECT",
    }
    assert "ALTER TABLE" in MIGRATION.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Exécution à blanc, sans base
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "script, attendu",
    [(MIGRATION, 13), (SITE_TRAFIC, 9), (VERSION_CLE, 10)],
    ids=lambda v: v.name if isinstance(v, Path) else str(v),
)
def test_dry_run_liste_les_instructions_sans_connexion(monkeypatch, script, attendu):
    _patch_connect_interdit(monkeypatch)

    resultat = asyncio.run(_db().execute_sql_file(script, dry_run=True))

    assert resultat.total_count == attendu
    assert resultat.ok
    assert resultat.executed_count == 0
    assert all(s.skipped for s in resultat.statements)
    assert resultat.sources == [str(script)]


def test_les_apercus_ne_divulguent_pas_les_parametres(monkeypatch):
    """Les aperçus partent dans les logs : ils sont tronqués et sans commentaires."""
    _patch_connect_interdit(monkeypatch)

    resultat = asyncio.run(_db().execute_sql_file(VERSION_CLE, dry_run=True))

    assert all(len(s.preview) <= 120 for s in resultat.statements)
    assert not any(s.preview.startswith("--") for s in resultat.statements)
