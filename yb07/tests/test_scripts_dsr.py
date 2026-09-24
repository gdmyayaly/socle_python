"""Les scripts SQL de `db/`, vérifiés sans base ni réseau.

Trois familles d'assertions :

* le **découpage** — chaque script rend le nombre d'instructions attendu, et le `dry_run` du
  socle les liste sans ouvrir de connexion ;
* l'**ordre des instructions** — c'est lui qui porte la rejouabilité : `@deja` calculé avant
  toute écriture, purge avant rechargement. Une réécriture qui l'inverserait casserait une
  garantie que rien d'autre ne vérifie ;
* le **périmètre** — aucun DDL de schéma, aucun `LOAD DATA`. Ces deux-là ne sont pas des
  détails de forme : ils délimitent ce que ce module a le droit de faire, et la note dans un
  README ne l'empêcherait pas de dériver.

Les scripts sont dupliqués depuis `yb05/db/`, comme le socle l'est entre modules. Les nombres
d'instructions figés ci-dessous sont le seul garde-fou contre une divergence silencieuse entre
les deux copies : les faire évoluer ensemble est délibéré.
"""

from __future__ import annotations

import asyncio
import re
from pathlib import Path

import pytest

from app.db.mysql import Database
from app.db.sql_script import first_keyword, is_ddl, split_sql_script

DB_DIR = Path(__file__).resolve().parent.parent / "db"

MIGRATION = DB_DIR / "DSR-696-699_migration.sql"
CORRECTIF = DB_DIR / "fix_error.sql"
AGREGATS = DB_DIR / "DSR-696_site_trafic.sql"
VERSIONS = DB_DIR / "DSR-698_version_cle.sql"
CLES = DB_DIR / "DSR-699_cles_calculees.sql"

#: Scripts de données — aucun DDL, donc entièrement annulables.
SCRIPTS_METIER = (AGREGATS, VERSIONS, CLES)
#: Scripts de schéma — leur DDL voyage dans PREPARE/EXECUTE, donc invisible à `is_ddl`.
SCRIPTS_SCHEMA = (MIGRATION, CORRECTIF)
TOUS_LES_SCRIPTS = (*SCRIPTS_SCHEMA, *SCRIPTS_METIER)

#: Nombre d'instructions attendu, script par script.
INSTRUCTIONS_ATTENDUES = {
    MIGRATION: 17,
    CORRECTIF: 9,
    AGREGATS: 9,
    VERSIONS: 10,
    CLES: 11,
}


# ---------------------------------------------------------------------------
# Schéma de référence
# ---------------------------------------------------------------------------

# Extrait du schéma réel, limité aux tables écrites par ces scripts. Recopié plutôt que lu
# depuis le projet voisin : yb07 ne doit pas dépendre de l'arborescence de yb05 ni de python/.
# À resynchroniser si le schéma évolue.
#
# État APRÈS la migration et le correctif : c'est celui que supposent les scripts de données.
SCHEMA_REFERENCE = """\
CREATE TABLE `trppu_cles_repartition` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `id_pdi` bigint NOT NULL,
  `pdi_rattache` bigint NOT NULL,
  `trafic_colis` decimal(25,19) NOT NULL,
  `trafic_oo` decimal(25,19) NOT NULL,
  `trafic_3s` decimal(25,19) NOT NULL,
  `nature` char(3) NOT NULL,
  `co_regate_site` char(6) NOT NULL,
  `type_site` varchar(10) NOT NULL,
  `lb_regate` varchar(100) NOT NULL,
  `co_regate_etablissement` char(6) DEFAULT NULL,
  `lb_etablissement` varchar(100) DEFAULT NULL,
  `co_regate_dex` char(6) NOT NULL,
  `lb_dex` varchar(100) NOT NULL,
  `nb_pre` smallint DEFAULT NULL,
  `potentielip` smallint DEFAULT NULL,
  `id_referentiel` int NOT NULL,
  `date_debut_validite` date NOT NULL,
  `date_fin_validite` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_pdi_ref` (`id_pdi`,`id_referentiel`),
  KEY `idx_cr_ref_actif` (`id_referentiel`,`date_fin_validite`,`co_regate_site`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `trppu_trafic_site` (
  `id_site_trafic` bigint NOT NULL AUTO_INCREMENT,
  `id_referentiel` int NOT NULL,
  `co_regate_site` varchar(10) NOT NULL,
  `trafic_colis_total` decimal(35,19) NOT NULL,
  `trafic_oo_total` decimal(35,19) NOT NULL,
  `trafic_3s_total` decimal(35,19) NOT NULL,
  `potentielip_total` bigint NOT NULL,
  `date_debut_validite` date NOT NULL,
  `date_fin_validite` date DEFAULT NULL,
  `date_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_site_trafic`),
  UNIQUE KEY `uq_site_trafic` (`id_referentiel`,`co_regate_site`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `trppu_version_cle` (
  `id_version_cle` int NOT NULL AUTO_INCREMENT,
  `id_referentiel` int NOT NULL,
  `libelle` varchar(100) DEFAULT NULL,
  `co_regate` char(6) NOT NULL,
  `actif` char(1) NOT NULL DEFAULT 'O',
  `date_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `date_debut_validite` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `date_fin_validite` datetime DEFAULT NULL,
  `commentaire` varchar(500) DEFAULT NULL,
  PRIMARY KEY (`id_version_cle`),
  KEY `idx_ref` (`id_referentiel`),
  KEY `idx_regate_actif` (`co_regate`,`actif`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `trppu_cles_repartition_calcule` (
  `id_cle_repartition` bigint NOT NULL AUTO_INCREMENT,
  `id_version_cle` int NOT NULL,
  `id_referentiel` int NOT NULL,
  `id_pdi` bigint NOT NULL,
  `co_regate_site` char(6) NOT NULL,
  `cle_colis` decimal(24,18) NOT NULL,
  `cle_oo` decimal(24,18) NOT NULL,
  `cle_3s` decimal(24,18) NOT NULL,
  `cle_potentielip` decimal(24,18) NOT NULL,
  `date_creation` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_cle_repartition`),
  UNIQUE KEY `uq_crc_version_pdi` (`id_version_cle`,`id_pdi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def _instructions(script: Path) -> list[str]:
    return split_sql_script(script.read_text(encoding="utf-8-sig"))


def _verbes(script: Path) -> list[str]:
    return [first_keyword(s) for s in _instructions(script)]


def _index_du_verbe(script: Path, verbe: str) -> int:
    return _verbes(script).index(verbe)


def _colonnes_du_schema(table: str) -> set[str]:
    corps = re.search(rf"CREATE TABLE `{table}` \((.*?)\n\) ENGINE", SCHEMA_REFERENCE, re.S)
    assert corps is not None, f"table absente du schéma de référence : {table}"
    return set(re.findall(r"^\s{2}`([a-z_0-9]+)` [a-z]", corps.group(1), re.M))


def _colonnes_inserees(script: Path, table: str) -> list[str]:
    liste = re.search(
        rf"INSERT INTO\s+{table}\s*\(([^)]*)\)", script.read_text(encoding="utf-8-sig")
    )
    assert liste is not None, f"aucun INSERT INTO {table} dans {script.name}"
    return [c.strip() for c in liste.group(1).split(",") if c.strip()]


# ---------------------------------------------------------------------------
# Présence et découpage
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("script", TOUS_LES_SCRIPTS, ids=lambda p: p.name)
def test_le_script_existe(script: Path):
    assert script.is_file(), f"script absent : {script}"


@pytest.mark.parametrize("script", TOUS_LES_SCRIPTS, ids=lambda p: p.name)
def test_nombre_d_instructions(script: Path):
    assert len(_instructions(script)) == INSTRUCTIONS_ATTENDUES[script]


@pytest.mark.parametrize("script", TOUS_LES_SCRIPTS, ids=lambda p: p.name)
def test_dry_run_liste_les_instructions_sans_connexion(script: Path, monkeypatch):
    """Le `dry_run` du socle ne doit toucher ni la base, ni le réseau."""

    async def _interdit(*args, **kwargs):  # pragma: no cover - ne doit jamais être appelé
        raise AssertionError("aucune connexion ne doit être ouverte en dry_run")

    monkeypatch.setattr("aiomysql.connect", _interdit)

    db = Database(host="h", user="u", password="p", database="base")
    resultat = asyncio.run(db.execute_sql_file(script, dry_run=True))

    assert resultat.ok
    assert resultat.total_count == INSTRUCTIONS_ATTENDUES[script]
    assert resultat.executed_count == 0
    assert all(instruction.skipped for instruction in resultat.statements)


# ---------------------------------------------------------------------------
# Ordre des instructions — la rejouabilité en dépend
# ---------------------------------------------------------------------------


def test_dsr696_purge_avant_de_recalculer():
    """`DELETE` puis `INSERT` : c'est cette séquence qui fait disparaître un site sans PDI."""
    verbes = _verbes(AGREGATS)
    assert verbes[:2] == ["SET", "SET"]
    assert verbes.index("DELETE") < verbes.index("INSERT")


def test_dsr698_calcule_deja_avant_toute_ecriture():
    """Sans cette mémorisation, relancer le script créerait une seconde version active."""
    instructions = _instructions(VERSIONS)
    rang_deja = next(i for i, s in enumerate(instructions) if "@deja :=" in s)
    verbes = [first_keyword(s) for s in instructions]
    assert rang_deja < verbes.index("UPDATE") < verbes.index("INSERT")


def test_dsr699_calcule_deja_avant_toute_ecriture():
    """CA4 : une version déjà calculée n'est jamais retouchée."""
    instructions = _instructions(CLES)
    rang_deja = next(i for i, s in enumerate(instructions) if "@deja :=" in s)
    verbes = [first_keyword(s) for s in instructions]
    assert rang_deja < verbes.index("INSERT")


def test_dsr699_durcit_le_mode_sql_avant_de_calculer():
    """Sans mode strict, une division par zéro passerait en avertissement et une clé fausse."""
    texte = CLES.read_text(encoding="utf-8-sig")
    assert "ERROR_FOR_DIVISION_BY_ZERO" in texte
    assert texte.index("SET SESSION sql_mode") < texte.index("INSERT INTO")


# ---------------------------------------------------------------------------
# Colonnes écrites, confrontées au schéma
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "script,table,absentes",
    [
        (AGREGATS, "trppu_trafic_site", {"id_site_trafic", "date_creation"}),
        (
            VERSIONS,
            "trppu_version_cle",
            {"id_version_cle", "date_creation", "date_debut_validite"},
        ),
        (
            CLES,
            "trppu_cles_repartition_calcule",
            {"id_cle_repartition", "date_creation"},
        ),
    ],
    ids=["dsr696", "dsr698", "dsr699"],
)
def test_les_colonnes_inserees_existent_et_laissent_la_base_faire(script, table, absentes):
    """Auto-incréments et horodatages ne sont jamais listés : la base les alimente seule."""
    colonnes = _colonnes_inserees(script, table)
    connues = _colonnes_du_schema(table)

    assert set(colonnes) <= connues, f"colonnes inconnues : {set(colonnes) - connues}"
    assert not (set(colonnes) & absentes), f"colonnes à laisser à la base : {absentes}"


def test_dsr696_alimente_bien_la_colonne_du_site():
    """`id_site_trafic` est la clé primaire, pas un identifiant de site — piège du ticket."""
    assert "co_regate_site" in _colonnes_inserees(AGREGATS, "trppu_trafic_site")


def test_dsr699_ecrit_les_quatre_cles():
    colonnes = _colonnes_inserees(CLES, "trppu_cles_repartition_calcule")
    assert {"cle_colis", "cle_oo", "cle_3s", "cle_potentielip"} <= set(colonnes)


def test_dsr699_preserve_la_precision_de_la_cle_potentiel_ip():
    """Sans le CAST, `smallint / bigint` ne rendrait que quatre décimales."""
    assert "CAST(COALESCE(c.potentielip, 0) AS DECIMAL(24,18))" in CLES.read_text(
        encoding="utf-8-sig"
    )


# ---------------------------------------------------------------------------
# Périmètre du module
# ---------------------------------------------------------------------------


def test_le_repertoire_db_ne_contient_aucun_ddl_de_schema():
    """Créer ou détruire des tables relève de DSR-721 / MD01, pas de ce module.

    Le garde-fou vit ici, et pas seulement dans une note du README : c'est la seule chose qui
    empêche `database.sql` — 22 `DROP TABLE` — d'arriver un jour dans ce répertoire par
    copie machinale, et d'effacer 24 M de lignes au premier `init`.
    """
    # Sur le texte brut et non sur les instructions découpées : le DDL de la migration et du
    # correctif voyage dans des chaînes `SET @sql := 'ALTER TABLE …'`, qu'un contrôle par
    # mot-clé d'instruction ne verrait pas. `TRUNCATE\s+TABLE` et non `TRUNCATE` : c'est aussi
    # une fonction, dont `fix_error.sql` se sert pour compter des chiffres entiers.
    interdits = re.compile(
        r"\b(DROP\s+TABLE|CREATE\s+TABLE|CREATE\s+DATABASE|TRUNCATE\s+TABLE)\b",
        re.IGNORECASE,
    )
    for script in DB_DIR.glob("*.sql"):
        trouve = interdits.search(script.read_text(encoding="utf-8-sig"))
        assert trouve is None, f"{script.name} : {trouve.group(0) if trouve else ''}"


def test_le_repertoire_db_ne_contient_aucun_load_data():
    """Le chargement passe par S3 et des lots commités, pas par un LOAD DATA côté serveur.

    `LOAD DATA INFILE` est lu par le serveur MySQL, son chemin est un littéral non
    paramétrable, et le socle n'active pas la variante `LOCAL` : le script DSR-697 n'a rien à
    faire ici. Ce test le prouve, plutôt que de compter sur la mémoire de qui copiera.
    """
    for script in DB_DIR.glob("*.sql"):
        assert "LOAD DATA" not in script.read_text(encoding="utf-8-sig").upper(), script.name


@pytest.mark.parametrize("script", SCRIPTS_METIER, ids=lambda p: p.name)
def test_les_scripts_de_donnees_sont_purement_transactionnels(script: Path):
    """Aucun DDL, donc un ROLLBACK les annule réellement."""
    assert not [s for s in _instructions(script) if is_ddl(s)]


@pytest.mark.parametrize("script", SCRIPTS_SCHEMA, ids=lambda p: p.name)
def test_le_ddl_de_schema_echappe_a_la_detection_du_socle(script: Path):
    """Leur DDL voyage dans PREPARE/EXECUTE : `is_ddl` ne le voit pas.

    C'est ce qui rend ces scripts rejouables (MySQL ne connaît pas `ADD INDEX IF NOT EXISTS`),
    et c'est aussi ce qui rend `transactional=False` obligatoire : l'avertissement du socle ne
    se déclenchera pas, alors que le COMMIT implicite, lui, a bien lieu.
    """
    assert "ALTER TABLE" in script.read_text(encoding="utf-8-sig")
    assert not [s for s in _instructions(script) if is_ddl(s)]


@pytest.mark.parametrize("script", TOUS_LES_SCRIPTS, ids=lambda p: p.name)
def test_aucun_ancien_nom_de_table(script: Path):
    """`trppu_site_trafic` a été renommé `trppu_trafic_site` le 17/08/2026."""
    assert "trppu_site_trafic" not in script.read_text(encoding="utf-8-sig")
