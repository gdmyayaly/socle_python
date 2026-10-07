"""Scripts SQL de `db/`, sans base : découpage, ordre des instructions, colonnes, périmètre.
Les nombres d'instructions figés gardent la copie alignée sur `yb05/db/`."""

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
    CLES: 12,
}


# ---------------------------------------------------------------------------
# Schéma de référence
# ---------------------------------------------------------------------------

# Extrait du schéma réel (tables écrites par ces scripts), état APRÈS migration et correctif.
# Recopié, non lu depuis yb05/ ou python/ : à resynchroniser si le schéma évolue.
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


def test_dsr699_n_insere_que_les_cles_absentes():
    """CA4 : l'INSERT saute les couples (version, PDI) déjà présents, d'où la reprise par lots."""
    insert = next(s for s in _instructions(CLES) if first_keyword(s) == "INSERT")
    texte = " ".join(insert.split())
    assert "NOT EXISTS (SELECT 1 FROM trppu_cles_repartition_calcule k" in texte
    assert "k.id_version_cle = v.id_version_cle AND k.id_pdi = c.id_pdi" in texte
    assert "@deja" not in CLES.read_text(encoding="utf-8-sig").split("Calcul des clés")[1]


def test_dsr699_borne_le_lot_sur_l_id():
    """Un lot = une tranche ]@id_debut ; @id_fin] de la clé primaire : borne la transaction."""
    insert = " ".join(
        next(s for s in _instructions(CLES) if first_keyword(s) == "INSERT").split()
    )
    assert "WHERE c.id > @id_debut AND c.id <= @id_fin" in insert


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
    """Créer ou détruire des tables relève de DSR-721 / MD01 (ex. `database.sql` copié ici
    effacerait tout au premier `init`)."""
    # Texte brut : le DDL voyage dans des chaînes `SET @sql := 'ALTER TABLE …'`.
    # `TRUNCATE\s+TABLE` car `TRUNCATE()` est aussi une fonction, utilisée par `fix_error.sql`.
    interdits = re.compile(
        r"\b(DROP\s+TABLE|CREATE\s+TABLE|CREATE\s+DATABASE|TRUNCATE\s+TABLE)\b",
        re.IGNORECASE,
    )
    for script in DB_DIR.glob("*.sql"):
        trouve = interdits.search(script.read_text(encoding="utf-8-sig"))
        assert trouve is None, f"{script.name} : {trouve.group(0) if trouve else ''}"


def test_le_repertoire_db_ne_contient_aucun_load_data():
    """Le chargement passe par S3 et des lots commités : le script DSR-697 n'a rien à faire ici."""
    for script in DB_DIR.glob("*.sql"):
        assert "LOAD DATA" not in script.read_text(encoding="utf-8-sig").upper(), script.name


@pytest.mark.parametrize("script", SCRIPTS_METIER, ids=lambda p: p.name)
def test_les_scripts_de_donnees_sont_purement_transactionnels(script: Path):
    """Aucun DDL, donc un ROLLBACK les annule réellement."""
    assert not [s for s in _instructions(script) if is_ddl(s)]


@pytest.mark.parametrize("script", SCRIPTS_SCHEMA, ids=lambda p: p.name)
def test_le_ddl_de_schema_echappe_a_la_detection_du_socle(script: Path):
    """DDL en PREPARE/EXECUTE, invisible à `is_ddl` : `transactional=False` est obligatoire,
    le COMMIT implicite a lieu sans avertissement du socle."""
    assert "ALTER TABLE" in script.read_text(encoding="utf-8-sig")
    assert not [s for s in _instructions(script) if is_ddl(s)]


@pytest.mark.parametrize("script", TOUS_LES_SCRIPTS, ids=lambda p: p.name)
def test_aucun_ancien_nom_de_table(script: Path):
    """`trppu_site_trafic` est l'ancien nom de `trppu_trafic_site`."""
    assert "trppu_site_trafic" not in script.read_text(encoding="utf-8-sig")
