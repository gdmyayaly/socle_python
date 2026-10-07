"""Scripts SQL métier de `db/` (DSR-696 à 699), analysés sans base (découpage, `dry_run`).

Les tickets se trompent de noms par endroits (`id_site`, `trppu_site_trafic`, `COUNT` sans
parenthèses) : les listes d'insertion sont confrontées à un schéma de référence.
"""

import asyncio
import re
from pathlib import Path

import pytest

from app.db import mysql
from app.db.sql_script import first_keyword, is_ddl, split_sql_script

DB_DIR = Path(__file__).resolve().parent.parent / "db"

MIGRATION = DB_DIR / "DSR-696-699_migration.sql"
FIX = DB_DIR / "fix_error.sql"
SUIVI = DB_DIR / "suivi.sql"
CHARGEMENT = DB_DIR / "DSR-697_chargement_cles_repartition.sql"
SITE_TRAFIC = DB_DIR / "DSR-696_site_trafic.sql"
VERSION_CLE = DB_DIR / "DSR-698_version_cle.sql"
CLES_CALCULEES = DB_DIR / "DSR-699_cles_calculees.sql"

# Ordre d'exécution de la chaîne : DSR-697 alimente la table que lisent les trois autres.
SCRIPTS_METIER = (CHARGEMENT, SITE_TRAFIC, VERSION_CLE, CLES_CALCULEES)

# `fix_error.sql` (DDL, une fois par base : ERROR 1264 de DSR-696) et `suivi.sql` (lecture
# seule, seconde session) ne sont pas des scripts métier.
TOUS_LES_SCRIPTS = (MIGRATION, FIX, SUIVI, *SCRIPTS_METIER)


# ---------------------------------------------------------------------------
# Schéma de référence
# ---------------------------------------------------------------------------

# Extrait de python/db/db_new.sql (recopié : yb05 ne dépend pas de python/), à resynchroniser.
# État APRÈS la migration (`trppu_version_cle.date_creation`, `uq_crc_version_pdi` pour le CA4
# de DSR-699) et `fix_error.sql` (totaux de `trppu_trafic_site` en decimal(35,19), ERROR 1264).
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
    return [c.strip() for c in liste.group(1).split(",") if c.strip()]


def _load_data(script: Path) -> str:
    """Instruction `LOAD DATA` du script, commentaires retirés."""
    instruction = next(
        (s for s in _instructions(script) if first_keyword(s) == "LOAD"), None
    )
    assert instruction is not None, f"aucun LOAD DATA dans {script.name}"
    return "\n".join(
        ligne
        for ligne in instruction.splitlines()
        if not ligne.lstrip().startswith("--")
    )


def _colonnes_chargées(script: Path) -> tuple[list[str], list[str]]:
    """Colonnes du `LOAD DATA` : `(depuis le fichier hors variables @…, depuis le SET)`."""
    sql = _load_data(script)

    liste = re.search(r"IGNORE\s+\d+\s+ROWS\s*\(([^)]*)\)", sql)
    assert liste is not None, "liste de colonnes du fichier introuvable"
    fichier = [
        c.strip()
        for c in liste.group(1).split(",")
        if c.strip() and not c.strip().startswith("@")
    ]

    bloc_set = sql[sql.index("SET", liste.end()) :]
    affectees = re.findall(r"([a-z_0-9]+)\s*=", bloc_set)

    return fichier, affectees


def _instructions(script: Path) -> list[str]:
    return split_sql_script(script.read_text(encoding="utf-8"))


def _sql_sans_commentaires(script: Path) -> str:
    """Script sans ses lignes de commentaire (qui citent les noms erronés des tickets)."""
    return "\n".join(
        ligne
        for ligne in script.read_text(encoding="utf-8").splitlines()
        if not ligne.lstrip().startswith("--")
    )


# ---------------------------------------------------------------------------
# Doublure : interdit toute connexion
# ---------------------------------------------------------------------------


def _patch_connect_interdit(monkeypatch):
    """Fait échouer tout appel à aiomysql.connect."""

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
    [(MIGRATION, 17), (FIX, 9), (SUIVI, 8), (CHARGEMENT, 11), (SITE_TRAFIC, 9),
     (VERSION_CLE, 10), (CLES_CALCULEES, 11)],
    ids=lambda v: v.name if isinstance(v, Path) else str(v),
)
def test_nombre_d_instructions(script, attendu):
    """Verrouille le découpage : une instruction perdue passerait sinon inaperçue."""
    assert len(_instructions(script)) == attendu


def test_dsr697_purge_avant_de_charger():
    """RG6 : purge du référentiel avant chargement."""
    verbes = [first_keyword(s) for s in _instructions(CHARGEMENT)]

    assert verbes[:2] == ["SET", "SET"], "les paramètres doivent précéder tout accès"
    assert verbes.index("DELETE") < verbes.index("LOAD")


def test_dsr697_ne_charge_que_des_colonnes_existantes():
    """Tout champ du fichier non capté dans une variable `@…` vise une colonne existante."""
    fichier, affectees = _colonnes_chargées(CHARGEMENT)
    schema = _colonnes_du_schema("trppu_cles_repartition")

    inconnues = (set(fichier) | set(affectees)) - schema
    assert not inconnues, f"colonnes inconnues : {inconnues}"
    # AUTO_INCREMENT : jamais alimentée, ni par le fichier ni par le SET.
    assert "id" not in fichier and "id" not in affectees
    # Les 15 champs du fichier : 11 colonnes directes + 4 captées en variables (RG3).
    assert len(fichier) == 11


def test_dsr697_convertit_les_quatre_champs_vides_en_null():
    """RG3 : sans `NULLIF`, un champ vide deviendrait `''` ou 0 au lieu de NULL."""
    sql = _load_data(CHARGEMENT)

    for colonne in ("co_regate_etablissement", "lb_etablissement", "nb_pre", "potentielip"):
        assert re.search(rf"{colonne}\s*=\s*NULLIF\(@\w+, ''\)", sql), (
            f"conversion en NULL absente : {colonne}"
        )


def test_dsr697_pose_les_colonnes_d_historisation():
    """RG1 + RG2 : le référentiel vient du paramètre, la ligne est chargée active."""
    sql = _load_data(CHARGEMENT)

    assert "id_referentiel          = @id_referentiel" in sql
    assert "date_debut_validite     = CURRENT_DATE()" in sql
    assert "date_fin_validite       = NULL" in sql


def test_dsr697_laisse_echouer_les_doublons_de_pdi():
    """Ni `IGNORE` ni `REPLACE` : un doublon (id_pdi, id_referentiel) échoue en 1062 (RG4)."""
    sql = _load_data(CHARGEMENT)

    assert re.search(r"LOAD DATA (LOCAL )?INFILE '[^']+'\s+INTO TABLE", sql), (
        "un modificateur IGNORE / REPLACE s'est glissé avant INTO TABLE"
    )


def test_dsr697_durcit_le_mode_sql():
    """Hors mode strict, `LOAD DATA` met 0 / tronque sur simple avertissement."""
    sql_seul = _sql_sans_commentaires(CHARGEMENT)

    assert "STRICT_ALL_TABLES" in sql_seul


@pytest.mark.parametrize("script", TOUS_LES_SCRIPTS, ids=lambda p: p.name)
def test_aucun_script_ne_reprend_le_count_sans_parentheses(script):
    """`SELECT COUNT FROM …` du ticket DSR-697 : `ERROR 1054`."""
    assert not re.search(
        r"\bCOUNT\s+FROM\b", _sql_sans_commentaires(script), re.IGNORECASE
    )


def test_dsr696_enchaine_bien_delete_puis_insert():
    """CA4 : purger avant de recalculer (un site sans PDI actif ne garde pas de total)."""
    verbes = [first_keyword(s) for s in _instructions(SITE_TRAFIC)]

    assert verbes.index("DELETE") < verbes.index("INSERT")
    assert verbes[:2] == ["SET", "SET"], "les paramètres doivent précéder tout accès"


def test_dsr698_calcule_deja_avant_toute_ecriture():
    """Rejouabilité : `@deja` est figé avant l'UPDATE (après, il vaudrait toujours 0)."""
    verbes = [first_keyword(s) for s in _instructions(VERSION_CLE)]
    instructions = _instructions(VERSION_CLE)

    index_deja = next(i for i, s in enumerate(instructions) if "@deja :=" in s)
    assert index_deja < verbes.index("UPDATE") < verbes.index("INSERT")


# ---------------------------------------------------------------------------
# Colonnes — le piège du ticket
# ---------------------------------------------------------------------------


def test_dsr696_n_insere_que_des_colonnes_existantes():
    """`id_site` du ticket n'existe pas : la colonne est `co_regate_site`."""
    colonnes = _colonnes_insérées(SITE_TRAFIC, "trppu_trafic_site")
    schema = _colonnes_du_schema("trppu_trafic_site")

    assert set(colonnes) <= schema, f"colonnes inconnues : {set(colonnes) - schema}"
    assert "co_regate_site" in colonnes
    # AUTO_INCREMENT et DEFAULT : jamais alimentées explicitement.
    assert "id_site_trafic" not in colonnes
    assert "date_creation" not in colonnes


def test_dsr698_n_insere_que_des_colonnes_existantes():
    """Les colonnes à DEFAULT ou AUTO_INCREMENT ne sont pas citées par l'INSERT."""
    colonnes = _colonnes_insérées(VERSION_CLE, "trppu_version_cle")
    schema = _colonnes_du_schema("trppu_version_cle")

    assert set(colonnes) <= schema, f"colonnes inconnues : {set(colonnes) - schema}"
    assert "id_version_cle" not in colonnes
    assert "date_creation" not in colonnes
    assert "date_debut_validite" not in colonnes


@pytest.mark.parametrize("script", TOUS_LES_SCRIPTS, ids=lambda p: p.name)
def test_aucun_script_ne_reprend_la_colonne_fantome_du_ticket(script):
    """Non-régression sur la colonne `id_site` de DSR-696."""
    assert not re.search(r"\bid_site\b", _sql_sans_commentaires(script))


@pytest.mark.parametrize("script", TOUS_LES_SCRIPTS, ids=lambda p: p.name)
def test_aucun_script_ne_vise_l_ancien_nom_de_table(script):
    """Le nom correct est `trppu_trafic_site` (l'ancien ne survit que dans le nom de fichier)."""
    sql_seul = _sql_sans_commentaires(script)

    assert "trppu_site_trafic" not in sql_seul


def test_dsr698_restitue_les_trois_dates():
    """Le contrôle final montre création, début et fin de validité de la version."""
    sql_seul = _sql_sans_commentaires(VERSION_CLE)

    for colonne in ("date_creation", "date_debut_validite", "date_fin_validite"):
        assert colonne in sql_seul, f"absente du script : {colonne}"


def test_dsr698_clot_la_version_desactivee():
    """La désactivation pose `date_fin_validite` avec `actif = 'N'`."""
    update = next(
        s for s in _instructions(VERSION_CLE) if first_keyword(s) == "UPDATE"
    )

    assert "actif = 'N'" in update
    assert "date_fin_validite" in update


def test_les_listes_insert_et_select_ont_la_meme_longueur():
    """Un décalage ne se verrait qu'en base."""
    contenu = SITE_TRAFIC.read_text(encoding="utf-8")
    colonnes = _colonnes_insérées(SITE_TRAFIC, "trppu_trafic_site")

    corps_select = re.search(r"SELECT id_referentiel,(.*?)\n\s*FROM", contenu, re.S)
    assert corps_select is not None
    # +1 : `id_referentiel` est consommé par le motif de recherche lui-même.
    expressions = 1 + len(
        [e for e in re.split(r",\n", corps_select.group(1)) if e.strip()]
    )

    assert expressions == len(colonnes)


# ---------------------------------------------------------------------------
# DSR-699 — le calcul des clés
# ---------------------------------------------------------------------------


def test_dsr699_n_insere_que_des_colonnes_existantes():
    colonnes = _colonnes_insérées(CLES_CALCULEES, "trppu_cles_repartition_calcule")
    schema = _colonnes_du_schema("trppu_cles_repartition_calcule")

    assert set(colonnes) <= schema, f"colonnes inconnues : {set(colonnes) - schema}"
    assert {"cle_colis", "cle_oo", "cle_3s", "cle_potentielip"} <= set(colonnes)
    # AUTO_INCREMENT et DEFAULT : jamais alimentées explicitement.
    assert "id_cle_repartition" not in colonnes
    assert "date_creation" not in colonnes


def test_dsr699_listes_insert_et_select_ont_la_meme_longueur():
    contenu = CLES_CALCULEES.read_text(encoding="utf-8")
    colonnes = _colonnes_insérées(CLES_CALCULEES, "trppu_cles_repartition_calcule")

    corps_select = re.search(r"SELECT v\.id_version_cle,(.*?)\n\s*FROM", contenu, re.S)
    assert corps_select is not None
    expressions = 1 + len(
        [e for e in re.split(r",\n", corps_select.group(1)) if e.strip()]
    )

    assert expressions == len(colonnes)


def test_dsr699_calcule_deja_avant_toute_ecriture():
    """CA4 : `@deja` figé avant l'INSERT, une version déjà calculée n'est jamais retouchée."""
    instructions = _instructions(CLES_CALCULEES)
    verbes = [first_keyword(s) for s in instructions]

    index_deja = next(i for i, s in enumerate(instructions) if "@deja :=" in s)
    assert index_deja < verbes.index("INSERT")


def test_dsr699_durcit_le_mode_sql():
    """Sans mode strict, une division par zéro donnerait silencieusement une clé à 0."""
    sql_seul = _sql_sans_commentaires(CLES_CALCULEES)

    assert "ERROR_FOR_DIVISION_BY_ZERO" in sql_seul
    assert "STRICT_ALL_TABLES" in sql_seul


def test_dsr699_ne_masque_aucun_denominateur_nul():
    """Aucun dénominateur masqué : pas de `NULLIF`, un seul `COALESCE` (sur `potentielip`)."""
    sql_seul = _sql_sans_commentaires(CLES_CALCULEES)

    assert "NULLIF" not in sql_seul.upper()
    assert sql_seul.upper().count("COALESCE") == 1


def test_dsr699_cast_la_cle_potentiel_ip():
    """Division entière smallint/bigint : sans CAST, 4 décimales au lieu de 18."""
    sql_seul = _sql_sans_commentaires(CLES_CALCULEES)

    assert re.search(
        r"CAST\(\s*COALESCE\(c\.potentielip, 0\)\s*AS DECIMAL\(24,18\)\s*\)", sql_seul
    ), "le CAST de la clé potentiel IP a disparu"


# ---------------------------------------------------------------------------
# DDL et transaction
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("script", SCRIPTS_METIER, ids=lambda p: p.name)
def test_les_scripts_metier_sont_purement_transactionnels(script):
    """Aucun DDL (ni TRUNCATE) : les scripts métier sont annulables par ROLLBACK."""
    assert not any(is_ddl(s) for s in _instructions(script))


def test_le_ddl_de_la_migration_echappe_a_la_detection():
    """Piège : l'ALTER via PREPARE/EXECUTE échappe à `is_ddl` (jouer en `transactional=False`)."""
    instructions = _instructions(MIGRATION)

    assert not any(is_ddl(s) for s in instructions)
    assert {first_keyword(s) for s in instructions} == {
        "SET", "PREPARE", "EXECUTE", "DEALLOCATE", "SELECT",
    }
    assert "ALTER TABLE" in MIGRATION.read_text(encoding="utf-8")


def test_le_ddl_du_fix_echappe_aussi_a_la_detection():
    """Même emballage PREPARE/EXECUTE que la migration : `transactional=False`."""
    instructions = _instructions(FIX)

    assert not any(is_ddl(s) for s in instructions)
    assert "ALTER TABLE" in FIX.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# fix_error.sql — l'ERROR 1264 sur les totaux de site
# ---------------------------------------------------------------------------


def _alter_du_fix() -> str:
    """`ALTER` du `SET @sql` du correctif, cherché hors commentaires (l'en-tête cite ALTER)."""
    nettoyees = (
        "\n".join(
            ligne for ligne in s.splitlines() if not ligne.lstrip().startswith("--")
        )
        for s in _instructions(FIX)
    )
    return next(s for s in nettoyees if "ALTER TABLE" in s)


def test_fix_elargit_les_trois_totaux_a_la_meme_definition():
    """Les trois totaux, chacun avec `NOT NULL` (omis, `MODIFY` lèverait la contrainte)."""
    alter = _alter_du_fix()

    for colonne in ("trafic_colis_total", "trafic_oo_total", "trafic_3s_total"):
        assert re.search(
            rf"MODIFY COLUMN `{colonne}`\s+decimal\(35,19\) NOT NULL", alter
        ), f"élargissement absent ou incomplet : {colonne}"


def test_fix_accorde_la_definition_cible_au_schema_de_reference():
    """SCHEMA_REFERENCE et le correctif doivent concorder."""
    corps = re.search(
        r"CREATE TABLE `trppu_trafic_site` \((.*?)\n\) ENGINE", SCHEMA_REFERENCE, re.S
    ).group(1)
    alter = _alter_du_fix()

    for colonne in ("trafic_colis_total", "trafic_oo_total", "trafic_3s_total"):
        type_schema = re.search(rf"`{colonne}` (decimal\(\d+,\d+\))", corps).group(1)
        assert type_schema in alter, f"{colonne} : {type_schema} absent du correctif"


def test_fix_ne_touche_pas_au_potentiel_ip():
    """`potentielip_total` (bigint) contient déjà la somme des smallint."""
    assert "potentielip_total" not in _alter_du_fix()


def test_fix_est_garde_par_la_definition_et_non_par_le_nom():
    """Rejouabilité : le garde-fou teste la largeur, pas l'existence de la colonne."""
    garde = next(s for s in _instructions(FIX) if "@sql :=" in s)

    assert "NUMERIC_PRECISION" in garde
    assert ">= 35" in garde


def test_le_suivi_est_strictement_en_lecture():
    """`suivi.sql` tourne en parallèle d'un traitement : `SET` et `SELECT` uniquement."""
    verbes = {first_keyword(s) for s in _instructions(SUIVI)}

    assert verbes <= {"SET", "SELECT"}, f"verbes interdits : {verbes - {'SET', 'SELECT'}}"
    assert not any(is_ddl(s) for s in _instructions(SUIVI))
    # Les `UPDATE performance_schema` restent en commentaire : ils modifient tout le serveur.
    assert not re.search(
        r"\bUPDATE\s+performance_schema", _sql_sans_commentaires(SUIVI), re.IGNORECASE
    )


def test_le_suivi_ne_compte_pas_les_lignes_de_la_table_source():
    """Volumétrie de la source (24 M) lue dans `information_schema.TABLES`, pas par COUNT(*)."""
    sql_seul = _sql_sans_commentaires(SUIVI)

    assert "information_schema.TABLES" in sql_seul
    assert not re.search(
        r"COUNT\(\*\)\s*FROM\s+trppu_cles_repartition\b(?!_calcule)", sql_seul
    )


def test_fix_conserve_l_echelle_de_la_source():
    """19 décimales comme la source, sinon `ecart_*` non nuls au contrôle CA1+CA3 de DSR-696."""
    corps = re.search(
        r"CREATE TABLE `trppu_cles_repartition` \((.*?)\n\) ENGINE", SCHEMA_REFERENCE, re.S
    ).group(1)

    echelle_source = re.search(r"`trafic_oo` decimal\(\d+,(\d+)\)", corps).group(1)

    assert f",{echelle_source})" in _alter_du_fix()


# ---------------------------------------------------------------------------
# Exécution à blanc, sans base
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "script, attendu",
    [(MIGRATION, 17), (FIX, 9), (SUIVI, 8), (CHARGEMENT, 11), (SITE_TRAFIC, 9),
     (VERSION_CLE, 10), (CLES_CALCULEES, 11)],
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
