"""Tests des scripts SQL métier de `db/` (DSR-696, DSR-698, DSR-699).

Aucune base MySQL n'est nécessaire : les scripts sont analysés par le découpeur du socle, et
leur exécution n'est vérifiée qu'en `dry_run`, mode qui n'ouvre aucune connexion. Le code
async passe par ``asyncio.run`` pour ne pas dépendre de pytest-asyncio.

L'enjeu principal est le contrôle des noms, que les tickets décrivent mal :

* DSR-696 écrit ``INSERT INTO trppu_site_trafic (id_site, …)``. La table s'appelle
  ``trppu_trafic_site`` et ne porte pas de colonne ``id_site`` — c'est ``co_regate_site``.
  L'amendement du ticket a remplacé ``id_site`` par ``id_site_trafic``, qui est la PK
  AUTO_INCREMENT : le contresens a changé de forme, pas de nature.
* DSR-698 attend une colonne ``date_creation`` sur ``trppu_version_cle``, que le schéma
  ré-extrait a fait disparaître au profit de ``date_debut_validite`` /
  ``date_fin_validite``. Ici c'est le ticket qui a raison : la migration rétablit la
  colonne, les trois dates coexistent et disent trois choses différentes.

* DSR-699 est le mieux écrit des trois — sa liste de colonnes correspond exactement à la
  table. Ses pièges sont ailleurs, dans le calcul : une division par zéro que le serveur
  peut accepter silencieusement, et une division entière qui coûterait quatorze décimales
  sur la clé potentiel IP.

Recopiés tels quels, ces scripts échoueraient en base. Le schéma de référence est donc
rappelé ici et confronté aux listes d'insertion.
"""

import asyncio
import re
from pathlib import Path

import pytest

from app.db import mysql
from app.db.sql_script import first_keyword, is_ddl, split_sql_script

DB_DIR = Path(__file__).resolve().parent.parent / "db"

MIGRATION = DB_DIR / "DSR-696-699_migration.sql"
SITE_TRAFIC = DB_DIR / "DSR-696_site_trafic.sql"
VERSION_CLE = DB_DIR / "DSR-698_version_cle.sql"
CLES_CALCULEES = DB_DIR / "DSR-699_cles_calculees.sql"

# Dans l'ordre d'exécution de la chaîne — cf. db/README.md.
SCRIPTS_METIER = (SITE_TRAFIC, VERSION_CLE, CLES_CALCULEES)
TOUS_LES_SCRIPTS = (MIGRATION, *SCRIPTS_METIER)


# ---------------------------------------------------------------------------
# Schéma de référence
# ---------------------------------------------------------------------------

# Extrait de python/db/db_new.sql, limité aux tables écrites par ces scripts. Recopié plutôt
# que lu depuis le projet voisin : yb05 ne doit pas dépendre de l'arborescence de python/.
# À resynchroniser si le schéma évolue.
#
# État APRÈS `DSR-696-699_migration.sql` : c'est lui que les trois scripts de données
# supposent. Deux écarts avec le dump du 17/08/2026, tous deux apportés par la migration —
# `trppu_version_cle.date_creation`, que DSR-698 spécifie, et `uq_crc_version_pdi`, qui rend
# le CA4 de DSR-699 vrai en base.
SCHEMA_REFERENCE = """\
CREATE TABLE `trppu_trafic_site` (
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
    # Les commentaires en fin de ligne ne peuvent pas apparaître dans une liste de colonnes,
    # un simple découpage sur les virgules suffit.
    return [c.strip() for c in liste.group(1).split(",") if c.strip()]


def _instructions(script: Path) -> list[str]:
    return split_sql_script(script.read_text(encoding="utf-8"))


def _sql_sans_commentaires(script: Path) -> str:
    """Contenu du script privé de ses lignes de commentaire.

    Les commentaires citent les tickets — donc leurs erreurs, anciens noms compris : seul le
    SQL exécutable doit être confronté au schéma.
    """
    return "\n".join(
        ligne
        for ligne in script.read_text(encoding="utf-8").splitlines()
        if not ligne.lstrip().startswith("--")
    )


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
    [(MIGRATION, 17), (SITE_TRAFIC, 9), (VERSION_CLE, 10), (CLES_CALCULEES, 11)],
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
    colonnes = _colonnes_insérées(SITE_TRAFIC, "trppu_trafic_site")
    schema = _colonnes_du_schema("trppu_trafic_site")

    assert set(colonnes) <= schema, f"colonnes inconnues : {set(colonnes) - schema}"
    assert "co_regate_site" in colonnes
    # AUTO_INCREMENT et DEFAULT : jamais alimentées explicitement.
    assert "id_site_trafic" not in colonnes
    assert "date_creation" not in colonnes


def test_dsr698_n_insere_que_des_colonnes_existantes():
    """Les trois colonnes horodatées portent un DEFAULT : l'INSERT ne doit en citer aucune,
    sous peine de figer une valeur là où la base sait faire."""
    colonnes = _colonnes_insérées(VERSION_CLE, "trppu_version_cle")
    schema = _colonnes_du_schema("trppu_version_cle")

    assert set(colonnes) <= schema, f"colonnes inconnues : {set(colonnes) - schema}"
    assert "id_version_cle" not in colonnes
    assert "date_creation" not in colonnes
    assert "date_debut_validite" not in colonnes


@pytest.mark.parametrize("script", TOUS_LES_SCRIPTS, ids=lambda p: p.name)
def test_aucun_script_ne_reprend_la_colonne_fantome_du_ticket(script):
    """Garde-fou de non-régression sur l'erreur de spécification de DSR-696."""
    assert not re.search(r"\bid_site\b", _sql_sans_commentaires(script))


@pytest.mark.parametrize("script", TOUS_LES_SCRIPTS, ids=lambda p: p.name)
def test_aucun_script_ne_vise_l_ancien_nom_de_table(script):
    """La table a été renommée `trppu_site_trafic` → `trppu_trafic_site`. L'ancien nom ne
    survit que dans le nom de fichier et dans celui de l'index `uq_site_trafic`, conservé
    pour rester détectable là où la migration a déjà été jouée."""
    sql_seul = _sql_sans_commentaires(script)

    assert "trppu_site_trafic" not in sql_seul


def test_dsr698_restitue_les_trois_dates():
    """Le contrôle final doit montrer les trois dates de la version : création (colonne du
    ticket, rétablie par la migration), début et fin de validité (colonnes du schéma
    ré-extrait). En omettre une masquerait l'écart entre le ticket et la base."""
    sql_seul = _sql_sans_commentaires(VERSION_CLE)

    for colonne in ("date_creation", "date_debut_validite", "date_fin_validite"):
        assert colonne in sql_seul, f"absente du script : {colonne}"


def test_dsr698_clot_la_version_desactivee():
    """La désactivation pose `date_fin_validite` en même temps que `actif = 'N'` : sans
    elle, une version inactive garderait une fin de validité vide."""
    update = next(
        s for s in _instructions(VERSION_CLE) if first_keyword(s) == "UPDATE"
    )

    assert "actif = 'N'" in update
    assert "date_fin_validite" in update


def test_les_listes_insert_et_select_ont_la_meme_longueur():
    """Un décalage entre les deux listes ne se voit qu'à l'exécution, en base."""
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
    """Même garde-fou que DSR-698, ici au service du CA4 : une version déjà calculée n'est
    jamais retouchée. Calculé après l'INSERT, `@deja` ne protégerait plus rien."""
    instructions = _instructions(CLES_CALCULEES)
    verbes = [first_keyword(s) for s in instructions]

    index_deja = next(i for i, s in enumerate(instructions) if "@deja :=" in s)
    assert index_deja < verbes.index("INSERT")


def test_dsr699_durcit_le_mode_sql():
    """Sans ce durcissement, un serveur non strict accepterait une division par zéro en la
    ramenant à NULL puis à 0 : le script chargerait des clés fausses au lieu d'échouer."""
    sql_seul = _sql_sans_commentaires(CLES_CALCULEES)

    assert "ERROR_FOR_DIVISION_BY_ZERO" in sql_seul
    assert "STRICT_ALL_TABLES" in sql_seul


def test_dsr699_ne_masque_aucun_denominateur_nul():
    """Décision de conception : aucun `NULLIF` ne protège les dénominateurs, et le seul
    `COALESCE` porte sur `potentielip`, numérateur nullable. Un COALESCE de plus signerait le
    retour du masquage — une clé à 0 indistinguable d'une clé réellement nulle."""
    sql_seul = _sql_sans_commentaires(CLES_CALCULEES)

    assert "NULLIF" not in sql_seul.upper()
    assert sql_seul.upper().count("COALESCE") == 1


def test_dsr699_cast_la_cle_potentiel_ip():
    """`potentielip` (smallint) / `potentielip_total` (bigint) est une division entière :
    MySQL rendrait quatre décimales là où la cible en attend dix-huit. Le CAST est ce qui
    évite une clé juste à 10⁻⁴ près, stockée comme si elle valait mieux."""
    sql_seul = _sql_sans_commentaires(CLES_CALCULEES)

    assert re.search(
        r"CAST\(\s*COALESCE\(c\.potentielip, 0\)\s*AS DECIMAL\(24,18\)\s*\)", sql_seul
    ), "le CAST de la clé potentiel IP a disparu"


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
    [(MIGRATION, 17), (SITE_TRAFIC, 9), (VERSION_CLE, 10), (CLES_CALCULEES, 11)],
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
