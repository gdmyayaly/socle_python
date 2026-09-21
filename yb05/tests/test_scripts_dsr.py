"""Tests des scripts SQL métier de `db/` (DSR-697, DSR-696, DSR-698, DSR-699).

Aucune base MySQL n'est nécessaire : les scripts sont analysés par le découpeur du socle, et
leur exécution n'est vérifiée qu'en `dry_run`, mode qui n'ouvre aucune connexion. Le code
async passe par ``asyncio.run`` pour ne pas dépendre de pytest-asyncio.

L'enjeu principal est le contrôle des noms, que les tickets décrivent mal :

* DSR-697 charge le CSV métier dans ``trppu_cles_repartition``. Sa liste de colonnes est
  juste — c'est celle du FICHIER, pas de la table, le mapping de ``LOAD DATA`` étant
  positionnel. Ses pièges sont ailleurs : les quatre champs vides à convertir en NULL
  (RG3), qu'un ``SET`` oublié laisserait à ``''`` ou à 0, et les contrôles du ticket qui
  écrivent ``SELECT COUNT FROM …``, sans parenthèses — ``ERROR 1054`` à l'exécution.
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
FIX = DB_DIR / "fix_error.sql"
SUIVI = DB_DIR / "suivi.sql"
CHARGEMENT = DB_DIR / "DSR-697_chargement_cles_repartition.sql"
SITE_TRAFIC = DB_DIR / "DSR-696_site_trafic.sql"
VERSION_CLE = DB_DIR / "DSR-698_version_cle.sql"
CLES_CALCULEES = DB_DIR / "DSR-699_cles_calculees.sql"

# Dans l'ordre d'exécution de la chaîne — cf. db/README.md. Le chargement DSR-697 vient en
# tête : les trois autres ne lisent rien d'autre que la table qu'il alimente.
SCRIPTS_METIER = (CHARGEMENT, SITE_TRAFIC, VERSION_CLE, CLES_CALCULEES)

# `fix_error.sql` n'est pas un script métier : comme la migration, il porte du DDL et ne se
# joue qu'une fois par base. Il élargit les totaux de `trppu_trafic_site`, trop étroits pour
# recevoir la somme qu'ils doivent porter — l'`ERROR 1264` rencontrée sur DSR-696.
# `suivi.sql` ne modifie rien : il s'exécute en parallèle des autres, depuis une seconde
# session, pour regarder où en est un traitement long.
TOUS_LES_SCRIPTS = (MIGRATION, FIX, SUIVI, *SCRIPTS_METIER)


# ---------------------------------------------------------------------------
# Schéma de référence
# ---------------------------------------------------------------------------

# Extrait de python/db/db_new.sql, limité aux tables écrites par ces scripts. Recopié plutôt
# que lu depuis le projet voisin : yb05 ne doit pas dépendre de l'arborescence de python/.
# À resynchroniser si le schéma évolue.
#
# État APRÈS `DSR-696-699_migration.sql` : c'est lui que supposent les scripts de données.
# Deux écarts avec le dump du 17/08/2026, tous deux apportés par la migration —
# `trppu_version_cle.date_creation`, que DSR-698 spécifie, et `uq_crc_version_pdi`, qui rend
# le CA4 de DSR-699 vrai en base.
#
# `trppu_cles_repartition` est la table SOURCE de la chaîne, et la cible du chargement
# DSR-697. Son `idx_cr_ref_actif`, posé par le bloc 2 de la migration, figure désormais dans
# le dump comme dans `python/db/03_contraintes.sql` : les deux sont d'accord, et le garde-fou
# de rejouabilité de la migration n'a plus rien à y faire.
#
# Troisième écart, apporté par `db/fix_error.sql` : les trois totaux de `trppu_trafic_site`
# passent de `decimal(24,18)` — six chiffres avant la virgule, d'où l'`ERROR 1264` sur
# `trafic_oo_total` — à `decimal(35,19)`, seize chiffres entiers et l'échelle de la source.
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
    # Les commentaires en fin de ligne ne peuvent pas apparaître dans une liste de colonnes,
    # un simple découpage sur les virgules suffit.
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
    """Colonnes alimentées par le `LOAD DATA` : `(depuis le fichier, depuis le SET)`.

    Les champs captés dans une variable utilisateur (``@regate_etab``…) ne sont pas des
    colonnes : ils sont écartés de la première liste et réapparaissent, convertis, dans la
    seconde.
    """
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
    [(MIGRATION, 17), (FIX, 9), (SUIVI, 8), (CHARGEMENT, 11), (SITE_TRAFIC, 9),
     (VERSION_CLE, 10), (CLES_CALCULEES, 11)],
    ids=lambda v: v.name if isinstance(v, Path) else str(v),
)
def test_nombre_d_instructions(script, attendu):
    """Verrouille le découpage : une instruction perdue passerait sinon inaperçue."""
    assert len(_instructions(script)) == attendu


def test_dsr697_purge_avant_de_charger():
    """RG6 : « chargement après suppression des données déjà présentes pour le référentiel ».
    Inversé, le DELETE emporterait les lignes qui viennent d'être chargées."""
    verbes = [first_keyword(s) for s in _instructions(CHARGEMENT)]

    assert verbes[:2] == ["SET", "SET"], "les paramètres doivent précéder tout accès"
    assert verbes.index("DELETE") < verbes.index("LOAD")


def test_dsr697_ne_charge_que_des_colonnes_existantes():
    """La liste du `LOAD DATA` décrit les champs du FICHIER — mais tout ce qui n'est pas
    capté dans une variable `@…` est écrit tel quel dans une colonne, qui doit exister."""
    fichier, affectees = _colonnes_chargées(CHARGEMENT)
    schema = _colonnes_du_schema("trppu_cles_repartition")

    inconnues = (set(fichier) | set(affectees)) - schema
    assert not inconnues, f"colonnes inconnues : {inconnues}"
    # AUTO_INCREMENT : jamais alimentée, ni par le fichier ni par le SET.
    assert "id" not in fichier and "id" not in affectees
    # Les 15 champs du fichier : 11 colonnes directes + 4 captées en variables (RG3).
    assert len(fichier) == 11


def test_dsr697_convertit_les_quatre_champs_vides_en_null():
    """RG3. Sans `NULLIF`, les deux colonnes texte prendraient `''` et les deux colonnes
    numériques 0 — or 0 et « inconnu » ne se confondent pas pour un potentiel IP."""
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
    """Ni `IGNORE` ni `REPLACE` entre le chemin et `INTO TABLE` : un doublon de
    (id_pdi, id_referentiel) doit faire échouer le chargement en 1062, pas disparaître.

    La déduplication RG4 porte sur la ligne entière : deux lignes d'un même PDI aux trafics
    différents y survivent toutes les deux, et c'est `uk_pdi_ref` qui les arrête.
    """
    sql = _load_data(CHARGEMENT)

    assert re.search(r"LOAD DATA (LOCAL )?INFILE '[^']+'\s+INTO TABLE", sql), (
        "un modificateur IGNORE / REPLACE s'est glissé avant INTO TABLE"
    )


def test_dsr697_durcit_le_mode_sql():
    """Hors mode strict, `LOAD DATA` ramène une valeur non numérique à 0 et tronque les
    chaînes trop longues, sur un simple avertissement : le référentiel serait chargé faux."""
    sql_seul = _sql_sans_commentaires(CHARGEMENT)

    assert "STRICT_ALL_TABLES" in sql_seul


@pytest.mark.parametrize("script", TOUS_LES_SCRIPTS, ids=lambda p: p.name)
def test_aucun_script_ne_reprend_le_count_sans_parentheses(script):
    """Les contrôles de DSR-697 sont écrits `SELECT COUNT FROM …` dans le ticket : sans
    parenthèses, `COUNT` est lu comme un nom de colonne — `ERROR 1054`."""
    assert not re.search(
        r"\bCOUNT\s+FROM\b", _sql_sans_commentaires(script), re.IGNORECASE
    )


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
    """Aucun DDL : ces scripts sont intégralement annulables par un ROLLBACK. Vaut aussi
    pour le `LOAD DATA` de DSR-697, qui ne provoque pas de commit implicite — contrairement
    au TRUNCATE qu'on serait tenté de lui substituer pour purger plus vite."""
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


def test_le_ddl_du_fix_echappe_aussi_a_la_detection():
    """`fix_error.sql` emploie le même emballage `PREPARE`/`EXECUTE` que la migration, et
    appelle donc la même consigne : `transactional=False`."""
    instructions = _instructions(FIX)

    assert not any(is_ddl(s) for s in instructions)
    assert "ALTER TABLE" in FIX.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# fix_error.sql — l'ERROR 1264 sur les totaux de site
# ---------------------------------------------------------------------------


def _alter_du_fix() -> str:
    """Chaîne d'`ALTER` portée par le `SET @sql` du correctif.

    Les commentaires sont retirés AVANT la recherche : `sqlparse` rattache à l'instruction
    qui suit la bannière qui la précède, et l'en-tête du fichier parle lui aussi d'`ALTER
    TABLE` — le chercher dans le texte brut désignerait la première instruction venue.
    """
    nettoyees = (
        "\n".join(
            ligne for ligne in s.splitlines() if not ligne.lstrip().startswith("--")
        )
        for s in _instructions(FIX)
    )
    return next(s for s in nettoyees if "ALTER TABLE" in s)


def test_fix_elargit_les_trois_totaux_a_la_meme_definition():
    """Les trois familles de trafic ont le même défaut : n'en élargir qu'une déplacerait
    l'`ERROR 1264` sur la suivante. `NOT NULL` est répété dans chaque `MODIFY` — une clause
    omise vaut suppression de la contrainte."""
    alter = _alter_du_fix()

    for colonne in ("trafic_colis_total", "trafic_oo_total", "trafic_3s_total"):
        assert re.search(
            rf"MODIFY COLUMN `{colonne}`\s+decimal\(35,19\) NOT NULL", alter
        ), f"élargissement absent ou incomplet : {colonne}"


def test_fix_accorde_la_definition_cible_au_schema_de_reference():
    """Le schéma recopié dans ce test et le correctif doivent dire la même chose : c'est le
    seul garde-fou contre une correction appliquée en base mais jamais répercutée ici."""
    corps = re.search(
        r"CREATE TABLE `trppu_trafic_site` \((.*?)\n\) ENGINE", SCHEMA_REFERENCE, re.S
    ).group(1)
    alter = _alter_du_fix()

    for colonne in ("trafic_colis_total", "trafic_oo_total", "trafic_3s_total"):
        type_schema = re.search(rf"`{colonne}` (decimal\(\d+,\d+\))", corps).group(1)
        assert type_schema in alter, f"{colonne} : {type_schema} absent du correctif"


def test_fix_ne_touche_pas_au_potentiel_ip():
    """`potentielip_total` est un `bigint` : il contient largement la somme des `smallint`
    de la source. L'élargir n'apporterait rien et reconstruirait la table pour rien."""
    assert "potentielip_total" not in _alter_du_fix()


def test_fix_est_garde_par_la_definition_et_non_par_le_nom():
    """Rejouabilité. Un garde-fou qui testerait l'existence de la colonne serait toujours
    vrai et reconstruirait la table à chaque exécution ; c'est la largeur qui décide."""
    garde = next(s for s in _instructions(FIX) if "@sql :=" in s)

    assert "NUMERIC_PRECISION" in garde
    assert ">= 35" in garde


def test_le_suivi_est_strictement_en_lecture():
    """`suivi.sql` se joue depuis une SECONDE session, pendant qu'un traitement écrit.

    Sa seule garantie, c'est de ne rien faire : `SET` de variables de session et `SELECT`, pas
    un verbe de plus. Une écriture glissée ici s'exécuterait en concurrence d'un chargement de
    24 M de lignes — au mieux une attente de verrou, au pire une corruption du référentiel en
    cours d'écriture.
    """
    verbes = {first_keyword(s) for s in _instructions(SUIVI)}

    assert verbes <= {"SET", "SELECT"}, f"verbes interdits : {verbes - {'SET', 'SELECT'}}"
    assert not any(is_ddl(s) for s in _instructions(SUIVI))
    # Les `UPDATE` d'activation de l'instrumentation `performance_schema` sont donnés en
    # commentaire, à jouer à la main : ils modifient le serveur entier.
    # (`UPDATE_TIME`, colonne lue au bloc 4, n'est évidemment pas concernée.)
    assert not re.search(
        r"\bUPDATE\s+performance_schema", _sql_sans_commentaires(SUIVI), re.IGNORECASE
    )


def test_le_suivi_ne_compte_pas_les_lignes_de_la_table_source():
    """`COUNT(*)` sur `trppu_cles_repartition` (24 M) à chaque rafraîchissement ferait du
    script de suivi une charge de plus sur un serveur déjà occupé. La volumétrie de cette
    table se lit dans `information_schema.TABLES`, approximative mais instantanée."""
    sql_seul = _sql_sans_commentaires(SUIVI)

    assert "information_schema.TABLES" in sql_seul
    assert not re.search(
        r"COUNT\(\*\)\s*FROM\s+trppu_cles_repartition\b(?!_calcule)", sql_seul
    )


def test_fix_conserve_l_echelle_de_la_source():
    """19 décimales, comme `trppu_cles_repartition.trafic_*`. À 18, la somme serait arrondie
    et le contrôle CA1+CA3 de DSR-696 afficherait des `ecart_*` non nuls."""
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
