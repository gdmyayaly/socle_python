"""Injection des paramètres de session dans un script SQL.

Ces tests portent sur `app/db/sql_parametres.py`, mais plusieurs se jouent sur le **texte réel**
des scripts de `db/` : l'intérêt n'est pas de vérifier une regex sur un exemple choisi, c'est de
prouver que la substitution tient sur les fichiers qui seront réellement exécutés — bannières de
commentaires, `SET SESSION` et variables de travail comprises.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from app.db.sql_parametres import (
    ParametreInconnu,
    injecter_parametres,
    litteral_sql,
    nom_variable,
)
from app.db.sql_script import first_keyword, split_sql_script

DB_DIR = Path(__file__).resolve().parent.parent / "db"

MIGRATION = DB_DIR / "DSR-696-699_migration.sql"
CORRECTIF = DB_DIR / "fix_error.sql"
AGREGATS = DB_DIR / "DSR-696_site_trafic.sql"
VERSIONS = DB_DIR / "DSR-698_version_cle.sql"
CLES = DB_DIR / "DSR-699_cles_calculees.sql"


def _lire(chemin: Path) -> str:
    return chemin.read_text(encoding="utf-8-sig")


# ---------------------------------------------------------------------------
# Substitution
# ---------------------------------------------------------------------------


def test_le_parametre_declare_prend_la_valeur_demandee():
    script = "SET @id_referentiel := 1;\nSELECT * FROM t WHERE r = @id_referentiel;"
    resultat = injecter_parametres(script, {"id_referentiel": 7})
    assert "SET @id_referentiel := 7" in resultat
    assert "SET @id_referentiel := 1" not in resultat


def test_le_parametre_est_pose_avant_toute_lecture():
    """Une affectation qui arriverait après le premier SELECT ne servirait à rien."""
    resultat = injecter_parametres(_lire(CLES), {"id_referentiel": 7, "co_regate": None})
    instructions = split_sql_script(resultat)

    dernier_set = max(
        i for i, s in enumerate(instructions) if nom_variable(s) == "id_referentiel"
    )
    premiere_lecture = min(
        i
        for i, s in enumerate(instructions)
        if first_keyword(s) in {"SELECT", "INSERT", "UPDATE", "DELETE"}
    )
    assert dernier_set < premiere_lecture


def test_le_nombre_d_instructions_est_conserve():
    """La substitution remplace, elle n'ajoute ni ne retire d'instruction."""
    cas = [
        (MIGRATION, {}),
        (CORRECTIF, {"id_referentiel": 1}),
        (AGREGATS, {"id_referentiel": 1, "co_regate": None}),
        (
            VERSIONS,
            {"id_referentiel": 1, "co_regate": "123456", "commentaire": "x", "libelle": None},
        ),
        (CLES, {"id_referentiel": 1, "co_regate": None}),
    ]
    for chemin, parametres in cas:
        texte = _lire(chemin)
        avant = len(split_sql_script(texte))
        apres = len(split_sql_script(injecter_parametres(texte, parametres)))
        assert apres == avant, chemin.name


# ---------------------------------------------------------------------------
# Ce qui ne doit surtout pas être touché
# ---------------------------------------------------------------------------


def test_le_set_session_sql_mode_est_preserve():
    """C'est lui qui fait échouer DSR-699 sur une division par zéro plutôt que d'écrire NULL."""
    resultat = injecter_parametres(_lire(CLES), {"id_referentiel": 1, "co_regate": None})
    assert "SET SESSION sql_mode" in resultat
    assert "ERROR_FOR_DIVISION_BY_ZERO" in resultat


def test_les_variables_de_travail_du_script_survivent():
    """`@deja` porte la rejouabilité de DSR-698 : le retirer casserait le CA4."""
    resultat = injecter_parametres(
        _lire(VERSIONS),
        {"id_referentiel": 1, "co_regate": "123456", "commentaire": "x", "libelle": None},
    )
    assert any(nom_variable(s) == "deja" for s in split_sql_script(resultat))


def test_les_variables_porteuses_du_ddl_survivent():
    """Les `@sql` de la migration transportent les ALTER : sans eux, elle ne fait rien."""
    resultat = injecter_parametres(_lire(MIGRATION), {})
    noms = [nom_variable(s) for s in split_sql_script(resultat)]
    assert noms.count("sql") == 4


def test_la_banniere_de_commentaire_ne_fait_pas_rater_le_set():
    """`sqlparse.split` rattache la bannière qui précède une instruction à cette instruction."""
    script = "-- ----------------------\n-- Paramètres\nSET @x := 1;\nSELECT 1;"
    assert nom_variable(split_sql_script(script)[0]) == "x"
    assert "SET @x := 9" in injecter_parametres(script, {"x": 9})


# ---------------------------------------------------------------------------
# Littéraux
# ---------------------------------------------------------------------------


def test_none_devient_null_sans_guillemets():
    """`'NULL'` filtrerait sur un site inexistant, sans la moindre erreur."""
    assert litteral_sql(None) == "NULL"
    resultat = injecter_parametres("SET @co_regate := '1';", {"co_regate": None})
    assert "SET @co_regate := NULL" in resultat
    assert "'NULL'" not in resultat


def test_une_chaine_est_quotee_et_echappee():
    assert litteral_sql("Réorganisation DEX") == "'Réorganisation DEX'"
    assert litteral_sql("L'Haÿ") == "'L''Haÿ'"
    assert litteral_sql("C:\\tmp") == "'C:\\\\tmp'"


def test_un_pourcent_passe_tel_quel():
    """Le runner n'envoie aucun paramètre à PyMySQL : `query % args` n'est jamais appliqué."""
    assert litteral_sql("100% recalculé") == "'100% recalculé'"


def test_un_booleen_ne_devient_pas_un_entier_par_accident():
    """`isinstance(True, int)` est vrai : sans test préalable, True passerait pour un entier."""
    assert litteral_sql(True) == "1"
    assert litteral_sql(False) == "0"


@pytest.mark.parametrize("valeur", [1, 0, -3, Decimal("1.5")])
def test_les_nombres_passent_sans_guillemets(valeur):
    assert "'" not in litteral_sql(valeur)


def test_les_dates_sont_rendues_au_format_mysql():
    assert litteral_sql(date(2026, 9, 1)) == "'2026-09-01'"
    assert litteral_sql(datetime(2026, 9, 1, 14, 30, 0)) == "'2026-09-01 14:30:00'"


@pytest.mark.parametrize("valeur", [object(), [1, 2], {"a": 1}, float("nan"), float("inf")])
def test_une_valeur_de_type_inattendu_est_refusee(valeur):
    """Jamais de repli sur `str(valeur)` : un objet inattendu doit casser le test."""
    with pytest.raises(TypeError):
        litteral_sql(valeur)


def test_un_caractere_de_controle_est_refuse():
    with pytest.raises(ValueError):
        litteral_sql("abc\x00def")


# ---------------------------------------------------------------------------
# Garde-fous
# ---------------------------------------------------------------------------


def test_un_parametre_absent_du_script_est_signale():
    """La faute de frappe produirait sinon un script valide et un résultat faux."""
    with pytest.raises(ParametreInconnu) as echec:
        injecter_parametres(_lire(VERSIONS), {"co_regates": "123456"})
    assert "@co_regates" in str(echec.value)


def test_un_parametre_absent_peut_etre_tolere_explicitement():
    resultat = injecter_parametres("SELECT 1;", {"x": 1}, exiger_presence=False)
    assert "SET @x := 1" in resultat


def test_un_script_a_delimiteur_personnalise_est_refuse():
    """Le rejoindre par `;` couperait le corps de la routine."""
    script = "DELIMITER $$\nCREATE PROCEDURE p() BEGIN SELECT 1; END$$\nDELIMITER ;"
    with pytest.raises(ValueError):
        injecter_parametres(script, {})
