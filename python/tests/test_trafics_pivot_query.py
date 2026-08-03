"""Tests de la requête agrégée DSR-679 (sans Databricks).

Portent sur la jointure de la dimension `g_trppu_obj_mapping` : c'est elle qui porte le
regroupement restitué et le libellé. Le point critique est l'absence de duplication des lignes
de trafic — le mapping compte plusieurs lignes par objet (une par code comptage CTR), donc une
jointure non pré-regroupée gonflerait les `SUM` silencieusement.
"""

from datetime import datetime

from app.routes.trppu_trafics.helpers import (
    LIBELLE_ALIAS,
    NIVEAU_REGROUPEMENT,
    OBJ_ALIAS,
    SOMME_ALIAS,
    accumulate_trafics,
    build_mapping_subquery,
    build_query,
    empty_accumulator,
    render_sql,
)

PLAGE_MOIS = [(datetime(2025, 9, 1), datetime(2026, 6, 30))]


def _sql_mois() -> str:
    sql, params = build_query("mois", "819650", PLAGE_MOIS, "trafic_constate")
    return render_sql(sql, params)


def test_mapping_pre_regroupe_une_ligne_par_code():
    """Le mapping est agrégé avant la jointure : pas de fan-out possible."""
    sous_requete = build_mapping_subquery()
    assert "GROUP BY co_type_objet" in sous_requete
    assert f"MAX(lb_type_objet) AS {LIBELLE_ALIAS}" in sous_requete
    # Un DISTINCT ne garantirait pas l'unicité si un code portait deux libellés.
    assert "DISTINCT" not in sous_requete


def test_requete_agregee_joint_le_mapping():
    sql = _sql_mois()
    assert "LEFT JOIN (" in sql  # jamais INNER : les objets hors mapping doivent survivre
    assert "g_trppu_obj_mapping" in sql
    assert "ON m.cle_jointure = t.co_type_objet" in sql


def test_regroupement_porte_par_le_mapping():
    """Le code restitué est celui du mapping, avec repli sur les trafics (PR/PPI)."""
    sql = _sql_mois()
    assert f"COALESCE(m.{OBJ_ALIAS}, t.co_type_objet) AS {OBJ_ALIAS}" in sql
    assert sql.rstrip().endswith("GROUP BY 1, 2")


def test_filtre_niveau_regroupement_present():
    """Sans ce filtre, les niveaux ETABLISSEMENT/PIC/NATIONAL s'additionnent."""
    sql = _sql_mois()
    assert (
        f"t.co_niveau_regroupement_operationnel = '{NIVEAU_REGROUPEMENT}'" in sql
    )


def test_pruning_partitions_conserve():
    sql = _sql_mois()
    assert "t.co_mois_comptage BETWEEN '2025-09' AND '2026-06'" in sql
    assert "t.co_annee_comptage IN (2025, 2026)" in sql


def test_table_jour_ajoute_la_partition_mois():
    sql, params = build_query(
        "jours", "819650", [(datetime(2026, 7, 1), datetime(2026, 7, 5))], "trafic_prevu"
    )
    rendu = render_sql(sql, params)
    assert "g_trppu_trafics_jour_3" in rendu
    assert "t.da_comptage BETWEEN '2026-07-01' AND '2026-07-05'" in rendu
    assert "t.co_mois_comptage IN ('2026-07')" in rendu


def test_accumulation_porte_le_libelle_et_somme_les_zones():
    acc = empty_accumulator()
    accumulate_trafics(
        [
            {OBJ_ALIAS: "OO", LIBELLE_ALIAS: "OBJETS ORDINAIRES", SOMME_ALIAS: 10},
            {OBJ_ALIAS: "PR", LIBELLE_ALIAS: None, SOMME_ALIAS: 5},
        ],
        "trafic_brut",
        acc,
    )
    accumulate_trafics(
        [{OBJ_ALIAS: "OO", LIBELLE_ALIAS: "OBJETS ORDINAIRES", SOMME_ALIAS: 3}],
        "trafic_previsionnel",
        acc,
    )
    assert acc["OO"] == {
        "lb_produit": "OBJETS ORDINAIRES",
        "trafic_brut": 10,
        "trafic_previsionnel": 3,
    }
    # Objet absent du mapping : conservé, libellé nul (cf. écart PR/PPI vs PQ/EQ, DSR-679).
    assert acc["PR"] == {"lb_produit": None, "trafic_brut": 5, "trafic_previsionnel": 0}


def test_accumulation_somme_les_mailles_sans_dupliquer_le_libelle():
    """Le même objet remonte par plusieurs mailles : sommes cumulées, libellé retenu une fois."""
    acc = empty_accumulator()
    for somme in (100, 20, 3):
        accumulate_trafics(
            [{OBJ_ALIAS: "IP", LIBELLE_ALIAS: "IMPRIMÉS PUBLICITAIRES", SOMME_ALIAS: somme}],
            "trafic_brut",
            acc,
        )
    assert acc["IP"]["trafic_brut"] == 123
    assert acc["IP"]["lb_produit"] == "IMPRIMÉS PUBLICITAIRES"
