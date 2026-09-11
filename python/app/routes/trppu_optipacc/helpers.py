"""Helpers SQL pour les services consommés par OPTIPACC (DSR-689, 690, 705, 707).

Composition du **volume brut final** par produit (DSR-689 RG4) — c'est le cœur du
ticket, chaque terme est justifié par le code existant :

- `volume_realise` est le **constaté** (jira DSR-648).
- `COALESCE(volume_previsionnel_recalcule, volume_previsionnel, 0)` est le
  **prévisionnel recalculé**. Le repli sur `volume_previsionnel` reproduit
  exactement celui de `insert_tmh_row` / `update_tmh_row`
  (app/routes/trppu_tmh/helpers.py) : la colonne est nullable et reste NULL pour
  les lignes antérieures à son introduction ou écrites par un batch.
- `SUM(...) GROUP BY co_produit` absorbe les **trafics manuels**. Depuis la
  migration du 24/06/2026 un même `co_produit` peut avoir plusieurs lignes dans
  `trppu_tmh` (`uq_tmh` inclut `id_tmh`, cf. docstring du module TMH) : un ajout
  manuel est une ligne supplémentaire, donc il s'additionne naturellement.
- `bl_manuel` n'est **jamais** testé : c'est un flag de provenance, pas un
  opérateur. Une correction DSR-649 écrase les colonnes de sa propre ligne tout en
  posant `bl_manuel = 1` ; filtrer dessus produirait des faux positifs.
- `bl_exclu = 0` reprend le seul précédent du projet, `SELECT_VARIATIONS_SQL`
  (app/routes/trppu_variations/helpers.py).

Hors périmètre : `trppu_scenario_comptages_manuels` n'est jamais reporté dans
`trppu_tmh` par le code, et RG4 vise explicitement « la table TRPPU_TMH ».
L'additionner créerait un double comptage.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException

from app.log_utils import ctx

logger = logging.getLogger(__name__)

# Statuts d'un scénario interrogeable par OPTIPACC. DSR-690 ne *liste* que les
# VALIDE (RG3), mais un scénario déjà sélectionné dans un projet OPTIPACC doit
# rester interrogeable après sa mise en production : asymétrie assumée.
STATUTS_EXPLOITABLES = ("VALIDE", "EN PRODUCTION")

# DSR-690 — scénarios proposables. Utilise l'index idx_scenario_site_statut
# (co_regate, statut) ; le flag agrébal est filtré après l'index.
SELECT_SCENARIOS_EXPLOITABLES_SQL = (
    "SELECT id_scenario, lb_scenario FROM trppu_scenario "
    "WHERE co_regate = %s AND statut = 'VALIDE' AND trafic_agrebal_calcule = 1 "
    "ORDER BY id_scenario"
)

# DSR-689 — garde d'accès. Volontairement limité aux colonnes nécessaires : la
# garde n'a besoin que du statut et du flag de calcul, pas de tout ScenarioOut.
SELECT_SCENARIO_GARDE_SQL = (
    "SELECT id_scenario, co_regate, statut, trafic_agrebal_calcule "
    "FROM trppu_scenario WHERE id_scenario = %s"
)

# La colonne `trppu_tmh.volume_brut` porte désormais cette même valeur ligne à
# ligne (écrite par compute_volume_brut, app/routes/trppu_tmh/helpers.py). On
# continue néanmoins de sommer les volumes plutôt que la colonne : elle reste
# NULL sur les lignes écrites avant sa mise en service, et un batch externe
# alimentant directement trppu_tmh ne la renseignerait pas forcément. Les deux
# formes sont identiques par construction (mêmes COALESCE, même repli).
_VOLUME_BRUT_EXPR = (
    "SUM(COALESCE(volume_realise, 0) "
    "+ COALESCE(volume_previsionnel_recalcule, volume_previsionnel, 0)) AS volume_brut"
)

SELECT_VOLUMES_BRUTS_SQL = (
    f"SELECT co_produit, {_VOLUME_BRUT_EXPR} FROM trppu_tmh "
    "WHERE id_scenario = %s AND bl_exclu = 0 "
    "GROUP BY co_produit ORDER BY co_produit"
)


def assert_exploitable(scenario: dict[str, Any], co_regate: str) -> None:
    """Contrôle qu'un scénario est interrogeable par OPTIPACC (DSR-689 cas 2 et 3).

    - 404 si le scénario n'appartient pas au site demandé (le scénario existe, mais
      pas pour ce Regate : du point de vue de l'appelant il est introuvable).
    - 409 si le statut n'est pas exploitable ou si les trafics Agrébal ne sont pas
      calculés.
    """
    id_scenario = scenario["id_scenario"]
    if str(scenario["co_regate"]) != co_regate:
        raise HTTPException(
            status_code=404,
            detail=f"Scénario {id_scenario} introuvable pour le site {co_regate}.",
        )
    statut = scenario["statut"]
    agrebal = int(scenario["trafic_agrebal_calcule"] or 0)
    if statut not in STATUTS_EXPLOITABLES or agrebal != 1:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Le scénario {id_scenario} n'est pas disponible pour OPTIPACC "
                f"(statut={statut}, trafic_agrebal_calcule={agrebal})."
            ),
        )


# --- DSR-707 : mise en production -------------------------------------------------

# Garde d'accès de la mise en production. `Calcul_trafic_en_cours` est la seule
# colonne capitalisée du schéma (db/db_new.sql) : l'alias ramène la clé du dict au
# snake_case attendu par assert_trafics_calcules.
SELECT_SCENARIO_MISE_EN_PROD_SQL = (
    "SELECT id_scenario, co_regate, statut, est_fige, "
    "trafic_pdi_calcule, trafic_agrebal_calcule, "
    "Calcul_trafic_en_cours AS calcul_trafic_en_cours "
    "FROM trppu_scenario WHERE id_scenario = %s"
)

UPDATE_MISE_EN_PROD_SQL = (
    "UPDATE trppu_scenario SET statut = 'EN PRODUCTION', est_fige = 1, "
    "dt_mise_en_oeuvre = %s, dt_mise_en_prod = %s, "
    # Reprend l'effet de bord de apply_transition_side_effects (statuts.py) : un
    # scénario mis en production sans date de validation est incohérent.
    # dt_maj est posée par la base (ON UPDATE CURRENT_TIMESTAMP).
    "dt_validation = COALESCE(dt_validation, NOW()) "
    "WHERE id_scenario = %s"
)


def assert_mise_en_prod_possible(scenario: dict[str, Any], co_regate: str) -> None:
    """DSR-707 C2 et C3 : appartenance au site, puis statut autorisé.

    C2 répond **400** et non 404 comme `assert_exploitable` : DSR-705 et DSR-707
    imposent tous deux 400 sur ce contrôle, alors que DSR-689 (livré avant) traite
    un scénario d'un autre site comme introuvable. Divergence assumée, les deux
    contrats étant figés côté OPTIPACC.

    C3 n'accepte que VALIDE : EN COURS, SIMULATION, ARCHIVE et EN PRODUCTION sont
    refusés en 409 — ce dernier couvrant le Cas 3 du ticket (scénario déjà en
    production).
    """
    id_scenario = scenario["id_scenario"]
    if str(scenario["co_regate"]) != co_regate:
        logger.warning(
            "Rejet mise en production OPTIPACC %s",
            ctx(
                id_scenario=id_scenario,
                co_regate=co_regate,
                co_regate_scenario=scenario["co_regate"],
                http=400,
                motif="scénario d'un autre site",
            ),
        )
        raise HTTPException(
            status_code=400,
            detail=f"Le scénario {id_scenario} n'appartient pas au site {co_regate}",
        )

    statut = scenario["statut"]
    if statut != "VALIDE":
        logger.warning(
            "Rejet mise en production OPTIPACC %s",
            ctx(
                id_scenario=id_scenario,
                co_regate=co_regate,
                statut=statut,
                http=409,
                motif="statut non autorisé pour la mise en production",
            ),
        )
        raise HTTPException(
            status_code=409,
            detail=(
                f"Les paramètres du scénario {id_scenario} ne permettent pas "
                "la mise en production du scénario"
            ),
        )


# --- DSR-705 : trafics Agrébal (amas) ---------------------------------------------

# Garde d'accès de /trafic-amas. Plus large que SELECT_SCENARIO_GARDE_SQL : C3
# contrôle cinq conditions là où DSR-689 n'en contrôle que deux.
SELECT_SCENARIO_VISIBLE_SQL = (
    "SELECT id_scenario, co_regate, statut, est_fige, "
    "trafic_pdi_calcule, trafic_agrebal_calcule, "
    "Calcul_trafic_en_cours AS calcul_trafic_en_cours "
    "FROM trppu_scenario WHERE id_scenario = %s"
)

COUNT_AMAS_SQL = (
    "SELECT COUNT(DISTINCT agrebal_uuid) AS nb FROM trppu_trafic_agrebal "
    "WHERE id_scenario = %s AND co_regate = %s"
)

# RG-API-008 : tri sur agrebal_uuid **avant** pagination, pour qu'un même Agrébal
# ne change pas de page entre deux appels.
SELECT_AMAS_PAGE_SQL = (
    "SELECT DISTINCT agrebal_uuid FROM trppu_trafic_agrebal "
    "WHERE id_scenario = %s AND co_regate = %s "
    "ORDER BY agrebal_uuid LIMIT %s OFFSET %s"
)


def select_amas_existants_sql(nb_uuid: int) -> str:
    """C4 : quels agrebal_uuid demandés existent réellement pour ce scénario.

    La clause IN est construite à partir du seul *nombre* de valeurs, jamais de
    leur contenu : les UUID restent des paramètres liés.
    """
    placeholders = ", ".join(["%s"] * nb_uuid)
    return (
        "SELECT DISTINCT agrebal_uuid FROM trppu_trafic_agrebal "
        "WHERE id_scenario = %s AND co_regate = %s "
        f"AND agrebal_uuid IN ({placeholders}) "
        "ORDER BY agrebal_uuid"
    )


# Pivot de `couleur_pic` : en base la densité est un discriminant de ligne (une
# ligne par DENSE / FAIBLE1 / FAIBLE2), alors que le JSON attendu porte les trois
# valeurs sur la même ligne produit. Mapping imposé par le ticket :
# DENSE -> fort, FAIBLE1 -> faible1, FAIBLE2 -> faible2.
_PIVOT_DENSITES = (
    "SUM(CASE WHEN t.couleur_pic = 'DENSE' THEN t.volume END) AS fort, "
    "SUM(CASE WHEN t.couleur_pic = 'FAIBLE1' THEN t.volume END) AS faible1, "
    "SUM(CASE WHEN t.couleur_pic = 'FAIBLE2' THEN t.volume END) AS faible2"
)

# Le GROUP BY n'agrège rien de métier : il replie les trois lignes de densité (et
# d'éventuels doublons techniques — la table n'a aucune clé unique) en une ligne
# produit. Aucune clé de répartition ni coefficient PIC n'est appliqué ici,
# conformément à RG-API-006.
#
# LEFT JOIN sur trppu_agrebal_pdi : `nom_amas` n'existe pas dans
# trppu_trafic_agrebal. La jointure passe par (agrebal_id, agrebal_code_regate),
# clé unique uq_agrpdi_courant — et non par agrebal_uuid, non indexé dans cette
# table. Un amas sans ligne référentiel conserve ses trafics, nom à NULL.
_SELECT_TRAFICS_AMAS_BASE = (
    "SELECT t.agrebal_uuid, MAX(a.agrebal_nom) AS nom_amas, "
    "t.jour_semaine, t.co_produit, "
    f"{_PIVOT_DENSITES} "
    "FROM trppu_trafic_agrebal t "
    "LEFT JOIN trppu_agrebal_pdi a "
    "ON a.agrebal_id = t.id_agrebal AND a.agrebal_code_regate = t.co_regate "
    "WHERE t.id_scenario = %s AND t.co_regate = %s "
    "AND t.agrebal_uuid IN ({placeholders}) "
    "GROUP BY t.agrebal_uuid, t.jour_semaine, t.co_produit"
)

# Isolé du SELECT : FIELD() est propre à MySQL et les tests rejouent les
# constantes SQL sur SQLite. L'ordre suit la semaine, pas l'alphabet.
ORDER_BY_TRAFICS_AMAS = (
    " ORDER BY t.agrebal_uuid, "
    "FIELD(t.jour_semaine, 'LUNDI', 'MARDI', 'MERCREDI', 'JEUDI', 'VENDREDI', 'SAMEDI'), "
    "t.co_produit"
)


def select_trafics_amas_sql(nb_uuid: int) -> str:
    """Requête de restitution des trafics, pour `nb_uuid` agrebal_uuid liés."""
    placeholders = ", ".join(["%s"] * nb_uuid)
    return (
        _SELECT_TRAFICS_AMAS_BASE.format(placeholders=placeholders)
        + ORDER_BY_TRAFICS_AMAS
    )


def assert_visible_optipacc(scenario: dict[str, Any], co_regate: str) -> None:
    """DSR-705 C2 et C3 : règles de visibilité OPTIPACC d'un scénario.

    Volontairement distincte de `assert_exploitable` (DSR-689), qui ne contrôle que
    le statut et le flag Agrébal : durcir cette dernière changerait le contrat d'un
    service déjà livré.

    Le ticket exige `STATUT = VALIDE` ; on accepte aussi EN PRODUCTION
    (`STATUTS_EXPLOITABLES`) car DSR-707 fait justement passer EN PRODUCTION le
    scénario qu'OPTIPACC vient de retenir — une lecture stricte rendrait ses
    propres trafics illisibles juste après la mise en production.

    Les flags sont nullables en base, d'où le `or 0`. Le détail des conditions non
    remplies part dans le log ; le message rendu reste celui du ticket.
    """
    id_scenario = scenario["id_scenario"]
    if str(scenario["co_regate"]) != co_regate:
        logger.warning(
            "Rejet trafic amas OPTIPACC %s",
            ctx(
                id_scenario=id_scenario,
                co_regate=co_regate,
                co_regate_scenario=scenario["co_regate"],
                http=400,
                motif="scénario d'un autre site",
            ),
        )
        raise HTTPException(
            status_code=400,
            detail=f"Le scénario {id_scenario} n'appartient pas au site {co_regate}",
        )

    statut = scenario["statut"]
    fige = int(scenario.get("est_fige") or 0)
    pdi = int(scenario.get("trafic_pdi_calcule") or 0)
    agrebal = int(scenario.get("trafic_agrebal_calcule") or 0)
    en_cours = int(scenario.get("calcul_trafic_en_cours") or 0)

    if (
        statut not in STATUTS_EXPLOITABLES
        or fige != 1
        or pdi != 1
        or agrebal != 1
        or en_cours != 0
    ):
        logger.warning(
            "Rejet trafic amas OPTIPACC %s",
            ctx(
                id_scenario=id_scenario,
                co_regate=co_regate,
                statut=statut,
                est_fige=fige,
                trafic_pdi_calcule=pdi,
                trafic_agrebal_calcule=agrebal,
                calcul_trafic_en_cours=en_cours,
                http=409,
                motif="scénario non visible OPTIPACC",
            ),
        )
        raise HTTPException(status_code=409, detail="Scenario non disponible")
