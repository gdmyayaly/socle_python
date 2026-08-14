"""Helpers SQL pour les services consommés par OPTIPACC (DSR-689, DSR-690).

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

from typing import Any

from fastapi import HTTPException

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

# DSR-689 — garde d'accès. Volontairement limité aux colonnes nécessaires :
# on ne passe pas par SELECT_SCENARIO_SQL / ScenarioOut, dont le Literal de statut
# ignore la valeur SIMULATION présente en base (écart n°2 de RAPPORT-ECARTS-db_new).
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

# Variante inclure_exclus=true : aucun filtre sur bl_exclu.
SELECT_VOLUMES_BRUTS_TOUS_SQL = (
    f"SELECT co_produit, {_VOLUME_BRUT_EXPR} FROM trppu_tmh "
    "WHERE id_scenario = %s "
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
