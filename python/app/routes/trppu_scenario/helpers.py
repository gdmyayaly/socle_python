"""Helpers pour trppu_scenario : SQL constants, défauts métier, recalcul des bornes."""

from datetime import date, timedelta
from typing import Any

from fastapi import HTTPException

from app.db.mysql import db_read

SELECT_SCENARIO_SQL = (
    "SELECT id_scenario, co_regate, lb_scenario, co_roc, statut, dt_creation, "
    "dt_validation, dt_mise_en_oeuvre, dt_mise_en_prod, dt_pivot AS dt_real_prev, "
    "periode_debut, periode_fin, "
    "periode_realise_debut, periode_realise_fin, periode_prev_debut, periode_prev_fin, "
    "nb_jours_semaine, nb_jours_ouvres, nb_jours_ouvrables, nb_jours_scenario, "
    "id_pic_version, version_scenario, est_fige, "
    "trafic_pdi_calcule, trafic_agrebal_calcule "
    "FROM trppu_scenario"
)


def default_periode() -> tuple[date, date]:
    """Période par défaut : today - 1 an a today + 1 an."""
    today = date.today()
    return today - timedelta(days=365), today + timedelta(days=365)


def recompute_realise_prev(
    periode_debut: date,
    periode_fin: date,
    today: date | None = None,
) -> tuple[date | None, date | None, date | None, date | None]:
    """Recalcule les bornes réalisé / prévision en fonction de la période et de today.

    Renvoie (realise_debut, realise_fin, prev_debut, prev_fin).

    Règles :
    - La portion réalisée (passée + présent) = [periode_debut, min(today, periode_fin)]
      si periode_debut <= today, sinon (None, None).
    - La portion prévision (futur + présent) = [max(today, periode_debut), periode_fin]
      si periode_fin >= today, sinon (None, None).
    - Cas où today est dans la période : realise_fin == prev_debut == today
      (les deux périodes se touchent sur la journée du jour).
    """
    today = today or date.today()
    if periode_debut <= today:
        realise_debut = periode_debut
        realise_fin = min(today, periode_fin)
    else:
        realise_debut = None
        realise_fin = None
    if periode_fin >= today:
        prev_debut = max(today, periode_debut)
        prev_fin = periode_fin
    else:
        prev_debut = None
        prev_fin = None
    return realise_debut, realise_fin, prev_debut, prev_fin


async def resolve_default_pic_version() -> int:
    """Première trppu_pic_version avec est_par_defaut=1, sinon id_pic_version=1.

    Lève 422 si aucune ligne candidate.
    """
    row = await db_read.fetch_one(
        "SELECT id_pic_version FROM trppu_pic_version "
        "WHERE est_par_defaut = 1 ORDER BY id_pic_version LIMIT 1"
    )
    if row:
        return int(row["id_pic_version"])
    row = await db_read.fetch_one(
        "SELECT id_pic_version FROM trppu_pic_version WHERE id_pic_version = 1"
    )
    if row:
        return int(row["id_pic_version"])
    raise HTTPException(
        status_code=422,
        detail=(
            "Aucun id_pic_version par défaut disponible : "
            "renseigner trppu_pic_version (est_par_defaut=1) ou fournir id_pic_version."
        ),
    )


async def fetch_scenario_or_404(id_scenario: int) -> dict[str, Any]:
    row = await db_read.fetch_one(
        SELECT_SCENARIO_SQL + " WHERE id_scenario = %s", (id_scenario,)
    )
    if not row:
        raise HTTPException(
            status_code=404, detail=f"Scénario {id_scenario} introuvable."
        )
    return row


def assert_not_fige(scenario: dict[str, Any]) -> None:
    """Lève HTTP 409 si le scénario est figé.

    Le PATCH /est-fige est la seule manière de défiger ; il n'utilise donc pas ce check.
    Le PATCH /statut a sa propre logique (transitions autorisées) et ne se sert pas non plus
    de ce check : un scénario EN PRODUCTION reste archivable.
    """
    if scenario.get("est_fige"):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Scénario {scenario['id_scenario']} figé "
                f"(statut={scenario['statut']}), modification interdite."
            ),
        )


def assert_not_archive(scenario: dict[str, Any]) -> None:
    """Lève HTTP 409 si le scénario est archivé.

    Un scénario ARCHIVE est terminal : aucune édition (ni de l'entête, ni des
    sous-ressources) n'est autorisée.
    """
    if scenario.get("statut") == "ARCHIVE":
        raise HTTPException(
            status_code=409,
            detail=f"Scénario {scenario['id_scenario']} archivé : modification interdite.",
        )


def assert_editable(scenario: dict[str, Any]) -> None:
    """Lève HTTP 409 si le scénario n'est pas modifiable (archivé ou figé)."""
    assert_not_archive(scenario)
    assert_not_fige(scenario)


# Tables détenant des données opérationnelles rattachées à un scénario.
# Note : db.sql définit des FK sur id_scenario, certaines ON DELETE CASCADE
# (trppu_neutralisations, trppu_tmh, trppu_scenario_variations_prev) et d'autres
# en RESTRICT (comptages, exclusions, scenario_pic_coeffs, trafic_agrebal,
# pic_version). On supprime donc explicitement les enfants avant le parent : ça
# couvre les RESTRICT et reste sûr pour les CASCADE (idempotent).
# Les tables de logs (trppu_api_log, trppu_recalcul_log) sont volontairement
# exclues pour préserver la traçabilité.
SCENARIO_CHILD_TABLES = (
    "trppu_neutralisations",
    "trppu_tmh",
    "trppu_scenario_comptages_manuels",
    "trppu_scenario_exclusions",
    "trppu_scenario_pic_coeffs",
    "trppu_scenario_variations_prev",
    "trppu_trafic_agrebal",
    "trppu_trafic_pdi",
    "trppu_pic_version",
)


async def delete_scenario_cascade(tx, id_scenario: int) -> None:
    """Supprime définitivement un scénario et toutes ses données rattachées.

    On supprime explicitement les lignes des tables enfants (cf.
    SCENARIO_CHILD_TABLES) avant le scénario lui-même : nécessaire pour les FK
    en ON DELETE RESTRICT, et sans effet de bord pour celles en CASCADE.
    """
    for table in SCENARIO_CHILD_TABLES:
        await tx.execute(f"DELETE FROM {table} WHERE id_scenario = %s", (id_scenario,))
    await tx.execute("DELETE FROM trppu_scenario WHERE id_scenario = %s", (id_scenario,))


# Miroir "copie" de SCENARIO_CHILD_TABLES pour la duplication profonde d'un
# scénario (trppu_pic_version est traitée à part, cf. duplicate_scenario_pic_version).
# Tuple : (table, colonnes copiées telles quelles, la table porte un id_rh à remplacer).
# Les PK auto-increment ne sont jamais listées ; dt_calcul / dt_creation sont
# copiées explicitement pour préserver l'historique (sinon les DEFAULT
# CURRENT_TIMESTAMP écraseraient les dates d'origine).
# Toute colonne NOT NULL sans DEFAULT doit figurer ici, sinon l'INSERT ... SELECT
# échoue en base : tests/test_duplicate_scenario.py confronte ces specs aux
# colonnes réelles de db/db_new.sql pour l'empêcher.
DUPLICATE_CHILD_SPECS: tuple[tuple[str, tuple[str, ...], bool], ...] = (
    (
        "trppu_tmh",
        (
            "co_produit",
            "volume_realise",
            "volume_previsionnel",
            "volume_previsionnel_recalcule",
            "moyenne_journaliere",
            "moyenne_hebdo",
            "dt_calcul",
            "bl_exclu",
            "bl_manuel",
            "motif",
        ),
        True,
    ),
    (
        "trppu_neutralisations",
        ("dt_debut", "dt_fin", "nb_jour", "motif", "dt_creation"),
        True,
    ),
    (
        "trppu_scenario_comptages_manuels",
        ("dt_comptage", "co_produit", "nb_produit"),
        False,
    ),
    ("trppu_scenario_exclusions", ("co_produit", "motif"), False),
    (
        "trppu_scenario_variations_prev",
        ("co_produit", "variation_pct", "dt_creation"),
        True,
    ),
    (
        "trppu_scenario_pic_coeffs",
        ("co_produit", "jour_semaine", "coef_dense", "coef_faible1", "coef_faible2"),
        False,
    ),
    (
        "trppu_trafic_agrebal",
        (
            "co_regate",
            "id_agrebal",
            "agrebal_uuid",
            "co_produit",
            "jour_semaine",
            "couleur_pic",
            "volume",
        ),
        False,
    ),
    (
        "trppu_trafic_pdi",
        (
            "co_regate",
            "id_agrebal",
            "agrebal_uuid",
            "id_pdi",
            "co_produit",
            "jour_semaine",
            "dense",
            "faible1",
            "faible2",
            "dt_calcul",
        ),
        False,
    ),
)


async def duplicate_scenario_children(
    tx, src_id: int, new_id: int, id_rh_token: str | None
) -> dict[str, int]:
    """Copie toutes les données filles de src_id vers new_id (INSERT ... SELECT).

    Retourne {table: nb_lignes_copiées}. Sur les tables portant un id_rh
    (cf. DUPLICATE_CHILD_SPECS), l'auteur d'origine est remplacé par
    id_rh_token (le demandeur de la duplication).
    """
    counts: dict[str, int] = {}
    for table, cols, replace_rh in DUPLICATE_CHILD_SPECS:
        col_list = ", ".join(cols)
        if replace_rh:
            sql = (
                f"INSERT INTO {table} (id_scenario, {col_list}, id_rh) "
                f"SELECT %s, {col_list}, %s FROM {table} WHERE id_scenario = %s"
            )
            params = (new_id, id_rh_token, src_id)
        else:
            sql = (
                f"INSERT INTO {table} (id_scenario, {col_list}) "
                f"SELECT %s, {col_list} FROM {table} WHERE id_scenario = %s"
            )
            params = (new_id, src_id)
        counts[table] = await tx.execute(sql, params)
    return counts


async def duplicate_scenario_pic_version(
    tx, src_id: int, new_id: int, co_regate: str, id_rh_token: str | None
) -> int | None:
    """Duplique la version PIC niveau SCENARIO de la source, si elle existe.

    Crée une nouvelle trppu_pic_version pour le clone, copie ses coefficients
    (trppu_pic_coefficients) et pointe l'entête du clone dessus. Retourne le
    nouvel id_pic_version, ou None si la source n'a pas de version SCENARIO
    active (le clone garde alors l'id_pic_version hérité de l'entête source,
    version nationale/partagée).
    """
    from app.routes.trppu_scenario_pic.helpers import fetch_scenario_pic_version

    src_version = await fetch_scenario_pic_version(tx, src_id)
    if not src_version:
        return None
    src_pic_id = int(src_version["id_pic_version"])

    await tx.execute(
        "INSERT INTO trppu_pic_version "
        "(lb_pic_version, niveau, co_regate, id_scenario, dt_activation, "
        " id_rh_creation, id_rh_maj) "
        "VALUES (%s, 'SCENARIO', %s, %s, NOW(), %s, %s)",
        (f"{co_regate}_{new_id}", co_regate, new_id, id_rh_token, id_rh_token),
    )
    row = await tx.fetch_one("SELECT LAST_INSERT_ID() AS id")
    new_pic_id = int(row["id"])

    await tx.execute(
        "INSERT INTO trppu_pic_coefficients "
        "(id_pic_version, co_produit, jour_semaine, dt_effet, dt_fin, coef, densite, id_rh) "
        "SELECT %s, co_produit, jour_semaine, dt_effet, dt_fin, coef, densite, %s "
        "FROM trppu_pic_coefficients WHERE id_pic_version = %s",
        (new_pic_id, id_rh_token, src_pic_id),
    )
    await tx.execute(
        "UPDATE trppu_scenario SET id_pic_version = %s WHERE id_scenario = %s",
        (new_pic_id, new_id),
    )
    return new_pic_id


async def ensure_site_exists(
    tx,
    co_regate: str,
    co_roc: str,
    lb_regate: str,
    type_site: str,
) -> bool:
    """Garantit la présence du site dans trppu_site avant insert d'un scénario.

    Retourne True si une ligne a été insérée, False si le site existait déjà.
    Aucun UPDATE n'est fait sur un site déjà présent.
    """
    row = await tx.fetch_one(
        "SELECT co_regate FROM trppu_site WHERE co_regate = %s", (co_regate,)
    )
    if row:
        return False

    await tx.execute(
        "INSERT INTO trppu_site (co_regate, lb_regate, type_site, co_roc) "
        "VALUES (%s, %s, %s, %s)",
        (co_regate, lb_regate, type_site, co_roc),
    )
    return True


async def increment_version(tx, id_scenario: int) -> int:
    """Incrémente version_scenario et retourne la nouvelle valeur."""
    await tx.execute(
        "UPDATE trppu_scenario SET version_scenario = version_scenario + 1 "
        "WHERE id_scenario = %s",
        (id_scenario,),
    )
    row = await tx.fetch_one(
        "SELECT version_scenario FROM trppu_scenario WHERE id_scenario = %s",
        (id_scenario,),
    )
    return int(row["version_scenario"])
