"""Routes CRUD et workflow de statut pour la table trppu_scenario."""

import logging
import time
from datetime import date

from fastapi import APIRouter, HTTPException, Query, status

from app.db.mysql import db_read, db_write
from app.log_utils import safe_preview
from app.security.crypto import encrypt_id_rh
from app.services.jours_service import compute_nb_jours

from .helpers import (
    SELECT_SCENARIO_SQL,
    assert_editable,
    assert_not_archive,
    default_periode,
    delete_scenario_cascade,
    ensure_site_exists,
    fetch_scenario_or_404,
    increment_version,
    recompute_realise_prev,
    resolve_default_pic_version,
)
from .schemas import (
    DuplicateRequest,
    FigeUpdate,
    LbScenarioUpdate,
    NbJoursUpdate,
    PeriodeUpdate,
    ScenarioCreate,
    ScenarioMajRequest,
    ScenarioOut,
    ScenarioPeriodesOut,
    StatutUpdate,
)
from .statuts import (
    STATUTS,
    apply_transition_side_effects,
    assert_internal_transition_allowed,
    assert_transition_allowed,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/scenarios", tags=["Scenarios"])


@router.get("", response_model=list[ScenarioOut])
async def list_scenarios(
    co_regate: str | None = Query(None, min_length=6, max_length=6),
    co_roc: str | None = Query(None, min_length=6, max_length=6),
    statut: str | None = Query(None),
    est_fige: bool | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    start = time.perf_counter()
    filters = {
        "co_regate": co_regate,
        "co_roc": co_roc,
        "statut": statut,
        "est_fige": est_fige,
        "limit": limit,
        "offset": offset,
    }
    logger.info("Début listing des scénarios (filtres=%s)", safe_preview(filters))

    where: list[str] = []
    params: list = []
    if co_regate is not None:
        where.append("co_regate = %s")
        params.append(co_regate)
    if co_roc is not None:
        where.append("co_roc = %s")
        params.append(co_roc)
    if statut is not None:
        where.append("statut = %s")
        params.append(statut)
    if est_fige is not None:
        where.append("est_fige = %s")
        params.append(1 if est_fige else 0)

    sql = SELECT_SCENARIO_SQL
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id_scenario DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    try:
        rows = await db_read.fetch_all(sql, tuple(params))
    except Exception as e:
        logger.exception(
            "Erreur listing des scénarios (filtres=%s)", safe_preview(filters)
        )
        raise HTTPException(status_code=500, detail="Erreur listing scenarios.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Listing des scénarios terminé (count=%d, duration_ms=%.1f)", len(rows), duration_ms
    )
    return rows


@router.get("/enums")
async def list_enums():
    """Valeurs autorisées pour les colonnes ENUM de trppu_scenario."""
    logger.info("Récupération des enums scénarios")
    return {"statut": list(STATUTS)}


@router.get("/{id_scenario}", response_model=ScenarioOut)
async def get_scenario(id_scenario: int):
    start = time.perf_counter()
    logger.info("Début récupération scénario (id_scenario=%d)", id_scenario)
    row = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Récupération scénario terminée (id_scenario=%d, duration_ms=%.1f)",
        id_scenario,
        duration_ms,
    )
    return row


@router.get("/{id_scenario}/periodes", response_model=ScenarioPeriodesOut)
async def get_scenario_periodes(
    id_scenario: int,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-655 : périodes + nombres de jours d'un scénario (actualisation du slider IHM)."""
    start = time.perf_counter()
    logger.info(
        "Début lecture périodes scénario (id_scenario=%d, id_session_ihm=%s)",
        id_scenario,
        safe_preview(id_session_ihm),
    )
    row = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Lecture périodes scénario terminée (id_scenario=%d, duration_ms=%.1f)",
        id_scenario,
        duration_ms,
    )
    return row


@router.post("", response_model=ScenarioOut, status_code=status.HTTP_201_CREATED)
async def create_scenario(payload: ScenarioCreate):
    start = time.perf_counter()
    # NB : on n'inclut jamais id_rh (en clair) dans les logs.
    logged_payload = payload.model_dump(mode="json", exclude={"id_rh"})
    logger.info(
        "Début création scénario (co_regate=%s, payload=%s)",
        payload.co_regate,
        safe_preview(logged_payload),
    )

    if payload.id_pic_version is not None:
        pic_version = payload.id_pic_version
    else:
        pic_version = await resolve_default_pic_version()
        logger.info(
            "Version PIC par défaut résolue (id_pic_version=%d)", pic_version
        )

    debut, fin = payload.periode_debut, payload.periode_fin
    if debut is None or fin is None:
        d, f = default_periode()
        debut = debut or d
        fin = fin or f
        logger.info(
            "Période par défaut appliquée (debut=%s, fin=%s)", debut, fin
        )

    realise_debut, realise_fin, prev_debut, prev_fin = recompute_realise_prev(debut, fin)
    logger.info(
        "Bornes réalisé/prévision calculées (réalisé=[%s, %s], prévision=[%s, %s])",
        realise_debut,
        realise_fin,
        prev_debut,
        prev_fin,
    )

    # Nombres de jours sur la période (fériés déduits) — DSR-613 / DSR-634.
    nbj = await compute_nb_jours(debut, fin)
    nb_jours_scenario = (
        nbj.nb_jours_ouvres if payload.nb_jours_semaine == 5 else nbj.nb_jours_ouvrables
    )
    logger.info(
        "Nombres de jours calculés (ouvres=%d, ouvrables=%d, scenario=%d)",
        nbj.nb_jours_ouvres,
        nbj.nb_jours_ouvrables,
        nb_jours_scenario,
    )

    dt_mise_en_oeuvre = payload.dt_mise_en_oeuvre or date.today()
    id_rh_token = encrypt_id_rh(payload.id_rh)

    try:
        async with db_write.transaction() as tx:
            site_created = await ensure_site_exists(
                tx,
                co_regate=payload.co_regate,
                co_roc=payload.co_roc,
                lb_regate=payload.lb_regate,
                type_site=payload.type_site,
            )
            if site_created:
                logger.info(
                    "Site créé automatiquement (co_regate=%s, type_site=%s)",
                    payload.co_regate,
                    payload.type_site,
                )
            else:
                logger.info(
                    "Site déjà présent dans trppu_site (co_regate=%s)",
                    payload.co_regate,
                )

            await tx.execute(
                "INSERT INTO trppu_scenario "
                "(co_regate, lb_scenario, co_roc, statut, dt_creation, "
                " dt_mise_en_oeuvre, dt_pivot, "
                " periode_debut, periode_fin, "
                " periode_realise_debut, periode_realise_fin, "
                " periode_prev_debut, periode_prev_fin, "
                " nb_jours_semaine, nb_jours_ouvres, nb_jours_ouvrables, nb_jours_scenario, "
                " id_pic_version, version_scenario, est_fige, id_rh_creation, id_rh_maj) "
                "VALUES (%s, %s, %s, 'EN COURS', NOW(), %s, NOW(), %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s, %s, 1, 0, %s, %s)",
                (
                    payload.co_regate,
                    payload.lb_scenario,
                    payload.co_roc,
                    dt_mise_en_oeuvre,
                    debut,
                    fin,
                    realise_debut,
                    realise_fin,
                    prev_debut,
                    prev_fin,
                    payload.nb_jours_semaine,
                    nbj.nb_jours_ouvres,
                    nbj.nb_jours_ouvrables,
                    nb_jours_scenario,
                    pic_version,
                    id_rh_token,
                    id_rh_token,
                ),
            )
            row = await tx.fetch_one("SELECT LAST_INSERT_ID() AS id")
            id_scenario = int(row["id"])

            # Trafics TMH (1 ligne par produit) — DSR-634 (réutilise le service TMH).
            if payload.tmh:
                from app.routes.trppu_tmh.helpers import upsert_tmh_rows

                nb_ins, _ = await upsert_tmh_rows(tx, id_scenario, payload.tmh)
                logger.info(
                    "Lignes TMH insérées à la création (id_scenario=%d, nb=%d)",
                    id_scenario,
                    nb_ins,
                )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur création scénario (payload=%s)",
            safe_preview(logged_payload),
        )
        raise HTTPException(status_code=500, detail="Erreur création scenario.") from e

    created = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Création scénario terminée (id_scenario=%d, co_regate=%s, duration_ms=%.1f)",
        id_scenario,
        payload.co_regate,
        duration_ms,
    )
    return created


@router.put("/{id_scenario}", response_model=ScenarioOut)
async def update_scenario(id_scenario: int, payload: ScenarioMajRequest):
    """DSR-656 : MAJ d'un scénario EN COURS après actualisation des trafics.

    Recalcule serveur les bornes réalisé/prév et les nb_jours (fériés + neutralisations),
    repositionne dt_pivot (exposé en API comme dt_real_prev) / dt_maj, crypte id_rh_maj,
    et met à jour le TMH (DSR-659).
    """
    start = time.perf_counter()
    logged = payload.model_dump(mode="json", exclude={"id_rh"})
    logger.info(
        "Début MAJ scénario (id_scenario=%d, payload=%s)", id_scenario, safe_preview(logged)
    )

    scenario = await fetch_scenario_or_404(id_scenario)
    if scenario["statut"] != "EN COURS":
        raise HTTPException(
            status_code=409,
            detail=(
                f"Mise à jour interdite : le scénario {id_scenario} est au statut "
                f"'{scenario['statut']}' (attendu 'EN COURS')."
            ),
        )

    realise_debut, realise_fin, prev_debut, prev_fin = recompute_realise_prev(
        payload.periode_debut, payload.periode_fin
    )
    nbj = await compute_nb_jours(payload.periode_debut, payload.periode_fin)
    base = nbj.nb_jours_ouvres if payload.nb_jours_semaine == 5 else nbj.nb_jours_ouvrables
    neut = await db_read.fetch_one(
        "SELECT COALESCE(SUM(nb_jour), 0) AS s FROM trppu_neutralisations WHERE id_scenario = %s",
        (id_scenario,),
    )
    nb_jours_scenario = base - (int(neut["s"]) if neut else 0)
    id_rh_token = encrypt_id_rh(payload.id_rh)

    set_parts = [
        "periode_debut = %s", "periode_fin = %s",
        "periode_realise_debut = %s", "periode_realise_fin = %s",
        "periode_prev_debut = %s", "periode_prev_fin = %s",
        "dt_pivot = NOW()",
        "nb_jours_semaine = %s", "nb_jours_ouvres = %s",
        "nb_jours_ouvrables = %s", "nb_jours_scenario = %s",
        "dt_maj = NOW()", "id_rh_maj = %s",
    ]
    params: list = [
        payload.periode_debut, payload.periode_fin,
        realise_debut, realise_fin, prev_debut, prev_fin,
        payload.nb_jours_semaine, nbj.nb_jours_ouvres, nbj.nb_jours_ouvrables,
        nb_jours_scenario, id_rh_token,
    ]
    if payload.dt_mise_en_oeuvre is not None:
        set_parts.insert(6, "dt_mise_en_oeuvre = %s")
        params.insert(6, payload.dt_mise_en_oeuvre)
    params.append(id_scenario)

    try:
        async with db_write.transaction() as tx:
            await tx.execute(
                f"UPDATE trppu_scenario SET {', '.join(set_parts)} WHERE id_scenario = %s",
                tuple(params),
            )
            await increment_version(tx, id_scenario)
            if payload.tmh:
                from app.routes.trppu_tmh.helpers import upsert_tmh_rows

                nb_ins, nb_upd = await upsert_tmh_rows(tx, id_scenario, payload.tmh)
                logger.info(
                    "TMH mis à jour à la MAJ scénario (id_scenario=%d, insérés=%d, modifiés=%d)",
                    id_scenario,
                    nb_ins,
                    nb_upd,
                )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erreur MAJ scénario (id_scenario=%d)", id_scenario)
        raise HTTPException(status_code=500, detail="Erreur mise à jour scenario.") from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "MAJ scénario terminée (id_scenario=%d, duration_ms=%.1f)", id_scenario, duration_ms
    )
    return updated


@router.get("/{id_scenario}/edition")
async def get_scenario_edition(
    id_scenario: int,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-654 : agrégateur d'édition — tous les blocs d'un scénario en un appel.

    Regroupe : entête scénario, périodes, TMH, comptages, variations, neutralisations,
    coefficients PIC (fusion défaut/scénario). Propage l'id de session IHM aux logs.
    """
    start = time.perf_counter()
    logger.info(
        "Début édition scénario (id_scenario=%d, id_session_ihm=%s)",
        id_scenario,
        safe_preview(id_session_ihm),
    )
    scenario = await fetch_scenario_or_404(id_scenario)

    # Imports locaux : évite tout cycle d'import entre modules de routes.
    from app.routes.trppu_comptages.helpers import SELECT_COMPTAGES_SQL
    from app.routes.trppu_neutralisations.helpers import (
        SELECT_NEUTRALISATIONS_SQL,
        group_neutralisations,
    )
    from app.routes.trppu_scenario_pic.helpers import (
        DEFAULT_PIC_VERSION,
        fetch_coeffs_for_version,
        fetch_scenario_pic_version,
        merge_coeffs,
    )
    from app.routes.trppu_tmh.helpers import fetch_tmh
    from app.routes.trppu_variations.helpers import SELECT_VARIATIONS_SQL

    try:
        tmh = await fetch_tmh(db_read, id_scenario)
        comptages = await db_read.fetch_all(SELECT_COMPTAGES_SQL, (id_scenario,))
        variations = await db_read.fetch_all(SELECT_VARIATIONS_SQL, (id_scenario,))
        neutralisations = group_neutralisations(
            await db_read.fetch_all(SELECT_NEUTRALISATIONS_SQL, (id_scenario,))
        )
        defaults = await fetch_coeffs_for_version(db_read, DEFAULT_PIC_VERSION)
        scen_v = await fetch_scenario_pic_version(db_read, id_scenario)
        overrides = (
            await fetch_coeffs_for_version(db_read, int(scen_v["id_pic_version"]))
            if scen_v
            else []
        )
        pic = {
            "id_pic_version_defaut": DEFAULT_PIC_VERSION,
            "id_pic_version_scenario": int(scen_v["id_pic_version"]) if scen_v else None,
            "niveau_scenario": scen_v["niveau"] if scen_v else None,
            "coefficients": merge_coeffs(defaults, overrides),
        }
    except Exception as e:
        logger.exception("Erreur édition scénario (id_scenario=%d)", id_scenario)
        raise HTTPException(status_code=500, detail="Erreur édition scenario.") from e

    periode_keys = (
        "periode_debut", "periode_fin", "periode_realise_debut", "periode_realise_fin",
        "periode_prev_debut", "periode_prev_fin", "nb_jours_semaine",
        "nb_jours_ouvres", "nb_jours_ouvrables", "nb_jours_scenario",
    )
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Édition scénario terminée (id_scenario=%d, tmh=%d, comptages=%d, variations=%d, duration_ms=%.1f)",
        id_scenario,
        len(tmh),
        len(comptages),
        len(variations),
        duration_ms,
    )
    return {
        "scenario": scenario,
        "periodes": {k: scenario.get(k) for k in periode_keys},
        "tmh": tmh,
        "comptages": comptages,
        "variations": variations,
        "neutralisations": neutralisations,
        "pic": pic,
    }


@router.delete("/{id_scenario}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_scenario(id_scenario: int):
    """Suppression DÉFINITIVE du scénario et de toutes ses données rattachées.

    Hard-delete (les tables enfants sont nettoyées explicitement, faute de FK).
    Pour un retrait réversible/conservant l'historique, utiliser plutôt
    POST /{id_scenario}/archive.
    """
    start = time.perf_counter()
    logger.info("Début suppression scénario (id_scenario=%d)", id_scenario)

    await fetch_scenario_or_404(id_scenario)

    try:
        async with db_write.transaction() as tx:
            await delete_scenario_cascade(tx, id_scenario)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erreur suppression scénario (id_scenario=%d)", id_scenario)
        raise HTTPException(status_code=500, detail="Erreur suppression scenario.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Suppression scénario terminée (id_scenario=%d, duration_ms=%.1f)",
        id_scenario,
        duration_ms,
    )
    return None


@router.post("/{id_scenario}/archive", response_model=ScenarioOut)
async def archive_scenario(id_scenario: int):
    """Archive le scénario : transition de statut vers ARCHIVE (retrait réversible).

    Conserve le scénario et ses données ; le scénario archivé devient non modifiable.
    Pour une suppression définitive, utiliser DELETE /{id_scenario}.
    """
    start = time.perf_counter()
    logger.info("Début archivage scénario (id_scenario=%d)", id_scenario)

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_transition_allowed(scenario["statut"], "ARCHIVE")
    logger.info(
        "Transition autorisée (depuis=%s vers ARCHIVE)", scenario["statut"]
    )

    try:
        async with db_write.transaction() as tx:
            await apply_transition_side_effects(tx, scenario, "ARCHIVE")
            await increment_version(tx, id_scenario)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur archivage scénario (id_scenario=%d, statut_courant=%s)",
            id_scenario,
            scenario.get("statut"),
        )
        raise HTTPException(status_code=500, detail="Erreur archivage scenario.") from e

    archived = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Archivage scénario terminé (id_scenario=%d, statut=ARCHIVE, duration_ms=%.1f)",
        id_scenario,
        duration_ms,
    )
    return archived


@router.patch("/{id_scenario}/periodes", response_model=ScenarioOut)
async def update_periodes(id_scenario: int, payload: PeriodeUpdate):
    """MAJ des bornes principales. Les bornes realise/prev sont recalculées serveur."""
    start = time.perf_counter()
    fields = payload.model_dump(exclude_unset=True)
    logger.info(
        "Début mise à jour périodes (id_scenario=%d, champs=%s)",
        id_scenario,
        safe_preview(fields),
    )

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    new_debut = fields.get("periode_debut", scenario["periode_debut"])
    new_fin = fields.get("periode_fin", scenario["periode_fin"])
    if new_fin < new_debut:
        raise HTTPException(
            status_code=400, detail="periode_fin doit être >= periode_debut."
        )

    realise_debut, realise_fin, prev_debut, prev_fin = recompute_realise_prev(new_debut, new_fin)
    logger.info(
        "Bornes réalisé/prévision recalculées (réalisé=[%s, %s], prévision=[%s, %s])",
        realise_debut,
        realise_fin,
        prev_debut,
        prev_fin,
    )

    try:
        async with db_write.transaction() as tx:
            await tx.execute(
                "UPDATE trppu_scenario SET "
                "periode_debut = %s, periode_fin = %s, "
                "periode_realise_debut = %s, periode_realise_fin = %s, "
                "periode_prev_debut = %s, periode_prev_fin = %s "
                "WHERE id_scenario = %s",
                (
                    new_debut,
                    new_fin,
                    realise_debut,
                    realise_fin,
                    prev_debut,
                    prev_fin,
                    id_scenario,
                ),
            )
            await increment_version(tx, id_scenario)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur mise à jour périodes (id_scenario=%d, champs=%s)",
            id_scenario,
            safe_preview(fields),
        )
        raise HTTPException(status_code=500, detail="Erreur mise à jour périodes.") from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Mise à jour périodes terminée (id_scenario=%d, duration_ms=%.1f)",
        id_scenario,
        duration_ms,
    )
    return updated


@router.patch("/{id_scenario}/nb-jours-semaine", response_model=ScenarioOut)
async def update_nb_jours_semaine(id_scenario: int, payload: NbJoursUpdate):
    start = time.perf_counter()
    logger.info(
        "Début mise à jour nb_jours_semaine (id_scenario=%d, nb_jours_semaine=%s)",
        id_scenario,
        payload.nb_jours_semaine,
    )

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    try:
        async with db_write.transaction() as tx:
            await tx.execute(
                "UPDATE trppu_scenario SET nb_jours_semaine = %s WHERE id_scenario = %s",
                (payload.nb_jours_semaine, id_scenario),
            )
            await increment_version(tx, id_scenario)
    except Exception as e:
        logger.exception(
            "Erreur update nb_jours_semaine (id_scenario=%d, nb_jours_semaine=%s)",
            id_scenario,
            payload.nb_jours_semaine,
        )
        raise HTTPException(
            status_code=500, detail="Erreur mise à jour nb_jours_semaine."
        ) from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Mise à jour nb_jours_semaine terminée (id_scenario=%d, duration_ms=%.1f)",
        id_scenario,
        duration_ms,
    )
    return updated


@router.patch("/{id_scenario}/statut", response_model=ScenarioOut)
async def update_statut(id_scenario: int, payload: StatutUpdate):
    """Change le statut via la machine à états + effets de bord automatiques.

    La transition VALIDE -> EN PRODUCTION n'est pas accessible ici : passer par
    POST /{id_scenario}/mise-en-prod.
    """
    start = time.perf_counter()
    logger.info(
        "Début changement de statut (id_scenario=%d, statut_cible=%s)",
        id_scenario,
        payload.statut,
    )

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_transition_allowed(scenario["statut"], payload.statut)
    logger.info(
        "Transition autorisée (depuis=%s vers %s)",
        scenario["statut"],
        payload.statut,
    )

    try:
        async with db_write.transaction() as tx:
            await apply_transition_side_effects(tx, scenario, payload.statut)
            await increment_version(tx, id_scenario)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur transition de statut (id_scenario=%d, depuis=%s, vers=%s)",
            id_scenario,
            scenario.get("statut"),
            payload.statut,
        )
        raise HTTPException(status_code=500, detail="Erreur transition de statut.") from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Changement de statut terminé (id_scenario=%d, statut=%s, duration_ms=%.1f)",
        id_scenario,
        payload.statut,
        duration_ms,
    )
    return updated


@router.post("/{id_scenario}/mise-en-prod", response_model=ScenarioOut)
async def mise_en_prod(id_scenario: int):
    """Notifie la mise en production : statut EN PRODUCTION + dt_mise_en_prod = NOW() + est_fige = 1.

    Seule manière d'atteindre le statut EN PRODUCTION. Transition autorisée uniquement
    depuis VALIDE.
    """
    start = time.perf_counter()
    logger.info("Début mise en production (id_scenario=%d)", id_scenario)

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_internal_transition_allowed(scenario["statut"], "EN PRODUCTION")
    logger.info(
        "Transition autorisée (depuis=%s vers EN PRODUCTION)", scenario["statut"]
    )

    try:
        async with db_write.transaction() as tx:
            await apply_transition_side_effects(tx, scenario, "EN PRODUCTION")
            await increment_version(tx, id_scenario)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur mise en production (id_scenario=%d, statut_courant=%s)",
            id_scenario,
            scenario.get("statut"),
        )
        raise HTTPException(status_code=500, detail="Erreur mise en production.") from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Mise en production terminée (id_scenario=%d, statut=EN PRODUCTION, duration_ms=%.1f)",
        id_scenario,
        duration_ms,
    )
    return updated


@router.patch("/{id_scenario}/est-fige", response_model=ScenarioOut)
async def update_est_fige(id_scenario: int, payload: FigeUpdate):
    """Force le flag est_fige (seul moyen de défiger un scénario après mise en prod)."""
    start = time.perf_counter()
    logger.info(
        "Début mise à jour est_fige (id_scenario=%d, est_fige=%s)",
        id_scenario,
        payload.est_fige,
    )

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_not_archive(scenario)

    try:
        async with db_write.transaction() as tx:
            await tx.execute(
                "UPDATE trppu_scenario SET est_fige = %s WHERE id_scenario = %s",
                (1 if payload.est_fige else 0, id_scenario),
            )
            await increment_version(tx, id_scenario)
    except Exception as e:
        logger.exception(
            "Erreur update est_fige (id_scenario=%d, est_fige=%s)",
            id_scenario,
            payload.est_fige,
        )
        raise HTTPException(status_code=500, detail="Erreur mise à jour est_fige.") from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Mise à jour est_fige terminée (id_scenario=%d, est_fige=%s, duration_ms=%.1f)",
        id_scenario,
        payload.est_fige,
        duration_ms,
    )
    return updated


@router.patch("/{id_scenario}/lb-scenario", response_model=ScenarioOut)
async def update_lb_scenario(id_scenario: int, payload: LbScenarioUpdate):
    start = time.perf_counter()
    logger.info(
        "Début mise à jour libellé scénario (id_scenario=%d, lb_scenario=%s)",
        id_scenario,
        safe_preview(payload.lb_scenario),
    )

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    try:
        async with db_write.transaction() as tx:
            await tx.execute(
                "UPDATE trppu_scenario SET lb_scenario = %s WHERE id_scenario = %s",
                (payload.lb_scenario, id_scenario),
            )
            await increment_version(tx, id_scenario)
    except Exception as e:
        logger.exception(
            "Erreur mise à jour libellé scénario (id_scenario=%d, lb_scenario=%s)",
            id_scenario,
            safe_preview(payload.lb_scenario),
        )
        raise HTTPException(status_code=500, detail="Erreur mise à jour libellé.") from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Mise à jour libellé scénario terminée (id_scenario=%d, duration_ms=%.1f)",
        id_scenario,
        duration_ms,
    )
    return updated


@router.post(
    "/{id_scenario}/duplicate",
    response_model=ScenarioOut,
    status_code=status.HTTP_201_CREATED,
)
async def duplicate_scenario(id_scenario: int, payload: DuplicateRequest | None = None):
    """Duplique un scénario en nouveau EN COURS, version 1, est_fige=0.

    Pas de tracking parent (id_scenario_parent retiré du modèle).
    """
    start = time.perf_counter()
    logger.info("Début duplication scénario (source_id=%d)", id_scenario)

    source = await fetch_scenario_or_404(id_scenario)

    new_lb = (
        payload.lb_scenario
        if payload and payload.lb_scenario
        else f"{source['lb_scenario']} (copie)"
    )
    if len(new_lb) > 50:
        new_lb = new_lb[:50]
    logger.info("Nouveau libellé résolu (lb_scenario=%s)", safe_preview(new_lb))

    try:
        async with db_write.transaction() as tx:
            await tx.execute(
                "INSERT INTO trppu_scenario "
                "(co_regate, lb_scenario, co_roc, statut, dt_creation, "
                " periode_debut, periode_fin, "
                " periode_realise_debut, periode_realise_fin, "
                " periode_prev_debut, periode_prev_fin, "
                " nb_jours_semaine, id_pic_version, version_scenario, est_fige) "
                "VALUES (%s, %s, %s, 'EN COURS', NOW(), %s, %s, %s, %s, %s, %s, %s, %s, 1, 0)",
                (
                    source["co_regate"],
                    new_lb,
                    source["co_roc"],
                    source["periode_debut"],
                    source["periode_fin"],
                    source["periode_realise_debut"],
                    source["periode_realise_fin"],
                    source["periode_prev_debut"],
                    source["periode_prev_fin"],
                    source["nb_jours_semaine"],
                    source["id_pic_version"],
                ),
            )
            row = await tx.fetch_one("SELECT LAST_INSERT_ID() AS id")
            new_id = int(row["id"])
    except Exception as e:
        logger.exception(
            "Erreur duplication scénario (source_id=%d, new_lb=%s)",
            id_scenario,
            safe_preview(new_lb),
        )
        raise HTTPException(status_code=500, detail="Erreur duplication scenario.") from e

    duplicated = await fetch_scenario_or_404(new_id)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Duplication scénario terminée (source_id=%d, new_id=%d, duration_ms=%.1f)",
        id_scenario,
        new_id,
        duration_ms,
    )
    return duplicated
