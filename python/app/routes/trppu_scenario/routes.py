"""Routes CRUD et workflow de statut pour la table trppu_scenario."""

import logging
import time
from datetime import date

from fastapi import APIRouter, HTTPException, Query, status

from app.db.mysql import db_read, db_write
from app.log_utils import ctx, diff_champs, params_loggables
from app.security.crypto import encrypt_id_rh
from app.services.api_log import (
    ACTION_ARCHIVAGE_SCENARIO,
    ACTION_CREATION_SCENARIO,
    ACTION_DUPLICATION_SCENARIO,
    ACTION_MAJ_SCENARIO,
    ACTION_SUPPRESSION_SCENARIO,
    ACTION_TRANSITION_STATUT,
    enregistrer_appel,
)
from app.services.jours_service import compute_nb_jours

from .helpers import (
    SELECT_SCENARIO_SQL,
    assert_editable,
    assert_not_archive,
    default_periode,
    delete_scenario_cascade,
    duplicate_scenario_children,
    duplicate_scenario_pic_version,
    ensure_site_exists,
    fetch_scenario_or_404,
    increment_version,
    last_insert_id,
    recompute_realise_prev,
    resolve_default_pic_version,
)
from .schemas import (
    DuplicateRequest,
    FigementParStatutRequest,
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
    STATUTS_EDITABLES,
    apply_transition_side_effects,
    assert_internal_transition_allowed,
    assert_transition_allowed,
    resolve_fige_from_statut,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/scenarios", tags=["Scenarios"])


async def _libelles_produits(tmh) -> dict[str, str]:
    """Libellés objets Databricks, pour créer à la volée les produits d'un lot TMH.

    Interrogé uniquement s'il y a des lignes TMH à écrire, et toujours **hors** transaction.
    """
    if not tmh:
        return {}
    from app.routes.trppu_tmh.helpers import resolve_libelles_produits

    return await resolve_libelles_produits()


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
    logger.info("Début listing scénarios %s", ctx(filtres=filters))

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
        logger.exception("Erreur listing scénarios %s", ctx(filtres=filters))
        raise HTTPException(status_code=500, detail="Erreur listing scenarios.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin listing scénarios %s", ctx(count=len(rows), duration_ms=duration_ms)
    )
    return rows


@router.get("/enums")
async def list_enums():
    """Valeurs autorisées pour les colonnes ENUM de trppu_scenario."""
    logger.debug("Lecture enums scénarios %s", ctx(count=len(STATUTS)))
    return {"statut": list(STATUTS)}


@router.get("/{id_scenario}", response_model=ScenarioOut)
async def get_scenario(id_scenario: int):
    start = time.perf_counter()
    logger.info("Début lecture scénario %s", ctx(id_scenario=id_scenario))
    row = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin lecture scénario %s",
        ctx(id_scenario=id_scenario, statut=row.get("statut"), duration_ms=duration_ms),
    )
    return row


@router.get("/{id_scenario}/periodes", response_model=ScenarioPeriodesOut)
async def get_scenario_periodes(
    id_scenario: int,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-655 : périodes + nombres de jours d'un scénario (actualisation du slider IHM)."""
    start = time.perf_counter()
    logger.info("Début lecture périodes scénario %s", ctx(id_scenario=id_scenario))
    row = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin lecture périodes scénario %s",
        ctx(id_scenario=id_scenario, duration_ms=duration_ms),
    )
    return row


@router.post("", response_model=ScenarioOut, status_code=status.HTTP_201_CREATED)
async def create_scenario(payload: ScenarioCreate):
    start = time.perf_counter()
    # `params_loggables` retire id_rh (et tout champ sensible) : la règle ne
    # dépend plus du soin apporté à chaque appel.
    logged_payload = params_loggables(payload)
    logger.info(
        "Début création scénario %s",
        ctx(co_regate=payload.co_regate, params=logged_payload),
    )

    if payload.id_pic_version is not None:
        pic_version = payload.id_pic_version
    else:
        pic_version = await resolve_default_pic_version()
        logger.debug(
            "Version PIC par défaut résolue %s", ctx(id_pic_version=pic_version)
        )

    debut, fin = payload.periode_debut, payload.periode_fin
    if debut is None or fin is None:
        d, f = default_periode()
        debut = debut or d
        fin = fin or f
        logger.debug("Période par défaut appliquée %s", ctx(debut=debut, fin=fin))

    realise_debut, realise_fin, prev_debut, prev_fin = recompute_realise_prev(debut, fin)
    logger.debug(
        "Bornes réalisé/prévision calculées %s",
        ctx(
            realise_debut=realise_debut,
            realise_fin=realise_fin,
            prev_debut=prev_debut,
            prev_fin=prev_fin,
        ),
    )

    # Nombres de jours sur la période (fériés déduits) — DSR-613 / DSR-634.
    nbj = await compute_nb_jours(debut, fin)
    nb_jours_scenario = (
        nbj.nb_jours_ouvres if payload.nb_jours_semaine == 5 else nbj.nb_jours_ouvrables
    )
    logger.debug(
        "Nombres de jours calculés %s",
        ctx(
            nb_jours_ouvres=nbj.nb_jours_ouvres,
            nb_jours_ouvrables=nbj.nb_jours_ouvrables,
            nb_jours_scenario=nb_jours_scenario,
        ),
    )

    dt_mise_en_oeuvre = payload.dt_mise_en_oeuvre or date.today()
    id_rh_token = encrypt_id_rh(payload.id_rh)

    # Résolu hors transaction (appel Databricks synchrone) : sert à libeller les produits
    # créés à la volée pour les lignes TMH.
    libelles_produits = await _libelles_produits(payload.tmh)

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
                    "Site créé automatiquement %s",
                    ctx(co_regate=payload.co_regate, type_site=payload.type_site),
                )
            else:
                logger.debug(
                    "Site déjà présent dans trppu_site %s",
                    ctx(co_regate=payload.co_regate),
                )

            rows_inseres = await tx.execute(
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
            id_scenario = await last_insert_id(tx)
            logger.info(
                "Scénario inséré %s",
                ctx(
                    id_scenario=id_scenario,
                    co_regate=payload.co_regate,
                    rows_affected=rows_inseres,
                ),
            )

            # Trafics TMH (1 ligne par produit) — DSR-634 (réutilise le service TMH).
            if payload.tmh:
                from app.routes.trppu_produit.helpers import ensure_produits_exist
                from app.routes.trppu_tmh.helpers import upsert_tmh_rows

                crees = await ensure_produits_exist(
                    tx, [item.co_produit for item in payload.tmh], libelles_produits
                )
                if crees:
                    logger.info(
                        "Produits créés automatiquement %s",
                        ctx(id_scenario=id_scenario, nb=len(crees), codes=crees),
                    )

                nb_ins, _ = await upsert_tmh_rows(
                    tx, id_scenario, payload.tmh, id_rh=id_rh_token
                )
                logger.info(
                    "Lignes TMH insérées à la création %s",
                    ctx(id_scenario=id_scenario, inseres=nb_ins),
                )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur création scénario %s",
            ctx(co_regate=payload.co_regate, params=logged_payload),
        )
        raise HTTPException(status_code=500, detail="Erreur création scenario.") from e

    created = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_CREATION_SCENARIO,
        id_scenario=id_scenario,
        regate=payload.co_regate,
        params=logged_payload,
    )
    logger.info(
        "Fin création scénario %s",
        ctx(
            id_scenario=id_scenario,
            co_regate=payload.co_regate,
            rows_affected=rows_inseres,
            duration_ms=duration_ms,
        ),
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
    logged = params_loggables(payload)
    logger.info(
        "Début MAJ scénario %s", ctx(id_scenario=id_scenario, params=logged)
    )

    scenario = await fetch_scenario_or_404(id_scenario)
    if scenario["statut"] not in STATUTS_EDITABLES:
        # Rejet métier : tracé en WARNING, sinon un 409 ne laisse aucune trace.
        logger.warning(
            "Rejet MAJ scénario %s",
            ctx(
                id_scenario=id_scenario,
                statut=scenario["statut"],
                http=409,
                motif="statut non éditable",
            ),
        )
        raise HTTPException(
            status_code=409,
            detail=(
                f"Mise à jour interdite : le scénario {id_scenario} est au statut "
                f"'{scenario['statut']}' (attendu : {' ou '.join(STATUTS_EDITABLES)})."
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

    libelles_produits = await _libelles_produits(payload.tmh)

    try:
        async with db_write.transaction() as tx:
            rows_maj = await tx.execute(
                f"UPDATE trppu_scenario SET {', '.join(set_parts)} WHERE id_scenario = %s",
                tuple(params),
            )
            await increment_version(tx, id_scenario)
            if payload.tmh:
                from app.routes.trppu_produit.helpers import ensure_produits_exist
                from app.routes.trppu_tmh.helpers import upsert_tmh_rows

                crees = await ensure_produits_exist(
                    tx, [item.co_produit for item in payload.tmh], libelles_produits
                )
                if crees:
                    logger.info(
                        "Produits créés automatiquement %s",
                        ctx(id_scenario=id_scenario, nb=len(crees), codes=crees),
                    )

                nb_ins, nb_upd = await upsert_tmh_rows(
                    tx, id_scenario, payload.tmh, id_rh=id_rh_token
                )
                logger.info(
                    "TMH mis à jour à la MAJ scénario %s",
                    ctx(id_scenario=id_scenario, inseres=nb_ins, modifies=nb_upd),
                )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur MAJ scénario %s", ctx(id_scenario=id_scenario, params=logged)
        )
        raise HTTPException(status_code=500, detail="Erreur mise à jour scenario.") from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    # `scenario` a été relu avant l'écriture : le delta rend la MAJ rejouable
    # depuis les seuls logs, sans requête supplémentaire.
    delta = diff_champs(scenario, updated)
    await enregistrer_appel(
        api_name=ACTION_MAJ_SCENARIO,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={"params": logged, "delta": delta},
    )
    logger.info(
        "Fin MAJ scénario %s",
        ctx(
            id_scenario=id_scenario,
            co_regate=scenario.get("co_regate"),
            rows_affected=rows_maj,
            delta=delta,
            duration_ms=duration_ms,
        ),
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
    logger.info("Début édition scénario %s", ctx(id_scenario=id_scenario))
    scenario = await fetch_scenario_or_404(id_scenario)

    # Imports locaux : évite tout cycle d'import entre modules de routes.
    from app.routes.trppu_comptages.helpers import SELECT_COMPTAGES_SQL
    from app.routes.trppu_neutralisations.helpers import SELECT_NEUTRALISATIONS_SQL
    from app.routes.trppu_scenario_pic.helpers import (
        fetch_coeffs_for_version,
        fetch_scenario_pic_version,
        merge_coeffs,
        resolve_default_pic_version,
    )
    from app.routes.trppu_tmh.helpers import fetch_tmh
    from app.routes.trppu_variations.helpers import SELECT_VARIATIONS_SQL

    try:
        tmh = await fetch_tmh(db_read, id_scenario)
        comptages = await db_read.fetch_all(SELECT_COMPTAGES_SQL, (id_scenario,))
        # SELECT_VARIATIONS_SQL attend DEUX paramètres id_scenario (sous-requête TMH
        # + jointure des variations) — cf. sa docstring.
        variations = await db_read.fetch_all(SELECT_VARIATIONS_SQL, (id_scenario, id_scenario))
        # DSR-645 (motif libre) : liste à plat des neutralisations (plus de regroupement).
        neutralisations = await db_read.fetch_all(SELECT_NEUTRALISATIONS_SQL, (id_scenario,))
        id_pic_version_defaut = await resolve_default_pic_version(db_read)
        defaults = await fetch_coeffs_for_version(db_read, id_pic_version_defaut)
        scen_v = await fetch_scenario_pic_version(db_read, id_scenario)
        overrides = (
            await fetch_coeffs_for_version(db_read, int(scen_v["id_pic_version"]))
            if scen_v
            else []
        )
        pic = {
            "id_pic_version_defaut": id_pic_version_defaut,
            "id_pic_version_scenario": int(scen_v["id_pic_version"]) if scen_v else None,
            "niveau_scenario": scen_v["niveau"] if scen_v else None,
            "coefficients": merge_coeffs(defaults, overrides),
        }
    except Exception as e:
        logger.exception("Erreur édition scénario %s", ctx(id_scenario=id_scenario))
        raise HTTPException(status_code=500, detail="Erreur édition scenario.") from e

    periode_keys = (
        "periode_debut", "periode_fin", "periode_realise_debut", "periode_realise_fin",
        "periode_prev_debut", "periode_prev_fin", "nb_jours_semaine",
        "nb_jours_ouvres", "nb_jours_ouvrables", "nb_jours_scenario",
    )
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin édition scénario %s",
        ctx(
            id_scenario=id_scenario,
            tmh=len(tmh),
            comptages=len(comptages),
            variations=len(variations),
            neutralisations=len(neutralisations),
            duration_ms=duration_ms,
        ),
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
    logger.info("Début suppression scénario %s", ctx(id_scenario=id_scenario))

    # Le hard-delete est irréversible : l'état complet est journalisé AVANT
    # l'écriture, c'est la seule base de reconstitution possible ensuite.
    avant = await fetch_scenario_or_404(id_scenario)
    etat_avant = params_loggables(dict(avant))
    logger.info(
        "État avant suppression scénario %s",
        ctx(id_scenario=id_scenario, etat=etat_avant),
    )

    try:
        async with db_write.transaction() as tx:
            supprimes = await delete_scenario_cascade(tx, id_scenario)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur suppression scénario %s",
            ctx(id_scenario=id_scenario, etat=etat_avant),
        )
        raise HTTPException(status_code=500, detail="Erreur suppression scenario.") from e

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    # id_scenario laissé à None : le scénario n'existe plus, la FK de
    # trppu_api_log le refuserait. L'id reste porté par `params`.
    await enregistrer_appel(
        api_name=ACTION_SUPPRESSION_SCENARIO,
        id_scenario=None,
        regate=avant.get("co_regate"),
        params={
            "id_scenario": id_scenario,
            "etat_avant": etat_avant,
            "lignes_supprimees": supprimes,
        },
    )
    logger.info(
        "Fin suppression scénario %s",
        ctx(
            id_scenario=id_scenario,
            co_regate=avant.get("co_regate"),
            rows_affected=sum(supprimes.values()),
            par_table={t: c for t, c in supprimes.items() if c},
            duration_ms=duration_ms,
        ),
    )
    return None


@router.post("/{id_scenario}/archive", response_model=ScenarioOut)
async def archive_scenario(id_scenario: int):
    """Archive le scénario : transition de statut vers ARCHIVE (retrait réversible).

    Conserve le scénario et ses données ; le scénario archivé devient non modifiable.
    Pour une suppression définitive, utiliser DELETE /{id_scenario}.
    """
    start = time.perf_counter()
    logger.info("Début archivage scénario %s", ctx(id_scenario=id_scenario))

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_transition_allowed(scenario["statut"], "ARCHIVE")
    logger.debug(
        "Transition autorisée %s", ctx(depuis=scenario["statut"], vers="ARCHIVE")
    )

    try:
        async with db_write.transaction() as tx:
            await apply_transition_side_effects(tx, scenario, "ARCHIVE")
            await increment_version(tx, id_scenario)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur archivage scénario %s",
            ctx(id_scenario=id_scenario, statut_courant=scenario.get("statut")),
        )
        raise HTTPException(status_code=500, detail="Erreur archivage scenario.") from e

    archived = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_ARCHIVAGE_SCENARIO,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={"statut_avant": scenario.get("statut"), "statut_apres": "ARCHIVE"},
    )
    logger.info(
        "Fin archivage scénario %s",
        ctx(
            id_scenario=id_scenario,
            statut_avant=scenario.get("statut"),
            statut="ARCHIVE",
            duration_ms=duration_ms,
        ),
    )
    return archived


@router.patch("/{id_scenario}/periodes", response_model=ScenarioOut)
async def update_periodes(id_scenario: int, payload: PeriodeUpdate):
    """MAJ des bornes principales. Les bornes realise/prev sont recalculées serveur."""
    start = time.perf_counter()
    fields = payload.model_dump(exclude_unset=True)
    logged = params_loggables(fields)
    logger.info(
        "Début MAJ périodes scénario %s",
        ctx(id_scenario=id_scenario, params=logged),
    )

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    new_debut = fields.get("periode_debut", scenario["periode_debut"])
    new_fin = fields.get("periode_fin", scenario["periode_fin"])
    if new_fin < new_debut:
        logger.warning(
            "Rejet MAJ périodes scénario %s",
            ctx(
                id_scenario=id_scenario,
                periode_debut=new_debut,
                periode_fin=new_fin,
                http=400,
                motif="periode_fin < periode_debut",
            ),
        )
        raise HTTPException(
            status_code=400, detail="periode_fin doit être >= periode_debut."
        )

    realise_debut, realise_fin, prev_debut, prev_fin = recompute_realise_prev(new_debut, new_fin)
    logger.debug(
        "Bornes réalisé/prévision recalculées %s",
        ctx(
            realise_debut=realise_debut,
            realise_fin=realise_fin,
            prev_debut=prev_debut,
            prev_fin=prev_fin,
        ),
    )

    try:
        async with db_write.transaction() as tx:
            rows_maj = await tx.execute(
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
            "Erreur MAJ périodes scénario %s",
            ctx(id_scenario=id_scenario, params=logged),
        )
        raise HTTPException(status_code=500, detail="Erreur mise à jour périodes.") from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    delta = diff_champs(scenario, updated)
    await enregistrer_appel(
        api_name=ACTION_MAJ_SCENARIO,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={"cible": "periodes", "params": logged, "delta": delta},
    )
    logger.info(
        "Fin MAJ périodes scénario %s",
        ctx(
            id_scenario=id_scenario,
            rows_affected=rows_maj,
            delta=delta,
            duration_ms=duration_ms,
        ),
    )
    return updated


@router.patch("/{id_scenario}/nb-jours-semaine", response_model=ScenarioOut)
async def update_nb_jours_semaine(id_scenario: int, payload: NbJoursUpdate):
    start = time.perf_counter()
    logger.info(
        "Début MAJ nb_jours_semaine %s",
        ctx(id_scenario=id_scenario, nb_jours_semaine=payload.nb_jours_semaine),
    )

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    try:
        async with db_write.transaction() as tx:
            rows_maj = await tx.execute(
                "UPDATE trppu_scenario SET nb_jours_semaine = %s WHERE id_scenario = %s",
                (payload.nb_jours_semaine, id_scenario),
            )
            await increment_version(tx, id_scenario)
    except Exception as e:
        logger.exception(
            "Erreur MAJ nb_jours_semaine %s",
            ctx(id_scenario=id_scenario, nb_jours_semaine=payload.nb_jours_semaine),
        )
        raise HTTPException(
            status_code=500, detail="Erreur mise à jour nb_jours_semaine."
        ) from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    delta = diff_champs(scenario, updated)
    await enregistrer_appel(
        api_name=ACTION_MAJ_SCENARIO,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={"cible": "nb_jours_semaine", "delta": delta},
    )
    logger.info(
        "Fin MAJ nb_jours_semaine %s",
        ctx(
            id_scenario=id_scenario,
            nb_jours_semaine=payload.nb_jours_semaine,
            rows_affected=rows_maj,
            delta=delta,
            duration_ms=duration_ms,
        ),
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
        "Début transition statut scénario %s",
        ctx(id_scenario=id_scenario, statut_cible=payload.statut),
    )

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_transition_allowed(scenario["statut"], payload.statut)
    logger.debug(
        "Transition autorisée %s",
        ctx(depuis=scenario["statut"], vers=payload.statut),
    )

    try:
        async with db_write.transaction() as tx:
            await apply_transition_side_effects(tx, scenario, payload.statut)
            await increment_version(tx, id_scenario)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur transition statut scénario %s",
            ctx(
                id_scenario=id_scenario,
                depuis=scenario.get("statut"),
                vers=payload.statut,
            ),
        )
        raise HTTPException(status_code=500, detail="Erreur transition de statut.") from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_TRANSITION_STATUT,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={
            "statut_avant": scenario.get("statut"),
            "statut_apres": payload.statut,
            "delta": diff_champs(scenario, updated),
        },
    )
    logger.info(
        "Fin transition statut scénario %s",
        ctx(
            id_scenario=id_scenario,
            statut_avant=scenario.get("statut"),
            statut=payload.statut,
            duration_ms=duration_ms,
        ),
    )
    return updated


@router.post("/{id_scenario}/mise-en-prod", response_model=ScenarioOut)
async def mise_en_prod(id_scenario: int):
    """Notifie la mise en production : statut EN PRODUCTION + dt_mise_en_prod = NOW() + est_fige = 1.

    Seule manière d'atteindre le statut EN PRODUCTION. Transition autorisée uniquement
    depuis VALIDE.
    """
    start = time.perf_counter()
    logger.info("Début mise en production scénario %s", ctx(id_scenario=id_scenario))

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_internal_transition_allowed(scenario["statut"], "EN PRODUCTION")
    logger.debug(
        "Transition autorisée %s",
        ctx(depuis=scenario["statut"], vers="EN PRODUCTION"),
    )

    try:
        async with db_write.transaction() as tx:
            await apply_transition_side_effects(tx, scenario, "EN PRODUCTION")
            await increment_version(tx, id_scenario)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur mise en production scénario %s",
            ctx(id_scenario=id_scenario, statut_courant=scenario.get("statut")),
        )
        raise HTTPException(status_code=500, detail="Erreur mise en production.") from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_TRANSITION_STATUT,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={
            "statut_avant": scenario.get("statut"),
            "statut_apres": "EN PRODUCTION",
            "delta": diff_champs(scenario, updated),
        },
    )
    logger.info(
        "Fin mise en production scénario %s",
        ctx(
            id_scenario=id_scenario,
            statut_avant=scenario.get("statut"),
            statut="EN PRODUCTION",
            duration_ms=duration_ms,
        ),
    )
    return updated


@router.patch("/{id_scenario}/est-fige", response_model=ScenarioOut)
async def update_est_fige(id_scenario: int, payload: FigeUpdate):
    """Force le flag est_fige (seul moyen de défiger un scénario après mise en prod)."""
    start = time.perf_counter()
    logger.info(
        "Début MAJ est_fige scénario %s",
        ctx(id_scenario=id_scenario, est_fige=payload.est_fige),
    )

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_not_archive(scenario)

    try:
        async with db_write.transaction() as tx:
            rows_maj = await tx.execute(
                "UPDATE trppu_scenario SET est_fige = %s WHERE id_scenario = %s",
                (1 if payload.est_fige else 0, id_scenario),
            )
            await increment_version(tx, id_scenario)
    except Exception as e:
        logger.exception(
            "Erreur MAJ est_fige scénario %s",
            ctx(id_scenario=id_scenario, est_fige=payload.est_fige),
        )
        raise HTTPException(status_code=500, detail="Erreur mise à jour est_fige.") from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_MAJ_SCENARIO,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={
            "cible": "est_fige",
            "est_fige_avant": scenario.get("est_fige"),
            "est_fige_apres": payload.est_fige,
        },
    )
    logger.info(
        "Fin MAJ est_fige scénario %s",
        ctx(
            id_scenario=id_scenario,
            est_fige_avant=scenario.get("est_fige"),
            est_fige=payload.est_fige,
            rows_affected=rows_maj,
            duration_ms=duration_ms,
        ),
    )
    return updated


@router.patch("/{id_scenario}/figement", response_model=ScenarioOut)
async def update_figement_par_statut(id_scenario: int, payload: FigementParStatutRequest):
    """DSR-669 : fige (1) ou défige (0) le scénario selon le statut reçu de l'IHM.

    "validé"/"simulation" -> est_fige=1 ; "en cours" -> est_fige=0 ;
    tout autre statut -> 422 (paramètre inconnu, aucune action réalisée).
    Met à jour uniquement le champ est_fige, pas le statut du scénario.
    """
    start = time.perf_counter()
    logger.info(
        "Début figement par statut scénario %s",
        ctx(id_scenario=id_scenario, statut=payload.statut),
    )

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_not_archive(scenario)
    est_fige = resolve_fige_from_statut(payload.statut)  # lève 422 si statut inconnu

    try:
        async with db_write.transaction() as tx:
            rows_maj = await tx.execute(
                "UPDATE trppu_scenario SET est_fige = %s WHERE id_scenario = %s",
                (1 if est_fige else 0, id_scenario),
            )
            await increment_version(tx, id_scenario)
    except Exception as e:
        logger.exception(
            "Erreur figement par statut scénario %s",
            ctx(id_scenario=id_scenario, statut=payload.statut),
        )
        raise HTTPException(status_code=500, detail="Erreur figement par statut.") from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_MAJ_SCENARIO,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={
            "cible": "est_fige",
            "statut_recu": payload.statut,
            "est_fige_avant": scenario.get("est_fige"),
            "est_fige_apres": est_fige,
        },
    )
    logger.info(
        "Fin figement par statut scénario %s",
        ctx(
            id_scenario=id_scenario,
            statut=payload.statut,
            est_fige_avant=scenario.get("est_fige"),
            est_fige=est_fige,
            rows_affected=rows_maj,
            duration_ms=duration_ms,
        ),
    )
    return updated


@router.patch("/{id_scenario}/lb-scenario", response_model=ScenarioOut)
async def update_lb_scenario(id_scenario: int, payload: LbScenarioUpdate):
    start = time.perf_counter()
    logger.info(
        "Début MAJ libellé scénario %s",
        ctx(id_scenario=id_scenario, lb_scenario=payload.lb_scenario),
    )

    scenario = await fetch_scenario_or_404(id_scenario)
    assert_editable(scenario)

    try:
        async with db_write.transaction() as tx:
            rows_maj = await tx.execute(
                "UPDATE trppu_scenario SET lb_scenario = %s WHERE id_scenario = %s",
                (payload.lb_scenario, id_scenario),
            )
            await increment_version(tx, id_scenario)
    except Exception as e:
        logger.exception(
            "Erreur MAJ libellé scénario %s",
            ctx(id_scenario=id_scenario, lb_scenario=payload.lb_scenario),
        )
        raise HTTPException(status_code=500, detail="Erreur mise à jour libellé.") from e

    updated = await fetch_scenario_or_404(id_scenario)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_MAJ_SCENARIO,
        id_scenario=id_scenario,
        regate=scenario.get("co_regate"),
        params={
            "cible": "lb_scenario",
            "lb_avant": scenario.get("lb_scenario"),
            "lb_apres": payload.lb_scenario,
        },
    )
    logger.info(
        "Fin MAJ libellé scénario %s",
        ctx(
            id_scenario=id_scenario,
            lb_avant=scenario.get("lb_scenario"),
            lb_scenario=payload.lb_scenario,
            rows_affected=rows_maj,
            duration_ms=duration_ms,
        ),
    )
    return updated


@router.post(
    "/{id_scenario}/duplicate",
    response_model=ScenarioOut,
    status_code=status.HTTP_201_CREATED,
)
async def duplicate_scenario(id_scenario: int, payload: DuplicateRequest):
    """Duplique un scénario et TOUT son historique en nouveau EN COURS, version 1, est_fige=0.

    Copie profonde : entête (périodes, nb_jours, dt_pivot, flags trafic) + toutes
    les données filles (tmh, neutralisations, comptages manuels, exclusions,
    variations prévisionnelles, scenario_pic_coeffs, trafic_agrebal, trafic_pdi).
    Si la source a une version PIC niveau SCENARIO, une nouvelle version est créée
    pour le clone avec ses coefficients (le clone est indépendant de la source) ;
    sinon le clone garde l'id_pic_version partagé de la source.

    La duplication d'un scénario figé ou archivé est permise (le clone repart en
    EN COURS / v1 / non figé). Pas de tracking parent. id_rh requis (traçabilité) :
    il devient l'auteur du clone et des lignes filles copiées.
    """
    start = time.perf_counter()
    logger.info("Début duplication scénario %s", ctx(source_id=id_scenario))

    source = await fetch_scenario_or_404(id_scenario)

    new_lb = (
        payload.lb_scenario
        if payload.lb_scenario
        else f"{source['lb_scenario']} (copie)"
    )
    if len(new_lb) > 20:  # lb_scenario : varchar(20) en base
        new_lb = new_lb[:20]
    logger.debug("Nouveau libellé résolu %s", ctx(lb_scenario=new_lb))

    id_rh_token = encrypt_id_rh(payload.id_rh)

    try:
        async with db_write.transaction() as tx:
            # Entête : INSERT ... SELECT sur la source (colonnes réelles, sans
            # dépendre des alias de SELECT_SCENARIO_SQL). dt_validation et
            # dt_mise_en_prod restent NULL (workflow incohérent avec EN COURS).
            await tx.execute(
                "INSERT INTO trppu_scenario "
                "(co_regate, lb_scenario, co_roc, statut, dt_creation, "
                " dt_mise_en_oeuvre, dt_pivot, "
                " periode_debut, periode_fin, "
                " periode_realise_debut, periode_realise_fin, "
                " periode_prev_debut, periode_prev_fin, "
                " nb_jours_semaine, nb_jours_ouvres, nb_jours_ouvrables, nb_jours_scenario, "
                " id_pic_version, version_scenario, est_fige, "
                " id_rh_creation, id_rh_maj, "
                " trafic_pdi_calcule, trafic_agrebal_calcule) "
                "SELECT co_regate, %s, co_roc, 'EN COURS', NOW(), "
                " dt_mise_en_oeuvre, dt_pivot, "
                " periode_debut, periode_fin, "
                " periode_realise_debut, periode_realise_fin, "
                " periode_prev_debut, periode_prev_fin, "
                " nb_jours_semaine, nb_jours_ouvres, nb_jours_ouvrables, nb_jours_scenario, "
                " id_pic_version, 1, 0, "
                " %s, %s, "
                " trafic_pdi_calcule, trafic_agrebal_calcule "
                "FROM trppu_scenario WHERE id_scenario = %s",
                (new_lb, id_rh_token, id_rh_token, id_scenario),
            )
            new_id = await last_insert_id(tx)

            new_pic_id = await duplicate_scenario_pic_version(
                tx, id_scenario, new_id, source["co_regate"], id_rh_token
            )
            counts = await duplicate_scenario_children(
                tx, id_scenario, new_id, id_rh_token
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur duplication scénario %s",
            ctx(source_id=id_scenario, lb_scenario=new_lb),
        )
        raise HTTPException(status_code=500, detail="Erreur duplication scenario.") from e

    duplicated = await fetch_scenario_or_404(new_id)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    await enregistrer_appel(
        api_name=ACTION_DUPLICATION_SCENARIO,
        id_scenario=new_id,
        regate=source.get("co_regate"),
        params={
            "source_id": id_scenario,
            "lb_scenario": new_lb,
            "id_pic_version": new_pic_id,
            "lignes_copiees": counts,
        },
    )
    logger.info(
        "Fin duplication scénario %s",
        ctx(
            source_id=id_scenario,
            id_scenario=new_id,
            lb_scenario=new_lb,
            id_pic_version=new_pic_id,
            lignes_copiees=counts,
            duration_ms=duration_ms,
        ),
    )
    return duplicated
