"""Services TRPPU consommés par OPTIPACC.

- DSR-690 : liste des scénarios exploitables d'un site.
- DSR-689 : volumes bruts par produit d'un scénario.
- DSR-705 : trafics Agrébal (amas) calculés d'un scénario.
- DSR-707 : déclaration d'un scénario en production.

Regroupés sous `/trppu-api/optipacc` pour que les applications tierces les
identifient sans ambiguïté et qu'ils puissent évoluer indépendamment des routes
servant l'IHM. Trois services sur quatre sont en lecture seule ; seul DSR-707
écrit, et il est le seul point d'entrée OPTIPACC vers le statut EN PRODUCTION.

Nommage des chemins en kebab-case, comme le reste de l'API : les tickets
écrivent `/trafic_amas`, on expose `/trafic-amas` (écart consigné dans la
résolution DSR-705).
"""

import logging
import time
from math import ceil

from fastapi import APIRouter, HTTPException, Query

from app.config import NB_AMAS_PAR_PAGE
from app.db.mysql import db_read, db_write
from app.log_utils import ctx
from app.routes.trppu_scenario.helpers import (
    assert_aucun_scenario_en_production,
    assert_trafics_calcules,
    increment_version,
)
from app.routes.trppu_site.helpers import fetch_site_or_404
from app.services.api_log import ACTION_TRANSITION_STATUT, enregistrer_appel

from .helpers import (
    COUNT_AMAS_SQL,
    SELECT_AMAS_PAGE_SQL,
    SELECT_SCENARIO_GARDE_SQL,
    SELECT_SCENARIO_MISE_EN_PROD_SQL,
    SELECT_SCENARIO_VISIBLE_SQL,
    SELECT_SCENARIOS_EXPLOITABLES_SQL,
    SELECT_VOLUMES_BRUTS_SQL,
    UPDATE_MISE_EN_PROD_SQL,
    assert_exploitable,
    assert_mise_en_prod_possible,
    assert_visible_optipacc,
    select_amas_existants_sql,
    select_trafics_amas_sql,
)
from .schemas import (
    CO_REGATE_PATTERN,
    AmasOut,
    MiseEnProductionRequest,
    MiseEnProductionResponse,
    PaginationOut,
    ProduitVolume,
    ProduitVolumes,
    ScenarioItem,
    SiteScenariosResponse,
    TraficAmasRequest,
    TraficAmasResponse,
    TraficBrutRequest,
    TraficBrutResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/optipacc", tags=["OPTIPACC"])


@router.get(
    "/site-liste-scenarios",
    response_model=SiteScenariosResponse,
    # `message` n'apparaît dans la réponse que lorsqu'il est renseigné : le cas
    # nominal reste strictement le contrat DSR-690.
    response_model_exclude_none=True,
)
async def site_liste_scenarios(
    co_regate: str = Query(
        ...,
        alias="codeRegate",
        min_length=6,
        max_length=6,
        pattern=CO_REGATE_PATTERN,
        description="Code Regate du site (6 caractères alphanumériques)",
    ),
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-690 (S_SiteListeScenarios) : scénarios exploitables d'un site.

    Ne sont retournés que les scénarios au statut `VALIDE` **et** dont les trafics
    Agrébal ont été calculés (`trafic_agrebal_calcule = 1`). Tout autre statut
    (« EN COURS », « SIMULATION », …) ou un calcul Agrébal non terminé exclut le
    scénario de la liste.

    L'absence de scénario éligible n'est pas une erreur : la réponse est un 200
    avec une liste vide et un `message` explicatif. L'existence du site n'est pas
    contrôlée — un code Regate inconnu donne la même réponse.
    """
    start = time.perf_counter()
    logger.info("Début liste scénarios OPTIPACC %s", ctx(co_regate=co_regate))
    try:
        rows = await db_read.fetch_all(SELECT_SCENARIOS_EXPLOITABLES_SQL, (co_regate,))
    except Exception as e:
        logger.exception(
            "Erreur liste scénarios OPTIPACC %s", ctx(co_regate=co_regate)
        )
        raise HTTPException(
            status_code=500,
            detail="Une erreur est survenue lors de la récupération des scénarios.",
        ) from e

    message = None if rows else f"Aucun scénario trouvé pour le site {co_regate}."
    if message:
        # Pas une erreur (200 + liste vide), mais l'appelant OPTIPACC repart sans
        # scénario : la trace évite d'avoir à rejouer la requête pour comprendre.
        logger.info(
            "Aucun scénario exploitable OPTIPACC %s",
            ctx(co_regate=co_regate, motif="aucun scénario VALIDE avec Agrébal calculé"),
        )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin liste scénarios OPTIPACC %s",
        ctx(co_regate=co_regate, count=len(rows), duration_ms=duration_ms),
    )
    return SiteScenariosResponse(
        code_regate=co_regate,
        scenarios=[ScenarioItem(**row) for row in rows],
        message=message,
    )


@router.post("/scenario-trafic-brut", response_model=TraficBrutResponse)
async def scenario_trafic_brut(
    payload: TraficBrutRequest,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-689 (S_ScenarioTraficBrut) : volumes bruts finaux par produit.

    Pour chaque produit du scénario, la somme `constaté + prévisionnel recalculé +
    trafics manuels` telle qu'elle est stockée dans `trppu_tmh` (cf. docstring du
    module helpers pour le détail de la formule). Aucun détail de calcul n'est
    restitué : ni constaté, ni prévisionnel, ni TMH, ni trafic manuel (RG6, CA5).

    Les produits marqués exclus dans le TMH ne sont jamais restitués : l'exclusion
    est une décision utilisateur que le service respecte sans dérogation possible.
    """
    start = time.perf_counter()
    co_regate = payload.code_regate
    id_scenario = payload.scenario_id
    logger.info(
        "Début trafic brut OPTIPACC %s",
        ctx(co_regate=co_regate, id_scenario=id_scenario),
    )

    await fetch_site_or_404(co_regate)
    scenario = await db_read.fetch_one(SELECT_SCENARIO_GARDE_SQL, (id_scenario,))
    if not scenario:
        logger.warning(
            "Rejet trafic brut OPTIPACC %s",
            ctx(
                co_regate=co_regate,
                id_scenario=id_scenario,
                http=404,
                motif="scénario introuvable",
            ),
        )
        raise HTTPException(
            status_code=404,
            detail=f"Scénario {id_scenario} introuvable pour le site {co_regate}.",
        )
    assert_exploitable(scenario, co_regate)

    try:
        rows = await db_read.fetch_all(SELECT_VOLUMES_BRUTS_SQL, (id_scenario,))
    except Exception as e:
        logger.exception(
            "Erreur trafic brut OPTIPACC %s",
            ctx(co_regate=co_regate, id_scenario=id_scenario),
        )
        raise HTTPException(
            status_code=500,
            detail="Une erreur est survenue lors de la récupération des trafics.",
        ) from e

    # SUM() renvoie un Decimal même sur des colonnes entières.
    produits = [
        ProduitVolume(
            code_produit=row["co_produit"],
            volume_brut=int(row["volume_brut"] or 0),
        )
        for row in rows
    ]

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin trafic brut OPTIPACC %s",
        ctx(
            co_regate=co_regate,
            id_scenario=id_scenario,
            count=len(produits),
            duration_ms=duration_ms,
        ),
    )
    return TraficBrutResponse(
        code_regate=co_regate,
        scenario_id=id_scenario,
        produits=produits,
    )


@router.post("/scenario/mise-en-production", response_model=MiseEnProductionResponse)
async def scenario_mise_en_production(
    payload: MiseEnProductionRequest,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-707 : OPTIPACC déclare un scénario en production.

    Le scénario passe `EN PRODUCTION`, devient définitivement figé (`est_fige = 1`)
    et reçoit la date transmise par OPTIPACC comme date de mise en œuvre **et** de
    mise en production (RG-API-PROD-006). Une fois figé, il n'est plus modifiable
    (`assert_editable`) ni supprimable, et le batch YB05 ne le recalcule plus.

    Contrôles dans l'ordre du ticket : existence (C1), appartenance au site (C2),
    statut VALIDE (C3), trafics complètement calculés (C4), unicité du scénario en
    production sur le site (C5). C5 est vérifié **dans** la transaction, avec
    verrou : c'est la seule protection contre deux appels concurrents, aucune
    contrainte d'unicité n'existant en base.

    Côté IHM, `PATCH /trppu-api/scenarios/{id}/statut` vers EN PRODUCTION applique
    aussi C4 et C5 ; il se distingue par sa date (NOW()) et par l'absence de
    contrôle du site.
    """
    start = time.perf_counter()
    co_regate = payload.code_regate
    id_scenario = payload.scenario_id
    dt_mise_en_oeuvre = payload.date_mise_en_oeuvre
    logger.info(
        "Début mise en production OPTIPACC %s",
        ctx(
            co_regate=co_regate,
            id_scenario=id_scenario,
            date_mise_en_oeuvre=dt_mise_en_oeuvre.isoformat(),
        ),
    )

    await fetch_site_or_404(co_regate)
    scenario = await db_read.fetch_one(SELECT_SCENARIO_MISE_EN_PROD_SQL, (id_scenario,))
    if not scenario:  # C1
        logger.warning(
            "Rejet mise en production OPTIPACC %s",
            ctx(
                co_regate=co_regate,
                id_scenario=id_scenario,
                http=404,
                motif="scénario introuvable",
            ),
        )
        raise HTTPException(
            status_code=404, detail=f"Scénario {id_scenario} introuvable."
        )

    assert_mise_en_prod_possible(scenario, co_regate)  # C2, C3
    assert_trafics_calcules(scenario)  # C4

    try:
        async with db_write.transaction() as tx:
            await assert_aucun_scenario_en_production(tx, co_regate, id_scenario)  # C5
            await tx.execute(
                UPDATE_MISE_EN_PROD_SQL,
                (dt_mise_en_oeuvre, dt_mise_en_oeuvre, id_scenario),
            )
            await increment_version(tx, id_scenario)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur mise en production OPTIPACC %s",
            ctx(co_regate=co_regate, id_scenario=id_scenario),
        )
        raise HTTPException(
            status_code=500,
            detail="Une erreur est survenue lors de la mise en production du scénario.",
        ) from e

    await enregistrer_appel(
        api_name=ACTION_TRANSITION_STATUT,
        id_scenario=id_scenario,
        regate=co_regate,
        params={
            "statut_avant": scenario["statut"],
            "statut_apres": "EN PRODUCTION",
            "date_mise_en_oeuvre": dt_mise_en_oeuvre.isoformat(),
            "origine": "OPTIPACC",
        },
    )

    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin mise en production OPTIPACC %s",
        ctx(
            co_regate=co_regate,
            id_scenario=id_scenario,
            statut_avant=scenario["statut"],
            statut="EN PRODUCTION",
            date_mise_en_oeuvre=dt_mise_en_oeuvre.isoformat(),
            duration_ms=duration_ms,
        ),
    )
    return MiseEnProductionResponse(
        scenario_id=id_scenario,
        code_regate=co_regate,
        statut="EN PRODUCTION",
        date_mise_en_oeuvre=dt_mise_en_oeuvre,
    )


def _regrouper_amas(rows: list[dict]) -> list[AmasOut]:
    """Replie les lignes SQL à plat en `amas -> jours -> produits`.

    L'ordre du `ORDER BY` (uuid, jour de la semaine, produit) est conservé : les
    dict Python préservent l'ordre d'insertion, la sortie est donc déterministe.

    Les volumes remontent en `Decimal` (SUM sur decimal(12,4)) mais sont entiers
    par construction — YB05 somme des `smallint unsigned` de `trppu_trafic_pdi`.
    On les restitue en `int`, comme `volume_brut` de DSR-689. Une densité absente
    pour un couple (jour, produit) donne NULL : elle vaut 0 côté réponse.
    """
    par_uuid: dict[str, AmasOut] = {}
    for row in rows:
        uuid = row["agrebal_uuid"]
        amas = par_uuid.get(uuid)
        if amas is None:
            amas = AmasOut(agrebal_uuid=uuid, nom_amas=row.get("nom_amas"), jours={})
            par_uuid[uuid] = amas
        jour = str(row["jour_semaine"]).lower()
        amas.jours.setdefault(jour, []).append(
            ProduitVolumes(
                produit=row["co_produit"],
                fort=int(row["fort"] or 0),
                faible1=int(row["faible1"] or 0),
                faible2=int(row["faible2"] or 0),
            )
        )
    return list(par_uuid.values())


@router.post("/trafic-amas", response_model=TraficAmasResponse)
async def trafic_amas(
    payload: TraficAmasRequest,
    id_session_ihm: str | None = Query(None, description="Id de session IHM (traçabilité)"),
):
    """DSR-705 : trafics Agrébal calculés d'un scénario, regroupés par amas.

    Service purement consultatif (RG-API-003) : deux SELECT sur `db_read`, aucun
    recalcul. Les volumes sont restitués tels que YB05 les a écrits dans
    `trppu_trafic_agrebal`, sans clé de répartition ni coefficient PIC
    (RG-API-006).

    Deux modes :

    - **sans `amas`** — tous les amas du scénario (RG-API-005), paginés par
      tranches de `NB_AMAS_PAR_PAGE`, triés par `agrebal_uuid` avant découpage pour
      qu'un amas ne change pas de page d'un appel à l'autre (RG-API-008) ;
    - **avec `amas`** — uniquement les UUID demandés, sans pagination (Cas 8). Les
      UUID inconnus sont ignorés mais restitués dans `amas_non_trouves` et tracés
      en WARNING (C4) ; si aucun n'est valide, la réponse est un 404.

    Une page au-delà de la dernière n'est pas une erreur : 200 avec `amas` vide.
    """
    start = time.perf_counter()
    co_regate = payload.code_regate
    id_scenario = payload.scenario_id
    logger.info(
        "Début trafic amas OPTIPACC %s",
        ctx(
            co_regate=co_regate,
            id_scenario=id_scenario,
            nb_amas_demandes=len(payload.amas) if payload.amas is not None else None,
            page=payload.page,
        ),
    )

    await fetch_site_or_404(co_regate)
    scenario = await db_read.fetch_one(SELECT_SCENARIO_VISIBLE_SQL, (id_scenario,))
    if not scenario:  # C1
        logger.warning(
            "Rejet trafic amas OPTIPACC %s",
            ctx(
                co_regate=co_regate,
                id_scenario=id_scenario,
                http=404,
                motif="scénario introuvable",
            ),
        )
        raise HTTPException(
            status_code=404, detail=f"Scénario {id_scenario} introuvable."
        )

    assert_visible_optipacc(scenario, co_regate)  # C2, C3

    taille_page = NB_AMAS_PAR_PAGE
    amas_non_trouves: list[str] = []

    try:
        if payload.amas is not None:
            # Mode filtre : le périmètre est déjà borné par l'appelant (Cas 8).
            # dict.fromkeys dédoublonne sans perdre l'ordre de la demande.
            demandes = list(dict.fromkeys(payload.amas))
            # Une liste vide ne part jamais en base : `IN ()` est une erreur de
            # syntaxe MySQL. `amas: []` est une demande explicite de rien, traitée
            # comme « aucun amas valide » (404) plus bas.
            rows_uuid = (
                await db_read.fetch_all(
                    select_amas_existants_sql(len(demandes)),
                    (id_scenario, co_regate, *demandes),
                )
                if demandes
                else []
            )
            trouves = {r["agrebal_uuid"] for r in rows_uuid}
            uuids = [u for u in demandes if u in trouves]
            amas_non_trouves = [u for u in demandes if u not in trouves]
            page = 1
            nb_amas_total = len(uuids)
            nb_pages = 1
            taille_page = len(uuids)
        else:
            row_count = await db_read.fetch_one(COUNT_AMAS_SQL, (id_scenario, co_regate))
            nb_amas_total = int((row_count or {}).get("nb") or 0)
            nb_pages = max(1, ceil(nb_amas_total / taille_page))
            page = payload.page
            offset = (page - 1) * taille_page
            rows_uuid = await db_read.fetch_all(
                SELECT_AMAS_PAGE_SQL, (id_scenario, co_regate, taille_page, offset)
            )
            uuids = [r["agrebal_uuid"] for r in rows_uuid]

        rows = (
            await db_read.fetch_all(
                select_trafics_amas_sql(len(uuids)),
                (id_scenario, co_regate, *uuids),
            )
            if uuids
            else []
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur trafic amas OPTIPACC %s",
            ctx(co_regate=co_regate, id_scenario=id_scenario),
        )
        raise HTTPException(
            status_code=500,
            detail="Une erreur est survenue lors de la récupération des trafics.",
        ) from e

    if amas_non_trouves:
        logger.warning(
            "Amas inconnus ignorés OPTIPACC %s",
            ctx(
                co_regate=co_regate,
                id_scenario=id_scenario,
                amas_non_trouves=amas_non_trouves,
                motif="agrebal_uuid absent des trafics du scénario",
            ),
        )

    if payload.amas is not None and not uuids:  # C4 : aucun amas valide
        logger.warning(
            "Rejet trafic amas OPTIPACC %s",
            ctx(
                co_regate=co_regate,
                id_scenario=id_scenario,
                http=404,
                motif="aucun des amas demandés n'existe pour ce scénario",
            ),
        )
        raise HTTPException(
            status_code=404,
            detail=(
                f"Aucun des amas demandés n'a été trouvé pour le scénario {id_scenario}."
            ),
        )

    amas = _regrouper_amas(rows)
    duration_ms = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "Fin trafic amas OPTIPACC %s",
        ctx(
            co_regate=co_regate,
            id_scenario=id_scenario,
            page=page,
            nb_pages=nb_pages,
            nb_amas_total=nb_amas_total,
            count=len(amas),
            nb_amas_non_trouves=len(amas_non_trouves),
            duration_ms=duration_ms,
        ),
    )
    return TraficAmasResponse(
        site=co_regate,
        scenario=id_scenario,
        pagination=PaginationOut(
            page=page,
            taille_page=taille_page,
            nb_amas_total=nb_amas_total,
            nb_pages=nb_pages,
            page_suivante=page + 1 if page < nb_pages else None,
        ),
        amas=amas,
        amas_non_trouves=amas_non_trouves,
    )
