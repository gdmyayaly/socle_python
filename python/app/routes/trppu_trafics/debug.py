"""DSR-679 — Routes de debug et de contrôle des données gold TRPPU.

- `/test/config`     : tables, colonnes et filtres réellement utilisés par la requête
- `/test/objets`     : objets et niveaux de regroupement présents pour un site
- `/test/pivot_test` : contrôle croisé de deux mailles sur le même jeu de données
- `/test/schema_raw` : `SELECT *` libre sur une table (introspection)
"""

import logging
from calendar import monthrange
from datetime import timedelta

from fastapi import APIRouter, HTTPException
from starlette.concurrency import run_in_threadpool

from app.config import DATABRICKS_CATALOG, DATABRICKS_SCHEMA
from app.db.databricks import databricks
from app.routes.trppu_trafics.helpers import (
    DATE_COLUMN_PERIODE,
    MAPPING_COL_CLE,
    MAPPING_COL_OBJET,
    NIVEAU_REGROUPEMENT,
    OBJ_ALIAS,
    OBJ_MAPPING_TABLE,
    TABLES_PERIODE,
    TRAFIC_COL_ANNEE,
    TRAFIC_COL_CONSTATE,
    TRAFIC_COL_MOIS,
    TRAFIC_COL_NIVEAU,
    TRAFIC_COL_OBJET,
    TRAFIC_COL_PREVISIONNEL,
    TRAFIC_COL_REGATE,
    accumulate_trafics,
    build_libelles_query,
    build_mapping_subquery,
    build_query,
    empty_accumulator,
    fetch_libelles_objets,
    fmt_date,
    fqtn,
    render_sql,
    split_by_pivot,
    validate_params_pivot,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trppu-api/trafics/test", tags=["Trafics (test DSR-679)"])

SITE_TEST = "400300"

# Paires de mailles contrôlées par /test/pivot_test. Chaque plage par défaut est alignée sur la
# maille concernée pour que les deux tables couvrent exactement les mêmes journées.
PAIRES_PIVOT = {
    "jour_mois": {
        "mailles": ("jours", "mois"),
        "defaut": {"date_debut": "20250301", "date_fin": "20260331", "date_pivot": "20251001"},
        "raison": "mois entiers : pivot au 1er du mois, fin au dernier jour",
    },
    "jour_semaine": {
        "mailles": ("jours", "semaines"),
        "defaut": {"date_debut": "20250303", "date_fin": "20260329", "date_pivot": "20250929"},
        "raison": "semaines entières : début et pivot un lundi, fin un dimanche",
    },
    "semaine_mois": {
        "mailles": ("semaines", "mois"),
        "defaut": {"date_debut": "20250301", "date_fin": "20260331", "date_pivot": "20251001"},
        "raison": (
            "même jeu de données que jour_mois. Semaine et mois ne pouvant être alignés "
            "simultanément, chacune est validée contre la table jour sur sa plage effective"
        ),
    },
}


def _config_effective() -> dict:
    """Tables, colonnes et filtres réellement utilisés par la requête."""
    return {
        "catalog": DATABRICKS_CATALOG,
        "schema_gold": DATABRICKS_SCHEMA,
        "tables_trafics": {p: fqtn(t) for p, t in TABLES_PERIODE.items()},
        "jointures": (
            f"LEFT JOIN {fqtn(OBJ_MAPPING_TABLE)} pré-regroupé "
            f"({MAPPING_COL_CLE} = {TRAFIC_COL_OBJET}) — regroupement sur "
            f"{MAPPING_COL_OBJET} du mapping"
        ),
        "mapping_pre_regroupe": build_mapping_subquery(),
        "libelles": {
            "source": fqtn(OBJ_MAPPING_TABLE),
            "query": build_libelles_query(),
            "note": (
                "portés par la jointure de la requête agrégée ; cette lecture séparée "
                "(mise en cache) sert au debug et à la création auto des produits"
            ),
        },
        "filtre_niveau": {
            "colonne": TRAFIC_COL_NIVEAU,
            "valeur": NIVEAU_REGROUPEMENT,
            "raison": "les tables gold empilent SITE/ETABLISSEMENT/PIC/NATIONAL...",
        },
        "colonnes_trafics": {
            "regate": TRAFIC_COL_REGATE,
            "objet": TRAFIC_COL_OBJET,
            "constate": TRAFIC_COL_CONSTATE,
            "previsionnel": TRAFIC_COL_PREVISIONNEL,
            "partition_annee": TRAFIC_COL_ANNEE,
            "partition_mois": TRAFIC_COL_MOIS,
            "dates_par_maille": DATE_COLUMN_PERIODE,
        },
    }


def _executer(sql: str, params: dict | None = None) -> dict:
    """Exécute une requête de debug : SQL rendu + lignes, ou l'erreur Databricks telle quelle."""
    resultat = {"sql": render_sql(sql, params or {})}
    try:
        rows = databricks.fetch_all(sql, params) if params else databricks.fetch_all(sql)
        resultat["nb_lignes"] = len(rows)
        resultat["lignes"] = rows
    except Exception as e:
        logger.warning("Requête de debug DSR-679 en échec : %s", e)
        resultat["error"] = str(e)
    return resultat


@router.get("/config")
def config():
    """DEBUG — configuration effective de la requête DSR-679."""
    return _config_effective()


@router.get("/objets")
def objets(co_regate: str = SITE_TEST, table: str = "mois"):
    """DEBUG — objets, libellés et niveaux de regroupement présents pour un site.

    Plus d'un niveau signifie que le filtre `co_niveau_regroupement_operationnel` est ce qui
    évite un sur-comptage. Les objets sans libellé sont ceux absents du mapping.
    """
    tbl = TABLES_PERIODE.get(
        {"jour": "jours", "semaine": "semaines", "mois": "mois"}.get(table, table),
        TABLES_PERIODE["mois"],
    )
    sql = (
        f"SELECT {TRAFIC_COL_NIVEAU} AS niveau, {TRAFIC_COL_OBJET} AS {OBJ_ALIAS}, "
        f"COUNT(*) AS nb_lignes "
        f"FROM {fqtn(tbl)} WHERE {TRAFIC_COL_REGATE} = :co_regate "
        f"GROUP BY 1, 2 ORDER BY 1, 2"
    )
    resultat = {"table_trafics": fqtn(tbl), **_executer(sql, {"co_regate": co_regate})}

    lignes = resultat.get("lignes")
    if lignes is not None:
        libelles = fetch_libelles_objets()
        niveaux = sorted({r["niveau"] for r in lignes if r.get("niveau") is not None})
        objets_du_site = sorted(
            {r[OBJ_ALIAS] for r in lignes
             if r.get("niveau") == NIVEAU_REGROUPEMENT and r.get(OBJ_ALIAS) is not None}
        )
        resultat["niveaux_presents"] = niveaux
        resultat["filtre_niveau_necessaire"] = len(niveaux) > 1
        resultat["objets"] = [{"co_produit": o, "lb_produit": libelles.get(o)} for o in objets_du_site]
        resultat["objets_sans_libelle"] = [o for o in objets_du_site if o not in libelles]
    return resultat


def _plage_effective(rng, periode: str):
    """Jours réellement couverts par les mailles qui intersectent `rng`.

    Une maille grossière interrogée sur une plage non alignée déborde aux bords : la semaine
    2025-09 commence le 24/02. C'est sur cette plage élargie que la table jour sert d'arbitre,
    ce qui rend la comparaison exacte même sans alignement.
    """
    if rng is None:
        return None
    debut, fin = rng
    if periode == "semaines":
        return (debut - timedelta(days=debut.weekday()), fin + timedelta(days=6 - fin.weekday()))
    if periode == "mois":
        return (debut.replace(day=1), fin.replace(day=monthrange(fin.year, fin.month)[1]))
    return rng


def _debordement(rng, periode: str) -> str:
    """De combien la maille déborde de la plage demandée (0 = alignée)."""
    if rng is None:
        return "zone vide"
    eff = _plage_effective(rng, periode)
    avant, apres = (rng[0] - eff[0]).days, (eff[1] - rng[1]).days
    if avant == 0 and apres == 0:
        return "aligné : la maille couvre exactement la plage demandée"
    return f"déborde de {avant} j avant et {apres} j après (comparaison sur la plage effective)"


def _chevauchement(reel, prev, periode: str):
    """Signale une maille à cheval sur le pivot, donc présente dans les deux zones.

    Artefact de ce test, qui force une maille grossière sur toute la période : l'endpoint de
    production découpe au jour près avant de choisir la maille, et n'emploie semaine ou mois
    que sur des périodes entières.
    """
    if reel is None or prev is None or periode == "jours":
        return None
    fin_reel, debut_prev = fmt_date(reel[1], periode), fmt_date(prev[0], periode)
    if debut_prev > fin_reel:
        return None
    return (
        f"la maille {debut_prev} est interrogée dans les deux zones (fin du réel = {fin_reel}) : "
        "le pivot ne tombe pas sur une frontière de maille"
    )


def _ecart(reference, valeur) -> dict:
    """Écart absolu et relatif par rapport à la référence (maille jour)."""
    delta = (valeur or 0) - (reference or 0)
    return {"delta": delta, "pct": round(100 * delta / reference, 2) if reference else None}


async def _sommes_par_objet(periode, co_regate, zones, execute):
    """Exécute les 2 requêtes (zone réelle / prévisionnelle) d'une maille et cumule par objet."""
    acc = empty_accumulator()
    requetes: list[dict] = []
    for rng, value_col, target_key in zones:
        if rng is None:
            continue
        sql, params = build_query(periode, co_regate, [rng], value_col)
        detail = {
            "zone": target_key,
            "plage": [fmt_date(rng[0]), fmt_date(rng[1])],
            "bornes_maille": [fmt_date(rng[0], periode), fmt_date(rng[1], periode)],
        }
        if execute:
            execution = await run_in_threadpool(_executer, sql, params)
            if "lignes" in execution:
                accumulate_trafics(execution["lignes"], target_key, acc)
            detail.update(execution)
        else:
            detail["sql"] = render_sql(sql, params)
        requetes.append(detail)
    return acc, requetes


def _trafics(acc, libelles) -> list[dict]:
    """Accumulateur au format de sortie de l'API (une ligne par objet présent).

    Le libellé vient de la jointure ; `libelles` sert de repli quand la requête n'a pas été
    exécutée (`execute=false`)."""
    return [
        {
            "co_produit": objet,
            "lb_produit": acc[objet]["lb_produit"] or libelles.get(objet),
            "trafic_brut": acc[objet]["trafic_brut"],
            "trafic_previsionnel": acc[objet]["trafic_previsionnel"],
        }
        for objet in sorted(acc)
    ]


@router.get("/pivot_test")
async def pivot_test(
    paire: str = "jour_mois",
    co_regate: str = SITE_TEST,
    date_debut: str | None = None,
    date_fin: str | None = None,
    date_pivot: str | None = None,
    execute: bool = True,
):
    """DEBUG — contrôle croisé de deux mailles sur le même jeu de données.

    `paire` = `jour_mois` (défaut) | `jour_semaine` | `semaine_mois` | `toutes`.

    L'endpoint de production découpe la période entre les 3 tables ; ici chaque maille de la
    paire est forcée à couvrir toute la période (2 requêtes : zone réelle `trafic_constate`,
    zone prévisionnelle `trafic_prevu`), puis confrontée à la table jour sur sa plage effective.

    Chaque paire a sa plage pré-remplie, alignée sur la maille concernée. `execute=false`
    renvoie le SQL sans toucher Databricks. `mailles_coherentes` vaut `true` quand tous les
    écarts par objet sont nuls.
    """
    if paire == "toutes":
        resultats = {
            nom: await pivot_test(
                paire=nom, co_regate=co_regate, date_debut=date_debut,
                date_fin=date_fin, date_pivot=date_pivot, execute=execute,
            )
            for nom in PAIRES_PIVOT
        }
        return {
            "note": "Contrôle croisé DSR-679 — les 3 paires de mailles.",
            "paires": resultats,
            "mailles_coherentes": (
                all(r["mailles_coherentes"] for r in resultats.values()) if execute else None
            ),
        }

    if paire not in PAIRES_PIVOT:
        raise HTTPException(status_code=400, detail={
            "error": True, "code": 400,
            "message": f"paire inconnue '{paire}'. Valeurs : {', '.join(PAIRES_PIVOT)}, toutes.",
        })

    config_paire = PAIRES_PIVOT[paire]
    defaut = config_paire["defaut"]
    pre_rempli = not (date_debut or date_fin or date_pivot)

    dt_debut, dt_fin, dt_pivot = validate_params_pivot(
        co_regate,
        date_debut or defaut["date_debut"],
        date_fin or defaut["date_fin"],
        date_pivot or defaut["date_pivot"],
    )
    reel_range, prev_range = split_by_pivot(dt_debut, dt_fin, dt_pivot)
    libelles = await run_in_threadpool(fetch_libelles_objets) if execute else {}

    par_maille: dict[str, dict] = {}
    for periode in config_paire["mailles"]:
        zones = (
            (reel_range, TRAFIC_COL_CONSTATE, "trafic_brut"),
            (prev_range, TRAFIC_COL_PREVISIONNEL, "trafic_previsionnel"),
        )
        acc, requetes = await _sommes_par_objet(periode, co_regate, zones, execute)

        zones_eff = tuple(
            (_plage_effective(rng, periode), col, cle) for rng, col, cle in zones
        )
        if periode == "jours":
            acc_ref, requetes_ref = acc, []
        else:
            acc_ref, requetes_ref = await _sommes_par_objet("jours", co_regate, zones_eff, execute)

        par_maille[periode] = {
            "table": fqtn(TABLES_PERIODE[periode]),
            "colonne_date": DATE_COLUMN_PERIODE[periode],
            "chevauchement_zones": _chevauchement(reel_range, prev_range, periode),
            "debordement": {
                "zone_reelle": _debordement(reel_range, periode),
                "zone_previsionnelle": _debordement(prev_range, periode),
            },
            "plage_effective": {
                cle: [fmt_date(d) for d in rng] if rng else None
                for cle, rng in (
                    ("zone_reelle", zones_eff[0][0]),
                    ("zone_previsionnelle", zones_eff[1][0]),
                )
            },
            "requetes": requetes,
            "requetes_reference_jour": requetes_ref,
            "trafics": _trafics(acc, libelles),
            "trafics_reference_jour": _trafics(acc_ref, libelles),
        }

    vide = {"trafic_brut": 0, "trafic_previsionnel": 0}
    par_objet: dict[str, dict] = {}
    for periode, donnees in par_maille.items():
        maille = {t["co_produit"]: t for t in donnees["trafics"]}
        ref = {t["co_produit"]: t for t in donnees["trafics_reference_jour"]}
        for objet in sorted(set(maille) | set(ref)):
            ligne = par_objet.setdefault(
                objet, {"co_produit": objet, "lb_produit": libelles.get(objet)}
            )
            valeurs, reference = maille.get(objet, vide), ref.get(objet, vide)
            ligne[periode] = {k: valeurs[k] for k in vide}
            ligne[f"ecart_{periode}_vs_jour"] = {k: _ecart(reference[k], valeurs[k]) for k in vide}

    comparaison = [par_objet[o] for o in sorted(par_objet)]
    coherent = all(
        ecart[k]["delta"] == 0
        for ligne in comparaison
        for cle, ecart in ligne.items() if cle.startswith("ecart_")
        for k in vide
    ) if execute else None

    return {
        "note": (
            f"Contrôle croisé DSR-679 — paire '{paire}' ({' vs '.join(config_paire['mailles'])}). "
            "Chaque maille est confrontée à la table jour sur sa plage effective."
        ),
        "paire": paire,
        "raison_plage": config_paire["raison"],
        "execute": execute,
        "parametres": {
            "co_regate": co_regate,
            "date_debut": fmt_date(dt_debut),
            "date_fin": fmt_date(dt_fin),
            "date_pivot": fmt_date(dt_pivot),
            "plage_pre_remplie": pre_rempli,
        },
        "zones": {
            "reelle": [fmt_date(reel_range[0]), fmt_date(reel_range[1])] if reel_range else None,
            "previsionnelle": (
                [fmt_date(prev_range[0]), fmt_date(prev_range[1])] if prev_range else None
            ),
        },
        "par_maille": par_maille,
        "comparaison": comparaison,
        "mailles_coherentes": coherent,
    }


@router.get("/schema_raw")
async def schema_raw(table: str, limit: int = 5, schema: str | None = None):
    """DEBUG — `SELECT *` libre sur une table (nom court résolu dans `schema`, ou FQTN)."""
    limit = max(1, min(int(limit), 100))
    table_fqtn = table if "." in table else fqtn(table, schema)
    sql = f"SELECT * FROM {table_fqtn} LIMIT {limit}"
    resultat = await run_in_threadpool(_executer, sql)
    if "lignes" in resultat:
        resultat["colonnes"] = list(resultat["lignes"][0].keys()) if resultat["lignes"] else []
    return {"table": table_fqtn, **resultat}
