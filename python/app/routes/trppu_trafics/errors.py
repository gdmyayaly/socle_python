"""Erreurs HTTP du domaine trafics : `{error, message, code}` + bloc debug optionnel.

Le bloc debug (requêtes rendues, requête fautive, erreur Databricks brute) n'est produit que si
`DEBUG_SHOW_QUERY=true` : hors debug, ni le SQL ni le message Databricks ne sortent de l'API.
Seul module du package à lire ce drapeau — ne pas le réimporter ailleurs, sinon le comportement
debug n'est plus pilotable d'un seul point.
"""

from fastapi import HTTPException

from app.config import DEBUG_SHOW_QUERY


def _resume(index: int, trace: dict) -> dict:
    """Trace allégée : SQL et statut, jamais les lignes de données."""
    resume = {"index": index, "statut": trace["statut"], "sql": trace["sql"]}
    if "nb_lignes" in trace:
        resume["nb_lignes"] = trace["nb_lignes"]
    if "erreur" in trace:
        resume["erreur"] = trace["erreur"]
    return resume


def bloc_debug(traces: list[dict], databricks_error: str | None = None) -> dict | None:
    """Bloc debug des requêtes exécutées, ou None si `DEBUG_SHOW_QUERY` est désactivé.

    `requete_en_echec` double l'information déjà portée par `statut` dans `queries` : sur 3
    requêtes, il évite de faire chercher laquelle a planté.
    """
    if not DEBUG_SHOW_QUERY:
        return None

    queries = [_resume(i, trace) for i, trace in enumerate(traces)]
    bloc: dict = {"queries": queries}

    echec = next((q for q in queries if q["statut"] == "echec"), None)
    if echec is not None:
        bloc["requete_en_echec"] = echec
    if databricks_error is not None:
        bloc["databricks_error"] = databricks_error
    return bloc


def erreur_400(message: str) -> HTTPException:
    """400 métier (paramètres) — n'expose jamais de SQL. À lever : `raise erreur_400(msg)`."""
    return HTTPException(
        status_code=400, detail={"error": True, "message": message, "code": 400}
    )


def erreur_500(
    message: str,
    traces: list[dict] | None = None,
    databricks_error: str | None = None,
) -> HTTPException:
    """500 Databricks, enrichi du bloc debug quand `DEBUG_SHOW_QUERY` est actif."""
    detail: dict = {"error": True, "message": message, "code": 500}
    debug = bloc_debug(traces or [], databricks_error)
    if debug is not None:
        detail["debug"] = debug
    return HTTPException(status_code=500, detail=detail)
