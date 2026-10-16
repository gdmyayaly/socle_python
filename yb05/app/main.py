"""Point d'entrée console du batch yb05 (`python -m app.main <commande>`, voir README).

Code de retour : 0 si concluant, 1 sinon (ordonnanceur, probe), 2 si le démarrage est refusé
parce qu'une autre instance YB05 est déjà en cours (DSR-729, `app/suivi_batch.py`).
"""

import argparse
import asyncio
import json
import logging
import sys
import time

from app.config import APP, APP_ENV, APP_VERSION, MODULE, NB_WORKER
from app.db.mysql import db_read, db_write
from app.health import (
    check_config,
    check_resources,
    describe_connection,
    fetch_server_info,
)
from app import suivi_batch
from app.json_formatter import setup_logging
from app.log_utils import ctx, reset_id_scenario, reset_site, set_id_scenario, set_site
from app.traitements import (
    calcul_trafic_agrebal,
    calcul_trafic_pdi,
    controle_eligibilite,
    executer_tout,
)
from app.traitements.rapport import ECHEC, Bilan, Rapport

log = logging.getLogger("yb05")

EXIT_OK = 0
EXIT_KO = 1
# Refus de démarrage (DSR-729) : distinct d'un échec, pour que la supervision ne lève pas
# d'alerte sur un lancement en doublon qui s'est correctement effacé.
EXIT_DEJA_EN_COURS = 2


def _print_json(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


def journaliser_rapport(commande: str, charge: dict, reussi: bool) -> None:
    """Rapport (ou bilan) final dans les logs, en plus de stdout : champ JSON racine `rapport`.

    Sous ordonnanceur, stdout n'est pas toujours conservé ; les logs, si. WARNING si le
    traitement n'est pas concluant, pour qu'un filtre de sévérité le remonte.
    """
    log.log(
        logging.INFO if reussi else logging.WARNING,
        "Rapport final %s",
        ctx(
            commande=commande,
            statut=charge.get("statut") or ("SUCCES" if reussi else "ECHEC"),
            alertes=len(charge.get("alertes") or []) or None,
        ),
        extra={"rapport": charge},
    )


def _print_connection(libelle: str, infos: dict) -> None:
    print(f"  {libelle:<9}: {infos['user']}@{infos['host']}:{infos['port']}/{infos['database']}")
    print(f"             retries={infos['max_retries']}, delai={infos['retry_delay']}s")


async def cmd_db_info(args: argparse.Namespace) -> int:
    """Informations de connexion configurées, puis informations du serveur MySQL."""
    config = check_config()
    lecture = describe_connection(db_read)
    ecriture = describe_connection(db_write)
    serveur = await fetch_server_info(db_read)

    if args.json:
        _print_json(
            {
                "application": {
                    "app": APP,
                    "env": APP_ENV,
                    "module": MODULE,
                    "version": APP_VERSION,
                },
                "config": config,
                "connexion": {"lecture": lecture, "ecriture": ecriture},
                "serveur": serveur,
            }
        )
    else:
        print(f"Application  : {APP}/{MODULE} v{APP_VERSION} (env {APP_ENV})")
        print(f"Configuration: {config['status']}")
        print("Connexion MySQL (mot de passe masqué)")
        _print_connection("lecture", lecture)
        _print_connection("écriture", ecriture)
        print("Serveur MySQL (via l'instance de lecture)")
        if serveur["status"] == "ok":
            print(f"  version        : {serveur['version']}")
            print(f"  schéma courant : {serveur['schema_courant']}")
            print(f"  utilisateur    : {serveur['utilisateur']}")
            print(f"  hôte serveur   : {serveur['hote_serveur']}")
            print(f"  date serveur   : {serveur['date_serveur']}")
            print(f"  nb tables      : {serveur['nb_tables']}")
        else:
            print(f"  injoignable : {serveur['error']}")

    return EXIT_OK if config["mysql_config"] and serveur["status"] == "ok" else EXIT_KO


async def cmd_db_check(args: argparse.Namespace) -> int:
    """Disponibilité réelle des instances MySQL lecture et écriture."""
    resultat = await check_resources()

    if args.json:
        _print_json(resultat)
    else:
        print(f"Disponibilité MySQL : {resultat['status']}")
        print(f"  lecture  : {resultat['mysql_read']}")
        print(f"  écriture : {resultat['mysql_write']}")

    return EXIT_OK if resultat["status"] == "ok" else EXIT_KO


# ---------------------------------------------------------------------------
# Traitements métier (DSR-701, DSR-702, DSR-703)
# ---------------------------------------------------------------------------


async def _executer_traitement(traitement, args: argparse.Namespace) -> int:
    """Joue un traitement et affiche son rapport ; une erreur devient un `[KO]`, pas une trace."""
    jeton = set_id_scenario(args.id_scenario)
    # Site et ROC : posés par `charger_scenario`, effacés ici.
    jeton_site = set_site(None, None)
    try:
        try:
            rapport = await traitement(args.id_scenario)
        except Exception as erreur:  # noqa: BLE001 — la CLI ne doit jamais rendre de stacktrace
            log.exception("Erreur traitement %s", ctx(commande=args.commande))
            rapport = Rapport(titre=f"Traitement {args.commande}", id_scenario=args.id_scenario)
            rapport.ko(f"Traitement interrompu : {erreur}")
            rapport.erreur = str(erreur)
            rapport.statut = ECHEC
        # Avant le reset : la ligne porte encore id_scenario, co_regate et co_roc.
        journaliser_rapport(args.commande, rapport.to_dict(), rapport.reussi)
        if not rapport.reussi:
            args.commentaire_suivi = rapport.erreur or "; ".join(rapport.motifs) or None
    finally:
        reset_site(jeton_site)
        reset_id_scenario(jeton)

    if args.json:
        _print_json(rapport.to_dict())
    else:
        print(rapport.texte())

    return EXIT_OK if rapport.reussi else EXIT_KO


async def cmd_eligibilite(args: argparse.Namespace) -> int:
    """DSR-701 — contrôle d'éligibilité, sans aucune écriture."""
    return await _executer_traitement(controle_eligibilite, args)


async def cmd_calcul_trafic_pdi(args: argparse.Namespace) -> int:
    """DSR-702 — calcul des trafics PDI (et traçabilité DSR-700)."""
    return await _executer_traitement(calcul_trafic_pdi, args)


async def cmd_calcul_trafic_agrebal(args: argparse.Namespace) -> int:
    """DSR-703 — agrégation des trafics PDI par Agrébal."""
    return await _executer_traitement(calcul_trafic_agrebal, args)


async def cmd_all(args: argparse.Namespace) -> int:
    """DSR-704 — tous les scénarios éligibles ; `1` si un scénario a échoué (non éligible ≠ échec)."""
    try:
        bilan = await executer_tout(args.id_scenario)
    except Exception as erreur:  # noqa: BLE001 — la CLI ne doit jamais rendre de stacktrace
        log.exception("Erreur mode ALL %s", ctx(id_scenario=args.id_scenario))
        bilan = Bilan(nb_workers=NB_WORKER)
        bilan.erreur = str(erreur)

    journaliser_rapport(args.commande, bilan.to_dict(), bilan.reussi)
    args.statut_suivi, args.commentaire_suivi = _suivi_du_bilan(bilan)
    if args.json:
        _print_json(bilan.to_dict())
    else:
        print(bilan.texte())

    return EXIT_OK if bilan.reussi else EXIT_KO


def _suivi_du_bilan(bilan: Bilan) -> tuple[str, str]:
    """Statut et commentaire `trppu_suivi_batch` du mode ALL.

    PARTIEL : des scénarios réussis et d'autres en échec. Les non éligibles ne sont pas des
    échecs (comme pour le code retour).
    """
    commentaire = (
        f"{len(bilan.succes)} succès, {len(bilan.echecs)} échec(s), "
        f"{len(bilan.non_eligibles)} non éligible(s)"
    )
    if bilan.erreur is not None:
        return suivi_batch.ECHEC, f"Mode ALL interrompu : {bilan.erreur}"
    if not bilan.echecs:
        return suivi_batch.SUCCES, commentaire
    if bilan.succes:
        return suivi_batch.PARTIEL, commentaire
    return suivi_batch.ECHEC, commentaire


# Nom de commande officiel -> orthographe des tickets, acceptée comme alias.
TRAITEMENTS = (
    ("eligibilite", "ELIGIBILITE", cmd_eligibilite, "Contrôle d'éligibilité d'un scénario"),
    (
        "calcul-trafic-pdi",
        "CALCUL_TRAFIC_PDI",
        cmd_calcul_trafic_pdi,
        "Calcul des trafics PDI d'un scénario",
    ),
    (
        "calcul-trafic-agrebal",
        "CALCUL_TRAFIC_AGREBAL",
        cmd_calcul_trafic_agrebal,
        "Calcul des trafics Agrébal d'un scénario",
    ),
)


def normaliser_argv(argv: list[str]) -> list[str]:
    """Réécrit la forme des tickets `--traitement=X [--scenario=N]` en sous-commande."""
    reste: list[str] = []
    traitement: str | None = None
    scenario: str | None = None
    i = 0
    while i < len(argv):
        argument = argv[i]
        for option, valeur in (("--traitement", "traitement"), ("--scenario", "scenario")):
            if argument == option and i + 1 < len(argv):
                cible = argv[i + 1]
                i += 1
                break
            if argument.startswith(f"{option}="):
                cible = argument.split("=", 1)[1]
                break
        else:
            reste.append(argument)
            i += 1
            continue
        if valeur == "traitement":
            traitement = cible
        else:
            scenario = cible
        i += 1

    if traitement is None:
        return reste if scenario is None else [*reste, scenario]
    if scenario is None:
        return [*reste, traitement]
    return [*reste, traitement, scenario]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.main",
        description="Batch yb05 — vérifications de la base et calcul des trafics d'un scénario.",
    )
    # `SUPPRESS` : sans lui, les mêmes options déclarées sur les sous-commandes écraseraient
    # avec leur défaut celles passées avant la sous-commande.
    commun = argparse.ArgumentParser(add_help=False)
    for cible in (parser, commun):
        cible.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            default=argparse.SUPPRESS if cible is commun else False,
            help="Détaille les logs applicatifs (niveau DEBUG) sur la sortie d'erreur.",
        )
        cible.add_argument(
            "--json",
            action="store_true",
            default=argparse.SUPPRESS if cible is commun else False,
            help="Sort le résultat en JSON plutôt qu'en texte.",
        )

    sous_commandes = parser.add_subparsers(dest="commande", required=True)
    sous_commandes.add_parser(
        "db-info",
        parents=[commun],
        help="Informations de connexion configurées et informations du serveur MySQL.",
    ).set_defaults(handler=cmd_db_info)
    sous_commandes.add_parser(
        "db-check",
        parents=[commun],
        help="Vérifie la disponibilité des instances MySQL lecture et écriture.",
    ).set_defaults(handler=cmd_db_check)

    for nom, alias, handler, aide in TRAITEMENTS:
        sous_parser = sous_commandes.add_parser(
            nom, aliases=[alias], parents=[commun], help=aide
        )
        sous_parser.add_argument(
            "id_scenario", type=int, help="Identifiant du scénario à traiter."
        )
        sous_parser.set_defaults(handler=handler)

    # Mode ALL : identifiant facultatif (sans lui, tous les scénarios éligibles).
    mode_all = sous_commandes.add_parser(
        "all",
        aliases=["ALL"],
        parents=[commun],
        help="Calcul complet des trafics de tous les scénarios éligibles (ou d'un seul).",
    )
    mode_all.add_argument(
        "id_scenario",
        type=int,
        nargs="?",
        default=None,
        help="Identifiant d'un scénario précis ; omis, tous les scénarios éligibles.",
    )
    mode_all.set_defaults(handler=cmd_all)

    return parser


# Commandes qui écrivent en base : suivies dans `trppu_suivi_batch` et exclusives (DSR-729).
# `eligibilite` (lecture seule), `db-info` et `db-check` restent libres à tout moment.
COMMANDES_SUIVIES = frozenset({"cmd_calcul_trafic_pdi", "cmd_calcul_trafic_agrebal", "cmd_all"})


def _commande_lancee(argv: list[str]) -> str:
    return " ".join(["python -m app.main", *argv])


async def _executer_suivi(args: argparse.Namespace) -> int:
    """Réserve l'exécution YB05, joue la commande, puis clôt la ligne de suivi."""
    try:
        demarrage = await suivi_batch.demarrer(_commande_lancee(getattr(args, "argv", None) or [args.commande]))
    except Exception as erreur:  # noqa: BLE001 — la CLI ne doit jamais rendre de stacktrace
        log.exception("Erreur démarrage batch %s", ctx(commande=args.commande))
        print(f"Démarrage impossible : suivi d'exécution indisponible ({erreur}).")
        return EXIT_KO
    if isinstance(demarrage, suivi_batch.Refus):
        print(demarrage.message)
        return EXIT_DEJA_EN_COURS

    try:
        code = await args.handler(args)
    except BaseException as erreur:
        # Annulation, Ctrl-C, erreur imprévue : la ligne ne doit pas rester EN_COURS.
        await suivi_batch.terminer(
            demarrage, suivi_batch.ECHEC, f"Exécution interrompue : {erreur!r}"
        )
        raise
    statut = getattr(args, "statut_suivi", None) or (
        suivi_batch.SUCCES if code == EXIT_OK else suivi_batch.ECHEC
    )
    await suivi_batch.terminer(demarrage, statut, getattr(args, "commentaire_suivi", None))
    return code


async def _run(args: argparse.Namespace) -> int:
    try:
        if args.handler.__name__ in COMMANDES_SUIVIES:
            return await _executer_suivi(args)
        return await args.handler(args)
    finally:
        await db_read.disconnect()
        await db_write.disconnect()


def main(argv: list[str] | None = None) -> int:
    argv = normaliser_argv(list(sys.argv[1:] if argv is None else argv))
    args = build_parser().parse_args(argv)
    args.argv = argv
    # INFO par défaut : seule trace sous ordonnanceur (rapport sur stdout, logs sur stderr).
    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)

    debut = time.perf_counter()
    log.info(
        "Début commande %s",
        ctx(commande=args.commande, id_scenario=getattr(args, "id_scenario", None)),
    )
    code = asyncio.run(_run(args))
    duration_ms = round((time.perf_counter() - debut) * 1000, 1)
    log.info(
        "Fin commande %s",
        ctx(
            commande=args.commande,
            id_scenario=getattr(args, "id_scenario", None),
            exit_code=code,
            duration_ms=duration_ms,
        ),
    )
    return code


if __name__ == "__main__":
    sys.exit(main())
