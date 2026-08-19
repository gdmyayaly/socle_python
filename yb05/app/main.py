"""Point d'entrée console du batch yb05.

Vérifications de la base de données :

    python -m app.main db-info    # informations de connexion et du serveur MySQL
    python -m app.main db-check   # disponibilité des instances lecture et écriture

Chaîne de calcul des trafics d'un scénario (DSR-701, DSR-702, DSR-703) :

    python -m app.main eligibilite 12345
    python -m app.main calcul-trafic-pdi 12345
    python -m app.main calcul-trafic-agrebal 12345

Les orthographes des tickets sont acceptées telles quelles, en majuscules
(`ELIGIBILITE`, `CALCUL_TRAFIC_PDI`, `CALCUL_TRAFIC_AGREBAL`), ainsi que la forme
`--traitement=ELIGIBILITE --scenario=12345`.

Le code de retour vaut 0 si la vérification ou le traitement est concluant, 1 sinon
(utilisable en ordonnanceur ou en probe).
"""

import argparse
import asyncio
import json
import logging
import sys

from app.config import APP, APP_ENV, APP_VERSION, MODULE
from app.db.mysql import db_read, db_write
from app.health import (
    check_config,
    check_resources,
    describe_connection,
    fetch_server_info,
)
from app.json_formatter import setup_logging
from app.traitements import (
    calcul_trafic_agrebal,
    calcul_trafic_pdi,
    controle_eligibilite,
)
from app.traitements.rapport import ECHEC, Rapport

log = logging.getLogger("yb05")

EXIT_OK = 0
EXIT_KO = 1


def _print_json(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


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
    """Joue un traitement, affiche son rapport, en déduit le code de retour.

    Une base injoignable ou une erreur inattendue est rendue dans le même format que le reste :
    l'exploitant lit un `[KO]` et un `RESULTAT`, pas une trace Python.
    """
    try:
        rapport = await traitement(args.id_scenario)
    except Exception as erreur:  # noqa: BLE001 — la CLI ne doit jamais rendre de stacktrace
        log.exception("Traitement %s interrompu", args.commande)
        rapport = Rapport(titre=f"Traitement {args.commande}", id_scenario=args.id_scenario)
        rapport.ko(f"Traitement interrompu : {erreur}")
        rapport.erreur = str(erreur)
        rapport.statut = ECHEC

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
    """Traduit la forme `--traitement=X [--scenario=N]` des tickets en sous-commande.

    Les tickets documentent les deux formes ; plutôt qu'un second analyseur, la ligne de
    commande est réécrite avant `parse_args`. Les options inconnues sont laissées telles
    quelles, argparse restant seul juge de leur validité.
    """
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
            help="Affiche aussi les logs applicatifs (niveau INFO) en plus du résultat.",
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

    return parser


async def _run(args: argparse.Namespace) -> int:
    try:
        return await args.handler(args)
    finally:
        # Les pools sont créés à la volée (lazy) : on les ferme proprement en sortie.
        await db_read.disconnect()
        await db_write.disconnect()


def main(argv: list[str] | None = None) -> int:
    argv = normaliser_argv(list(sys.argv[1:] if argv is None else argv))
    args = build_parser().parse_args(argv)
    # Par défaut on limite les logs aux avertissements pour ne pas polluer la sortie console.
    setup_logging(level=logging.INFO if args.verbose else logging.WARNING)
    log.info("Commande %s", args.commande)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
