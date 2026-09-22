"""Point d'entrée console du batch yb07.

Vérifications des ressources :

    python -m app.main db-info    # informations de connexion et du serveur MySQL
    python -m app.main db-check   # disponibilité des instances lecture et écriture

Les traitements métier du module s'ajoutent en sous-commandes, sur le modèle de
`db-info` / `db-check` : une coroutine `cmd_<nom>(args) -> int` enregistrée dans
`build_parser()` avec `parents=[commun]`.

Le code de retour vaut 0 si la vérification ou le traitement est concluant, 1 sinon
(utilisable en ordonnanceur ou en probe).
"""

import argparse
import asyncio
import json
import logging
import sys
import time

from app.config import APP, APP_ENV, APP_VERSION, MODULE
from app.db.mysql import db_read, db_write
from app.health import (
    check_config,
    check_resources,
    describe_connection,
    fetch_server_info,
)
from app.json_formatter import setup_logging
from app.log_utils import ctx

log = logging.getLogger("yb07")

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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.main",
        description="Batch yb07 — socle technique : vérifications de la base de données.",
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
        help="Disponibilité réelle des instances MySQL lecture et écriture.",
    ).set_defaults(handler=cmd_db_check)

    return parser


async def _run(args: argparse.Namespace) -> int:
    try:
        return await args.handler(args)
    finally:
        # Les pools sont créés à la volée (lazy) : on les ferme proprement en sortie.
        await db_read.disconnect()
        await db_write.disconnect()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(list(sys.argv[1:] if argv is None else argv))
    # INFO par défaut : le batch tourne sous ordonnanceur, sans `-v`, et c'est la
    # seule trace de ce qu'il a fait. La sortie console de l'exploitant n'en pâtit
    # pas — le rapport part sur stdout, les logs JSON sur stderr.
    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)

    debut = time.perf_counter()
    log.info(
        "Début commande %s",
        ctx(commande=args.commande, id_traitement=getattr(args, "id_traitement", None)),
    )
    code = asyncio.run(_run(args))
    duration_ms = round((time.perf_counter() - debut) * 1000, 1)
    log.info(
        "Fin commande %s",
        ctx(
            commande=args.commande,
            id_traitement=getattr(args, "id_traitement", None),
            exit_code=code,
            duration_ms=duration_ms,
        ),
    )
    return code


if __name__ == "__main__":
    sys.exit(main())
