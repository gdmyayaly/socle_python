"""Point d'entrée console du batch yb06.

Vérifications des ressources :

    python -m app.main db-info    # informations de connexion et du serveur MySQL
    python -m app.main db-check   # disponibilité des instances lecture et écriture
    python -m app.main s3-check   # configuration S3, accès, et contenu du bucket

Traitements métier :

    python -m app.main charger-cles-repartition 1
    python -m app.main charger-cles-repartition 1 --fichier autre.csv --json

Les traitements métier s'ajoutent en sous-commandes, sur le modèle de `db-info` :
une coroutine `cmd_<nom>(args) -> int` enregistrée dans `build_parser()` avec
`parents=[commun]`. Celles qui rendent un `Rapport` passent par `_executer_traitement`.

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
from app.erreurs import TraitementImpossible
from app.json_formatter import setup_logging
from app.log_utils import ctx, reset_id_traitement, set_id_traitement
from app.services import s3
from app.traitements import charger_cles_repartition
from app.traitements.rapport import ECHEC, Rapport

log = logging.getLogger("yb06")

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


async def cmd_s3_check(args: argparse.Namespace) -> int:
    """Configuration S3, test d'accès, et contenu du bucket.

    Sert à régler `S3_PREFIXE` et `CSV_CLES_REPARTITION` sans tâtonner : on voit ce que le
    bucket contient réellement, au lieu de deviner un chemin et d'attendre l'échec d'un
    chargement.
    """
    config = s3.decrire_configuration()
    acces = await asyncio.to_thread(s3.verifier_acces)

    prefixe = args.prefixe if args.prefixe is not None else s3.S3_PREFIXE
    contenu = None
    erreur_listing = None
    if acces["status"] == "ok" and config["bucket"] != "(non renseigné)":
        try:
            contenu = await asyncio.to_thread(
                s3.lister, prefixe, recursif=args.recursif, limite=args.limite
            )
        except TraitementImpossible as erreur:
            erreur_listing = str(erreur)

    if args.json:
        _print_json(
            {
                "config": config,
                "acces": acces,
                "contenu": contenu,
                "erreur_listing": erreur_listing,
            }
        )
    else:
        print("Configuration S3")
        print(f"  endpoint     : {config['endpoint']}")
        print(f"  region       : {config['region']}")
        print(f"  bucket       : {config['bucket']}")
        print(f"  prefixe      : {config['prefixe']}")
        print(f"  adressage    : {config['adressage']}")
        identifiants = config["identifiants"]
        if config["access_key"]:
            identifiants += f" (S3_ACCESS_KEY={config['access_key']})"
        print(f"  identifiants : {identifiants}")
        if config["avertissement"]:
            print(f"  ATTENTION    : {config['avertissement']}")

        print(f"Accès : {acces['status']}")
        if acces["error"]:
            print(f"  {acces['error']}")
        if acces["buckets_visibles"]:
            print(f"  buckets visibles : {', '.join(acces['buckets_visibles'])}")

        if erreur_listing:
            print(f"\nListing impossible : {erreur_listing}")
        elif contenu is not None:
            _print_contenu(contenu, prefixe)

    return EXIT_OK if acces["status"] == "ok" and not erreur_listing else EXIT_KO


def _print_contenu(contenu: dict, prefixe: str) -> None:
    """Affiche le contenu d'un préfixe : sous-dossiers puis objets."""
    print(f"\nContenu de s3://{contenu['bucket']}/{prefixe}")
    if not contenu["dossiers"] and not contenu["objets"]:
        print("  (vide)")
        return

    for dossier in contenu["dossiers"]:
        nom = dossier[len(prefixe):] if prefixe else dossier
        print(f"  [dossier]  {nom}")
    for objet in contenu["objets"]:
        date_modif = objet["modifie_le"]
        # En JSON la date est sérialisée par défaut ; en texte on la raccourcit.
        libelle_date = date_modif.strftime("%Y-%m-%d %H:%M") if date_modif else ""
        print(
            f"  {s3.taille_lisible(objet['taille']):>10}  "
            f"{objet['nom']}  {libelle_date}"
        )

    resume = f"{len(contenu['objets'])} objet(s), {len(contenu['dossiers'])} dossier(s)"
    if contenu["tronque"]:
        resume += " — listing tronqué, relancer avec --limite"
    print(resume)


# ---------------------------------------------------------------------------
# Traitements métier
# ---------------------------------------------------------------------------


async def _executer_traitement(traitement, args: argparse.Namespace) -> int:
    """Exécute un traitement, affiche son rapport et en déduit le code de retour.

    Le contexte de corrélation est posé ici : toutes les lignes de log émises pendant le
    traitement, y compris celles de `app.db.mysql`, porteront `id_traitement`.

    Toute exception qui remonterait malgré tout est convertie en rapport d'échec : la CLI
    ne doit jamais rendre de stacktrace à l'exploitant.
    """
    jeton = set_id_traitement(args.id_traitement)
    try:
        rapport = await traitement(args)
    except Exception as erreur:  # noqa: BLE001 - dernier filet avant la sortie console
        log.exception("Erreur commande %s", ctx(commande=args.commande))
        rapport = Rapport(
            titre=args.commande.upper().replace("-", " "),
            id_traitement=args.id_traitement,
        )
        rapport.ko(f"{type(erreur).__name__} : {erreur}")
        rapport.statut = ECHEC
    finally:
        reset_id_traitement(jeton)

    if args.json:
        _print_json(rapport.to_dict())
    else:
        print(rapport.texte())

    return EXIT_OK if rapport.reussi else EXIT_KO


async def cmd_charger_cles_repartition(args: argparse.Namespace) -> int:
    """Charge `trppu_cles_repartition` depuis le CSV déposé sur S3."""
    return await _executer_traitement(
        lambda a: charger_cles_repartition(a.id_traitement, a.fichier), args
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.main",
        description=(
            "Batch yb06 — vérifications de la base de données et chargement des "
            "référentiels."
        ),
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

    s3_check = sous_commandes.add_parser(
        "s3-check",
        parents=[commun],
        help="Configuration S3, test d'accès et contenu du bucket.",
    )
    s3_check.add_argument(
        "--prefixe",
        default=None,
        help="Dossier à lister, à défaut de S3_PREFIXE. Chaîne vide pour la racine.",
    )
    s3_check.add_argument(
        "--recursif",
        action="store_true",
        help="Déroule toute l'arborescence au lieu du seul niveau courant.",
    )
    s3_check.add_argument(
        "--limite",
        type=int,
        default=200,
        help="Nombre maximum d'objets listés (défaut : 200).",
    )
    s3_check.set_defaults(handler=cmd_s3_check)

    chargement = sous_commandes.add_parser(
        "charger-cles-repartition",
        parents=[commun],
        help="Charge trppu_cles_repartition depuis le CSV déposé sur S3.",
    )
    # Stocké sous `id_traitement` : c'est le nom que `main()` reprend dans ses logs
    # `Début`/`Fin commande` et que `_executer_traitement` pose comme corrélation.
    chargement.add_argument(
        "id_traitement",
        type=int,
        metavar="id_referentiel",
        help="Référentiel à charger. Le fichier doit porter le même.",
    )
    chargement.add_argument(
        "--fichier",
        default=None,
        help="Nom du fichier dans le bucket, à défaut de CSV_CLES_REPARTITION.",
    )
    chargement.set_defaults(handler=cmd_charger_cles_repartition)

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
