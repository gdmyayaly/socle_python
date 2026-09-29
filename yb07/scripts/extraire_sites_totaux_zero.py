"""Trie un CSV de clés de répartition en trois fichiers, pour une première version sans les
éléments problématiques.

    python -m scripts.extraire_sites_totaux_zero C:/data/cles.csv
    python -m scripts.extraire_sites_totaux_zero C:/data/cles.csv --sortie C:/data/tri

Fichiers produits dans le dossier `--sortie` (défaut : celui du source), nommés d'après le
fichier source :

    <nom>_lignes_incompletes.csv   lignes non conformes : numero_ligne, motif, puis la ligne
    <nom>_sites_totaux_zero.csv    toutes les lignes des sites dont un total est nul :
                                   totaux_a_zero, actif (O/N), puis la ligne
    <nom>_bon.csv                  tout le reste, au format d'origine — rechargeable tel quel

Règles :

* une ligne est **incomplète** si le chargement la refuserait (`convertir`, importé de
  `app/traitements/cles_repartition.py`) ou si MySQL la refuserait à l'insertion (longueurs,
  bornes — `controler_bornes` du script de nettoyage). Mêmes règles, pas de copie ;
* les totaux sont ceux de l'étape `agregats` (`db/DSR-696_site_trafic.sql`), calculés sur les
  seules lignes conformes : somme par site des PDI actifs (`date_fin_validite` vide) de
  `trafic_colis`, `trafic_oo`, `trafic_3s` et `potentielip` (vide = 0). Un total nul
  donnerait une division par zéro au calcul des clés.

Les doublons de PDI ne sont pas traités ici : `scripts/nettoyer_csv_cles.py` s'en charge,
ou le chargement avec `--skip-errors`.

Deux lectures en streaming ; en mémoire, un total par site et les numéros des lignes
incomplètes seulement — utilisable sur 22 M de lignes.
"""

from __future__ import annotations

import argparse
import csv
import sys
from decimal import Decimal
from pathlib import Path

from app.config import CSV_DELIMITEUR, CSV_ENCODAGE
from app.erreurs import TraitementImpossible
from app.traitements.cles_repartition import COLONNES_CSV, convertir, verifier_entete
from scripts.nettoyer_csv_cles import controler_bornes

# Rang, dans le tuple rendu par `convertir`, des valeurs qui entrent dans les totaux.
TOTAUX = {"colis": 2, "oo": 3, "3s": 4, "potentielip": 14}
RANG_DATE_FIN = 17
PROGRESSION_TOUTES_LES = 1_000_000


def chemins_de_sortie(source: Path, dossier: Path | None) -> dict[str, Path]:
    dossier = dossier or source.parent
    base = source.name
    for suffixe in (".gz", ".csv"):
        base = base.removesuffix(suffixe)
    return {
        "incompletes": dossier / f"{base}_lignes_incompletes.csv",
        "totaux_zero": dossier / f"{base}_sites_totaux_zero.csv",
        "bon": dossier / f"{base}_bon.csv",
    }


def _referentiel_de(ligne: dict[str, str]) -> int | None:
    try:
        return int((ligne.get("id_referentiel") or "").strip())
    except ValueError:
        return None


def _valider(brute: list[str], numero: int, referentiel: int) -> tuple:
    """Valeurs converties de la ligne ; lève `TraitementImpossible` si elle est incomplète."""
    if len(brute) != len(COLONNES_CSV):
        raise TraitementImpossible(
            f"Ligne {numero} : {len(brute)} colonne(s) au lieu de {len(COLONNES_CSV)}."
        )
    ligne = dict(zip(COLONNES_CSV, brute))
    valeurs = convertir(ligne, numero, referentiel)
    controler_bornes(ligne, valeurs, numero)
    return valeurs


def _progression(etape: str, lues: int) -> None:
    if lues % PROGRESSION_TOUTES_LES == 0:
        print(f"... {etape} : {lues:,} lignes".replace(",", " "), file=sys.stderr, flush=True)


def trier(
    source: Path,
    *,
    dossier_sortie: Path | None = None,
    referentiel: int | None = None,
    delimiteur: str = CSV_DELIMITEUR,
    encodage: str = CSV_ENCODAGE,
) -> dict:
    """Produit les trois fichiers et rend le bilan. Lève `TraitementImpossible` si l'en-tête
    est faux : rien d'autre n'aurait de sens."""
    sorties = chemins_de_sortie(source, dossier_sortie)
    sorties["bon"].parent.mkdir(parents=True, exist_ok=True)
    options = {"delimiter": delimiteur, "lineterminator": "\n"}

    # --- Lecture 1 : lignes incomplètes (écrites au fil de l'eau) et totaux par site -------
    totaux: dict[str, dict[str, Decimal]] = {}
    incompletes: set[int] = set()
    with (
        open(source, encoding=encodage, newline="") as entree,
        open(sorties["incompletes"], "w", encoding="utf-8", newline="") as f_incompletes,
    ):
        lecteur = csv.reader(entree, delimiter=delimiteur)
        entete = next(lecteur, None)
        verifier_entete(entete)
        ecrivain = csv.writer(f_incompletes, **options)
        ecrivain.writerow(["numero_ligne", "motif", *COLONNES_CSV])

        lues = 0
        for brute in lecteur:
            if not brute or all(not champ.strip() for champ in brute):
                continue
            lues += 1
            _progression("lecture 1/2", lues)
            numero = lecteur.line_num
            if referentiel is None and len(brute) == len(COLONNES_CSV):
                referentiel = _referentiel_de(dict(zip(COLONNES_CSV, brute)))
            try:
                valeurs = _valider(brute, numero, referentiel or 0)
            except TraitementImpossible as erreur:
                incompletes.add(numero)
                ecrivain.writerow([numero, str(erreur), *brute])
                continue
            if valeurs[RANG_DATE_FIN] is not None:
                continue  # PDI inactif : exclu des agrégats
            cumul = totaux.setdefault(valeurs[6], dict.fromkeys(TOTAUX, Decimal(0)))
            for nom, rang in TOTAUX.items():
                cumul[nom] += valeurs[rang] or 0

    sites = {
        site: ",".join(nom for nom, total in cumul.items() if total == 0)
        for site, cumul in totaux.items()
        if any(total == 0 for total in cumul.values())
    }

    # --- Lecture 2 : lignes des sites à total nul, et fichier bon --------------------------
    rang_site = COLONNES_CSV.index("regate_site")
    rang_fin = COLONNES_CSV.index("date_fin_validite")
    nb_zero = nb_bon = 0
    with (
        open(source, encoding=encodage, newline="") as entree,
        open(sorties["totaux_zero"], "w", encoding="utf-8", newline="") as f_zero,
        open(sorties["bon"], "w", encoding="utf-8", newline="") as f_bon,
    ):
        lecteur = csv.reader(entree, delimiter=delimiteur)
        next(lecteur)
        zero = csv.writer(f_zero, **options)
        bon = csv.writer(f_bon, **options)
        zero.writerow(["totaux_a_zero", "actif", *COLONNES_CSV])
        bon.writerow(COLONNES_CSV)

        lues = 0
        for brute in lecteur:
            if not brute or all(not champ.strip() for champ in brute):
                continue
            lues += 1
            _progression("lecture 2/2", lues)
            if lecteur.line_num in incompletes:
                continue
            site = brute[rang_site].strip()
            if site in sites:
                actif = "N" if brute[rang_fin].strip() else "O"
                zero.writerow([sites[site], actif, *brute])
                nb_zero += 1
            else:
                bon.writerow(brute)
                nb_bon += 1

    return {
        "sorties": sorties,
        "referentiel": referentiel,
        "lignes_lues": lues,
        "incompletes": len(incompletes),
        "sites_actifs": len(totaux),
        "sites_totaux_zero": sites,
        "lignes_totaux_zero": nb_zero,
        "lignes_bonnes": nb_bon,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.extraire_sites_totaux_zero",
        description="Trie un CSV de clés de répartition : lignes incomplètes, sites à total "
        "nul, et fichier bon rechargeable.",
    )
    parser.add_argument("fichier", type=Path, help="CSV source.")
    parser.add_argument(
        "--sortie",
        type=Path,
        default=None,
        help="Dossier des trois fichiers produits (défaut : celui du source).",
    )
    parser.add_argument(
        "--referentiel",
        type=int,
        default=None,
        help="Référentiel attendu. Par défaut, celui de la première ligne.",
    )
    parser.add_argument("--delimiteur", default=CSV_DELIMITEUR, help="Défaut : ';'.")
    parser.add_argument("--encodage", default=CSV_ENCODAGE, help="Défaut : utf-8-sig.")
    args = parser.parse_args(argv)

    if not args.fichier.is_file():
        print(f"Fichier introuvable : {args.fichier}", file=sys.stderr)
        return 1
    if args.sortie is not None and args.sortie.exists() and not args.sortie.is_dir():
        print(f"--sortie doit être un dossier : {args.sortie}", file=sys.stderr)
        return 1

    try:
        bilan = trier(
            args.fichier,
            dossier_sortie=args.sortie,
            referentiel=args.referentiel,
            delimiteur=args.delimiteur,
            encodage=args.encodage,
        )
    except TraitementImpossible as erreur:
        print(f"Tri impossible : {erreur}", file=sys.stderr)
        return 1

    sites = bilan["sites_totaux_zero"]
    print(f"Référentiel            : {bilan['referentiel']}")
    print(f"Lignes lues            : {bilan['lignes_lues']}")
    print(f"Lignes incomplètes     : {bilan['incompletes']}")
    print(f"Sites à total nul      : {len(sites)} sur {bilan['sites_actifs']} site(s) actif(s)")
    for site in sorted(sites):
        print(f"    {site} : {sites[site]}")
    print(f"Lignes de ces sites    : {bilan['lignes_totaux_zero']}")
    print(f"Lignes bonnes          : {bilan['lignes_bonnes']}")
    print("Fichiers produits :")
    for chemin in bilan["sorties"].values():
        print(f"    {chemin}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
