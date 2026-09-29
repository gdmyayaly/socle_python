"""Explique, site par site, pourquoi un total de trafic est nul.

    python -m scripts.expliquer_sites_totaux_zero C:/data/tri/cles_sites_totaux_zero.csv

Lit le fichier produit par `scripts/extraire_sites_totaux_zero.py` et écrit, à côté,
`<nom>_explication.csv` : une ligne par site et par total nul, avec les chiffres qui
l'expliquent et une cause probable. La même synthèse s'affiche à l'écran.

Le total d'un site est la somme de la colonne sur ses PDI **actifs** (étape `agregats`) ;
c'est donc sur eux que porte le diagnostic, les PDI inactifs servant à reconnaître un trafic
porté uniquement par des PDI fermés.

Bibliothèque standard uniquement.
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path

COLONNES = {"colis": "trafic_colis", "oo": "trafic_oo", "3s": "trafic_3s",
            "potentielip": "potentielip"}


def _nombre(texte: str) -> Decimal | None:
    try:
        return Decimal(texte)
    except InvalidOperation:
        return None


@dataclass
class Compteurs:
    vides: int = 0
    zeros: int = 0
    non_nuls: int = 0
    somme: Decimal = Decimal(0)

    def ajouter(self, valeur: str) -> None:
        texte = valeur.strip()
        if not texte:
            self.vides += 1
            return
        nombre = _nombre(texte) or Decimal(0)
        self.somme += nombre
        if nombre == 0:
            self.zeros += 1
        else:
            self.non_nuls += 1


@dataclass
class Site:
    code: str
    libelle: str = ""
    dex: str = ""
    totaux_a_zero: list[str] = field(default_factory=list)
    actifs: int = 0
    inactifs: int = 0
    par_total: dict[str, dict[str, Compteurs]] = field(default_factory=dict)


#: Catégories de cause, dans l'ordre où elles sont testées.
PORTE_PAR_INACTIFS = "porté uniquement par des PDI inactifs"
VIDE = "non renseigné (vide) sur tous les PDI actifs"
ZERO = "à 0 sur tous les PDI actifs"
MIXTE = "vide ou à 0 selon les PDI actifs"


def cause(site: Site, total: str) -> tuple[str, str]:
    """(catégorie, explication chiffrée). La catégorie sert au regroupement."""
    actifs = site.par_total[total]["O"]
    inactifs = site.par_total[total]["N"]
    if inactifs.non_nuls and not actifs.non_nuls:
        return PORTE_PAR_INACTIFS, (
            f"{inactifs.non_nuls} PDI inactif(s) (date_fin_validite renseignée) portent "
            f"{inactifs.somme}, aucun PDI actif : le trafic a quitté le périmètre actif"
        )
    if actifs.vides and not actifs.zeros:
        return VIDE, f"{actifs.vides} PDI actif(s) sans valeur : donnée manquante à l'extraction"
    if actifs.zeros and not actifs.vides:
        return ZERO, (
            f"{actifs.zeros} PDI actif(s) à 0 : absence réelle de trafic ou valeur par défaut ?"
        )
    return MIXTE, (
        f"{actifs.vides} PDI actif(s) vide(s), {actifs.zeros} à 0 : donnée manquante ou nulle"
    )


def lire(chemin: Path, delimiteur: str) -> dict[str, Site]:
    sites: dict[str, Site] = {}
    with open(chemin, encoding="utf-8-sig", newline="") as flux:
        for ligne in csv.DictReader(flux, delimiter=delimiteur):
            code = ligne["regate_site"].strip()
            site = sites.get(code)
            if site is None:
                site = sites[code] = Site(
                    code=code,
                    libelle=ligne.get("libelle_site", "").strip(),
                    dex=ligne.get("libelle_dex", "").strip(),
                    totaux_a_zero=[t for t in ligne["totaux_a_zero"].split(",") if t],
                )
                site.par_total = {
                    total: {"O": Compteurs(), "N": Compteurs()} for total in site.totaux_a_zero
                }
            etat = "O" if ligne["actif"] == "O" else "N"
            if etat == "O":
                site.actifs += 1
            else:
                site.inactifs += 1
            for total in site.totaux_a_zero:
                site.par_total[total][etat].ajouter(ligne[COLONNES[total]])
    return sites


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.expliquer_sites_totaux_zero",
        description="Explique, site par site, pourquoi un total de trafic est nul.",
    )
    parser.add_argument("fichier", type=Path, help="<nom>_sites_totaux_zero.csv")
    parser.add_argument("--delimiteur", default=";", help="Défaut : ';'.")
    args = parser.parse_args(argv)

    if not args.fichier.is_file():
        print(f"Fichier introuvable : {args.fichier}", file=sys.stderr)
        return 1

    sites = lire(args.fichier, args.delimiteur)
    sortie = args.fichier.with_name(f"{args.fichier.stem}_explication.csv")
    causes: dict[str, int] = {}
    with open(sortie, "w", encoding="utf-8", newline="") as flux:
        ecrivain = csv.writer(flux, delimiter=args.delimiteur, lineterminator="\n")
        ecrivain.writerow([
            "regate_site", "libelle_site", "libelle_dex", "total_nul", "pdi_actifs",
            "pdi_inactifs", "actifs_vides", "actifs_a_zero", "inactifs_non_nuls",
            "somme_inactifs", "cause", "explication",
        ])
        for code in sorted(sites):
            site = sites[code]
            for total in site.totaux_a_zero:
                actifs = site.par_total[total]["O"]
                inactifs = site.par_total[total]["N"]
                categorie, explication = cause(site, total)
                cle = f"{total} {categorie}"
                causes[cle] = causes.get(cle, 0) + 1
                ecrivain.writerow([
                    code, site.libelle, site.dex, total, site.actifs, site.inactifs,
                    actifs.vides, actifs.zeros, inactifs.non_nuls, inactifs.somme,
                    categorie, explication,
                ])
                print(f"{code} {site.libelle[:28]:<28} {total:<11} {explication}")

    print(f"\n{len(sites)} site(s). Causes, par total :")
    for explication, nombre in sorted(causes.items(), key=lambda c: -c[1]):
        print(f"  {nombre:>4}  {explication}")
    print(f"\nDétail : {sortie}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
