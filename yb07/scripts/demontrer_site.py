"""Démonstration, à partir du CSV livré, du total nul d'un site — pour le métier.

    python -m scripts.demontrer_site C:/data/cles.csv 122200
    python -m scripts.demontrer_site C:/data/cles.csv 122200 --sortie C:/data/demo

Produit, dans le dossier `--sortie` (défaut : celui du fichier) :

    <nom>_site_<code>.csv                 toutes les lignes du site, telles que livrées,
                                          précédées de numero_ligne, actif (O/N) et
                                          conforme (O/N)
    <nom>_site_<code>_demonstration.txt   la démonstration pas à pas : PDI du site, ceux qui
                                          comptent, leurs valeurs, la somme posée terme à
                                          terme, et la division impossible qui en découle

Mêmes règles que la chaîne : le total d'un site est la somme, sur ses PDI **actifs**
(`date_fin_validite` vide) et **conformes** (acceptés par le chargement), de `trafic_colis`,
`trafic_oo`, `trafic_3s` et `potentielip` (vide = 0) — `db/DSR-696_site_trafic.sql`. La clé
d'un PDI vaut sa valeur divisée par ce total — `db/DSR-699_cles_calculees.sql`.

Le rapport est en texte brut, 72 colonnes, sans accents : il passe tel quel dans Teams.
Une seule lecture du fichier, seules les lignes du site sont gardées en mémoire.
"""

from __future__ import annotations

import argparse
import csv
import sys
import textwrap
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from app.config import CSV_DELIMITEUR, CSV_ENCODAGE
from app.erreurs import TraitementImpossible
from app.traitements.cles_repartition import COLONNES_CSV, verifier_entete
from scripts.extraire_sites_totaux_zero import _referentiel_de, _valider

LARGEUR = 72
#: Totaux de site : (nom, colonne du fichier, libellé).
TOTAUX = (
    ("colis", "trafic_colis", "trafic colis"),
    ("oo", "trafic_oo", "trafic OO"),
    ("3s", "trafic_3s", "trafic 3S"),
    ("potentielip", "potentielip", "potentiel IP"),
)
TERMES_MAX = 20  # termes affichés dans la somme posée, le reste est résumé


@dataclass
class Ligne:
    numero: int
    brute: list[str]
    actif: bool
    conforme: bool
    motif: str = ""

    def valeur(self, colonne: str) -> str:
        return self.brute[COLONNES_CSV.index(colonne)].strip()

    @property
    def compte(self) -> bool:
        """Entre dans le total du site : actif et accepté par le chargement."""
        return self.actif and self.conforme


def _nombre(texte: str) -> Decimal:
    try:
        return Decimal(texte) if texte else Decimal(0)
    except InvalidOperation:
        return Decimal(0)


def _ascii(texte: str) -> str:
    texte = texte.replace("—", "-").replace("…", "...").replace("÷", "/")
    return unicodedata.normalize("NFKD", texte).encode("ascii", "ignore").decode("ascii")


def extraire_site(
    source: Path, site: str, delimiteur: str, encodage: str
) -> tuple[list[Ligne], int]:
    """Lignes du site (une lecture du fichier) et nombre total de lignes lues."""
    lignes: list[Ligne] = []
    referentiel: int | None = None
    rang_site = COLONNES_CSV.index("regate_site")
    rang_fin = COLONNES_CSV.index("date_fin_validite")
    lues = 0
    with open(source, encoding=encodage, newline="") as flux:
        lecteur = csv.reader(flux, delimiter=delimiteur)
        verifier_entete(next(lecteur, None))
        for brute in lecteur:
            if not brute or all(not champ.strip() for champ in brute):
                continue
            lues += 1
            if referentiel is None and len(brute) == len(COLONNES_CSV):
                referentiel = _referentiel_de(dict(zip(COLONNES_CSV, brute)))
            if len(brute) <= rang_site or brute[rang_site].strip() != site:
                continue
            numero = lecteur.line_num
            try:
                _valider(brute, numero, referentiel or 0)
                conforme, motif = True, ""
            except TraitementImpossible as erreur:
                conforme, motif = False, str(erreur)
            actif = len(brute) > rang_fin and not brute[rang_fin].strip()
            lignes.append(Ligne(numero, brute, actif, conforme, motif))
    return lignes, lues


def ecrire_extrait(lignes: list[Ligne], chemin: Path, delimiteur: str) -> None:
    with open(chemin, "w", encoding="utf-8", newline="") as flux:
        ecrivain = csv.writer(flux, delimiter=delimiteur, lineterminator="\n")
        ecrivain.writerow(["numero_ligne", "actif", "conforme", *COLONNES_CSV])
        for ligne in lignes:
            ecrivain.writerow([
                ligne.numero, "O" if ligne.actif else "N", "O" if ligne.conforme else "N",
                *ligne.brute,
            ])


def _somme_posee(valeurs: list[str]) -> str:
    termes = [v or "(vide)" for v in valeurs]
    if len(termes) > TERMES_MAX:
        reste = len(termes) - TERMES_MAX
        termes = termes[:TERMES_MAX] + [f"... ({reste} autres termes)"]
    return " + ".join(termes) if termes else "(aucun terme)"


def demonstration(source: Path, site: str, lignes: list[Ligne], lues: int) -> str:
    trait, sous_trait = "=" * LARGEUR, "-" * LARGEUR
    actives = [l for l in lignes if l.compte]
    inactives = [l for l in lignes if not l.actif]
    rejetees = [l for l in lignes if l.actif and not l.conforme]
    premiere = lignes[0] if lignes else None

    sortie = [
        trait,
        f"DEMONSTRATION - TOTAL DE TRAFIC DU SITE {site}",
        trait,
        "",
        f"Fichier    : {source}",
        f"Date       : {datetime.now():%Y-%m-%d %H:%M}",
        f"Site       : {site} - {premiere.valeur('libelle_site') if premiere else '?'}",
        f"DEX        : {premiere.valeur('libelle_dex') if premiere else '?'}",
        f"Lignes lues dans le fichier : {lues}",
        "",
        sous_trait,
        "1. LES LIGNES DU SITE DANS LE FICHIER",
        sous_trait,
        "",
        f"{len(lignes)} ligne(s) portent regate_site = {site} :",
        f"  - {len(actives)} PDI actif(s) : date_fin_validite vide.",
        "    Ce sont les seuls qui comptent dans le total du site.",
        f"  - {len(inactives)} PDI inactif(s) : date_fin_validite renseignee,",
        "    exclus du total (PDI fermes).",
    ]
    if rejetees:
        sortie += [
            f"  - {len(rejetees)} ligne(s) active(s) non conforme(s), refusee(s) au",
            "    chargement, donc absente(s) de la base et du total :",
        ]
        sortie += [f"      ligne {l.numero} : {l.motif}" for l in rejetees[:10]]
    sortie += [
        "",
        "Regle de calcul (etape agregats) :",
        "  total du site = somme des valeurs de ses PDI actifs",
        "  (une case vide compte pour 0)",
    ]

    conclusions = []
    for rang, (nom, colonne, libelle) in enumerate(TOTAUX, start=2):
        valeurs = [l.valeur(colonne) for l in actives]
        total = sum((_nombre(v) for v in valeurs), Decimal(0))
        non_nuls = sum(1 for v in valeurs if _nombre(v) != 0)
        titre = f"{rang}. {libelle.upper()} (colonne {colonne})"
        sortie += ["", sous_trait, titre, sous_trait, ""]
        if total != 0:
            sortie.append(
                f"Total = {total} ({non_nuls} PDI actif(s) sur {len(actives)} avec une valeur) :"
            )
            sortie.append("non nul, les cles de ce trafic se calculent normalement.")
            continue

        vides = sum(1 for v in valeurs if not v)
        sortie += [
            f"Valeur de chacun des {len(actives)} PDI actif(s) :",
            "",
            f"  {'ligne':>9}  {'id_pdi':>12}  valeur",
        ]
        sortie += [
            f"  {l.numero:>9}  {l.valeur('id'):>12}  {l.valeur(colonne) or '(vide)'}"
            for l in actives
        ]
        sortie += [
            "",
            "Somme posee :",
            f"  {_somme_posee(valeurs)}",
            f"  = {total}",
            "",
            f"Constat : {non_nuls} PDI actif(s) avec une valeur, {vides} vide(s),",
            f"{len(valeurs) - vides - non_nuls} a 0. Le total vaut 0 parce que le fichier",
            "ne contient aucune valeur pour ce trafic sur les PDI actifs du site.",
        ]
        porteurs = [l for l in inactives if _nombre(l.valeur(colonne)) != 0]
        if porteurs:
            somme = sum((_nombre(l.valeur(colonne)) for l in porteurs), Decimal(0))
            sortie += [
                "",
                f"En revanche, {len(porteurs)} PDI INACTIF(S) portent {somme} :",
            ]
            sortie += [
                f"  ligne {l.numero}, PDI {l.valeur('id')} : {l.valeur(colonne)}, "
                f"ferme le {l.valeur('date_fin_validite')}"
                for l in porteurs[:10]
            ]
            sortie.append("Ce trafic existe, mais seulement sur des PDI fermes.")
        sortie += [
            "",
            "Consequence sur la cle (etape cles) :",
            f"  cle {nom} d'un PDI = {colonne} du PDI / total du site",
            "                     = 0 / 0",
            "  -> division impossible : aucune part ne peut etre calculee.",
        ]
        conclusions.append(libelle)

    sortie += ["", sous_trait, f"{len(TOTAUX) + 2}. CONCLUSION", sous_trait, ""]
    if conclusions:
        sortie += [
            f"Pour le site {site}, le total est nul pour : {', '.join(conclusions)}.",
            "Ce n'est pas le traitement qui additionne des zeros : c'est le fichier",
            "livre qui ne porte aucune valeur pour ce trafic sur les PDI actifs du",
            "site. Sans total, la repartition entre PDI n'a pas de sens.",
            "",
            "Regle metier attendue, au choix :",
            "  1. corriger les donnees du fichier et le relivrer",
            "  2. cle = 0 pour ce trafic",
            "  3. repartition egale : cle = 1 / nombre de PDI actifs",
            "  4. exclure le site du calcul des cles",
        ]
    else:
        sortie.append(f"Aucun total nul pour le site {site} : ses cles se calculent.")
    sortie += ["", "Lignes du site, telles que livrees : fichier CSV joint."]

    rendu = []
    for ligne in sortie:
        ligne = _ascii(ligne)
        if len(ligne) <= LARGEUR or ligne.startswith("Fichier"):
            rendu.append(ligne)
        else:
            retrait = " " * (len(ligne) - len(ligne.lstrip()) + 2)
            rendu += textwrap.wrap(ligne, LARGEUR, subsequent_indent=retrait)
    return "\n".join(rendu) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.demontrer_site",
        description="Démontre, depuis le CSV livré, le total de trafic nul d'un site.",
    )
    parser.add_argument("fichier", type=Path, help="CSV d'origine.")
    parser.add_argument("site", help="Code régate du site, ex. 122200.")
    parser.add_argument(
        "--sortie", type=Path, default=None, help="Dossier des fichiers produits."
    )
    parser.add_argument("--delimiteur", default=CSV_DELIMITEUR, help="Défaut : ';'.")
    parser.add_argument("--encodage", default=CSV_ENCODAGE, help="Défaut : utf-8-sig.")
    args = parser.parse_args(argv)

    if not args.fichier.is_file():
        print(f"Fichier introuvable : {args.fichier}", file=sys.stderr)
        return 1
    site = args.site.strip()
    dossier = args.sortie or args.fichier.parent
    dossier.mkdir(parents=True, exist_ok=True)
    base = args.fichier.name.removesuffix(".gz").removesuffix(".csv")

    print(f"Lecture de {args.fichier} ...", file=sys.stderr, flush=True)
    try:
        lignes, lues = extraire_site(args.fichier, site, args.delimiteur, args.encodage)
    except TraitementImpossible as erreur:
        print(f"Lecture impossible : {erreur}", file=sys.stderr)
        return 1
    if not lignes:
        print(f"Aucune ligne pour le site {site} dans le fichier ({lues} lignes lues).")
        return 1

    extrait = dossier / f"{base}_site_{site}.csv"
    rapport = dossier / f"{base}_site_{site}_demonstration.txt"
    ecrire_extrait(lignes, extrait, args.delimiteur)
    texte = demonstration(args.fichier, site, lignes, lues)
    rapport.write_text(texte, encoding="utf-8")
    print(texte)
    print(f"Fichiers : {extrait}\n           {rapport}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
