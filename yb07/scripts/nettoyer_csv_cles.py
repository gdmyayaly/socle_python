"""Nettoyage d'un CSV de clés de répartition, avant chargement.

    python -m scripts.nettoyer_csv_cles data/cles.csv
    python -m scripts.nettoyer_csv_cles data/cles.csv --referentiel 1 --sortie data/propre

Outil autonome (ni base, ni S3), à lancer depuis `yb07/`. Il lit le fichier en streaming et
produit, dans le dossier de sortie (par défaut celui du fichier) :

    <nom>_propre.csv     lignes valides, doublons retirés — prêt pour
                         `charger-cles-repartition-local`
    <nom>_erreurs.csv    lignes non conformes, avec leur numéro et le motif
    <nom>_doublons.csv   doublons de PDI écartés, avec la ligne conservée et leur type
    <nom>_rapport.txt    synthèse (texte brut, 72 colonnes, sans accents)

Les règles sont **celles du chargement** : `convertir` et `verifier_entete` sont importés de
`app/traitements/cles_repartition.py`, ils ne sont pas recopiés. S'y ajoutent les contraintes
que seul MySQL vérifie au chargement (longueurs, bornes des nombres — cf.
`yb05/db/database.sql`), pour que le fichier propre se charge sans rejet.

Doublons : clé `(id_pdi, id_referentiel)`, comme `uk_pdi_ref`. La **première** occurrence est
conservée. Un doublon est « IDENTIQUE » s'il porte exactement les mêmes données que la ligne
conservée, « CONFLIT » sinon — deux lignes d'un même PDI aux trafics différents : le choix de
la première est arbitraire, ces cas sont à faire valider par le métier.

Mémoire : un index des PDI conservés est tenu en mémoire — compter 2 à 3 Go pour 22 M de
lignes.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import textwrap
import time
import unicodedata
from array import array
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from app.config import CSV_DELIMITEUR, CSV_ENCODAGE
from app.erreurs import TraitementImpossible
from app.traitements.cles_repartition import COLONNES_CSV, convertir, verifier_entete

LARGEUR_RAPPORT = 72
EXEMPLES_MAX = 20
PROGRESSION_TOUTES_LES = 1_000_000

# Contraintes de `trppu_cles_repartition` que la conversion ne vérifie pas et que MySQL
# refuserait au chargement (mode strict) — source : `yb05/db/database.sql`.
LONGUEURS_MAX = {
    "nature": 3,  # char(3)
    "regate_site": 6,  # char(6)
    "type": 10,  # varchar(10)
    "libelle_site": 100,  # varchar(100)
    "regate_etab": 6,  # char(6)
    "libelle_etab": 100,  # varchar(100)
    "regate_dex": 6,  # char(6)
    "libelle_dex": 100,  # varchar(100)
}
# decimal(25,19) : 6 chiffres avant la virgule.
BORNE_DECIMAL = Decimal(10) ** 6
DECIMAUX = {2: "trafic_colis", 3: "trafic_oo", 4: "trafic_3s"}
SMALLINT = {13: "nb_pre", 14: "potentielip"}
BIGINT = {0: "id", 1: "pdi_rattache"}
BORNE_SMALLINT = (-32768, 32767)
BORNE_BIGINT = (-(2**63), 2**63 - 1)


# ---------------------------------------------------------------------------
# Contrôles
# ---------------------------------------------------------------------------


def controler_bornes(ligne: dict[str, str], valeurs: tuple[Any, ...], numero: int) -> None:
    """Contrôles MySQL absents de `convertir`. Lève `TraitementImpossible` comme lui."""
    for colonne, maximum in LONGUEURS_MAX.items():
        longueur = len((ligne.get(colonne) or "").strip())
        if longueur > maximum:
            raise TraitementImpossible(
                f"Ligne {numero} : '{colonne}' fait {longueur} caractères, "
                f"{maximum} au maximum."
            )
    for rang, colonne in DECIMAUX.items():
        if abs(valeurs[rang]) >= BORNE_DECIMAL:
            raise TraitementImpossible(
                f"Ligne {numero} : '{colonne}' = '{valeurs[rang]}' dépasse 6 chiffres avant "
                "la virgule."
            )
    for bornes, colonnes in ((BORNE_SMALLINT, SMALLINT), (BORNE_BIGINT, BIGINT)):
        for rang, colonne in colonnes.items():
            valeur = valeurs[rang]
            if valeur is not None and not bornes[0] <= valeur <= bornes[1]:
                raise TraitementImpossible(
                    f"Ligne {numero} : '{colonne}' = '{valeur}' hors bornes "
                    f"[{bornes[0]} ; {bornes[1]}]."
                )


def categorie(motif: str) -> str:
    """Motif générique, pour regrouper : sans numéro de ligne, sans valeur."""
    texte = re.sub(r"^Ligne \d+ : ", "", motif)
    texte = re.sub(r" = '[^']*'", "", texte)
    return re.sub(r"\d+", "N", texte)


def _referentiel_de(ligne: dict[str, str]) -> int | None:
    try:
        return int((ligne.get("id_referentiel") or "").strip())
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Nettoyage
# ---------------------------------------------------------------------------


@dataclass
class Bilan:
    source: Path
    sorties: dict[str, Path]
    referentiel: int | None = None
    referentiel_impose: bool = False
    lues: int = 0
    vides: int = 0
    conservees: int = 0
    actives: int = 0
    erreurs: int = 0
    doublons_identiques: int = 0
    doublons_conflits: int = 0
    categories: Counter = field(default_factory=Counter)
    exemples_erreurs: list[str] = field(default_factory=list)
    exemples_conflits: list[str] = field(default_factory=list)
    duree_s: float = 0.0

    @property
    def doublons(self) -> int:
        return self.doublons_identiques + self.doublons_conflits


def chemins_de_sortie(source: Path, dossier: Path | None) -> dict[str, Path]:
    dossier = dossier or source.parent
    base = source.name
    for suffixe in (".gz", ".csv"):
        base = base.removesuffix(suffixe)
    return {
        "propre": dossier / f"{base}_propre.csv",
        "erreurs": dossier / f"{base}_erreurs.csv",
        "doublons": dossier / f"{base}_doublons.csv",
        "rapport": dossier / f"{base}_rapport.txt",
    }


def nettoyer(
    source: Path,
    *,
    dossier_sortie: Path | None = None,
    referentiel: int | None = None,
    delimiteur: str = CSV_DELIMITEUR,
    encodage: str = CSV_ENCODAGE,
    progression=None,
) -> Bilan:
    """Trie le fichier en propre / erreurs / doublons. Lève `TraitementImpossible` si
    l'en-tête est faux : rien d'autre n'aurait de sens."""
    debut = time.perf_counter()
    sorties = chemins_de_sortie(source, dossier_sortie)
    sorties["propre"].parent.mkdir(parents=True, exist_ok=True)
    bilan = Bilan(
        source=source,
        sorties=sorties,
        referentiel=referentiel,
        referentiel_impose=referentiel is not None,
    )

    # PDI conservé -> rang ; au même rang, sa ligne dans le fichier et l'empreinte de ses
    # données. Les `array` coûtent 8 octets par ligne, là où des tuples en coûteraient 100.
    rang_du_pdi: dict[int, int] = {}
    lignes_conservees = array("Q")
    empreintes = array("q")

    with (
        _ouvrir(source, encodage) as entree,
        open(sorties["propre"], "w", encoding="utf-8", newline="") as f_propre,
        open(sorties["erreurs"], "w", encoding="utf-8", newline="") as f_erreurs,
        open(sorties["doublons"], "w", encoding="utf-8", newline="") as f_doublons,
    ):
        lecteur = csv.reader(entree, delimiter=delimiteur)
        options = {"delimiter": delimiteur, "lineterminator": "\n"}
        propre = csv.writer(f_propre, **options)
        erreurs = csv.writer(f_erreurs, **options)
        doublons = csv.writer(f_doublons, **options)

        entete = next(lecteur, None)
        verifier_entete(entete)
        propre.writerow(COLONNES_CSV)
        erreurs.writerow(("numero_ligne", "motif", *COLONNES_CSV))
        doublons.writerow(("numero_ligne", "ligne_conservee", "type_doublon", *COLONNES_CSV))

        for brute in lecteur:
            numero = lecteur.line_num
            if not brute or all(not champ.strip() for champ in brute):
                bilan.vides += 1
                continue
            bilan.lues += 1
            if progression and bilan.lues % PROGRESSION_TOUTES_LES == 0:
                progression(bilan.lues)

            try:
                if len(brute) != len(COLONNES_CSV):
                    raise TraitementImpossible(
                        f"Ligne {numero} : {len(brute)} colonne(s) au lieu de "
                        f"{len(COLONNES_CSV)}."
                    )
                ligne = dict(zip(COLONNES_CSV, brute))
                if bilan.referentiel is None:
                    bilan.referentiel = _referentiel_de(ligne)
                valeurs = convertir(ligne, numero, bilan.referentiel or 0)
                controler_bornes(ligne, valeurs, numero)
            except TraitementImpossible as erreur:
                motif = str(erreur)
                bilan.erreurs += 1
                bilan.categories[categorie(motif)] += 1
                if len(bilan.exemples_erreurs) < EXEMPLES_MAX:
                    bilan.exemples_erreurs.append(motif)
                erreurs.writerow((numero, motif, *brute))
                continue

            id_pdi = valeurs[0]
            empreinte = hash(valeurs)
            rang = rang_du_pdi.get(id_pdi)
            if rang is not None:
                ligne_gardee = lignes_conservees[rang]
                if empreintes[rang] == empreinte:
                    type_doublon = "IDENTIQUE"
                    bilan.doublons_identiques += 1
                else:
                    type_doublon = "CONFLIT"
                    bilan.doublons_conflits += 1
                    if len(bilan.exemples_conflits) < EXEMPLES_MAX:
                        bilan.exemples_conflits.append(
                            f"PDI {id_pdi} : ligne {numero} ecartee, ligne {ligne_gardee} "
                            "conservee"
                        )
                doublons.writerow((numero, ligne_gardee, type_doublon, *brute))
                continue

            rang_du_pdi[id_pdi] = len(lignes_conservees)
            lignes_conservees.append(numero)
            empreintes.append(empreinte)
            bilan.conservees += 1
            if valeurs[17] is None:  # date_fin_validite
                bilan.actives += 1
            propre.writerow(brute)

    bilan.duree_s = time.perf_counter() - debut
    return bilan


def _ouvrir(source: Path, encodage: str):
    if source.suffix == ".gz":
        import gzip

        return gzip.open(source, "rt", encoding=encodage, newline="")
    return open(source, encoding=encodage, newline="")


# ---------------------------------------------------------------------------
# Rapport texte
# ---------------------------------------------------------------------------


def _ascii(texte: str) -> str:
    """Sans accents : le rapport doit passer tel quel dans Teams."""
    texte = texte.replace("—", "-").replace("…", "...").replace("’", "'")
    decompose = unicodedata.normalize("NFKD", texte)
    return decompose.encode("ascii", "ignore").decode("ascii")


def _nombre(valeur: int) -> str:
    return f"{valeur:,}".replace(",", " ")


def _duree(secondes: float) -> str:
    minutes, secondes = divmod(int(secondes), 60)
    heures, minutes = divmod(minutes, 60)
    if heures:
        return f"{heures} h {minutes:02d} min {secondes:02d} s"
    if minutes:
        return f"{minutes} min {secondes:02d} s"
    return f"{secondes} s"


def rapport_texte(bilan: Bilan) -> str:
    trait = "=" * LARGEUR_RAPPORT
    sous_trait = "-" * LARGEUR_RAPPORT

    def section(titre: str) -> list[str]:
        return ["", sous_trait, titre, sous_trait, ""]

    origine = "impose (--referentiel)" if bilan.referentiel_impose else "deduit du fichier"
    lignes = [
        trait,
        "NETTOYAGE DU CSV DES CLES DE REPARTITION",
        trait,
        "",
        f"Fichier source : {bilan.source}",
        f"Date           : {datetime.now():%Y-%m-%d %H:%M}",
        f"Referentiel    : {bilan.referentiel} ({origine})",
        f"Duree          : {_duree(bilan.duree_s)}",
        *section("1. SYNTHESE"),
        f"Lignes lues (hors en-tete et lignes vides) : {_nombre(bilan.lues)}",
        f"Lignes conservees                          : {_nombre(bilan.conservees)}",
        f"  dont actives (date_fin_validite vide)    : {_nombre(bilan.actives)}",
        f"Lignes en erreur                           : {_nombre(bilan.erreurs)}",
        f"Doublons de PDI ecartes                    : {_nombre(bilan.doublons)}",
        f"  dont identiques                          : "
        f"{_nombre(bilan.doublons_identiques)}",
        f"  dont en conflit (donnees differentes)    : "
        f"{_nombre(bilan.doublons_conflits)}",
        f"Lignes vides ignorees                      : {_nombre(bilan.vides)}",
        "",
    ]
    if not bilan.erreurs and not bilan.doublons:
        lignes.append("Verdict : fichier conforme, aucune ligne ecartee.")
    else:
        lignes.append(
            f"Verdict : {_nombre(bilan.erreurs + bilan.doublons)} ligne(s) ecartee(s). "
            "Les PDI concernes n'auront pas de cle calculee si le fichier propre est "
            "charge en l'etat."
        )
    if bilan.doublons_conflits:
        lignes.append(
            "ATTENTION : des doublons portent des donnees differentes. La premiere "
            "occurrence a ete conservee, ce choix est a faire valider par le metier."
        )

    if bilan.categories:
        lignes += section("2. ERREURS PAR TYPE")
        for motif, nombre in bilan.categories.most_common():
            lignes.append(f"{_nombre(nombre):>12}  {motif}")

    if bilan.exemples_erreurs:
        lignes += section(f"3. PREMIERES ERREURS ({len(bilan.exemples_erreurs)} max)")
        lignes += [f"- {motif}" for motif in bilan.exemples_erreurs]

    if bilan.exemples_conflits:
        lignes += section(f"4. DOUBLONS EN CONFLIT ({len(bilan.exemples_conflits)} max)")
        lignes += [f"- {conflit}" for conflit in bilan.exemples_conflits]

    lignes += section("5. FICHIERS PRODUITS")
    libelles = {
        "propre": "Fichier propre (a charger)",
        "erreurs": "Lignes en erreur",
        "doublons": "Doublons ecartes",
        "rapport": "Ce rapport",
    }
    for cle, libelle in libelles.items():
        lignes += [f"{libelle} :", f"  {bilan.sorties[cle]}"]

    lignes += section("6. SUITE")
    lignes += [
        "Charger le fichier propre, depuis yb07/ :",
        "",
        f"  python -m app.main charger-cles-repartition-local {bilan.referentiel} "
        f"{bilan.sorties['propre']}",
        "",
        "Les numeros de ligne des fichiers d'erreurs et de doublons sont ceux du "
        "fichier SOURCE (en-tete = ligne 1).",
    ]

    rendu: list[str] = []
    for ligne in lignes:
        ligne = _ascii(ligne)
        # Les lignes indentées sont des chemins ou une commande : coupées, elles ne se
        # copieraient plus. Elles seules peuvent dépasser la largeur.
        if len(ligne) <= LARGEUR_RAPPORT or ligne.startswith("  "):
            rendu.append(ligne)
            continue
        retrait = " " * (len(ligne) - len(ligne.lstrip()) + 2)
        rendu += textwrap.wrap(
            ligne, width=LARGEUR_RAPPORT, subsequent_indent=retrait, break_on_hyphens=False
        )
    return "\n".join(rendu) + "\n"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.nettoyer_csv_cles",
        description="Nettoie un CSV de clés de répartition : doublons retirés, erreurs "
        "extraites dans des CSV, rapport texte.",
    )
    parser.add_argument("fichier", type=Path, help="CSV source (ou .csv.gz).")
    parser.add_argument(
        "--referentiel",
        type=int,
        default=None,
        help="Référentiel attendu. Par défaut, celui de la première ligne.",
    )
    parser.add_argument(
        "--sortie",
        type=Path,
        default=None,
        help="Dossier des fichiers produits. Par défaut, celui du fichier source.",
    )
    parser.add_argument(
        "--delimiteur", default=CSV_DELIMITEUR, help="Défaut : CSV_DELIMITEUR (';')."
    )
    parser.add_argument(
        "--encodage", default=CSV_ENCODAGE, help="Défaut : CSV_ENCODAGE (utf-8-sig)."
    )
    args = parser.parse_args(argv)

    if not args.fichier.is_file():
        print(f"Fichier introuvable : {args.fichier}", file=sys.stderr)
        return 1

    def progression(lues: int) -> None:
        print(f"... {_nombre(lues)} lignes lues", file=sys.stderr, flush=True)

    try:
        bilan = nettoyer(
            args.fichier,
            dossier_sortie=args.sortie,
            referentiel=args.referentiel,
            delimiteur=args.delimiteur,
            encodage=args.encodage,
            progression=progression,
        )
    except TraitementImpossible as erreur:
        print(f"Nettoyage impossible : {erreur}", file=sys.stderr)
        return 1

    texte = rapport_texte(bilan)
    bilan.sorties["rapport"].write_text(texte, encoding="utf-8")
    print(texte)
    return 0


if __name__ == "__main__":
    sys.exit(main())
