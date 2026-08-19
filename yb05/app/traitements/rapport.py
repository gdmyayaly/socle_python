"""Rapport d'exécution d'un traitement : contrôles `[OK]` / `[KO]`, verdict, motifs.

Les trois tickets (DSR-701, DSR-702, DSR-703) décrivent la même sortie console : un bandeau,
une ligne par contrôle préfixée `[OK]` ou `[KO]`, un `RESULTAT :` final et, en cas de refus, la
liste des motifs bloquants. Ce module porte ce format une seule fois, pour que les trois
commandes se ressemblent — et pour que le format soit testable sans base.

Le même objet sait se rendre en JSON (option `--json` de la CLI) : le texte est destiné à
l'exploitant, le JSON à un ordonnanceur ou à une supervision.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

LARGEUR_BANDEAU = 50

# Verdicts, repris mot pour mot des tickets — ils sont lus par l'exploitation.
ELIGIBLE = "ELIGIBLE AU CALCUL COMPLET DES TRAFICS (PDI + Agrébals)"
NON_ELIGIBLE = "NON ELIGIBLE"
SUCCES = "SUCCES"
ECHEC = "ECHEC"


@dataclass(frozen=True)
class Controle:
    """Un point de contrôle, ou une information de déroulement.

    `libelle` est ce qui s'affiche quand tout va bien (« Scénario figé »), `motif` ce qui
    s'affiche à la place en cas d'échec (« Le scénario n'est pas figé ») — c'est la convention
    des tickets, qui n'affichent jamais les deux.
    """

    libelle: str
    ok: bool = True
    motif: str | None = None

    def ligne(self) -> str:
        if self.ok:
            return f"[OK] {self.libelle}"
        return f"[KO] {self.motif or self.libelle}"


@dataclass
class Rapport:
    """Résultat complet d'un traitement, rendu en texte ou en JSON."""

    titre: str
    id_scenario: int
    controles: list[Controle] = field(default_factory=list)
    statut: str = ""
    erreur: str | None = None
    etats: dict[str, Any] = field(default_factory=dict)
    """Indicateurs affichés en fin de rapport, ex. `TRAFIC_PDI_CALCULE = 1`."""

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def ok(self, libelle: str) -> Controle:
        """Ajoute un contrôle réussi (ou une étape franchie) et le retourne."""
        controle = Controle(libelle=libelle, ok=True)
        self.controles.append(controle)
        return controle

    def ko(self, motif: str, libelle: str | None = None) -> Controle:
        """Ajoute un contrôle en échec ; `motif` est le message bloquant du ticket."""
        controle = Controle(libelle=libelle or motif, ok=False, motif=motif)
        self.controles.append(controle)
        return controle

    def ajouter(self, ok: bool, libelle: str, motif: str) -> Controle:
        """Ajoute un contrôle dont l'issue est calculée — évite un `if` chez l'appelant."""
        return self.ok(libelle) if ok else self.ko(motif, libelle=libelle)

    # ------------------------------------------------------------------
    # Lecture
    # ------------------------------------------------------------------

    @property
    def motifs(self) -> list[str]:
        return [c.motif or c.libelle for c in self.controles if not c.ok]

    @property
    def reussi(self) -> bool:
        """Vrai si aucun contrôle n'a échoué et qu'aucune erreur n'a été rencontrée."""
        return not self.motifs and self.erreur is None

    # ------------------------------------------------------------------
    # Rendu
    # ------------------------------------------------------------------

    def texte(self) -> str:
        bandeau = "-" * LARGEUR_BANDEAU
        lignes = [bandeau, self.titre, f"Scénario : {self.id_scenario}", bandeau, ""]
        lignes += [c.ligne() for c in self.controles]

        if self.erreur:
            lignes += ["", "[ERREUR]", self.erreur]

        if self.etats:
            lignes.append("")
            lignes += [f"{cle} = {valeur}" for cle, valeur in self.etats.items()]

        lignes += ["", f"RESULTAT : {self.statut}"]

        if self.motifs:
            lignes += ["", "Motifs :", ""]
            lignes += [f"  - {motif}" for motif in self.motifs]

        return "\n".join(lignes)

    def to_dict(self) -> dict[str, Any]:
        return {
            "titre": self.titre,
            "id_scenario": self.id_scenario,
            "statut": self.statut,
            "reussi": self.reussi,
            "controles": [
                {"libelle": c.libelle, "ok": c.ok, "motif": c.motif} for c in self.controles
            ],
            "etats": self.etats,
            "erreur": self.erreur,
            "motifs": self.motifs,
        }


__all__ = [
    "ECHEC",
    "ELIGIBLE",
    "NON_ELIGIBLE",
    "SUCCES",
    "Controle",
    "Rapport",
]
