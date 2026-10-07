"""Rapport d'exécution commun aux commandes : contrôles `[OK]` / `[KO]`, verdict, motifs.

Rendu en texte pour l'exploitant ou en JSON (`--json`) pour un ordonnanceur.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

LARGEUR_BANDEAU = 50

# Verdicts lus par l'exploitation : ne pas les reformuler.
SUCCES = "SUCCES"
ECHEC = "ECHEC"


@dataclass(frozen=True)
class Controle:
    """Un point de contrôle : `libelle` affiché si OK, `motif` à la place en cas d'échec."""

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
    id_traitement: int
    libelle_identifiant: str = "Traitement"
    """Intitulé de l'identifiant dans le bandeau, ex. « Référentiel »."""
    controles: list[Controle] = field(default_factory=list)
    statut: str = ""
    erreur: str | None = None
    etats: dict[str, Any] = field(default_factory=dict)
    """Indicateurs affichés en fin de rapport, ex. `LIGNES_CHARGEES = 22395341`."""
    avertissements: list[str] = field(default_factory=list)
    """Anomalies tolérées, ex. les lignes écartées par `--skip-errors`. Affichées, mais sans
    effet sur le verdict : c'est ce qui les distingue d'un contrôle `[KO]`."""

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def ok(self, libelle: str) -> Controle:
        """Ajoute un contrôle réussi (ou une étape franchie) et le retourne."""
        controle = Controle(libelle=libelle, ok=True)
        self.controles.append(controle)
        return controle

    def ko(self, motif: str, libelle: str | None = None) -> Controle:
        """Ajoute un contrôle en échec ; `motif` est le message bloquant."""
        controle = Controle(libelle=libelle or motif, ok=False, motif=motif)
        self.controles.append(controle)
        return controle

    def ajouter(self, ok: bool, libelle: str, motif: str) -> Controle:
        """Ajoute un contrôle réussi ou en échec selon `ok`."""
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
        lignes = [
            bandeau,
            self.titre,
            f"{self.libelle_identifiant} : {self.id_traitement}",
            bandeau,
            "",
        ]
        lignes += [c.ligne() for c in self.controles]

        if self.erreur:
            lignes += ["", "[ERREUR]", self.erreur]

        if self.etats:
            lignes.append("")
            lignes += [f"{cle} = {valeur}" for cle, valeur in self.etats.items()]

        if self.avertissements:
            lignes += ["", "Avertissements :", ""]
            lignes += [f"  - {avertissement}" for avertissement in self.avertissements]

        lignes += ["", f"RESULTAT : {self.statut}"]

        if self.motifs:
            lignes += ["", "Motifs :", ""]
            lignes += [f"  - {motif}" for motif in self.motifs]

        return "\n".join(lignes)

    def to_dict(self) -> dict[str, Any]:
        return {
            "titre": self.titre,
            "id_traitement": self.id_traitement,
            "statut": self.statut,
            "reussi": self.reussi,
            "controles": [
                {"libelle": c.libelle, "ok": c.ok, "motif": c.motif} for c in self.controles
            ],
            "etats": self.etats,
            "avertissements": self.avertissements,
            "erreur": self.erreur,
            "motifs": self.motifs,
        }
