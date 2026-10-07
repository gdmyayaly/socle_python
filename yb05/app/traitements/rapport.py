"""Rapport d'exécution (format console commun à DSR-701/702/703) : contrôles `[OK]`/`[KO]`,
verdict, motifs ; rendu texte ou JSON (`--json`).
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
    """Point de contrôle : `libelle` affiché si OK, `motif` à la place en cas d'échec."""

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


@dataclass(frozen=True)
class ResultatScenario:
    """Issue du traitement d'un scénario par le mode ALL."""

    id_scenario: int
    statut: str
    """`SUCCES`, `ECHEC` ou `NON_ELIGIBLE`."""
    motif: str | None = None

    @property
    def reussi(self) -> bool:
        return self.statut == SUCCES


@dataclass
class Bilan:
    """Résultat d'une exécution du mode ALL (DSR-704), rendu texte ou JSON."""

    nb_workers: int
    scenarios_trouves: list[int] = field(default_factory=list)
    resultats: list[ResultatScenario] = field(default_factory=list)
    scenarios_a_moitie_calcules: list[int] = field(default_factory=list)
    """Trafics PDI calculés mais pas les Agrébal : hors critères du mode ALL, donc jamais repris
    automatiquement. Listés pour que l'exploitant les voie au lieu de les découvrir plus tard."""
    duree_s: float = 0.0
    erreur: str | None = None
    """Erreur système ayant empêché le batch de fonctionner (base injoignable…)."""

    def ajouter(self, id_scenario: int, statut: str, motif: str | None = None) -> None:
        self.resultats.append(ResultatScenario(id_scenario, statut, motif))

    # ------------------------------------------------------------------
    # Compteurs
    # ------------------------------------------------------------------

    @property
    def succes(self) -> list[ResultatScenario]:
        return [r for r in self.resultats if r.statut == SUCCES]

    @property
    def echecs(self) -> list[ResultatScenario]:
        return [r for r in self.resultats if r.statut == ECHEC]

    @property
    def non_eligibles(self) -> list[ResultatScenario]:
        return [r for r in self.resultats if r.statut == NON_ELIGIBLE]

    @property
    def reussi(self) -> bool:
        """Faux dès un échec ; les non éligibles ne sont pas des échecs."""
        return not self.echecs and self.erreur is None

    @property
    def duree_moyenne_s(self) -> float:
        traites = len(self.resultats)
        return self.duree_s / traites if traites else 0.0

    # ------------------------------------------------------------------
    # Rendu
    # ------------------------------------------------------------------

    def texte(self) -> str:
        bandeau = "-" * LARGEUR_BANDEAU
        lignes = [bandeau, "YB05 - Mode ALL", bandeau, ""]
        lignes.append(f"NB_WORKER = {self.nb_workers}")
        lignes.append("")
        lignes.append(f"{len(self.scenarios_trouves)} scénario(s) détecté(s)")
        lignes.append("")

        if self.erreur:
            lignes += ["[ERREUR]", self.erreur, ""]

        for resultat in self.resultats:
            # Non éligible (`--`) distinct d'un échec (`KO`).
            if resultat.reussi:
                marque = "OK"
            elif resultat.statut == NON_ELIGIBLE:
                marque = "--"
            else:
                marque = "KO"
            lignes.append(f"[{marque}] {resultat.id_scenario}")
            if resultat.motif:
                lignes.append(f"     {resultat.motif}")

        if self.scenarios_a_moitie_calcules:
            lignes += ["", "À REPRENDRE À LA MAIN"]
            for id_scenario in self.scenarios_a_moitie_calcules:
                lignes.append(
                    f"  - {id_scenario} : trafics PDI calculés, Agrébal non calculés — "
                    f"jouer calcul-trafic-agrebal {id_scenario}"
                )

        lignes += ["", bandeau, "BILAN", bandeau, ""]
        lignes.append(f"NB_WORKER            : {self.nb_workers}")
        lignes.append(f"Scénarios trouvés    : {len(self.scenarios_trouves)}")
        lignes.append(f"Scénarios éligibles  : {len(self.resultats) - len(self.non_eligibles)}")
        lignes.append(f"Succès               : {len(self.succes)}")
        lignes.append(f"Échecs               : {len(self.echecs)}")
        lignes.append(f"Non éligibles        : {len(self.non_eligibles)}")
        lignes.append(f"Durée totale         : {duree_hms(self.duree_s)}")
        lignes.append(f"Durée moyenne        : {duree_hms(self.duree_moyenne_s)}")

        lignes += ["", f"RESULTAT : {SUCCES if self.reussi else ECHEC}"]
        return "\n".join(lignes)

    def to_dict(self) -> dict[str, Any]:
        return {
            "nb_workers": self.nb_workers,
            "scenarios_trouves": self.scenarios_trouves,
            "scenarios_a_moitie_calcules": self.scenarios_a_moitie_calcules,
            "resultats": [
                {"id_scenario": r.id_scenario, "statut": r.statut, "motif": r.motif}
                for r in self.resultats
            ],
            "compteurs": {
                "trouves": len(self.scenarios_trouves),
                "eligibles": len(self.resultats) - len(self.non_eligibles),
                "succes": len(self.succes),
                "echecs": len(self.echecs),
                "non_eligibles": len(self.non_eligibles),
            },
            "duree_totale": duree_hms(self.duree_s),
            "duree_moyenne": duree_hms(self.duree_moyenne_s),
            "erreur": self.erreur,
            "reussi": self.reussi,
        }


def duree_hms(secondes: float) -> str:
    """Durée en `HH:MM:SS`, format du bilan attendu par le ticket."""
    total = int(round(secondes))
    return f"{total // 3600:02d}:{(total % 3600) // 60:02d}:{total % 60:02d}"


__all__ = [
    "ECHEC",
    "ELIGIBLE",
    "NON_ELIGIBLE",
    "SUCCES",
    "Bilan",
    "Controle",
    "Rapport",
    "ResultatScenario",
    "duree_hms",
]
