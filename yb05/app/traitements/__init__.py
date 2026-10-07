"""Chaîne de calcul des trafics : éligibilité (DSR-701), PDI (DSR-702), Agrébal (DSR-703),
ALL (DSR-704). Chaque traitement retourne un `Rapport` (`Bilan` pour ALL) ; la CLI l'affiche."""

from app.traitements.eligibilite import controle_eligibilite
from app.traitements.erreurs import TraitementImpossible
from app.traitements.orchestrateur import executer_tout
from app.traitements.rapport import Bilan, Controle, Rapport
from app.traitements.trafic_agrebal import calcul_trafic_agrebal
from app.traitements.trafic_pdi import calcul_trafic_pdi

__all__ = [
    "Bilan",
    "Controle",
    "Rapport",
    "TraitementImpossible",
    "calcul_trafic_agrebal",
    "calcul_trafic_pdi",
    "controle_eligibilite",
    "executer_tout",
]
