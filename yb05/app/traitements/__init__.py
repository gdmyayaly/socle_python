"""Traitements métier du batch YB05 — chaîne de calcul des trafics d'un scénario.

    ELIGIBILITE            DSR-701   contrôle, sans aucune écriture
    CALCUL_TRAFIC_PDI      DSR-702   TMH × coefficient × clé, par PDI (et DSR-700)
    CALCUL_TRAFIC_AGREBAL  DSR-703   agrégation des trafics PDI par Agrébal

Chaque fonction retourne un `Rapport` — elle ne lève pas et n'écrit pas sur la sortie standard :
c'est la CLI (`app/main.py`) qui choisit de l'afficher en texte ou en JSON, et qui en déduit le
code de retour du processus.
"""

from app.traitements.eligibilite import controle_eligibilite
from app.traitements.erreurs import TraitementImpossible
from app.traitements.rapport import Controle, Rapport
from app.traitements.trafic_agrebal import calcul_trafic_agrebal
from app.traitements.trafic_pdi import calcul_trafic_pdi

__all__ = [
    "Controle",
    "Rapport",
    "TraitementImpossible",
    "calcul_trafic_agrebal",
    "calcul_trafic_pdi",
    "controle_eligibilite",
]
