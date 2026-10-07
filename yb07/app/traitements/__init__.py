"""Traitements métier : chargement des clés (S3 ou local) et initialisation DSR-696→699.

Chaque traitement rend un `Rapport` sans lever ni écrire sur la sortie : la CLI l'affiche.
"""

from app.erreurs import TraitementImpossible
from app.traitements.cles_repartition import charger_cles_repartition
from app.traitements.initialisation import ETAPES, initialiser_cles_repartition
from app.traitements.rapport import Controle, Rapport

__all__ = [
    "ETAPES",
    "Controle",
    "Rapport",
    "TraitementImpossible",
    "charger_cles_repartition",
    "initialiser_cles_repartition",
]
