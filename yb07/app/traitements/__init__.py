"""Traitements métier du module.

| Traitement | Commande | Description |
| --- | --- | --- |
| `charger_cles_repartition` | `charger-cles-repartition` | Charge `trppu_cles_repartition` depuis un CSV déposé sur S3 |
| `initialiser_cles_repartition` | `init` | Enchaîne la chaîne DSR-696→699 : chargement, migration, agrégats, versions, clés |

Chaque traitement retourne un `Rapport`. **Aucun ne lève ni n'écrit sur la sortie
standard** : c'est la CLI (`app/main.py`) qui choisit de l'afficher en texte ou en JSON, et
qui en déduit le code de retour du processus.
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
