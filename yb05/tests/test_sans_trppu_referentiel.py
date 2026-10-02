"""`trppu_referentiel` n'est plus utilisée par YB05.

Aucun ticket ne l'alimente, et le rédacteur des tickets a retenu (02/10/2026) de lire le
référentiel dans `trppu_version_cle` — variante prévue par DSR-701 règle 10 et DSR-702
étape 3. Ce test empêche qu'une lecture ou une écriture de la table revienne par mégarde.

`db/database.sql` est exclu : c'est le reflet du schéma, pas un usage.
"""

import re
from pathlib import Path

import pytest

RACINE = Path(__file__).resolve().parent.parent
FICHIERS = sorted(
    [*RACINE.joinpath("app").rglob("*.py"), *RACINE.joinpath("db").glob("*.sql")]
)

# Un usage SQL réel ; les commentaires qui expliquent pourquoi la table n'est plus lue restent
# permis.
USAGE_SQL = re.compile(r"\b(FROM|JOIN|INTO|UPDATE|TABLE)\s+`?trppu_referentiel\b", re.I)


@pytest.mark.parametrize(
    "fichier",
    [f for f in FICHIERS if f.name != "database.sql"],
    ids=lambda f: f.relative_to(RACINE).as_posix(),
)
def test_aucune_reference_a_trppu_referentiel(fichier):
    usages = USAGE_SQL.findall(fichier.read_text(encoding="utf-8"))
    assert not usages, f"trppu_referentiel encore utilisée : {usages}"


def test_le_motif_reconnait_un_usage():
    assert USAGE_SQL.search("SELECT id_referentiel\n  FROM trppu_referentiel WHERE x")
    assert USAGE_SQL.search("INSERT INTO `trppu_referentiel` (co_regate)")
    assert USAGE_SQL.search("DELETE FROM trppu_referentiel WHERE co_regate IN (%s)")
    assert not USAGE_SQL.search("-- `trppu_referentiel` n'est pas interrogée")


def test_le_test_couvre_bien_le_code():
    """Garde-fou du garde-fou : un chemin faux rendrait le test vide, donc toujours vert."""
    noms = {f.name for f in FICHIERS}
    assert {"eligibilite.py", "trafic_pdi.py", "generateur.py", "DSR-698_version_cle.sql"} <= noms
