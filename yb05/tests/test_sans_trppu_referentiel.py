"""`trppu_referentiel` n'est plus utilisée : le référentiel est lu dans `trppu_version_cle`.

DSR-701 règle 10, DSR-702 étape 3. `db/database.sql` (reflet du schéma) est exclu.
"""

import re
from pathlib import Path

import pytest

RACINE = Path(__file__).resolve().parent.parent
FICHIERS = sorted(
    [*RACINE.joinpath("app").rglob("*.py"), *RACINE.joinpath("db").glob("*.sql")]
)

# Un usage SQL réel ; un commentaire qui cite la table reste permis.
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
