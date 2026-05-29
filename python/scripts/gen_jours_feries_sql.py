"""Génère le script SQL de seed des jours fériés nationaux français.

Source de vérité : app.services.jours_service.compute_french_holidays (Computus).
Sortie : db_migrations/004_seed_trppu_jours_feries.sql

Usage :
    python scripts/gen_jours_feries_sql.py [annee_debut] [annee_fin]
    (par défaut 2020 -> 2035)
"""

from __future__ import annotations

import sys
from pathlib import Path

# Permet l'import de `app.*` quand le script est lancé depuis la racine du repo.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.services.jours_service import compute_french_holidays  # noqa: E402

OUTPUT = ROOT / "db_migrations" / "004_seed_trppu_jours_feries.sql"


def _escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "''")


def main(annee_debut: int = 2020, annee_fin: int = 2035) -> None:
    rows: list[tuple[str, str]] = []
    for year in range(annee_debut, annee_fin + 1):
        for dt, libelle in sorted(compute_french_holidays(year).items()):
            rows.append((dt.isoformat(), libelle))

    values = ",\n".join(
        f"  ('{dt}', '{_escape(lib)}', 1)" for dt, lib in rows
    )
    sql = (
        "-- DSR — Socle (NEW-3) : seed des jours fériés nationaux français.\n"
        "-- GÉNÉRÉ par scripts/gen_jours_feries_sql.py — ne pas éditer à la main.\n"
        f"-- Années : {annee_debut} -> {annee_fin} ({len(rows)} lignes)\n"
        "-- Pré-requis : 003_create_trppu_jours_feries.sql\n\n"
        "USE `trppu`;\n\n"
        "INSERT INTO `trppu_jours_feries` (`dt`, `libelle`, `est_national`) VALUES\n"
        f"{values}\n"
        "ON DUPLICATE KEY UPDATE `libelle` = VALUES(`libelle`), "
        "`est_national` = VALUES(`est_national`);\n"
    )
    OUTPUT.write_text(sql, encoding="utf-8")
    print(f"=> {len(rows)} jours fériés écrits dans {OUTPUT}")


if __name__ == "__main__":
    a = int(sys.argv[1]) if len(sys.argv) > 1 else 2020
    b = int(sys.argv[2]) if len(sys.argv) > 2 else 2035
    main(a, b)
