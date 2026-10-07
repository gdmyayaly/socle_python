"""Injection des paramètres `SET @… :=` d'un script de `db/` (module pur, sans I/O).

Les variables de session meurent avec la connexion dédiée de `execute_sql_script` : on
remplace donc le bloc de paramètres dans le texte. Seules les variables demandées sont
retirées ; `SET SESSION sql_mode` et les variables internes (`@sql`, `@deja`) restent.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from typing import Any, Mapping

import sqlparse

from app.db.sql_script import split_sql_script

# Affectation de variable utilisateur : le `@` exclut les réglages de session (sql_mode…).
_AFFECTATION = re.compile(r"^SET\s+@([A-Za-z0-9_$]+)\s*:?=", re.IGNORECASE)

# Caractères de contrôle interdits dans une chaîne littérale.
_CARACTERES_INTERDITS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

# Un script à délimiteur personnalisé ne se rejoint pas par `;` : refusé.
_CONTIENT_DELIMITER = re.compile(r"^\s*DELIMITER\s+\S+\s*$", re.IGNORECASE | re.MULTILINE)


class ParametreInconnu(ValueError):
    """Un paramètre demandé ne correspond à aucune variable du script."""


def litteral_sql(valeur: Any) -> str:
    """Rend une valeur Python en littéral SQL ; lève `TypeError` pour un type inconnu."""
    if valeur is None:
        # Sans guillemets : la chaîne 'NULL' filtrerait silencieusement sur un site inexistant.
        return "NULL"

    # Avant `int` : `isinstance(True, int)` est vrai.
    if isinstance(valeur, bool):
        return "1" if valeur else "0"

    if isinstance(valeur, (int, Decimal)):
        return str(valeur)

    if isinstance(valeur, float):
        if valeur != valeur or valeur in (float("inf"), float("-inf")):
            raise TypeError(f"valeur flottante non représentable en SQL : {valeur!r}")
        return repr(valeur)

    if isinstance(valeur, datetime):
        return _chaine(valeur.strftime("%Y-%m-%d %H:%M:%S"))

    if isinstance(valeur, date):
        return _chaine(valeur.strftime("%Y-%m-%d"))

    if isinstance(valeur, str):
        return _chaine(valeur)

    raise TypeError(
        f"type non convertible en littéral SQL : {type(valeur).__name__} ({valeur!r})"
    )


def _chaine(valeur: str) -> str:
    """Littéral chaîne échappé pour MySQL (antislashs d'abord, puis apostrophes)."""
    if _CARACTERES_INTERDITS.search(valeur):
        raise ValueError("caractère de contrôle interdit dans une valeur SQL")
    echappee = valeur.replace("\\", "\\\\").replace("'", "''")
    return f"'{echappee}'"


def nom_variable(instruction: str) -> str | None:
    """Nom de la variable affectée par cette instruction, ou `None`.

    Commentaires retirés : `sqlparse.split` rattache la bannière précédente à l'instruction.
    """
    nu = sqlparse.format(instruction, strip_comments=True).strip()
    trouve = _AFFECTATION.match(nu)
    return trouve.group(1) if trouve else None


def injecter_parametres(
    script: str,
    parametres: Mapping[str, Any],
    *,
    exiger_presence: bool = True,
) -> str:
    """Remplace le bloc de paramètres du script par les valeurs demandées.

    `exiger_presence` lève `ParametreInconnu` pour une clé absente du script (faute de frappe).
    """
    instructions = instructions_parametrees(
        script, parametres, exiger_presence=exiger_presence
    )
    # `split_sql_script` a retiré les délimiteurs, il faut les remettre.
    return ";\n".join(instructions) + ";\n"


def instructions_parametrees(
    script: str,
    parametres: Mapping[str, Any],
    *,
    exiger_presence: bool = True,
) -> list[str]:
    """Comme `injecter_parametres`, mais rend les instructions déjà découpées
    (pour `Database.execute_sql_units`, sans redécoupage par site)."""
    conservees, trouves = _decomposer(script, tuple(parametres))

    if exiger_presence:
        manquants = sorted(set(parametres) - trouves)
        if manquants:
            raise ParametreInconnu(
                "paramètre(s) absent(s) du script : "
                + ", ".join(f"@{nom}" for nom in manquants)
            )

    # Ordre du mapping conservé : script déterministe.
    prefixe = [f"SET @{nom} := {litteral_sql(valeur)}" for nom, valeur in parametres.items()]
    return prefixe + list(conservees)


@lru_cache(maxsize=16)
def _decomposer(script: str, noms: tuple[str, ...]) -> tuple[tuple[str, ...], frozenset[str]]:
    """Instructions à conserver et noms de paramètres trouvés.

    Mémoïsé : l'étape « versions » rejoue le même script pour des milliers de sites.
    """
    if _CONTIENT_DELIMITER.search(script):
        raise ValueError(
            "script portant une directive DELIMITER : le rejoindre par ';' le corromprait"
        )

    demandes = set(noms)
    trouves: set[str] = set()
    conservees: list[str] = []

    for instruction in split_sql_script(script):
        nom = nom_variable(instruction)
        if nom is not None and nom in demandes:
            trouves.add(nom)
            continue
        conservees.append(instruction)

    return tuple(conservees), frozenset(trouves)


__all__ = [
    "ParametreInconnu",
    "injecter_parametres",
    "litteral_sql",
    "nom_variable",
]
