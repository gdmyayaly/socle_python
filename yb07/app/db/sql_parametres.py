"""Paramétrage d'un script SQL par ses variables de session, sans le réécrire sur disque.

Les scripts de `db/` se règlent par un bloc `SET @ma_variable := …` en tête de fichier — c'est
la convention posée par `db/README.md`. Un appelant Python qui veut jouer le même script sur un
autre référentiel ou un autre site ne peut ni éditer le fichier, ni poser la variable dans un
appel séparé : `execute_sql_script` ouvre une connexion dédiée par appel, et les variables de
session meurent avec elle.

D'où ce module : il rend un **texte** de script dont le bloc de paramètres a été remplacé, que
l'appelant passe ensuite à `Database.execute_sql_script`. Un seul appel, une seule connexion,
les variables tiennent d'une instruction à l'autre.

    texte = injecter_parametres(script, {"id_referentiel": 1, "co_regate": "123456"})
    await db_write.execute_sql_script(texte, label="db/DSR-698_version_cle.sql")

Ne sont retirées que les affectations dont le nom est explicitement demandé. `SET SESSION
sql_mode = …` n'est jamais touché — c'est lui qui porte le durcissement (échec sur division par
zéro, mode strict) de plusieurs scripts —, pas plus que les variables de travail internes
(`@sql` de la migration, `@deja` des scripts rejouables).

Module pur : aucune I/O, aucune connexion, aucune dépendance vers `mysql.py`. Même parti pris
que `sql_script.py`, dont il est le voisin.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from typing import Any, Mapping

import sqlparse

from app.db.sql_script import split_sql_script

# Une affectation de variable utilisateur, en tête d'instruction. Le `@` est ce qui distingue
# `SET @id_referentiel := 1` de `SET SESSION sql_mode = …` et de `SET FOREIGN_KEY_CHECKS = 0` :
# sans lui, on neutraliserait des réglages de session dont dépend le comportement du script.
_AFFECTATION = re.compile(r"^SET\s+@([A-Za-z0-9_$]+)\s*:?=", re.IGNORECASE)

# Interdits dans une chaîne littérale : ils ne peuvent venir que d'une valeur mal construite.
_CARACTERES_INTERDITS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

# Un script à délimiteur personnalisé ne se rejoint pas par `;`. Aucun script de `db/` n'en
# porte ; le garde-fou coûte une ligne et évite d'en corrompre un silencieusement.
_CONTIENT_DELIMITER = re.compile(r"^\s*DELIMITER\s+\S+\s*$", re.IGNORECASE | re.MULTILINE)


class ParametreInconnu(ValueError):
    """Un paramètre demandé ne correspond à aucune variable du script."""


def litteral_sql(valeur: Any) -> str:
    """Rend une valeur Python sous forme de littéral SQL.

    Refuse ce qu'elle ne sait pas rendre plutôt que de retomber sur `str(valeur)` : un objet
    inattendu doit casser le test, pas produire du SQL syntaxiquement valide et sémantiquement
    faux.
    """
    if valeur is None:
        # Sans guillemets, délibérément : `@co_regate` vaudrait sinon la chaîne 'NULL' et
        # filtrerait sur un site qui n'existe pas, sans la moindre erreur.
        return "NULL"

    # Avant `int` : `isinstance(True, int)` est vrai, et `SET @actif := True` n'a pas de sens.
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
    """Littéral chaîne, échappé pour MySQL.

    L'ordre des deux remplacements compte : doubler les apostrophes d'abord laisserait les
    antislashs introduits par le second remplacement transformer `''` en `'\\''`.
    """
    if _CARACTERES_INTERDITS.search(valeur):
        raise ValueError("caractère de contrôle interdit dans une valeur SQL")
    echappee = valeur.replace("\\", "\\\\").replace("'", "''")
    return f"'{echappee}'"


def nom_variable(instruction: str) -> str | None:
    """Nom de la variable affectée par cette instruction, ou `None`.

    Le retrait des commentaires n'est pas cosmétique : `sqlparse.split` rattache la bannière
    `-- ------` qui *précède* une instruction à cette instruction. Sans lui, le `^SET` ne
    matcherait jamais sur un script commenté — c'est-à-dire sur aucun des scripts de `db/`.
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

    `exiger_presence` (défaut) lève `ParametreInconnu` si une clé ne correspond à aucune
    affectation du script. C'est ce qui attrape la faute de frappe : `@co_regates` au lieu de
    `@co_regate` produirait sinon un script parfaitement valide, et un résultat faux.
    """
    conservees, trouves = _decomposer(script, tuple(parametres))

    if exiger_presence:
        manquants = sorted(set(parametres) - trouves)
        if manquants:
            raise ParametreInconnu(
                "paramètre(s) absent(s) du script : "
                + ", ".join(f"@{nom}" for nom in manquants)
            )

    # L'ordre du mapping est conservé (dict ordonné depuis Python 3.7) : le script produit est
    # déterministe, donc comparable dans un test.
    prefixe = [f"SET @{nom} := {litteral_sql(valeur)}" for nom, valeur in parametres.items()]

    # `split_sql_script` a retiré les délimiteurs, il faut les remettre.
    return ";\n".join(prefixe + list(conservees)) + ";\n"


@lru_cache(maxsize=16)
def _decomposer(script: str, noms: tuple[str, ...]) -> tuple[tuple[str, ...], frozenset[str]]:
    """Instructions à conserver, et noms de paramètres effectivement trouvés.

    Mémoïsé, et c'est le seul endroit qui le justifie : l'étape « versions » rejoue le même
    script une fois par site — plusieurs milliers de fois sur un référentiel réel — avec des
    valeurs différentes mais un texte et des noms identiques. Sans cache, chaque site paierait
    un découpage complet plus un `sqlparse.format` par instruction, pour un résultat invariant.

    La fonction est pure : même script et mêmes noms donnent toujours les mêmes instructions.
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
