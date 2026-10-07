"""Découpage de scripts SQL et modèles de résultat ; module pur (sans I/O), l'exécution
est dans ``Database.execute_sql_*``."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import sqlparse

DEFAULT_DELIMITER = ";"
PREVIEW_MAX_LENGTH = 120

# Mots-clés provoquant un COMMIT implicite MySQL (non annulables par ROLLBACK).
DDL_KEYWORDS = frozenset({
    "CREATE", "DROP", "ALTER", "TRUNCATE", "RENAME",
    "GRANT", "REVOKE", "FLUSH", "LOCK", "UNLOCK",
    "ANALYZE", "OPTIMIZE", "REPAIR", "INSTALL", "UNINSTALL",
})

# DELIMITER est une directive du client `mysql`, seule sur sa ligne.
_DELIMITER_LINE_RE = re.compile(r"^\s*DELIMITER\s+(\S+)\s*$", re.IGNORECASE)


@dataclass
class StatementResult:
    """Résultat d'exécution d'une instruction SQL du script."""

    source: str
    """Chemin du fichier d'origine, ou libellé du script brut."""

    index: int
    """Index 1-based de l'instruction **dans son fichier**."""

    preview: str
    """Aperçu tronqué, commentaires retirés (jamais le SQL complet)."""

    is_ddl: bool
    """True si l'instruction provoque un COMMIT implicite (non annulable)."""

    rowcount: int = -1
    """Nombre de lignes affectées, ou -1 si non applicable (DDL, dry_run, échec)."""

    duration_ms: float = 0.0
    error: str | None = None
    skipped: bool = False
    """True en dry_run, ou si l'instruction n'a jamais été tentée."""


@dataclass
class ScriptResult:
    """Résultat global de l'exécution d'un ou plusieurs scripts SQL."""

    sources: list[str]
    statements: list[StatementResult] = field(default_factory=list)
    duration_ms: float = 0.0
    transactional: bool = True
    dry_run: bool = False
    committed: bool = False
    """False en cas de rollback, de dry_run, ou en mode non transactionnel."""

    @property
    def total_count(self) -> int:
        return len(self.statements)

    @property
    def executed_count(self) -> int:
        return sum(1 for s in self.statements if not s.skipped and s.error is None)

    @property
    def errors(self) -> list[StatementResult]:
        return [s for s in self.statements if s.error is not None]

    @property
    def error_count(self) -> int:
        return len(self.errors)

    @property
    def ok(self) -> bool:
        return not self.errors

    @property
    def ddl_count(self) -> int:
        return sum(1 for s in self.statements if s.is_ddl)


class SqlScriptError(RuntimeError):
    """Échec de l'exécution d'un script SQL, avec sa localisation précise."""

    def __init__(
        self,
        message: str,
        *,
        source: str,
        index: int,
        statement: str,
        original: BaseException,
        result: "ScriptResult | None" = None,
    ):
        super().__init__(message)
        self.source = source
        """Fichier (ou libellé) fautif."""
        self.index = index
        """Index 1-based de l'instruction fautive dans ce fichier."""
        self.statement = statement
        """SQL complet de l'instruction fautive (absent des logs)."""
        self.original = original
        """Exception d'origine remontée par aiomysql/pymysql."""
        self.result = result
        """État PARTIEL du ScriptResult au moment de l'échec."""


def split_sql_script(script: str) -> list[str]:
    """Découpe un script en instructions (``sqlparse`` + directive ``DELIMITER``).

    Limite : avec un délimiteur personnalisé le découpage est textuel (un délimiteur
    dans une chaîne littérale couperait à tort).
    """
    statements: list[str] = []
    for segment, delimiter in _split_on_delimiter_directives(script):
        if delimiter == DEFAULT_DELIMITER:
            fragments = sqlparse.split(segment)
        else:
            # Découpage textuel : préserve les ``;`` internes au corps de procédure.
            fragments = segment.split(delimiter)
        for fragment in fragments:
            cleaned = _clean_fragment(fragment, delimiter)
            if cleaned:
                statements.append(cleaned)
    return statements


def _split_on_delimiter_directives(script: str):
    """Génère des couples ``(texte, délimiteur courant)`` ; les lignes DELIMITER sont consommées."""
    delimiter = DEFAULT_DELIMITER
    buffer: list[str] = []
    for line in script.splitlines(keepends=True):
        match = _DELIMITER_LINE_RE.match(line)
        if match:
            if buffer:
                yield "".join(buffer), delimiter
                buffer = []
            delimiter = match.group(1)
            continue
        buffer.append(line)
    if buffer:
        yield "".join(buffer), delimiter


def _clean_fragment(fragment: str, delimiter: str) -> str:
    """Normalise un fragment : délimiteur final retiré, vide si non exécutable."""
    text = fragment.strip()
    if not text:
        return ""
    if text.endswith(delimiter):
        text = text[: -len(delimiter)].strip()
    if not text:
        return ""
    if _is_comment_only(text):
        return ""
    return text


def _is_comment_only(text: str) -> bool:
    """True si le fragment ne contient que des commentaires et des blancs."""
    parsed = sqlparse.parse(text)
    if not parsed:
        return True
    return parsed[0].token_first(skip_ws=True, skip_cm=True) is None


def first_keyword(statement: str) -> str:
    """Premier mot-clé SQL en majuscules, commentaires de tête ignorés (``""`` sinon)."""
    parsed = sqlparse.parse(statement)
    if not parsed:
        return ""
    token = parsed[0].token_first(skip_ws=True, skip_cm=True)
    return token.value.upper() if token is not None else ""


def is_ddl(statement: str) -> bool:
    """True si l'instruction provoque un COMMIT implicite en MySQL (non annulable)."""
    return first_keyword(statement) in DDL_KEYWORDS


def statement_preview(statement: str, max_length: int = PREVIEW_MAX_LENGTH) -> str:
    """Aperçu mono-ligne tronqué pour les logs : jamais le SQL complet (données personnelles)."""
    text = sqlparse.format(statement, strip_comments=True)
    text = " ".join(text.split())
    if len(text) <= max_length:
        return text
    return text[: max_length - 1] + "…"


__all__ = [
    "DDL_KEYWORDS",
    "DEFAULT_DELIMITER",
    "PREVIEW_MAX_LENGTH",
    "ScriptResult",
    "SqlScriptError",
    "StatementResult",
    "first_keyword",
    "is_ddl",
    "split_sql_script",
    "statement_preview",
]
