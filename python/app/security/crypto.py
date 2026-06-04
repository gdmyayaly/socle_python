"""Cryptage réversible de l'`id_rh` (Fernet) — module transverse.

Utilisé par tous les services qui stockent un `id_rh` en base
(DSR-634, 644, 645, 646, 656, 661).

Clé : variable d'environnement / config `ID_RH_CRYPTO_KEY`.
- Si la valeur est une clé Fernet valide (44 caractères base64 url-safe), elle est
  utilisée telle quelle.
- Sinon, une clé Fernet est **dérivée** du secret fourni (SHA-256 -> 32 octets ->
  base64 url-safe). Cela permet d'utiliser n'importe quel secret d'exploitation.
- **Si la clé est vide** (`ID_RH_CRYPTO_KEY` non définie), le cryptage est **désactivé** :
  `encrypt_id_rh` / `decrypt_id_rh` renvoient la valeur telle quelle (pass-through).

NB : un token Fernet fait ~100+ caractères ; les colonnes `id_rh*` doivent donc
être en `VARCHAR(255)` (cf. db_migrations/001_widen_id_rh_columns.sql).
"""

from __future__ import annotations

import base64
import hashlib
import logging
from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken

from app.config import ID_RH_CRYPTO_KEY

logger = logging.getLogger(__name__)


def _derive_fernet_key(secret: str) -> bytes:
    """Construit une clé Fernet valide à partir d'un secret arbitraire."""
    raw = secret.encode("utf-8")
    # Une clé Fernet "native" fait 44 caractères base64 url-safe (32 octets).
    if len(secret) == 44:
        try:
            # Valide le format ; lève si ce n'est pas une clé Fernet.
            Fernet(raw)
            return raw
        except (ValueError, TypeError):
            pass
    digest = hashlib.sha256(raw).digest()  # 32 octets
    return base64.urlsafe_b64encode(digest)


@lru_cache(maxsize=1)
def _get_fernet() -> Fernet | None:
    """Retourne l'instance Fernet, ou `None` si le cryptage est désactivé (clé vide)."""
    if not ID_RH_CRYPTO_KEY:
        logger.warning(
            "ID_RH_CRYPTO_KEY non configurée : cryptage de l'id_rh DÉSACTIVÉ "
            "(valeurs stockées en clair)."
        )
        return None
    return Fernet(_derive_fernet_key(ID_RH_CRYPTO_KEY))


def encrypt_id_rh(clear: str) -> str:
    """Chiffre un id_rh en clair et retourne un token (str) à stocker en base.

    Si la clé n'est pas configurée, retourne la valeur en clair (cryptage désactivé).
    Lève ValueError si l'entrée est vide.
    """
    if clear is None or str(clear).strip() == "":
        raise ValueError("id_rh vide : rien à crypter.")
    fernet = _get_fernet()
    if fernet is None:
        return str(clear).strip()
    token = fernet.encrypt(str(clear).strip().encode("utf-8"))
    return token.decode("ascii")


def decrypt_id_rh(token: str) -> str:
    """Déchiffre un token id_rh stocké en base et retourne l'id_rh en clair.

    Si la clé n'est pas configurée, retourne la valeur telle quelle (cryptage désactivé).
    Lève ValueError si le token est vide ou invalide.
    """
    if token is None or str(token).strip() == "":
        raise ValueError("token id_rh vide : rien à décrypter.")
    fernet = _get_fernet()
    if fernet is None:
        return str(token).strip()
    try:
        clear = fernet.decrypt(str(token).encode("ascii"))
    except InvalidToken as e:
        raise ValueError("Token id_rh invalide ou clé incorrecte.") from e
    return clear.decode("utf-8")
