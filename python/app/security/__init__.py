"""Sous-package sécurité : cryptage des données sensibles (id_rh)."""

from .crypto import decrypt_id_rh, encrypt_id_rh

__all__ = ["encrypt_id_rh", "decrypt_id_rh"]
