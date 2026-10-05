"""Helpers pour l'audit id_rh : balayage des tables porteuses d'un id_rh + déchiffrement.

Le chiffrement Fernet est **non déterministe** : on ne peut pas comparer les tokens
entre eux. Il faut donc déchiffrer chaque `id_rh` stocké avec la clé fournie et
comparer la valeur **en clair**. On balaie toutes les tables qui possèdent une colonne
id_rh (schéma de PROD = référence) :

- `trppu_scenario`        : id_rh_creation (CREATION), id_rh_maj (MAJ)
- `trppu_pic_version`     : id_rh_creation (CREATION), id_rh_maj (MAJ)
- `trppu_pic_coefficients`: id_rh (ECRITURE)
- `trppu_neutralisations` : id_rh (ECRITURE)
- `trppu_tmh`             : id_rh (ECRITURE)
- `trppu_scenario_variations_prev` : id_rh (ECRITURE)

Le même module porte aussi l'audit Agrébals / PDI (DSR-737), en fin de fichier.
"""

from __future__ import annotations

import json
from typing import Any

from cryptography.fernet import Fernet, InvalidToken

# Début typique d'un token Fernet (octet de version 0x80 -> base64 "gAAAAA").
FERNET_PREFIX = "gAAAAA"


def looks_like_fernet(token: str | None) -> bool:
    return bool(token) and str(token).strip().startswith(FERNET_PREFIX)


def safe_decrypt(fernet: Fernet, token: str | None) -> tuple[str | None, bool]:
    """Déchiffre un token. Retourne (clair, True) si réussi, sinon (token_brut, False).

    Le fallback (token brut) couvre le cas où le cryptage serait désactivé (valeurs
    stockées en clair) : la comparaison se fait alors clair-à-clair.
    """
    if token is None:
        return None, False
    s = str(token).strip()
    if s == "":
        return None, False
    try:
        return fernet.decrypt(s.encode("ascii")).decode("utf-8"), True
    except (InvalidToken, ValueError):
        return s, False


def _match(fernet: Fernet, stored: str | None, target_clear: str) -> bool:
    clear, _ = safe_decrypt(fernet, stored)
    return clear is not None and clear == target_clear


async def collect_actions(db, fernet: Fernet, target_clear: str) -> list[dict[str, Any]]:
    """Retourne toutes les actions (toutes tables) effectuées par `target_clear`."""
    actions: list[dict[str, Any]] = []

    # 1. trppu_scenario — création + dernière modification.
    rows = await db.fetch_all(
        "SELECT id_scenario, co_regate, lb_scenario, statut, dt_creation, dt_maj, "
        "id_rh_creation, id_rh_maj FROM trppu_scenario "
        "WHERE id_rh_creation IS NOT NULL OR id_rh_maj IS NOT NULL"
    )
    for r in rows:
        if _match(fernet, r["id_rh_creation"], target_clear):
            actions.append({
                "ressource": "trppu_scenario", "action": "CREATION_SCENARIO",
                "id": r["id_scenario"], "id_scenario": r["id_scenario"], "date": r["dt_creation"],
                "details": {"co_regate": r["co_regate"], "lb_scenario": r["lb_scenario"],
                            "statut": r["statut"]},
            })
        if _match(fernet, r["id_rh_maj"], target_clear):
            actions.append({
                "ressource": "trppu_scenario", "action": "MAJ_SCENARIO",
                "id": r["id_scenario"], "id_scenario": r["id_scenario"], "date": r["dt_maj"],
                "details": {"co_regate": r["co_regate"], "lb_scenario": r["lb_scenario"],
                            "statut": r["statut"]},
            })

    # 2. trppu_pic_version — création + modification.
    rows = await db.fetch_all(
        "SELECT id_pic_version, co_regate, id_scenario, niveau, dt_creation, dt_maj, "
        "id_rh_creation, id_rh_maj FROM trppu_pic_version "
        "WHERE id_rh_creation IS NOT NULL OR id_rh_maj IS NOT NULL"
    )
    for r in rows:
        if _match(fernet, r["id_rh_creation"], target_clear):
            actions.append({
                "ressource": "trppu_pic_version", "action": "CREATION_PIC_VERSION",
                "id": r["id_pic_version"], "id_scenario": r["id_scenario"], "date": r["dt_creation"],
                "details": {"co_regate": r["co_regate"], "niveau": r["niveau"]},
            })
        if _match(fernet, r["id_rh_maj"], target_clear):
            actions.append({
                "ressource": "trppu_pic_version", "action": "MAJ_PIC_VERSION",
                "id": r["id_pic_version"], "id_scenario": r["id_scenario"], "date": r["dt_maj"],
                "details": {"co_regate": r["co_regate"], "niveau": r["niveau"]},
            })

    # 3. trppu_pic_coefficients — écriture d'un coefficient.
    rows = await db.fetch_all(
        "SELECT id_pic_coef, id_pic_version, co_produit, jour_semaine, dt_maj, id_rh "
        "FROM trppu_pic_coefficients WHERE id_rh IS NOT NULL"
    )
    for r in rows:
        if _match(fernet, r["id_rh"], target_clear):
            actions.append({
                "ressource": "trppu_pic_coefficients", "action": "ECRITURE_PIC_COEFFICIENT",
                "id": r["id_pic_coef"], "id_scenario": None, "date": r["dt_maj"],
                "details": {"id_pic_version": r["id_pic_version"], "co_produit": r["co_produit"],
                            "jour_semaine": r["jour_semaine"]},
            })

    # 4. trppu_neutralisations — ajout/MAJ d'une neutralisation.
    rows = await db.fetch_all(
        "SELECT id_neutralisation, id_scenario, motif, dt_debut, dt_fin, dt_creation, id_rh "
        "FROM trppu_neutralisations WHERE id_rh IS NOT NULL"
    )
    for r in rows:
        if _match(fernet, r["id_rh"], target_clear):
            actions.append({
                "ressource": "trppu_neutralisations", "action": "NEUTRALISATION",
                "id": r["id_neutralisation"], "id_scenario": r["id_scenario"], "date": r["dt_creation"],
                "details": {"motif": r["motif"], "dt_debut": str(r["dt_debut"]),
                            "dt_fin": str(r["dt_fin"])},
            })

    # 5. trppu_tmh — écriture des trafics.
    rows = await db.fetch_all(
        "SELECT id_tmh, id_scenario, co_produit, motif, dt_calcul, id_rh "
        "FROM trppu_tmh WHERE id_rh IS NOT NULL"
    )
    for r in rows:
        if _match(fernet, r["id_rh"], target_clear):
            actions.append({
                "ressource": "trppu_tmh", "action": "ECRITURE_TMH",
                "id": r["id_tmh"], "id_scenario": r["id_scenario"], "date": r["dt_calcul"],
                "details": {"co_produit": r["co_produit"], "motif": r["motif"]},
            })

    # 6. trppu_scenario_variations_prev — écriture d'une variation prévisionnelle.
    rows = await db.fetch_all(
        "SELECT id_variation, id_scenario, co_produit, variation_pct, dt_creation, id_rh "
        "FROM trppu_scenario_variations_prev WHERE id_rh IS NOT NULL"
    )
    for r in rows:
        if _match(fernet, r["id_rh"], target_clear):
            actions.append({
                "ressource": "trppu_scenario_variations_prev", "action": "ECRITURE_VARIATION",
                "id": r["id_variation"], "id_scenario": r["id_scenario"], "date": r["dt_creation"],
                "details": {"co_produit": r["co_produit"],
                            "variation_pct": str(r["variation_pct"])},
            })

    return actions


# ---------------------------------------------------------------------------
# DSR-737 — Agrébals et PDI d'un scénario ou d'un site
# ---------------------------------------------------------------------------

SELECT_SCENARIO_AUDIT_SQL = (
    "SELECT id_scenario, co_regate, trafic_pdi_calcule "
    "FROM trppu_scenario WHERE id_scenario = %s"
)
SELECT_SITE_AUDIT_SQL = "SELECT co_regate, lb_regate FROM trppu_site WHERE co_regate = %s"

# RG-003 / CA-03 : ce que le calcul a RÉELLEMENT utilisé. YB05 (DSR-702) écrit dans
# `trppu_trafic_pdi` l'Agrébal et le PDI de chaque ligne de trafic : c'est une photo du
# calcul, qui reste juste même si l'Agrébal a changé depuis. `trppu_agrebal_pdi` ne
# garde aucun historique (une ligne par Agrébal et par site, `uq_agrpdi_courant`), elle
# ne permettrait pas de reconstituer un calcul passé. Index de tête : id_scenario.
SELECT_AGREBALS_PDI_SCENARIO_SQL = (
    "SELECT DISTINCT agrebal_uuid, id_pdi FROM trppu_trafic_pdi "
    "WHERE id_scenario = %s ORDER BY agrebal_uuid, id_pdi"
)

# RG-002 : données actives du site. `trppu_agrebal_pdi` n'a pas de DATE_FIN_VALIDITE ;
# le « mécanisme équivalent » que le ticket autorise est la suppression logique
# `agrebal_deleteddAt IS NULL`. Index : idx_agrpdi_site.
SELECT_AGREBALS_PDI_SITE_SQL = (
    "SELECT agrebal_uuid, agrebal_pdiList FROM trppu_agrebal_pdi "
    "WHERE agrebal_code_regate = %s AND agrebal_deleteddAt IS NULL "
    "ORDER BY agrebal_uuid"
)


def extraire_pdi_ids(brut: Any) -> list[int]:
    """PDI d'un `agrebal_pdiList` (`[{"pdi_id": …}, …]`), liste vide si illisible.

    Même lecture que YB05 (`scenario._extraire_pdi_ids`) : les deux modules doivent voir
    les mêmes PDI dans un Agrébal.
    """
    if not brut:
        return []
    if isinstance(brut, (str, bytes, bytearray)):
        try:
            brut = json.loads(brut)
        except (ValueError, TypeError):
            return []
    if not isinstance(brut, list):
        return []
    ids: list[int] = []
    for element in brut:
        pdi = element.get("pdi_id") if isinstance(element, dict) else element
        try:
            ids.append(int(pdi))
        except (TypeError, ValueError):
            continue
    return ids


def regrouper_par_agrebal(lignes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """`(agrebal_uuid, id_pdi)` à plat -> `[{agrebal_uuid, pdis}]`, ordre de lecture conservé."""
    par_uuid: dict[str, list[int]] = {}
    for ligne in lignes:
        par_uuid.setdefault(str(ligne["agrebal_uuid"]), []).append(int(ligne["id_pdi"]))
    return [{"agrebal_uuid": uuid, "pdis": pdis} for uuid, pdis in par_uuid.items()]


def agrebals_du_site(lignes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Lignes de `trppu_agrebal_pdi` -> `[{agrebal_uuid, pdis}]`."""
    return [
        {
            "agrebal_uuid": str(ligne["agrebal_uuid"]),
            "pdis": extraire_pdi_ids(ligne.get("agrebal_pdiList")),
        }
        for ligne in lignes
    ]
