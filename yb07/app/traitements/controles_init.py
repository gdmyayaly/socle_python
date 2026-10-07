"""Prérequis (avant toute écriture) et contrôles d'acceptation (après chaque étape) de l'init.

Rejoués en Python : le runner de scripts ne garde que le `rowcount`, pas les valeurs, or
DSR-699 exige une alerte dans les logs sur une somme hors tolérance. Un contrôle écarté pour
son coût est signalé comme tel au rapport, jamais affiché `[OK]` en silence.
"""

from __future__ import annotations

import logging
from typing import Any, Sequence

from app.config import INIT_MAX_ANOMALIES_LOGUEES
from app.db.sql_script import ScriptResult
from app.log_utils import ctx
from app.traitements.rapport import Rapport

logger = logging.getLogger(__name__)

# Index requis par la chaîne. `idx_regate_actif` vient de la base, pas de la migration, mais
# l'étape `versions` en dépend.
INDEX_ATTENDUS = ("uq_site_trafic", "idx_cr_ref_actif", "uq_crc_version_pdi", "idx_regate_actif")

TOTAUX_SITE = ("trafic_colis_total", "trafic_oo_total", "trafic_3s_total")

# Totaux en decimal(35,19) (`fix_error.sql`) ; en deçà, DSR-696 échoue en ERROR 1264 après
# avoir déjà purgé.
PRECISION_MINIMALE = 35
ECHELLE_ATTENDUE = 19

# Tolérance du ticket DSR-699 sur la somme des clés d'un site.
TOLERANCE_BASSE = "0.9999"
TOLERANCE_HAUTE = "1.0001"


# ---------------------------------------------------------------------------
# Requêtes
# ---------------------------------------------------------------------------

# `LIMIT 1` plutôt que `COUNT(*)`, qui balaierait 24 M entrées d'index.
LIGNES_PRESENTES_SQL = """
SELECT 1 AS ok FROM trppu_cles_repartition WHERE id_referentiel = %s LIMIT 1
"""

# Chargement finalisé : `uk_pdi_ref` présent (sinon doublons possibles), index temporaire
# absent.
CHARGEMENT_FINALISE_SQL = """
SELECT COALESCE(SUM(INDEX_NAME = 'uk_pdi_ref'), 0)          AS chargement_uk,
       COALESCE(SUM(INDEX_NAME = 'idx_cr_pdi_doublons'), 0) AS chargement_tmp
  FROM information_schema.STATISTICS
 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'trppu_cles_repartition'
"""

INDEX_PRESENTS_SQL = """
SELECT DISTINCT INDEX_NAME AS nom
  FROM information_schema.STATISTICS
 WHERE TABLE_SCHEMA = DATABASE()
   AND INDEX_NAME IN ('uq_site_trafic', 'idx_cr_ref_actif',
                      'uq_crc_version_pdi', 'idx_regate_actif')
"""

COLONNE_DATE_CREATION_SQL = """
SELECT COUNT(*) AS nb
  FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA = DATABASE()
   AND TABLE_NAME = 'trppu_version_cle'
   AND COLUMN_NAME = 'date_creation'
"""

DEFINITION_TOTAUX_SQL = """
SELECT COLUMN_NAME        AS colonne,
       NUMERIC_PRECISION  AS nb_chiffres,
       NUMERIC_SCALE      AS nb_decimales
  FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA = DATABASE()
   AND TABLE_NAME = 'trppu_trafic_site'
   AND COLUMN_NAME IN ('trafic_colis_total', 'trafic_oo_total', 'trafic_3s_total')
"""

SITES_AGREGES_SQL = """
SELECT COUNT(*) AS nb FROM trppu_trafic_site WHERE id_referentiel = %s
"""

SITES_VERSIONNES_SQL = """
SELECT COUNT(*)                 AS nb_versions,
       COUNT(DISTINCT co_regate) AS nb_sites
  FROM trppu_version_cle
 WHERE id_referentiel = %s AND actif = 'O'
"""

# DSR-701 règle 9 lit `co_regate = ? AND actif = 'O'` sans borner le référentiel : deux
# versions actives d'un site la rendraient ambiguë.
VERSIONS_ACTIVES_MULTIPLES_SQL = """
SELECT COUNT(*) AS nb FROM (
  SELECT co_regate FROM trppu_version_cle
   WHERE actif = 'O' GROUP BY co_regate HAVING COUNT(*) > 1) x
"""

# Garde-fou n°2 de DSR-699 (totaux de site nuls), sur quelques milliers de lignes.
DENOMINATEURS_NULS_SQL = """
SELECT co_regate_site,
       trafic_colis_total, trafic_oo_total, trafic_3s_total, potentielip_total
  FROM trppu_trafic_site
 WHERE id_referentiel = %s
   AND (trafic_colis_total = 0
     OR trafic_oo_total    = 0
     OR trafic_3s_total    = 0
     OR potentielip_total  = 0)
 ORDER BY co_regate_site
"""

# CA4 de DSR-699, en amont : on part des versions et on sonde `uq_crc_version_pdi` (tête
# `id_version_cle`) au lieu de joindre 24 M lignes.
PERIMETRE_DEJA_CALCULE_SQL = """
SELECT COUNT(*) AS nb
  FROM trppu_version_cle v
 WHERE v.id_referentiel = %s
   AND EXISTS (SELECT 1 FROM trppu_cles_repartition_calcule k
                WHERE k.id_version_cle = v.id_version_cle)
"""

# CA2 de DSR-699, même inversion : une clé rattachée à une version désactivée.
CLES_SUR_VERSION_INACTIVE_SQL = """
SELECT COUNT(*) AS nb
  FROM trppu_version_cle v
 WHERE v.id_referentiel = %s
   AND v.actif <> 'O'
   AND EXISTS (SELECT 1 FROM trppu_cles_repartition_calcule k
                WHERE k.id_version_cle = v.id_version_cle)
"""

PHOTO_REFERENTIELS_SQL = """
SELECT id_referentiel, COUNT(*) AS nb FROM trppu_trafic_site GROUP BY id_referentiel
"""

DOUBLONS_SITE_REFERENTIEL_SQL = """
SELECT COUNT(*) AS nb FROM (
  SELECT co_regate_site FROM trppu_trafic_site WHERE id_referentiel = %s
   GROUP BY co_regate_site HAVING COUNT(*) > 1) x
"""

# Constat n°2 de `fix_error.sql` (chiffres entiers atteints), lu sur la cible et non sur les
# 24 M lignes sources.
CHIFFRES_ENTIERS_SQL = """
SELECT MAX(LENGTH(TRUNCATE(GREATEST(trafic_colis_total,
                                    trafic_oo_total,
                                    trafic_3s_total), 0))) AS chiffres_max
  FROM trppu_trafic_site
 WHERE id_referentiel = %s
"""

PDI_ACTIFS_SQL = """
SELECT COUNT(*) AS nb
  FROM trppu_cles_repartition
 WHERE id_referentiel = %s AND date_fin_validite IS NULL
"""

# Clés déjà écrites pour le référentiel — lu seulement en reprise d'un calcul interrompu.
CLES_PRESENTES_SQL = """
SELECT COUNT(*) AS nb
  FROM trppu_cles_repartition_calcule
 WHERE id_referentiel = %s
"""

# Somme attendue par composante : 1, ou 0 si le total du site est nul (clés à 0, règle
# métier DSR-699).
ECART_TOLERE = "0.0001"
SOMMES_HORS_TOLERANCE_SQL = f"""
SELECT k.co_regate_site,
       SUM(k.cle_colis)       AS somme_colis,
       SUM(k.cle_oo)          AS somme_oo,
       SUM(k.cle_3s)          AS somme_3s,
       SUM(k.cle_potentielip) AS somme_potentielip
  FROM trppu_cles_repartition_calcule k
  JOIN trppu_trafic_site s ON s.id_referentiel = k.id_referentiel
                          AND s.co_regate_site = k.co_regate_site
 WHERE k.id_referentiel = %s
 GROUP BY k.co_regate_site
HAVING ABS(SUM(k.cle_colis)       - IF(MAX(s.trafic_colis_total) = 0, 0, 1)) > {ECART_TOLERE}
    OR ABS(SUM(k.cle_oo)          - IF(MAX(s.trafic_oo_total)    = 0, 0, 1)) > {ECART_TOLERE}
    OR ABS(SUM(k.cle_3s)          - IF(MAX(s.trafic_3s_total)    = 0, 0, 1)) > {ECART_TOLERE}
    OR ABS(SUM(k.cle_potentielip) - IF(MAX(s.potentielip_total)  = 0, 0, 1)) > {ECART_TOLERE}
 ORDER BY k.co_regate_site
"""

# PDI actifs des sites à total nul (servi par `idx_cr_ref_actif`).
PDI_ACTIFS_DES_SITES_SQL = """
SELECT co_regate_site, COUNT(*) AS nb
  FROM trppu_cles_repartition
 WHERE id_referentiel = %s AND date_fin_validite IS NULL AND co_regate_site IN ({sites})
 GROUP BY co_regate_site
"""


# ---------------------------------------------------------------------------
# Lecture d'un ScriptResult
# ---------------------------------------------------------------------------


def rowcount(resultat: ScriptResult, debut_apercu: str) -> int:
    """`rowcount` de la première instruction dont l'aperçu commence par `debut_apercu`.

    Repérage par aperçu plutôt que par indice (robuste à un ajout d'instruction) ; `-1` si
    introuvable (`dry_run`).
    """
    cible = " ".join(debut_apercu.split()).upper()
    for instruction in resultat.statements:
        if " ".join(instruction.preview.split()).upper().startswith(cible):
            return instruction.rowcount
    return -1


# ---------------------------------------------------------------------------
# Prérequis — joués AVANT toute écriture
# ---------------------------------------------------------------------------


async def verifier_prerequis(
    rapport: Rapport,
    db,
    id_referentiel: int,
    controles: Sequence[str],
) -> bool:
    """Vérifie que la chaîne peut démarrer à sa première étape ; `False` au premier échec."""
    for controle in controles:
        verificateur = _VERIFICATEURS[controle]
        if not await verificateur(rapport, db, id_referentiel):
            return False
    return True


async def _verifier_lignes(rapport: Rapport, db, id_referentiel: int) -> bool:
    presente = await db.fetch_one(LIGNES_PRESENTES_SQL, (id_referentiel,))
    if presente:
        index = await db.fetch_one(CHARGEMENT_FINALISE_SQL) or {}
        if not index.get("chargement_uk") or index.get("chargement_tmp"):
            return _refuser(
                rapport,
                id_referentiel,
                f"Index du chargement du référentiel {id_referentiel} incomplets (uk_pdi_ref "
                "absent ou index temporaire idx_cr_pdi_doublons présent) : relancer "
                "l'étape « chargement », qui les remet en place avant la première ligne.",
            )
        rapport.ok(f"Référentiel {id_referentiel} chargé")
        return True
    return _refuser(
        rapport,
        id_referentiel,
        f"Le référentiel {id_referentiel} ne porte aucune ligne dans "
        f"trppu_cles_repartition : jouer l'étape « chargement ».",
    )


async def _verifier_schema(rapport: Rapport, db, id_referentiel: int) -> bool:
    """Objets de la migration, puis largeur des totaux (l'ordre désigne la bonne étape)."""
    presents = {ligne["nom"] for ligne in await db.fetch_all(INDEX_PRESENTS_SQL)}
    manquants = [nom for nom in INDEX_ATTENDUS if nom not in presents]

    colonne = await db.fetch_one(COLONNE_DATE_CREATION_SQL)
    if not (colonne and colonne["nb"]):
        manquants.append("trppu_version_cle.date_creation")

    if manquants:
        return _refuser(
            rapport,
            id_referentiel,
            "Objets de schéma absents (" + ", ".join(manquants) + ") : jouer l'étape "
            "« migration ».",
        )
    rapport.ok(f"Migration en place : {len(INDEX_ATTENDUS)} index, colonne date_creation")

    etroites = [
        f"{ligne['colonne']}({ligne['nb_chiffres']},{ligne['nb_decimales']})"
        for ligne in await db.fetch_all(DEFINITION_TOTAUX_SQL)
        if int(ligne["nb_chiffres"] or 0) < PRECISION_MINIMALE
        or int(ligne["nb_decimales"] or 0) != ECHELLE_ATTENDUE
    ]
    if etroites:
        return _refuser(
            rapport,
            id_referentiel,
            "Totaux de site trop étroits (" + ", ".join(etroites) + ") : jouer l'étape "
            "« correctif », sinon l'agrégation échouera en ERROR 1264.",
        )
    rapport.ok(
        f"Totaux de site en decimal({PRECISION_MINIMALE}+,{ECHELLE_ATTENDUE})"
    )
    return True


async def _verifier_agregats(rapport: Rapport, db, id_referentiel: int) -> bool:
    nb = await nb_sites_agreges(db, id_referentiel)
    if nb:
        rapport.ok(f"Agrégats présents : {nb} site(s)")
        return True
    return _refuser(
        rapport,
        id_referentiel,
        f"Aucun agrégat dans trppu_trafic_site pour le référentiel {id_referentiel} : "
        f"jouer l'étape « agregats ».",
    )


async def _verifier_versions(rapport: Rapport, db, id_referentiel: int) -> bool:
    """Les versions actives doivent couvrir tous les sites agrégés.

    Un site sans version est écarté du calcul DSR-699 sans trace, et le CA4 figerait le
    référentiel incomplet.
    """
    attendus = await nb_sites_agreges(db, id_referentiel)
    couverture = await db.fetch_one(SITES_VERSIONNES_SQL, (id_referentiel,))
    couverts = int((couverture or {}).get("nb_sites") or 0)

    if couverts != attendus:
        return _refuser(
            rapport,
            id_referentiel,
            f"Reprise impossible : {attendus} site(s) agrégé(s) pour le référentiel "
            f"{id_referentiel} mais {couverts} version(s) active(s). Jouer « --depuis versions ».",
        )
    rapport.ok(f"Versions actives : {couverts} site(s) couvert(s)")
    return True


async def _verifier_denominateurs(rapport: Rapport, db, id_referentiel: int) -> bool:
    """Sites à total nul : non bloquant, clés à 0 (règle métier), signalées au rapport."""
    nuls = await db.fetch_all(DENOMINATEURS_NULS_SQL, (id_referentiel,))
    if not nuls:
        rapport.ok("Aucun total de site nul")
        return True

    codes = [ligne["co_regate_site"] for ligne in nuls]
    marqueurs = ", ".join(["%s"] * len(codes))
    comptes = {
        ligne["co_regate_site"]: int(ligne["nb"])
        for ligne in await db.fetch_all(
            PDI_ACTIFS_DES_SITES_SQL.format(sites=marqueurs), (id_referentiel, *codes)
        )
    }
    nb_pdi = sum(comptes.values())
    rapport.ok(
        f"{len(nuls)} site(s) à total de trafic nul : clés enregistrées à 0 pour "
        f"{nb_pdi} PDI actif(s) (règle métier, voir avertissements)"
    )
    rapport.etats["SITES_CLE_A_ZERO"] = len(nuls)
    rapport.etats["PDI_CLE_A_ZERO"] = nb_pdi

    for ligne in nuls[:INIT_MAX_ANOMALIES_LOGUEES]:
        composantes = ", ".join(
            nom
            for nom, colonne in (
                ("colis", "trafic_colis_total"),
                ("oo", "trafic_oo_total"),
                ("3s", "trafic_3s_total"),
                ("potentielip", "potentielip_total"),
            )
            if not ligne[colonne]
        )
        code = ligne["co_regate_site"]
        rapport.avertissements.append(
            f"Site {code} : total {composantes} nul — clé(s) {composantes} enregistrée(s) à 0 "
            f"pour ses {comptes.get(code, 0)} PDI actif(s)."
        )
    if len(nuls) > INIT_MAX_ANOMALIES_LOGUEES:
        rapport.avertissements.append(
            f"… et {len(nuls) - INIT_MAX_ANOMALIES_LOGUEES} autre(s) site(s) à total nul, non "
            f"détaillé(s) (INIT_MAX_ANOMALIES_LOGUEES = {INIT_MAX_ANOMALIES_LOGUEES})."
        )
    logger.warning(
        "Rejet division par zéro, clés à 0 %s",
        ctx(
            id_referentiel=id_referentiel,
            sites=len(nuls),
            pdi=nb_pdi,
            premiers=",".join(str(c) for c in codes[:20]),
        ),
    )
    return True


async def verifier_reprise_cles(
    rapport: Rapport, db, id_referentiel: int, lignes_attendues: int | None
) -> int | None:
    """CA4 de DSR-699 : rend le nombre de clés déjà présentes, ou None si refusé.

    Clés partielles : reprise (seules les absentes sont insérées). Clés complètes : refus,
    le périmètre est déjà calculé et ne peut l'être à nouveau.
    """
    versions = await db.fetch_one(PERIMETRE_DEJA_CALCULE_SQL, (id_referentiel,))
    if not int((versions or {}).get("nb") or 0):
        rapport.ok("Aucune clé déjà calculée sur ce périmètre")
        return 0

    deja = int(((await db.fetch_one(CLES_PRESENTES_SQL, (id_referentiel,))) or {}).get("nb") or 0)
    if lignes_attendues is None:
        actifs = await db.fetch_one(PDI_ACTIFS_SQL, (id_referentiel,))
        lignes_attendues = int((actifs or {}).get("nb") or 0)

    if deja >= lignes_attendues:
        _refuser(
            rapport,
            id_referentiel,
            f"CA4 — le référentiel {id_referentiel} est déjà entièrement calculé ({deja} clé(s) "
            f"pour {lignes_attendues} PDI actif(s)) : le CA4 de DSR-699 interdit de le "
            f"recalculer. Créer une nouvelle version (nouveau référentiel), qui désactivera "
            f"les précédentes.",
        )
        return None

    logger.warning(
        "Reprise calcul des clés %s",
        ctx(
            id_referentiel=id_referentiel,
            cles_presentes=deja,
            pdi_actifs=lignes_attendues,
            manquantes=lignes_attendues - deja,
        ),
    )
    rapport.ok(
        f"Reprise du calcul : {deja} clé(s) déjà présente(s) pour {lignes_attendues} PDI "
        f"actif(s) — seules les {lignes_attendues - deja} manquante(s) seront écrites "
        f"(CA4 : aucune clé existante n'est modifiée)"
    )
    return deja


_VERIFICATEURS = {
    "lignes": _verifier_lignes,
    "schema": _verifier_schema,
    "agregats": _verifier_agregats,
    "versions": _verifier_versions,
    "denominateurs": _verifier_denominateurs,
}


def _refuser(rapport: Rapport, id_referentiel: int, motif: str) -> bool:
    logger.warning(
        "Rejet prérequis initialisation %s",
        ctx(id_referentiel=id_referentiel, motif=motif),
    )
    rapport.ko(motif)
    return False


# ---------------------------------------------------------------------------
# Lectures partagées
# ---------------------------------------------------------------------------


async def nb_sites_agreges(db, id_referentiel: int) -> int:
    ligne = await db.fetch_one(SITES_AGREGES_SQL, (id_referentiel,))
    return int((ligne or {}).get("nb") or 0)


async def photo_referentiels(db) -> dict[Any, int]:
    """Nombre d'agrégats par référentiel, comparé avant/après « agregats » pour le CA5."""
    return {
        ligne["id_referentiel"]: int(ligne["nb"] or 0)
        for ligne in await db.fetch_all(PHOTO_REFERENTIELS_SQL)
    }


# ---------------------------------------------------------------------------
# Contrôles d'acceptation, après chaque étape
# ---------------------------------------------------------------------------


async def controler_migration(rapport: Rapport, db) -> None:
    """Relit les objets posés par la migration."""
    presents = {ligne["nom"] for ligne in await db.fetch_all(INDEX_PRESENTS_SQL)}
    manquants = [nom for nom in INDEX_ATTENDUS if nom not in presents]
    colonne = await db.fetch_one(COLONNE_DATE_CREATION_SQL)
    if not (colonne and colonne["nb"]):
        manquants.append("trppu_version_cle.date_creation")

    rapport.ajouter(
        not manquants,
        f"Migration : {len(INDEX_ATTENDUS)} index et la colonne date_creation en place",
        "Migration incomplète, objets absents : " + ", ".join(manquants),
    )


async def controler_correctif(rapport: Rapport, db) -> None:
    definitions = await db.fetch_all(DEFINITION_TOTAUX_SQL)
    etroites = [
        f"{ligne['colonne']}({ligne['nb_chiffres']},{ligne['nb_decimales']})"
        for ligne in definitions
        if int(ligne["nb_chiffres"] or 0) < PRECISION_MINIMALE
        or int(ligne["nb_decimales"] or 0) != ECHELLE_ATTENDUE
    ]
    rapport.ajouter(
        len(definitions) == len(TOTAUX_SITE) and not etroites,
        f"Totaux de site : decimal({PRECISION_MINIMALE}+,{ECHELLE_ATTENDUE}) "
        f"sur les {len(TOTAUX_SITE)} colonnes",
        "Totaux de site encore trop étroits : " + ", ".join(etroites or ["colonnes absentes"]),
    )


async def controler_agregats(
    rapport: Rapport,
    db,
    id_referentiel: int,
    photo_avant: dict[Any, int],
    lignes_ecrites: int,
) -> None:
    """CA1 à CA5 de DSR-696, dans les formes que le volume rend jouables.

    L'écart site par site du script (ré-agrégation des 24 M lignes) n'est pas rejoué.
    """
    en_base = await nb_sites_agreges(db, id_referentiel)
    rapport.ajouter(
        lignes_ecrites < 0 or en_base == lignes_ecrites,
        f"Agrégats : {en_base} site(s)",
        f"Volumétrie incohérente : {en_base} site(s) en base pour "
        f"{lignes_ecrites} écrit(s) par l'agrégation.",
    )

    doublons = await db.fetch_one(DOUBLONS_SITE_REFERENTIEL_SQL, (id_referentiel,))
    nb_doublons = int((doublons or {}).get("nb") or 0)
    rapport.ajouter(
        nb_doublons == 0,
        "CA2 — aucun doublon (site, référentiel)",
        f"CA2 — {nb_doublons} site(s) en double dans le référentiel.",
    )

    # CA4 : anti-jointure sur 24 M lignes non jouée, le rapport le dit.
    rapport.ok(
        "CA4 — garanti par la séquence DELETE/INSERT sur le même prédicat "
        "(anti-jointure sur 24 M lignes non rejouée)"
    )

    apres = await photo_referentiels(db)
    bouges = [
        str(ref)
        for ref, nb in apres.items()
        if ref != id_referentiel and photo_avant.get(ref) != nb
    ]
    disparus = [str(ref) for ref in photo_avant if ref != id_referentiel and ref not in apres]
    rapport.ajouter(
        not bouges and not disparus,
        f"CA5 — historisation : {len(apres)} référentiel(s), les autres intacts",
        "CA5 — d'autres référentiels ont bougé : " + ", ".join(bouges + disparus),
    )

    chiffres = await db.fetch_one(CHIFFRES_ENTIERS_SQL, (id_referentiel,))
    chiffres_max = int((chiffres or {}).get("chiffres_max") or 0)
    rapport.etats["CHIFFRES_ENTIERS_MAX"] = chiffres_max
    if chiffres_max > PRECISION_MINIMALE - ECHELLE_ATTENDUE:
        rapport.ok(
            f"Totaux atteignant {chiffres_max} chiffres entiers — à confirmer avec l'équipe "
            f"data (volumes, ou ratios chargés comme des volumes ?)"
        )


async def controler_versions(
    rapport: Rapport, db, id_referentiel: int, nb_sites_attendus: int
) -> None:
    """CA3 de DSR-698, en un seul agrégat plutôt que rejoué site par site."""
    couverture = await db.fetch_one(SITES_VERSIONNES_SQL, (id_referentiel,))
    nb_versions = int((couverture or {}).get("nb_versions") or 0)
    nb_sites = int((couverture or {}).get("nb_sites") or 0)

    rapport.ajouter(
        nb_versions == nb_sites == nb_sites_attendus,
        f"CA3 — une version active par site, portant le référentiel {id_referentiel}",
        f"CA3 — {nb_versions} version(s) active(s) pour {nb_sites} site(s) distinct(s), "
        f"{nb_sites_attendus} attendu(s).",
    )

    multiples = await db.fetch_one(VERSIONS_ACTIVES_MULTIPLES_SQL)
    nb_multiples = int((multiples or {}).get("nb") or 0)
    rapport.ajouter(
        nb_multiples == 0,
        "Aucun site ne porte deux versions actives",
        f"{nb_multiples} site(s) portent plusieurs versions actives : la lecture "
        f"d'éligibilité de DSR-701 deviendrait ambiguë.",
    )


async def controler_cles(
    rapport: Rapport,
    db,
    id_referentiel: int,
    lignes_ecrites: int,
    *,
    lignes_attendues: int | None = None,
    controles_longs: bool = True,
) -> None:
    """CA1 à CA4 de DSR-699.

    Sans `lignes_attendues` (reprise), le CA1 recompte 24 M lignes : réservé à `controles_longs`.
    """
    if lignes_attendues is None and controles_longs:
        actifs = await db.fetch_one(PDI_ACTIFS_SQL, (id_referentiel,))
        lignes_attendues = int((actifs or {}).get("nb") or 0)

    if lignes_attendues is None:
        rapport.ok(
            f"CA1 — {lignes_ecrites} clé(s) écrite(s) (comparaison au nombre de PDI actifs "
            f"non jouée : --sans-controles-longs)"
        )
    else:
        rapport.ajouter(
            lignes_ecrites == lignes_attendues,
            f"CA1 — {lignes_ecrites} clé(s) pour {lignes_attendues} PDI actif(s)",
            f"CA1 — {lignes_ecrites} clé(s) écrite(s) pour {lignes_attendues} PDI actif(s) : "
            f"{lignes_attendues - lignes_ecrites} PDI sans clé.",
        )

    inactives = await db.fetch_one(CLES_SUR_VERSION_INACTIVE_SQL, (id_referentiel,))
    nb_inactives = int((inactives or {}).get("nb") or 0)
    rapport.ajouter(
        nb_inactives == 0,
        "CA2 — toutes les clés sont rattachées à une version active",
        f"CA2 — {nb_inactives} version(s) désactivée(s) portent des clés.",
    )

    # CA4 : garanti en base par `uq_crc_version_pdi` (vérifié à l'étape « migration »).
    rapport.ok("CA4 — unicité (version, PDI) garantie par uq_crc_version_pdi")

    if not controles_longs:
        rapport.ok(
            "CA3 — contrôle des sommes non joué (--sans-controles-longs) : "
            "une clé fausse ne serait pas détectée"
        )
        return

    await _controler_sommes(rapport, db, id_referentiel)


async def _controler_sommes(rapport: Rapport, db, id_referentiel: int) -> None:
    """CA3 — la somme des clés d'un site vaut 1 à 10⁻⁴ près ; alerte dans les logs sinon.

    Coûteux (aucun index ne couvre `cle_*`), mais seul détecteur de clés fausses.
    """
    anomalies = await db.fetch_all(SOMMES_HORS_TOLERANCE_SQL, (id_referentiel,))

    if not anomalies:
        rapport.ok(
            f"CA3 — sommes des clés dans la tolérance {TOLERANCE_BASSE}–{TOLERANCE_HAUTE}"
        )
        return

    for ligne in anomalies[:INIT_MAX_ANOMALIES_LOGUEES]:
        logger.warning(
            "Rejet contrôle somme des clés %s",
            ctx(
                id_referentiel=id_referentiel,
                co_regate=ligne.get("co_regate_site"),
                somme_colis=ligne.get("somme_colis"),
                somme_oo=ligne.get("somme_oo"),
                somme_3s=ligne.get("somme_3s"),
                somme_potentielip=ligne.get("somme_potentielip"),
                tolerance=f"{TOLERANCE_BASSE}-{TOLERANCE_HAUTE}",
            ),
        )
    if len(anomalies) > INIT_MAX_ANOMALIES_LOGUEES:
        logger.warning(
            "Rejet contrôle somme des clés %s",
            ctx(
                id_referentiel=id_referentiel,
                anomalies_non_journalisees=len(anomalies) - INIT_MAX_ANOMALIES_LOGUEES,
            ),
        )

    premiers = ", ".join(str(ligne["co_regate_site"]) for ligne in anomalies[:5])
    suite = "…" if len(anomalies) > 5 else ""
    rapport.ko(
        f"CA3 — {len(anomalies)} site(s) hors tolérance "
        f"{TOLERANCE_BASSE}–{TOLERANCE_HAUTE} (premiers : {premiers}{suite})"
    )
    rapport.etats["SITES_HORS_TOLERANCE"] = len(anomalies)


__all__ = [
    "controler_agregats",
    "controler_cles",
    "controler_correctif",
    "controler_migration",
    "controler_versions",
    "nb_sites_agreges",
    "photo_referentiels",
    "rowcount",
    "verifier_prerequis",
    "verifier_reprise_cles",
]
