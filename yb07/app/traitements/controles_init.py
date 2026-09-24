"""Garde-fous et critères d'acceptation de la chaîne d'initialisation, rejoués en Python.

Pourquoi ne pas se contenter des `SELECT` de contrôle que portent déjà les scripts : le runner
du socle exécute chaque instruction sur un curseur nu et n'appelle jamais `fetchall()`
(`app/db/mysql.py`, `_run_statement`). Il conserve le `rowcount`, ce qui suffit aux contrôles
« doit renvoyer 0 ligne » — mais les **valeurs** (les sommes de clés) et les **identités** (quel
site) sont perdues. Or DSR-699 demande explicitement une alerte dans les logs sur une somme hors
tolérance : le SQL ne sait pas journaliser, l'appelant si.

Deux familles :

* les **prérequis**, joués avant toute écriture. Ils répondent à « peut-on démarrer ici ? »,
  question qui se pose dès qu'on reprend la chaîne au milieu. Tous sont bon marché — un garde-fou
  qui coûte vingt minutes ne serait pas joué, donc ne protégerait rien ;
* les **contrôles d'acceptation**, joués après chaque étape.

Toutes les requêtes de ce module sont en lecture, et passent par la base de lecture. Plusieurs
contrôles des scripts ont été écartés pour leur coût : chaque fois, le rapport le dit, plutôt que
d'afficher un `[OK]` qui laisserait croire à une vérification qui n'a pas eu lieu.
"""

from __future__ import annotations

import logging
from typing import Any, Sequence

from app.config import INIT_MAX_ANOMALIES_LOGUEES
from app.db.sql_script import ScriptResult
from app.log_utils import ctx
from app.traitements.rapport import Rapport

logger = logging.getLogger(__name__)

# Les quatre objets que pose `DSR-696-699_migration.sql`. `idx_regate_actif` est fourni par la
# base depuis la ré-extraction du 17/08/2026 : la migration ne le crée plus, mais l'étape
# `versions` en dépend, donc on vérifie sa présence comme les autres.
INDEX_ATTENDUS = ("uq_site_trafic", "idx_cr_ref_actif", "uq_crc_version_pdi", "idx_regate_actif")

TOTAUX_SITE = ("trafic_colis_total", "trafic_oo_total", "trafic_3s_total")

# `fix_error.sql` porte les trois totaux à decimal(35,19). En deçà, DSR-696 échoue en
# `ERROR 1264 (Out of range value)` après avoir purgé — donc après avoir déjà fait des dégâts.
PRECISION_MINIMALE = 35
ECHELLE_ATTENDUE = 19

# Tolérance du ticket DSR-699 sur la somme des clés d'un site.
TOLERANCE_BASSE = "0.9999"
TOLERANCE_HAUTE = "1.0001"


# ---------------------------------------------------------------------------
# Requêtes
# ---------------------------------------------------------------------------

# `LIMIT 1` sur l'index, et non `COUNT(*)` : la question est « y a-t-il des lignes ? », à
# laquelle un comptage répondrait en balayant 24 M entrées de `idx_cr_ref_actif`.
LIGNES_PRESENTES_SQL = """
SELECT 1 AS ok FROM trppu_cles_repartition WHERE id_referentiel = %s LIMIT 1
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

# L'ambiguïté que redoute DSR-701 règle 9 : sa lecture d'éligibilité filtre sur
# `co_regate = ? AND actif = 'O'` sans borner le référentiel. Deux versions actives sur des
# référentiels différents lui rendraient deux lignes.
VERSIONS_ACTIVES_MULTIPLES_SQL = """
SELECT COUNT(*) AS nb FROM (
  SELECT co_regate FROM trppu_version_cle
   WHERE actif = 'O' GROUP BY co_regate HAVING COUNT(*) > 1) x
"""

# Garde-fou n°2 de DSR-699, restreint à `trppu_trafic_site` : quelques milliers de lignes, et il
# annonce l'`ERROR 1365` avant que le calcul ne consomme des heures pour rien.
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

# CA4 de DSR-699, en amont. Parcours inversé par rapport au script : on part de
# `trppu_version_cle` (quelques milliers de lignes) et on sonde les clés par
# `uq_crc_version_pdi`, dont `id_version_cle` est le membre de tête. Quelques milliers de sondes
# d'index, au lieu d'une jointure sur 24 M lignes.
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

# Substitut du constat n°2 de `fix_error.sql`, lu sur la cible et non sur les 24 M lignes
# sources : même information — combien de chiffres entiers un total atteint réellement — pour
# quelques milliers de lignes au lieu d'une ré-agrégation complète.
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

SOMMES_HORS_TOLERANCE_SQL = f"""
SELECT co_regate_site,
       SUM(cle_colis)       AS somme_colis,
       SUM(cle_oo)          AS somme_oo,
       SUM(cle_3s)          AS somme_3s,
       SUM(cle_potentielip) AS somme_potentielip
  FROM trppu_cles_repartition_calcule
 WHERE id_referentiel = %s
 GROUP BY co_regate_site
HAVING SUM(cle_colis)       NOT BETWEEN {TOLERANCE_BASSE} AND {TOLERANCE_HAUTE}
    OR SUM(cle_oo)          NOT BETWEEN {TOLERANCE_BASSE} AND {TOLERANCE_HAUTE}
    OR SUM(cle_3s)          NOT BETWEEN {TOLERANCE_BASSE} AND {TOLERANCE_HAUTE}
    OR SUM(cle_potentielip) NOT BETWEEN {TOLERANCE_BASSE} AND {TOLERANCE_HAUTE}
 ORDER BY co_regate_site
"""


# ---------------------------------------------------------------------------
# Lecture d'un ScriptResult
# ---------------------------------------------------------------------------


def rowcount(resultat: ScriptResult, debut_apercu: str) -> int:
    """Nombre de lignes de la première instruction dont l'aperçu commence par `debut_apercu`.

    Le repérage se fait sur l'aperçu, pas sur l'indice : ajouter un commentaire en tête de
    script décalerait un contrôle indexé par position, sans que rien ne le signale.

    Rend `-1` si l'instruction n'a pas été trouvée — cas d'un `dry_run`, où rien n'a tourné.
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
    """Vérifie que la chaîne peut démarrer à l'étape demandée. Aucune écriture.

    `controles` est la liste des vérifications à jouer, décidée par l'orchestrateur en fonction
    de la première étape : elles ne s'appliquent qu'au point de départ, puisque les prérequis
    des étapes suivantes sont produits par les précédentes au cours du même passage.

    Retourne `False` au premier échec, après avoir posé le motif au rapport.
    """
    for controle in controles:
        verificateur = _VERIFICATEURS[controle]
        if not await verificateur(rapport, db, id_referentiel):
            return False
    return True


async def _verifier_lignes(rapport: Rapport, db, id_referentiel: int) -> bool:
    presente = await db.fetch_one(LIGNES_PRESENTES_SQL, (id_referentiel,))
    if presente:
        rapport.ok(f"Référentiel {id_referentiel} chargé")
        return True
    return _refuser(
        rapport,
        id_referentiel,
        f"Le référentiel {id_referentiel} ne porte aucune ligne dans "
        f"trppu_cles_repartition : jouer l'étape « chargement ».",
    )


async def _verifier_schema(rapport: Rapport, db, id_referentiel: int) -> bool:
    """Objets de la migration, puis largeur des totaux — dans cet ordre.

    L'ordre compte pour le diagnostic : sans les index, le message doit désigner la migration ;
    avec les index mais des colonnes étroites, il doit désigner le correctif.
    """
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
    """Les versions doivent couvrir **tous** les sites agrégés, pas seulement exister.

    DSR-699 joint les trois tables : un site agrégé mais sans version active est écarté de son
    calcul, sans erreur et sans trace. Démarrer à « cles » sur une couverture partielle
    produirait donc un référentiel incomplet — et, par le CA4, définitivement figé.
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
    nuls = await db.fetch_all(DENOMINATEURS_NULS_SQL, (id_referentiel,))
    if not nuls:
        rapport.ok("Aucun dénominateur nul")
        return True

    sites = ", ".join(str(ligne["co_regate_site"]) for ligne in nuls[:5])
    suite = "…" if len(nuls) > 5 else ""
    return _refuser(
        rapport,
        id_referentiel,
        f"{len(nuls)} site(s) ont un total de trafic à zéro ({sites}{suite}) : le calcul "
        f"échouerait en division par zéro. Question métier avant d'aller plus loin.",
    )


async def _verifier_non_calcule(rapport: Rapport, db, id_referentiel: int) -> bool:
    """CA4 de DSR-699 : une version déjà calculée n'est jamais retouchée.

    Sans ce garde-fou, relancer la chaîne sur un périmètre déjà calculé ferait écrire zéro clé
    au script, qui se terminerait néanmoins en succès — un faux `[OK]` sur l'étape la plus
    lourde de la chaîne.
    """
    deja = await db.fetch_one(PERIMETRE_DEJA_CALCULE_SQL, (id_referentiel,))
    nb = int((deja or {}).get("nb") or 0)
    if not nb:
        rapport.ok("Aucune clé déjà calculée sur ce périmètre")
        return True

    return _refuser(
        rapport,
        id_referentiel,
        f"{nb} version(s) du référentiel {id_referentiel} portent déjà des clés : le CA4 de "
        f"DSR-699 interdit de les recalculer. Créer une nouvelle version (nouveau référentiel), "
        f"qui désactivera les précédentes.",
    )


_VERIFICATEURS = {
    "lignes": _verifier_lignes,
    "schema": _verifier_schema,
    "agregats": _verifier_agregats,
    "versions": _verifier_versions,
    "denominateurs": _verifier_denominateurs,
    "non_calcule": _verifier_non_calcule,
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
    """Nombre d'agrégats par référentiel — à prendre avant l'étape « agregats ».

    Comparée à la même photo prise après, elle prouve le CA5 (historisation) : seul le
    référentiel visé doit avoir bougé.
    """
    return {
        ligne["id_referentiel"]: int(ligne["nb"] or 0)
        for ligne in await db.fetch_all(PHOTO_REFERENTIELS_SQL)
    }


# ---------------------------------------------------------------------------
# Contrôles d'acceptation, après chaque étape
# ---------------------------------------------------------------------------


async def controler_migration(rapport: Rapport, db) -> None:
    """Relit les objets posés — le `rowcount` du script ne dirait que « 13 lignes »."""
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

    Le contrôle d'écart site par site du script n'est **pas** rejoué : il ré-agrège les 24 M
    lignes sources, donc il coûte une seconde fois le prix de l'étape. Ce qui est vérifié ici,
    c'est que l'`INSERT` a bien écrit ce que la table contient — une écriture partielle se voit,
    une somme fausse non. L'écart complet reste disponible dans le `.sql`, joué à la main en
    recette.
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

    # On ne maquille pas : le CA4 demande une anti-jointure sur 24 M lignes, on ne la joue pas,
    # et le rapport dit pourquoi elle est tenue pour acquise.
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

    `lignes_attendues` vient de l'étape « chargement » quand la chaîne tourne de bout en bout :
    le CA1 est alors gratuit. En reprise, il faut le recompter — un balayage d'index sur 24 M
    lignes, réservé à `controles_longs`.
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

    # CA4 : garanti en base par `uq_crc_version_pdi`, dont l'étape « migration » a vérifié la
    # présence. Le `GROUP BY` du script sur 24 M lignes ne dirait rien de plus.
    rapport.ok("CA4 — unicité (version, PDI) garantie par uq_crc_version_pdi")

    if not controles_longs:
        rapport.ok(
            "CA3 — contrôle des sommes non joué (--sans-controles-longs) : "
            "une clé fausse ne serait pas détectée"
        )
        return

    await _controler_sommes(rapport, db, id_referentiel)


async def _controler_sommes(rapport: Rapport, db, id_referentiel: int) -> None:
    """CA3 — la somme des clés d'un site vaut 1, à 10⁻⁴ près.

    Le seul contrôle capable de détecter des clés silencieusement fausses, et celui dont le
    ticket demande qu'il alerte dans les logs. Il n'en existe aucune forme bon marché sur le
    schéma actuel : `uq_crc_version_pdi` ne couvre pas les colonnes `cle_*`, donc le
    regroupement paie une remontée en clé primaire par ligne. C'est assumé.
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
]
