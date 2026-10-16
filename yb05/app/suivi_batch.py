"""DSR-729 — suivi des exécutions et unicité d'instance, via `trppu_suivi_batch`.

Une ligne par exécution d'un traitement YB05 : `EN_COURS` au démarrage, puis `SUCCES`,
`ECHEC` ou `PARTIEL` (mode ALL avec des scénarios réussis et d'autres en échec) à la fin,
avec `dt_fin` et `duree`.

Unicité : le verrou porte sur le **module** (`lb_batch = 'YB05'`) — tant qu'une exécution
YB05 est `EN_COURS`, toute nouvelle instance s'arrête sans rien calculer.

Démarrage, dans cet ordre :

1. **Abandons** — une exécution `EN_COURS` depuis plus de `SUIVI_BATCH_TIMEOUT_MINUTES`
   est considérée comme un pod tombé (crash, éviction, OOM) : passée en `ECHEC`, avec le
   commentaire du ticket. L'enum ne connaît pas `ABANDONNE` ; on garde la table telle quelle.
2. **Réservation** — la nouvelle exécution insère d'abord sa propre ligne `EN_COURS`
   (commitée), *puis* cherche une autre exécution `EN_COURS` récente.
3. **Refus** — s'il y en a une, la ligne réservée est supprimée et l'instance s'arrête.

Pourquoi insérer avant de chercher : « chercher puis insérer » laisse passer deux pods
démarrés à la même seconde (les deux cherchent, ne voient rien, puis insèrent). Ici chacun
commit sa ligne avant de regarder : le second à chercher voit forcément le premier. Au pire,
deux démarrages strictement simultanés se voient mutuellement et s'arrêtent tous les deux —
jamais deux calculs en parallèle ; le lancement suivant passera.

Toutes les lectures se font sur l'instance d'**écriture** (l'instance de lecture est un
secondaire de réplication, potentiellement en retard) et les dates viennent de `NOW()`
MySQL : l'horloge des pods n'intervient pas.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from app.config import MODULE, SUIVI_BATCH_TIMEOUT_MINUTES
from app.db.mysql import db_write
from app.log_utils import ctx

logger = logging.getLogger(__name__)

LB_BATCH = MODULE.upper()

EN_COURS = "EN_COURS"
SUCCES = "SUCCES"
ECHEC = "ECHEC"
PARTIEL = "PARTIEL"

COMMENTAIRE_ABANDON = (
    "Exécution considérée abandonnée suite dépassement du timeout d'exécution."
)

# Colonnes `commande` et `commentaire` : varchar(255).
LONGUEUR_MAX = 255

SELECT_ABANDONNEES_SQL = """
    SELECT id_calcul_batch, dt_debut, commande
      FROM trppu_suivi_batch
     WHERE lb_batch = %s
       AND statut = 'EN_COURS'
       AND dt_debut < NOW() - INTERVAL %s MINUTE
     ORDER BY id_calcul_batch
"""

# `statut = 'EN_COURS'` dans le WHERE : deux pods qui clôturent le même abandon en même
# temps ne l'écrivent qu'une fois (le second affecte 0 ligne).
CLORE_SQL = """
    UPDATE trppu_suivi_batch
       SET statut = %s,
           dt_fin = NOW(),
           duree = SEC_TO_TIME(TIMESTAMPDIFF(SECOND, dt_debut, NOW())),
           commentaire = %s
     WHERE id_calcul_batch = %s
       AND statut = 'EN_COURS'
"""

INSERT_EXECUTION_SQL = """
    INSERT INTO trppu_suivi_batch (lb_batch, dt_debut, statut, commande)
    VALUES (%s, NOW(), 'EN_COURS', %s)
"""

DERNIER_ID_SQL = "SELECT LAST_INSERT_ID() AS id"

SELECT_AUTRE_ACTIVE_SQL = """
    SELECT id_calcul_batch, dt_debut, commande
      FROM trppu_suivi_batch
     WHERE lb_batch = %s
       AND statut = 'EN_COURS'
       AND id_calcul_batch <> %s
       AND dt_debut >= NOW() - INTERVAL %s MINUTE
     ORDER BY id_calcul_batch
     LIMIT 1
"""

SUPPRIMER_RESERVATION_SQL = """
    DELETE FROM trppu_suivi_batch
     WHERE id_calcul_batch = %s
       AND statut = 'EN_COURS'
"""


@dataclass(frozen=True)
class Execution:
    """Exécution autorisée : identifiant de sa ligne `trppu_suivi_batch`."""

    id_calcul_batch: int


@dataclass(frozen=True)
class Refus:
    """Démarrage refusé : l'exécution déjà en cours qui le bloque."""

    id_calcul_batch: int
    dt_debut: object
    commande: str

    @property
    def message(self) -> str:
        return (
            f"Une instance du batch {LB_BATCH} est déjà en cours d'exécution "
            f"(id_calcul_batch={self.id_calcul_batch}, depuis {self.dt_debut}, "
            f"commande « {self.commande} »). Le démarrage est refusé."
        )


async def demarrer(
    commande: str,
    *,
    db=db_write,
    timeout_minutes: int | None = None,
) -> Execution | Refus:
    """Clôt les abandons, réserve l'exécution, puis l'autorise ou la refuse."""
    timeout = timeout_minutes or SUIVI_BATCH_TIMEOUT_MINUTES
    await _clore_abandons(db, timeout)

    # INSERT et LAST_INSERT_ID sur la même connexion ; pas de rejeu automatique (un INSERT
    # rejoué après une coupure post-commit laisserait une ligne EN_COURS orpheline).
    async with db.transaction() as tx:
        await tx.execute(INSERT_EXECUTION_SQL, (LB_BATCH, commande[:LONGUEUR_MAX]))
        ligne = await tx.fetch_one(DERNIER_ID_SQL)
    id_calcul_batch = int(ligne["id"])

    autre = await db.fetch_one(SELECT_AUTRE_ACTIVE_SQL, (LB_BATCH, id_calcul_batch, timeout))
    if autre:
        await db.execute(SUPPRIMER_RESERVATION_SQL, (id_calcul_batch,))
        refus = Refus(int(autre["id_calcul_batch"]), autre["dt_debut"], autre["commande"])
        logger.warning(
            "Rejet démarrage batch %s",
            ctx(
                lb_batch=LB_BATCH,
                id_calcul_batch_actif=refus.id_calcul_batch,
                dt_debut_actif=str(refus.dt_debut),
                commande_active=refus.commande,
                verdict="REFUSE",
                motif="instance déjà en cours d'exécution",
            ),
        )
        return refus

    logger.info(
        "Début exécution batch %s",
        ctx(
            lb_batch=LB_BATCH,
            id_calcul_batch=id_calcul_batch,
            commande=commande,
            timeout_minutes=timeout,
        ),
    )
    return Execution(id_calcul_batch)


async def terminer(
    execution: Execution, statut: str, commentaire: str | None = None, *, db=db_write
) -> None:
    """Clôt l'exécution. Best-effort : un échec ici ne doit pas masquer le résultat métier.

    Si la clôture échoue, la ligne reste `EN_COURS` et bloque les relances jusqu'au timeout,
    après quoi elle est clôturée comme abandonnée : d'où le niveau ERROR.
    """
    texte = commentaire[:LONGUEUR_MAX] if commentaire else None
    try:
        await db.execute(CLORE_SQL, (statut, texte, execution.id_calcul_batch))
    except Exception:
        logger.exception(
            "Erreur clôture exécution batch %s",
            ctx(lb_batch=LB_BATCH, id_calcul_batch=execution.id_calcul_batch, statut=statut),
        )
        return
    logger.info(
        "Fin exécution batch %s",
        ctx(
            lb_batch=LB_BATCH,
            id_calcul_batch=execution.id_calcul_batch,
            statut=statut,
            commentaire=texte,
        ),
    )


async def _clore_abandons(db, timeout: int) -> None:
    for ligne in await db.fetch_all(SELECT_ABANDONNEES_SQL, (LB_BATCH, timeout)):
        id_calcul_batch = int(ligne["id_calcul_batch"])
        if not await db.execute(CLORE_SQL, (ECHEC, COMMENTAIRE_ABANDON, id_calcul_batch)):
            continue  # déjà clôturée par un autre pod entre-temps
        logger.warning(
            "Exécution batch abandonnée clôturée %s",
            ctx(
                lb_batch=LB_BATCH,
                id_calcul_batch=id_calcul_batch,
                dt_debut=str(ligne["dt_debut"]),
                commande=ligne["commande"],
                timeout_minutes=timeout,
                statut=ECHEC,
            ),
        )
