"""DSR-729 — suivi des exécutions (`trppu_suivi_batch`) et unicité d'instance YB05."""

import asyncio
import logging

import pytest

from app import main as cli
from app import suivi_batch
from app.traitements.rapport import ECHEC, SUCCES, Bilan, Rapport
from tests.conftest import FausseBase

ABANDONNEES = "dt_debut < NOW() - INTERVAL %s MINUTE"
AUTRE_ACTIVE = "id_calcul_batch <> %s"


def _base(*, abandonnees=(), autre=None, nouvel_id=42, rowcounts=None):
    return FausseBase(
        {
            ABANDONNEES: list(abandonnees),
            "SELECT LAST_INSERT_ID() AS id": {"id": nouvel_id},
            AUTRE_ACTIVE: [autre] if autre else [],
        },
        rowcounts=rowcounts,
    )


def _demarrer(base, commande="python -m app.main all", timeout=60):
    return asyncio.run(suivi_batch.demarrer(commande, db=base, timeout_minutes=timeout))


# ---------------------------------------------------------------------------
# Démarrage
# ---------------------------------------------------------------------------


def test_cas_1_aucune_instance_active_demarre(caplog):
    """CA-01 : une ligne EN_COURS est créée et le traitement démarre."""
    base = _base()
    with caplog.at_level(logging.INFO, logger="app.suivi_batch"):
        resultat = _demarrer(base)

    assert resultat == suivi_batch.Execution(42)
    assert base.parametres_de("INSERT INTO trppu_suivi_batch") == (
        "YB05",
        "python -m app.main all",
    )
    assert "VALUES (%s, NOW(), 'EN_COURS', %s)" in base.ecritures()[0]
    assert base.transactions_commitees == 1
    assert any(r.getMessage().startswith("Début exécution batch") for r in caplog.records)


def test_cas_2_instance_active_refuse_le_demarrage(caplog):
    """CA-02 : arrêt immédiat, la réservation est retirée, la cause est tracée."""
    base = _base(
        autre={
            "id_calcul_batch": 7,
            "dt_debut": "2026-10-16 08:00:00",
            "commande": "python -m app.main all",
        }
    )
    with caplog.at_level(logging.WARNING, logger="app.suivi_batch"):
        resultat = _demarrer(base)

    assert isinstance(resultat, suivi_batch.Refus)
    assert resultat.id_calcul_batch == 7
    assert "Une instance du batch YB05 est déjà en cours d'exécution" in resultat.message
    assert "Le démarrage est refusé." in resultat.message
    assert base.parametres_de("DELETE FROM trppu_suivi_batch") == (42,)
    (rejet,) = [r.getMessage() for r in caplog.records]
    assert rejet.startswith("Rejet démarrage batch")
    assert "id_calcul_batch_actif=7" in rejet and "verdict=REFUSE" in rejet


def test_la_reservation_est_ecrite_avant_la_recherche_d_une_autre_instance():
    """Deux pods simultanés : chacun commit sa ligne avant de chercher, le second voit le premier."""
    base = _base()
    _demarrer(base)

    journal = [sql for _, sql, _ in base.journal]
    insertion = next(i for i, sql in enumerate(journal) if sql.startswith("INSERT INTO"))
    recherche = next(i for i, sql in enumerate(journal) if AUTRE_ACTIVE in sql)
    assert insertion < recherche
    # La recherche exclut sa propre ligne et ignore les EN_COURS au-delà du timeout.
    params = next(p for _, sql, p in base.journal if AUTRE_ACTIVE in sql)
    assert params == ("YB05", 42, 60)


def test_cas_3_execution_abandonnee_cloturee_puis_demarrage(caplog):
    """CA-03 : EN_COURS au-delà du timeout -> ECHEC + commentaire du ticket, puis démarrage."""
    base = _base(
        abandonnees=[
            {"id_calcul_batch": 5, "dt_debut": "2026-10-16 06:00:00", "commande": "all"}
        ]
    )
    with caplog.at_level(logging.WARNING, logger="app.suivi_batch"):
        resultat = _demarrer(base, timeout=60)

    assert resultat == suivi_batch.Execution(42)
    assert base.parametres_de("UPDATE trppu_suivi_batch") == (
        "ECHEC",
        "Exécution considérée abandonnée suite dépassement du timeout d'exécution.",
        5,
    )
    params_recherche = next(p for _, sql, p in base.journal if ABANDONNEES in sql)
    assert params_recherche == ("YB05", 60)
    (abandon,) = [r.getMessage() for r in caplog.records]
    assert abandon.startswith("Exécution batch abandonnée clôturée")
    assert "id_calcul_batch=5" in abandon


def test_abandon_deja_cloture_par_un_autre_pod_n_est_pas_trace(caplog):
    base = _base(
        abandonnees=[{"id_calcul_batch": 5, "dt_debut": "x", "commande": "all"}],
        rowcounts={"UPDATE trppu_suivi_batch": 0},
    )
    with caplog.at_level(logging.WARNING, logger="app.suivi_batch"):
        _demarrer(base)

    assert not [r for r in caplog.records if "abandonnée" in r.getMessage()]


def test_la_cloture_ne_touche_qu_une_ligne_encore_en_cours():
    assert "AND statut = 'EN_COURS'" in suivi_batch.CLORE_SQL


def test_timeout_par_defaut_lu_dans_la_config(monkeypatch):
    monkeypatch.setattr(suivi_batch, "SUIVI_BATCH_TIMEOUT_MINUTES", 90)
    base = _base()
    asyncio.run(suivi_batch.demarrer("all", db=base))

    params = next(p for _, sql, p in base.journal if AUTRE_ACTIVE in sql)
    assert params[-1] == 90


def test_commande_tronquee_a_la_taille_de_la_colonne():
    base = _base()
    _demarrer(base, commande="x" * 400)

    assert len(base.parametres_de("INSERT INTO trppu_suivi_batch")[1]) == 255


# ---------------------------------------------------------------------------
# Fin d'exécution
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("statut", ["SUCCES", "ECHEC", "PARTIEL"])
def test_terminer_renseigne_statut_fin_et_duree(statut):
    base = FausseBase({})
    asyncio.run(suivi_batch.terminer(suivi_batch.Execution(42), statut, "c" * 300, db=base))

    sql = base.ecritures()[0]
    assert "dt_fin = NOW()" in sql
    assert "duree = SEC_TO_TIME(TIMESTAMPDIFF(SECOND, dt_debut, NOW()))" in sql
    assert base.parametres_de("UPDATE trppu_suivi_batch") == (statut, "c" * 255, 42)


def test_terminer_ne_leve_pas_si_la_base_echoue(caplog):
    """Le résultat métier prime ; la ligne restée EN_COURS sera clôturée au timeout."""

    class BaseEnPanne(FausseBase):
        async def execute(self, query, params=None, retries=None):
            raise RuntimeError("MySQL injoignable")

    with caplog.at_level(logging.ERROR, logger="app.suivi_batch"):
        asyncio.run(
            suivi_batch.terminer(suivi_batch.Execution(42), "SUCCES", db=BaseEnPanne({}))
        )

    assert any(
        r.getMessage().startswith("Erreur clôture exécution batch") for r in caplog.records
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _bilan(succes=(), echecs=()):
    bilan = Bilan(nb_workers=1)
    for id_scenario in succes:
        bilan.ajouter(id_scenario, SUCCES)
    for id_scenario in echecs:
        bilan.ajouter(id_scenario, ECHEC, "en échec")
    return bilan


def _mode_all(monkeypatch, bilan=None, erreur=None):
    appels = []

    async def executer_tout(id_scenario):
        appels.append(id_scenario)
        if erreur:
            raise erreur
        return bilan

    monkeypatch.setattr(cli, "executer_tout", executer_tout)
    return appels


def test_cli_refus_rend_2_sans_lancer_le_traitement(monkeypatch, capsys, suivi):
    suivi.refus = suivi_batch.Refus(7, "2026-10-16 08:00:00", "python -m app.main all")
    appels = _mode_all(monkeypatch, _bilan(succes=[1]))

    code = cli.main(["all"])

    assert code == cli.EXIT_DEJA_EN_COURS == 2
    assert appels == []
    assert suivi.fins == []
    assert "Le démarrage est refusé." in capsys.readouterr().out


def test_cli_enregistre_la_commande_lancee(monkeypatch, suivi):
    _mode_all(monkeypatch, _bilan(succes=[1]))

    cli.main(["--traitement=ALL"])

    assert suivi.demarrages == ["python -m app.main ALL"]


@pytest.mark.parametrize(
    "succes, echecs, statut, code",
    [
        ([1, 2], [], "SUCCES", 0),
        ([1], [2], "PARTIEL", 1),
        ([], [1, 2], "ECHEC", 1),
        ([], [], "SUCCES", 0),
    ],
)
def test_cli_mode_all_statut_de_fin(monkeypatch, suivi, succes, echecs, statut, code):
    _mode_all(monkeypatch, _bilan(succes, echecs))

    assert cli.main(["all"]) == code
    ((id_calcul_batch, statut_fin, commentaire),) = suivi.fins
    assert (id_calcul_batch, statut_fin) == (42, statut)
    assert f"{len(succes)} succès, {len(echecs)} échec(s)" in commentaire


def test_cli_mode_all_interrompu_est_en_echec(monkeypatch, suivi):
    _mode_all(monkeypatch, erreur=RuntimeError("MySQL injoignable"))

    assert cli.main(["all"]) == cli.EXIT_KO
    ((_, statut, commentaire),) = suivi.fins
    assert statut == "ECHEC"
    assert "MySQL injoignable" in commentaire


def test_cli_traitement_mono_scenario_en_echec_donne_sa_cause(monkeypatch, suivi):
    async def calcul(id_scenario):
        rapport = Rapport(titre="Calcul", id_scenario=id_scenario)
        rapport.statut = ECHEC
        rapport.erreur = "Coefficients de rétention manquants"
        return rapport

    monkeypatch.setattr(cli, "calcul_trafic_pdi", calcul)

    assert cli.main(["calcul-trafic-pdi", "24"]) == cli.EXIT_KO
    assert suivi.fins == [(42, "ECHEC", "Coefficients de rétention manquants")]


def test_cli_interruption_cloture_la_ligne_et_propage(monkeypatch, suivi):
    """Ctrl-C ou annulation : la ligne passe en ECHEC au lieu de rester EN_COURS."""
    _mode_all(monkeypatch, erreur=KeyboardInterrupt())

    with pytest.raises(KeyboardInterrupt):
        cli.main(["all"])
    ((_, statut, commentaire),) = suivi.fins
    assert statut == "ECHEC" and commentaire.startswith("Exécution interrompue")


def test_cli_suivi_indisponible_rend_1_sans_traiter(monkeypatch, capsys, suivi):
    suivi.erreur = RuntimeError("table absente")
    appels = _mode_all(monkeypatch, _bilan(succes=[1]))

    assert cli.main(["all"]) == cli.EXIT_KO
    assert appels == []
    assert "suivi d'exécution indisponible" in capsys.readouterr().out


def test_eligibilite_n_est_pas_suivie(monkeypatch, suivi):
    """Lecture seule : libre même pendant un calcul en cours."""

    async def eligibilite(id_scenario):
        rapport = Rapport(titre="Éligibilité", id_scenario=id_scenario)
        rapport.statut = SUCCES
        return rapport

    monkeypatch.setattr(cli, "controle_eligibilite", eligibilite)

    cli.main(["eligibilite", "24"])

    assert suivi.demarrages == []
    assert cli.COMMANDES_SUIVIES == {
        "cmd_calcul_trafic_pdi",
        "cmd_calcul_trafic_agrebal",
        "cmd_all",
    }
