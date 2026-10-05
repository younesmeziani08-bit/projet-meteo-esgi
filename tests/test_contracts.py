"""Exécute les règles des data contracts ODCS sur les tables réelles."""
from pathlib import Path

import duckdb
import pandas as pd
import pytest

import check_contracts as cc
from conftest import GOLD_PQ, SILVER_PQ, besoin

CONTRATS = sorted(cc.CONTRACTS.glob("*.odcs.yaml"))


@pytest.mark.donnees
@besoin(SILVER_PQ, GOLD_PQ)
@pytest.mark.parametrize("contrat", CONTRATS, ids=lambda p: p.name)
def test_contrat_respecte(contrat):
    res = cc.verifier_contrat(contrat)
    echecs = [f"{r['controle']} -> {r['detail']}" for r in res if r["statut"] in ("ECHEC", "ABSENT")]
    assert not echecs, "\n".join(echecs)


def test_le_verificateur_detecte_une_table_corrompue(monkeypatch):
    """Le vérificateur doit échouer si on lui donne une table Silver qui viole le contrat."""
    dates = pd.date_range("2021-01-01", periods=5, freq="D")
    df = pd.DataFrame({"ic_id": "A", "date": dates, "tn": [1, 2, 9, 1, 1.0], "tx": [5, 6, 3, 5, 5.0],
                       "valide": [True, True, True, False, True]})
    df = pd.concat([df, df.iloc[[0]]])                    # doublon de clé
    df = df[df.date != dates[1]]                          # trou de calendrier
    monkeypatch.setitem(cc.TABLES, "meteo_journaliere", lambda: df)
    res = cc.verifier_contrat(cc.CONTRACTS / "silver_meteo_journaliere.odcs.yaml", duckdb.connect())
    echecs = {r["controle"] for r in res if r["statut"] == "ECHEC"}
    for attendu in ["Clé unique", "Calendrier complet par station", "Valeurs nettoyées cohérentes",
                    "Pas de valeur nettoyée sur une ligne invalide", "schéma : colonnes déclarées présentes"]:
        assert attendu in echecs, f"non détecté : {attendu}"
