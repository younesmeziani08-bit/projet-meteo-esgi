"""
Test d'équivalence pandas (étalon) vs PySpark (S3 p23 : « version optimisée = sortie identique »).

Vérifie que les tables Silver et Gold produites par le pipeline Spark sont identiques, colonne par
colonne et à la tolérance flottante près, à celles du pipeline pandas de référence. Ignoré tant que
l'une des deux sorties n'a pas été produite (comme les autres tests dépendant des données).
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from spark.equivalence import TABLES, comparer   # noqa: E402

from conftest import besoin   # noqa: E402


def _dispo(t) -> bool:
    return t["pandas"].exists() and t["spark"].exists()


@pytest.mark.parametrize("t", TABLES, ids=[t["nom"] for t in TABLES])
def test_equivalence_spark_vs_pandas(t):
    if not _dispo(t):
        pytest.skip(f"sortie manquante pour {t['nom']} (lancer les pipelines pandas et Spark)")
    ok, rap = comparer(t["nom"], t["pandas"], t["spark"], t["cles"],
                       t["renommage"], t["ajouts_attendus"])
    a = rap.attrs
    en_diff = rap[rap["diffs"] > 0]
    assert a["non_apparies"] == 0, f"{a['non_apparies']} ligne(s) non appariée(s)"
    assert not a["seule_pa"], f"colonnes manquantes côté Spark : {a['seule_pa']}"
    assert not a["seule_sp"], f"colonnes inattendues côté Spark : {a['seule_sp']}"
    assert en_diff.empty, f"colonnes en écart :\n{en_diff.to_string(index=False)}"
    assert ok
