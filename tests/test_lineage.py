"""
Valide le lineage OpenLineage émis (S3 p16-20) : events bien formés et graphe Bronze→Silver→Gold connecté.
Ignoré tant que la table Gold Spark n'a pas été produite.
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
pytest.importorskip("pyarrow")

GOLD_SPARK = ROOT / "data" / "gold" / "jeu_prevision_spark"
pytestmark = pytest.mark.skipif(not GOLD_SPARK.exists(),
                                reason="table Gold Spark absente : lancer le pipeline lakehouse")

from spark import lineage as lin   # noqa: E402

CHAMPS_EVENT = {"eventType", "eventTime", "run", "job", "inputs", "outputs", "producer"}


@pytest.fixture(scope="module")
def graphe():
    return lin.construire()


def test_events_bien_formes():
    for f in lin.LINEAGE.glob("*_COMPLETE_*.json"):
        import json
        ev = json.loads(f.read_text())
        assert CHAMPS_EVENT <= set(ev), f"champs manquants dans {f.name}"
        assert ev["eventType"] in ("START", "COMPLETE", "FAIL", "ABORT")
        assert ev["run"]["runId"] and ev["job"]["namespace"] and ev["job"]["name"]


def test_graphe_connecte(graphe):
    jobs = {e["job"]: e for e in graphe}
    assert "bronze/obs" in jobs["ingest_bronze"]["outputs"]
    assert "bronze/obs" in jobs["build_silver"]["inputs"]
    assert "silver/observations" in jobs["build_silver"]["outputs"]
    assert "silver/observations" in jobs["build_gold"]["inputs"]
    assert "gold/jeu_prevision_spark" in jobs["build_gold"]["outputs"]
