from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SILVER_PQ = ROOT / "data" / "silver" / "meteo_journaliere.parquet"
GOLD_PQ = ROOT / "data" / "gold" / "jeu_prevision.parquet"


def besoin(*fichiers: Path):
    """Ignore le test si les données du pipeline n'ont pas encore été produites."""
    manquants = [str(f.relative_to(ROOT)) for f in fichiers if not f.exists()]
    return pytest.mark.skipif(bool(manquants), reason=f"données absentes : {manquants}")
