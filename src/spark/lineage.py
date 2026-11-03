"""
Lineage OpenLineage (S3 p16-20), transport « fichier ».

Émet des RunEvents OpenLineage (START/COMPLETE) décrivant le graphe Bronze → Silver → Gold :
chaque job a un namespace+name, un run (runId UUID), des inputs/outputs (datasets) et des facets
(schema, columnLineage simplifié). Les events sont écrits en JSON dans data/_meta/lineage/.

En production, les mêmes events seraient produits automatiquement par le listener Spark OpenLineage
(spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener) et envoyés en HTTP à
Marquez/DataHub (S3 p19). Ici, transport fichier → aucun service requis, lineage inspectable et testable.

Usage : python3 src/spark/lineage.py            (construit le graphe à partir des tables présentes)
"""
from __future__ import annotations

import datetime as dt
import json
import uuid
from pathlib import Path

import pyarrow.dataset as ds
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
LINEAGE = DATA / "_meta" / "lineage"
NS = "meteo-esgi"                       # namespace des jobs
NS_DATA = f"file://{DATA}"              # namespace des datasets
PRODUCER = "https://github.com/projet-meteo-esgi/src/spark/lineage.py"


def _schema_facet(chemin: Path) -> dict | None:
    """Facet schema OpenLineage à partir d'un parquet (ou dataset partitionné)."""
    try:
        sch = pq.read_schema(chemin) if chemin.is_file() else ds.dataset(chemin).schema
    except Exception:
        return None
    return {"_producer": PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/SchemaDatasetFacet.json",
            "fields": [{"name": f.name, "type": str(f.type)} for f in sch]}


def _dataset(nom: str, chemin: Path, col_lineage_from: list[str] | None = None) -> dict:
    facets = {}
    sf = _schema_facet(chemin)
    if sf:
        facets["schema"] = sf
    if col_lineage_from:
        facets["columnLineage"] = {
            "_producer": PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json",
            "fields": {}, "dataSource": col_lineage_from}
    return {"namespace": NS_DATA, "name": nom, "facets": facets}


def emettre(job: str, inputs: list[dict], outputs: list[dict]) -> str:
    """Écrit un couple d'events START puis COMPLETE pour un run du job. Renvoie le runId."""
    LINEAGE.mkdir(parents=True, exist_ok=True)
    run_id = str(uuid.uuid4())
    base = {"producer": PRODUCER, "job": {"namespace": NS, "name": job}, "run": {"runId": run_id}}
    for etat in ("START", "COMPLETE"):
        ev = {"eventType": etat,
              "eventTime": dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds"),
              **base, "inputs": inputs if etat == "COMPLETE" else [],
              "outputs": outputs if etat == "COMPLETE" else []}
        (LINEAGE / f"{job}_{etat}_{run_id[:8]}.json").write_text(
            json.dumps(ev, ensure_ascii=False, indent=1), encoding="utf-8")
    return run_id


def construire() -> list[dict]:
    """Construit le graphe de lineage des trois jobs à partir des tables réellement présentes."""
    raw = {"namespace": NS_DATA, "name": "raw/source=infoclimat", "facets": {}}
    bronze = _dataset("bronze/obs", DATA / "bronze" / "obs")
    stations = _dataset("ref/stations.parquet", DATA / "ref" / "stations.parquet")
    silver = _dataset("silver/observations", DATA / "silver" / "observations", ["bronze/obs"])
    quarantaine = _dataset("silver/quarantaine", DATA / "silver" / "quarantaine", ["bronze/obs"])
    gold = _dataset("gold/jeu_prevision_spark", DATA / "gold" / "jeu_prevision_spark",
                    ["silver/observations"])

    jobs = [
        ("ingest_bronze", [raw], [bronze, stations]),
        ("build_silver", [bronze, stations], [silver, quarantaine]),
        ("build_gold", [silver], [gold]),
    ]
    graphe = []
    for nom, ins, outs in jobs:
        rid = emettre(nom, ins, outs)
        graphe.append({"job": nom, "runId": rid,
                       "inputs": [i["name"] for i in ins], "outputs": [o["name"] for o in outs]})
    return graphe


def main() -> None:
    graphe = construire()
    (LINEAGE / "_graph.json").write_text(json.dumps(graphe, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"✓ Lineage OpenLineage écrit dans {LINEAGE.relative_to(ROOT)} "
          f"({len(list(LINEAGE.glob('*.json')))} fichiers)")
    for e in graphe:
        print(f"  {' , '.join(e['inputs']):<45} ──[{e['job']}]──▶ {' , '.join(e['outputs'])}")


if __name__ == "__main__":
    main()
