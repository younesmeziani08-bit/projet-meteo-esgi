#!/usr/bin/env bash
# Pipeline « lakehouse » PySpark (architecture Big Data du cours, séances S1-S3).
# Reproductible : lit la zone brute immuable data/raw/, jamais l'API.
# Prérequis : les réponses API déjà téléchargées dans data/bronze/ (sinon : python3 src/ingest_bronze.py).
set -euo pipefail
cd "$(dirname "$0")"

python3 src/promote_to_raw.py              # zone brute immuable + checksums + batch log
python3 src/spark/ingest_bronze_spark.py   # Bronze Spark (colonnes techniques, idempotent)
python3 src/spark/build_silver_spark.py    # Silver Spark (qc_flag, dedup, quarantaine, réconciliation)
python3 src/spark/build_gold_spark.py      # Gold Spark (features, cibles, split)
python3 src/spark/equivalence.py           # Gold/Silver Spark == référence pandas (tolérance 1e-6)
python3 src/spark/lineage.py               # events OpenLineage Bronze->Silver->Gold (data/_meta/lineage)
python3 src/spark/exploitation.py          # invariants : idempotence, backfill, recovery
# Mesure 1/N workers (optionnel, ~15 min) :
# python3 bench/run_bench.py --workers 1 2 4 8 --reps 3 && python3 bench/plot_bench.py
