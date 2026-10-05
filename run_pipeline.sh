#!/usr/bin/env bash
# Pipeline complet : Bronze -> Silver -> Gold -> modèles -> graphiques -> contrôles qualité
set -euo pipefail
cd "$(dirname "$0")"
python3 src/ingest_bronze.py
python3 src/build_silver.py
python3 src/build_gold.py
python3 src/check_contracts.py        # arrête le pipeline si un data contract est violé
python3 src/train_models.py           # reprenable : relancer si interrompu
python3 src/plot_results.py
python3 -m pytest
