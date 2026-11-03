"""
Banc de mesure 1 worker / N workers du Gold Spark (S3 p25-29).

Protocole (S3 p26) : même instantané (Silver figé), même code, même configuration Spark ; une seule
chose varie (le nombre de workers `local[N]`) ; R répétitions, médiane rapportée ; tout compté (build
complet du Gold + écriture). Deux versions : « initiale » (ordre naïf) et « optimisée » (leviers O2/O3).

Mesures collectées via l'API de l'historique Spark (S3 p25) : GET {uiWebUrl}/api/v1/applications/{id}/stages
→ durée, shuffle read/write, spill mémoire+disque. Coût vCPU·h = N × durée.

Sorties : results/bench.csv (une ligne par exécution) et results/bench_resume.csv (médianes + speedup/efficiency).
Met aussi à jour le tableau d'expériences (docs/04) à la main à partir du résumé.

Usage : python3 bench/run_bench.py [--workers 1 2 4 8] [--reps 3] [--shuffle 64]
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
import urllib.request
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from spark.session import SILVER, spark_session                     # noqa: E402
import spark.build_gold_spark as g                                  # noqa: E402

RES = ROOT / "results"
SCRATCH = Path("/tmp/meteo_bench")        # sortie jetable (on mesure le calcul + l'écriture)
KEEP = ["ic_id", "libelle", "latitude", "longitude", "altitude", "date", "tn", "tx", "tm", "rr", "flag_partiel"]


def metriques_stages(ui_url: str, app_id: str) -> dict:
    """Somme shuffle/spill sur tous les stages terminés de l'application (API historique Spark)."""
    url = f"{ui_url}/api/v1/applications/{app_id}/stages"
    with urllib.request.urlopen(url, timeout=30) as r:
        stages = json.loads(r.read().decode())
    agg = {"shuffle_read_mo": 0.0, "shuffle_write_mo": 0.0, "spill_mem_mo": 0.0, "spill_disk_mo": 0.0}
    for s in stages:
        if s.get("status") not in ("COMPLETE", "ACTIVE"):
            continue
        agg["shuffle_read_mo"] += s.get("shuffleReadBytes", 0) / 1e6
        agg["shuffle_write_mo"] += s.get("shuffleWriteBytes", 0) / 1e6
        agg["spill_mem_mo"] += s.get("memoryBytesSpilled", 0) / 1e6
        agg["spill_disk_mo"] += s.get("diskBytesSpilled", 0) / 1e6
    return agg


def une_execution(workers: int, optimise: bool, shuffle: int) -> dict:
    spark = spark_session(f"bench-{'opt' if optimise else 'init'}-n{workers}",
                          workers=workers, shuffle_partitions=shuffle)
    try:
        silver = spark.read.parquet(str(SILVER / "observations")).select(*KEEP)
        clim = g.climatologie_train(silver).cache()
        gold = g.build_features(silver, clim, optimise=optimise)
        out = SCRATCH / f"n{workers}_{'opt' if optimise else 'init'}"
        if out.exists():
            shutil.rmtree(out, ignore_errors=True)
        t0 = time.perf_counter()
        gold.write.mode("overwrite").partitionBy("split").parquet(str(out))   # action mesurée
        duree = time.perf_counter() - t0
        met = metriques_stages(spark.sparkContext.uiWebUrl, spark.sparkContext.applicationId)
        shutil.rmtree(out, ignore_errors=True)
        return {"workers": workers, "version": "optimisee" if optimise else "initiale",
                "duree_s": round(duree, 2), **{k: round(v, 1) for k, v in met.items()}}
    finally:
        spark.stop()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8])
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--shuffle", type=int, default=64, help="spark.sql.shuffle.partitions (fixe, équité)")
    args = ap.parse_args()
    RES.mkdir(exist_ok=True)
    SCRATCH.mkdir(parents=True, exist_ok=True)

    import pandas as pd
    runs = []
    plan = [(n, False) for n in args.workers] + [(n, True) for n in (args.workers[:1] + args.workers[-1:])]
    for workers, opt in plan:
        for rep in range(args.reps):
            r = une_execution(workers, opt, args.shuffle)
            r["rep"] = rep
            runs.append(r)
            print(f"  n={workers} {'opt' if opt else 'init'} rep{rep}: "
                  f"{r['duree_s']}s shuffle={r['shuffle_read_mo']}Mo spill={r['spill_mem_mo']}Mo", flush=True)
    df = pd.DataFrame(runs)
    df.to_csv(RES / "bench.csv", index=False)

    # Résumé : médiane par (version, workers) + speedup/efficiency (référence = 1 worker même version)
    res = (df.groupby(["version", "workers"]).agg(
        duree_med_s=("duree_s", "median"),
        shuffle_read_mo=("shuffle_read_mo", "median"),
        spill_mem_mo=("spill_mem_mo", "median")).reset_index())
    res["vcpu_h"] = (res["workers"] * res["duree_med_s"] / 3600).round(4)
    t1 = res.merge(res[res.workers == min(args.workers)][["version", "duree_med_s"]]
                   .rename(columns={"duree_med_s": "t1"}), on="version", how="left")
    res["speedup"] = (t1["t1"] / res["duree_med_s"]).round(2)
    res["efficiency"] = (res["speedup"] / res["workers"]).round(2)
    res = res.sort_values(["version", "workers"])
    res.to_csv(RES / "bench_resume.csv", index=False)
    print("\n=== Résumé ===")
    print(res.to_string(index=False))
    print(f"\n✓ results/bench.csv ({len(df)} exécutions) et results/bench_resume.csv")


if __name__ == "__main__":
    main()
