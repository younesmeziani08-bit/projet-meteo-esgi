"""
Fabrique de SparkSession pour le projet (S2/S3).

`spark_session(app, workers=N)` construit une session locale `local[N]` : N est le nombre de cœurs,
c.-à-d. le nombre de « workers » logiques utilisés pour la démonstration 1 worker / N workers (S3 p26).
Le code applicatif ne dépend jamais de `workers` : seules changent `master` et les partitions, de sorte
que le pipeline est transposable à un vrai cluster sans modification (docs/02_architecture.md §6).
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _assurer_java_compatible() -> None:
    """Spark 4.x exige Java 17 ou 21 ; les JDK plus récents (ex. 26) suppriment des API internes
    (jdk.internal.ref.Cleaner) et font échouer le démarrage. On sélectionne un JDK compatible via
    /usr/libexec/java_home (macOS) et on positionne JAVA_HOME avant tout lancement de la JVM."""
    for v in ("21", "17"):
        try:
            home = subprocess.check_output(["/usr/libexec/java_home", "-v", v],
                                           stderr=subprocess.DEVNULL, text=True).strip()
        except Exception:
            continue
        # java_home renvoie le plus haut JDK si la version demandée est absente : on vérifie le vrai numéro
        if home and (f"-{v}." in home or home.rstrip("/").endswith(f"-{v}")):
            os.environ["JAVA_HOME"] = home
            return
    # dernier recours : laisser JAVA_HOME tel quel (peut échouer si JDK trop récent)


_assurer_java_compatible()
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession   # noqa: E402  (après JAVA_HOME)

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
WAREHOUSE = DATA                      # nos tables Parquet vivent sous data/
BRONZE = DATA / "bronze" / "obs"      # table Bronze Spark (≠ data/bronze/*.json, source brute)
REF = DATA / "ref"                    # référentiels matérialisés (stations)
SILVER = DATA / "silver"
GOLD = DATA / "gold"


def spark_session(app: str, workers: int | None = None, shuffle_partitions: int | None = None,
                  aqe: bool = True, extra: dict | None = None) -> SparkSession:
    master = f"local[{workers}]" if workers else "local[*]"
    b = (SparkSession.builder.appName(app).master(master)
         # Arrow : échange colonnaire zero-copy Spark <-> pandas (S2 p9)
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
         # Idempotence : réécrire une partition ne touche qu'elle (S2 p17)
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         # AQE : coalesce automatique des partitions et gestion du skew des joins (S3 p12)
         .config("spark.sql.adaptive.enabled", "true" if aqe else "false")
         .config("spark.sql.session.timeZone", "UTC"))
    if shuffle_partitions:
        b = b.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    for k, v in (extra or {}).items():
        b = b.config(k, str(v))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
