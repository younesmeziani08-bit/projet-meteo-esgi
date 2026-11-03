"""
Étape Bronze en PySpark (S2 p13, p17, p19).

Lit la zone brute immuable data/raw/ (JSON figés + checksums), et écrit :
  - data/bronze/obs/            table Bronze : une ligne par observation, champs en TEXTE,
                                colonnes techniques, partition par _batch_id ;
  - data/ref/stations.parquet   référentiel des stations (matérialisé pour le join Silver).

Principes Bronze respectés :
  - Fidélité : les valeurs source sont conservées telles quelles (cast en string), même invalides.
  - Provenance : _source, _batch_id, _fichier, _ligne.
  - Checksum et timestamp d'ingestion : _checksum (du batch), _ingere_le.
  - Idempotence : un batch déjà CHARGÉ avec le même checksum est ignoré ; sinon sa partition
    est réécrite en mode overwrite dynamique → rejouer donne exactement le même état.

Aucune règle métier ici (pas de conversion, pas de filtre, pas de déduplication) : c'est le rôle de Silver.

Usage : python3 src/spark/ingest_bronze_spark.py            [--refaire] [--workers N]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
from spark.batch_log import BatchLog          # noqa: E402
from spark.session import BRONZE, REF, spark_session   # noqa: E402

RAW = ROOT / "data" / "raw"
SOURCE = "infoclimat"


def _batch_dir(batch_id: str) -> Path:
    return RAW / f"source={SOURCE}" / f"batch={batch_id}"


def ingerer_observations(spark, batch_id: str, checksum: str) -> int:
    """Lit les pages JSON d'un batch station, explose les mesures, ajoute les colonnes techniques,
    écrit la partition _batch_id=<batch_id>. Renvoie le nombre de lignes écrites."""
    chemin = str(_batch_dir(batch_id) / "page_*.json")
    brut = spark.read.option("multiLine", True).json(chemin)   # un objet JSON par fichier
    # posexplode : _ligne = position de la mesure dans le tableau (provenance fine)
    exploded = brut.select(F.posexplode("mesures").alias("_ligne", "m"),
                           F.input_file_name().alias("_fichier_uri"))
    champs = exploded.select("m.*").columns                    # champs réellement présents
    cols_texte = [F.col(f"m.{c}").cast("string").alias(c) for c in champs]
    df = (exploded.select(*cols_texte, "_ligne", "_fichier_uri")
          .withColumn("_source", F.lit(SOURCE))
          .withColumn("_batch_id", F.lit(batch_id))
          .withColumn("_fichier", F.element_at(F.split("_fichier_uri", "/"), -1))
          .withColumn("_checksum", F.lit(checksum))
          .withColumn("_ingere_le", F.current_timestamp())
          .drop("_fichier_uri"))
    (df.write.mode("overwrite").partitionBy("_batch_id")
       .parquet(str(BRONZE)))                                  # overwrite dynamique → partition seule
    return df.count()


def materialiser_referentiel(spark, batch_id: str) -> int:
    """Référentiel des stations : FeatureCollection -> table plate (ic_id, mfid, libellé, position)."""
    chemin = str(_batch_dir(batch_id) / "stations*.json")
    brut = spark.read.option("multiLine", True).json(chemin)
    props = brut.select(F.explode("features").alias("f")).select("f.properties.*")
    cols = ["ic_id", "mfid", "libelle", "latitude", "longitude", "altitude", "departement"]
    ref = props.select(*[F.col(c) for c in cols if c in props.columns]).dropDuplicates(["ic_id"])
    REF.mkdir(parents=True, exist_ok=True)
    ref.write.mode("overwrite").parquet(str(REF / "stations.parquet"))
    return ref.count()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--refaire", action="store_true", help="réingère même si déjà CHARGÉ")
    ap.add_argument("--workers", type=int, default=None, help="local[N] (démonstration distribuée)")
    args = ap.parse_args()

    log = BatchLog()
    batchs = log.lire()
    if batchs.empty:
        print("Batch log vide : lancer src/promote_to_raw.py d'abord.")
        return

    spark = spark_session("bronze", workers=args.workers)
    total, ignores = 0, 0
    try:
        # Référentiel (une fois)
        ref_rows = batchs[batchs.kind == "ref"]
        if not ref_rows.empty:
            n = materialiser_referentiel(spark, ref_rows.iloc[0]["batch_id"])
            print(f"  référentiel stations : {n} stations -> data/ref/stations.parquet")

        # Observations, batch par batch
        obs = batchs[batchs.kind == "v2_journaliere"].sort_values("batch_id")
        for _, b in obs.iterrows():
            bid, chk = b["batch_id"], b["checksum"]
            deja = log.statut_de(bid) in ("CHARGE", "PUBLIE")
            if deja and not args.refaire and log.deja_charge(bid, chk):
                ignores += 1
                continue
            n = ingerer_observations(spark, bid, chk)
            log.marquer(bid, "CHARGE", nb_lignes=n)            # statut seulement après écriture réussie
            total += n
            print(f"  {b['ic_id']} : {n} lignes -> bronze/obs/_batch_id={bid}")
    finally:
        spark.stop()

    print(f"\n✓ Bronze : {total} lignes écrites, {ignores} batch(s) ignoré(s) (idempotence).")
    print(f"  Table : {BRONZE.relative_to(ROOT)}  ·  partition par _batch_id")


if __name__ == "__main__":
    main()
