"""
Étape Silver en PySpark (S2 p22, p26-30).

Entrée : data/bronze/obs/ (texte) + data/ref/stations.parquet
Sorties :
  - data/silver/observations/  (annee=/mois=)  table publiée : une ligne par station × jour,
        calendrier complet, valeurs nettoyées, qc_flag, flags détaillés, version, provenance ;
  - data/silver/quarantaine/   (annee=/mois=)  lignes à valeur présente mais règle violée (Q1/Q2/Q4/Q6) ;
  - data/silver/rapport_qualite.csv            synthèse des flags par station ;
  - data/silver/reconciliation.csv             Bronze = Silver + doublons + quarantaine + illisibles, par batch.

La logique qualité reproduit src/build_silver.py (implémentation pandas de référence) : bornes physiques,
tn>tx, cohérence spatiale (climatologie lissée ±15 j circulaire + score robuste MAD, seuil 10), pluie,
journée partielle. Le grain reste « large » (station × jour), valide pour le Silver (S1 p30) et attendu
par la couche Gold. Choix qualité : « conserver avec indicateur » dans la table publiée (l'aval filtre via
qc_flag/valide) ET copier les lignes invalides en quarantaine pour revue et replay (S1 p18).

Usage : python3 src/spark/build_silver_spark.py [--workers N]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
from spark.batch_log import BatchLog                     # noqa: E402
from spark.session import BRONZE, REF, SILVER, spark_session   # noqa: E402

# Mêmes constantes que src/build_silver.py (référence pandas)
BORNES = {"tn": (-35.0, 35.0), "tx": (-25.0, 47.0)}
BORNES_RR = (0.0, 1000.0)
SEUIL_SPATIAL = 10.0
FENETRE_CLIM_J = 15
MIN_OBS_RATIO = 0.5


def charger_bronze(spark):
    """Lit la table Bronze (texte) et type les colonnes. Dédup déterministe par (ic_id, date)."""
    b = spark.read.parquet(str(BRONZE))
    df = b.select(
        F.col("ic_id").cast("string"),
        F.col("mfid").cast("string"),
        F.to_date("date").alias("date"),
        F.col("tn").cast("double"), F.col("tx").cast("double"),
        F.col("rr").cast("double"),
        F.col("n_obs").cast("int"), F.col("n_obs_rejetees").cast("int"),
        F.col("statut_extremes").cast("string"),
        F.lit(1).alias("version"),          # incrémentée lors d'une correction (backfill)
        F.col("_batch_id"), F.col("_source"),
    )
    # Déduplication déterministe (S2 p26) : un gagnant par clé, ordre total reproductible.
    w = Window.partitionBy("ic_id", "date").orderBy(
        F.col("version").desc(), F.col("_batch_id").desc())
    deduped = (df.withColumn("_rang", F.row_number().over(w)).filter("_rang = 1").drop("_rang"))
    return deduped


def completer_calendrier(df):
    """Calendrier journalier complet par station (jours absents matérialisés en lignes vides)."""
    bornes = df.groupBy("ic_id").agg(F.min("date").alias("d0"), F.max("date").alias("d1"))
    cal = bornes.select("ic_id", F.explode(
        F.sequence("d0", "d1", F.expr("interval 1 day"))).alias("date"))
    mfids = df.filter("mfid is not null").groupBy("ic_id").agg(F.first("mfid").alias("mfid"))
    full = (cal.join(df.drop("mfid"), ["ic_id", "date"], "left")
               .join(mfids, "ic_id", "left"))
    return full


def _doy(c):
    return F.least(F.dayofyear(c), F.lit(365))


def climatologie_lissee(df, col_ok: str, nom: str):
    """Climatologie par (ic_id, doy) lissée ±15 j (circulaire), sur les valeurs valides uniquement.
    Reproduit le rolling-mean circulaire de pandas : moyenne égale des moyennes journalières voisines."""
    base = (df.select("ic_id", _doy("date").alias("doy"), F.col(col_ok).alias("v"))
              .groupBy("ic_id", "doy").agg(F.avg("v").alias("m")))      # moyenne par jour de l'année
    offs = df.sparkSession.range(-FENETRE_CLIM_J, FENETRE_CLIM_J + 1).withColumnRenamed("id", "off")
    voisin = (base.crossJoin(offs)
                  .withColumn("doy_cible", F.pmod(F.col("doy") - 1 + F.col("off"), F.lit(365)) + 1)
                  .groupBy("ic_id", "doy_cible").agg(F.avg("m").alias(nom))
                  .withColumnRenamed("doy_cible", "doy"))
    return voisin                                                       # (ic_id, doy, <nom>)


def appliquer_regles(df):
    """Flags qualité + valeurs nettoyées (mêmes règles que la référence pandas)."""
    df = df.withColumn("doy", _doy("date"))
    # 1-3 : règles locales
    df = (df.withColumn("flag_absent", F.col("tn").isNull() | F.col("tx").isNull())
            .withColumn("flag_hors_bornes",
                        (F.col("tn") < BORNES["tn"][0]) | (F.col("tn") > BORNES["tn"][1]) |
                        (F.col("tx") < BORNES["tx"][0]) | (F.col("tx") > BORNES["tx"][1]))
            .withColumn("flag_incoherent", F.col("tn") > F.col("tx")))
    df = df.fillna({"flag_hors_bornes": False, "flag_incoherent": False})
    base_ok = ~(F.col("flag_absent") | F.col("flag_hors_bornes") | F.col("flag_incoherent"))
    df = df.withColumn("_base_ok", base_ok)
    for v in ("tn", "tx"):
        df = df.withColumn(f"{v}_ok", F.when(F.col("_base_ok"), F.col(v)))

    # 4 : cohérence spatiale (climatologie + anomalie + score robuste)
    df = df.withColumn("flag_spatial", F.lit(False))
    for v in ("tn", "tx"):
        clim = climatologie_lissee(df, f"{v}_ok", f"clim_{v}")
        df = df.join(F.broadcast(clim), ["ic_id", "doy"], "left")
        # comme la référence pandas (climatologie calculée sur df.where(base_ok)) : clim nul hors base_ok
        df = df.withColumn(f"clim_{v}", F.when(F.col("_base_ok"), F.col(f"clim_{v}")))
        df = df.withColumn(f"{v}_anom", F.col(f"{v}_ok") - F.col(f"clim_{v}"))
        med_jour = df.groupBy("date").agg(
            F.expr(f"percentile({v}_anom, 0.5)").alias(f"{v}_medj"))
        df = df.join(F.broadcast(med_jour), "date", "left")
        df = df.withColumn(f"{v}_resid", F.col(f"{v}_anom") - F.col(f"{v}_medj"))
        mad = df.groupBy("ic_id").agg(
            (F.expr(f"percentile(abs({v}_resid), 0.5)") * F.lit(1.4826)).alias(f"{v}_mad"))
        df = df.join(F.broadcast(mad), "ic_id", "left")
        df = df.withColumn(f"{v}_score_spatial",
                           F.col(f"{v}_resid") / F.when(F.col(f"{v}_mad") == 0, None).otherwise(F.col(f"{v}_mad")))
        df = df.withColumn("flag_spatial",
                           F.col("flag_spatial") | (F.abs(F.col(f"{v}_score_spatial")) > SEUIL_SPATIAL))
    df = df.fillna({"flag_spatial": False})

    # 5 : journée partielle (informatif)
    med_obs = df.groupBy("ic_id").agg(F.expr("percentile(n_obs, 0.5)").alias("_med_obs"))
    df = df.join(F.broadcast(med_obs), "ic_id", "left")
    df = df.withColumn("flag_partiel",
                       (F.coalesce(F.col("statut_extremes"), F.lit("")) != "quotidien") |
                       (F.col("n_obs") < F.lit(MIN_OBS_RATIO) * F.col("_med_obs")))
    df = df.fillna({"flag_partiel": False})

    # Valeurs nettoyées
    invalide = (F.col("flag_absent") | F.col("flag_hors_bornes") |
                F.col("flag_incoherent") | F.col("flag_spatial"))
    df = df.withColumn("valide", ~invalide)
    for v in ("tn", "tx"):
        df = df.withColumn(f"{v}_brut", F.col(v)).withColumn(v, F.when(~invalide, F.col(v)))
    df = (df.withColumn("rr_brut", F.col("rr"))
            .withColumn("flag_rr_invalide",
                        F.col("rr").isNotNull() & ((F.col("rr") < BORNES_RR[0]) | (F.col("rr") > BORNES_RR[1])))
            .withColumn("rr", F.when(~F.col("flag_rr_invalide"), F.col("rr")))
            .withColumn("tm", (F.col("tn") + F.col("tx")) / 2))
    df = df.fillna({"flag_rr_invalide": False})

    # qc_flag (vocabulaire du cours) : INVALIDE (règle bloquante) / SUSPECT (marqué) / OK
    df = df.withColumn("qc_flag",
                       F.when(invalide, F.lit("INVALIDE"))
                        .when(F.col("flag_partiel") | F.col("flag_rr_invalide"), F.lit("SUSPECT"))
                        .otherwise(F.lit("OK")))
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=None)
    args = ap.parse_args()
    spark = spark_session("silver", workers=args.workers)
    log = BatchLog()
    try:
        bronze = charger_bronze(spark)
        ref = spark.read.parquet(str(REF / "stations.parquet"))
        df = completer_calendrier(bronze)
        df = appliquer_regles(df)
        df = df.join(F.broadcast(ref.select("ic_id", "libelle", "latitude", "longitude",
                                            "altitude", "departement")), "ic_id", "left")

        cols = ["ic_id", "mfid", "libelle", "latitude", "longitude", "altitude", "departement",
                "date", "tn", "tx", "tm", "rr", "tn_brut", "tx_brut", "rr_brut",
                "clim_tn", "clim_tx", "tn_score_spatial", "tx_score_spatial",
                "n_obs", "n_obs_rejetees", "statut_extremes", "version", "_source", "_batch_id",
                "flag_absent", "flag_hors_bornes", "flag_incoherent", "flag_spatial",
                "flag_rr_invalide", "flag_partiel", "valide", "qc_flag"]
        silver = (df.select(*cols)
                    .withColumn("annee", F.year("date")).withColumn("mois", F.month("date")))

        (silver.repartition("annee", "mois").sortWithinPartitions("ic_id", "date")
               .write.mode("overwrite").partitionBy("annee", "mois")
               .parquet(str(SILVER / "observations")))

        # Quarantaine : valeur présente mais règle Q1/Q2/Q4 violée (lignes à revoir)
        quarantaine = (df.filter(
            (F.col("flag_hors_bornes") | F.col("flag_incoherent") | F.col("flag_spatial")) &
            (F.col("tn_brut").isNotNull() | F.col("tx_brut").isNotNull()))
            .withColumn("regle", F.concat_ws(",",
                F.when(F.col("flag_hors_bornes"), F.lit("Q1_hors_bornes")),
                F.when(F.col("flag_incoherent"), F.lit("Q2_tn_gt_tx")),
                F.when(F.col("flag_spatial"), F.lit("Q4_spatial"))))
            .select("ic_id", "date", "tn_brut", "tx_brut", "rr_brut",
                    "tn_score_spatial", "tx_score_spatial", "regle", "_batch_id",
                    F.year("date").alias("annee"), F.month("date").alias("mois")))
        (quarantaine.write.mode("overwrite").partitionBy("annee", "mois")
                    .parquet(str(SILVER / "quarantaine")))

        # Rapport qualité par station
        rapport = (silver.groupBy("ic_id", "libelle").agg(
            F.count("*").alias("jours"), F.min("date").alias("debut"), F.max("date").alias("fin"),
            F.sum(F.col("flag_absent").cast("int")).alias("absent"),
            F.sum(F.col("flag_hors_bornes").cast("int")).alias("hors_bornes"),
            F.sum(F.col("flag_incoherent").cast("int")).alias("incoherent"),
            F.sum(F.col("flag_spatial").cast("int")).alias("spatial"),
            F.sum(F.col("flag_rr_invalide").cast("int")).alias("rr_invalide"),
            F.sum(F.col("flag_partiel").cast("int")).alias("partiel"),
            F.avg(F.col("valide").cast("int")).alias("taux_valide")).orderBy("ic_id"))
        SILVER.mkdir(parents=True, exist_ok=True)
        rapport.toPandas().to_csv(SILVER / "rapport_qualite.csv", index=False)

        # Réconciliation par batch : Bronze = Silver publié (jours observés) + quarantaine + illisibles(0)
        bronze_cnt = (spark.read.parquet(str(BRONZE)).groupBy("_batch_id").count()
                      .withColumnRenamed("count", "bronze"))
        silver_cnt = (silver.filter("not flag_absent").groupBy("_batch_id").count()
                      .withColumnRenamed("count", "silver_publie"))
        quar_cnt = quarantaine.groupBy("_batch_id").count().withColumnRenamed("count", "quarantaine")
        recon = (bronze_cnt.join(silver_cnt, "_batch_id", "left")
                           .join(quar_cnt, "_batch_id", "left").fillna(0)
                           .withColumn("ecart", F.col("bronze") - F.col("silver_publie"))
                           .orderBy("_batch_id"))
        recon.toPandas().to_csv(SILVER / "reconciliation.csv", index=False)

        n = silver.count()
        nq = quarantaine.count()
        print(f"✓ Silver : {n} lignes (station × jour) -> data/silver/observations")
        print(f"  Quarantaine : {nq} ligne(s) -> data/silver/quarantaine")
        print("  qc_flag :")
        for r in silver.groupBy("qc_flag").count().orderBy("qc_flag").collect():
            print(f"    {r['qc_flag']:<9} {r['count']}")
        # Marquer les batchs d'observations PUBLIE
        for bid in [r["_batch_id"] for r in silver.select("_batch_id").distinct().collect() if r["_batch_id"]]:
            log.marquer(bid, "PUBLIE")
        print("  Batchs d'observations marqués PUBLIE dans le log.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
