"""
Étape 3 — Couche Gold (projet météo ESGI, prévision à court terme).

Entrée : data/silver/meteo_journaliere.(parquet|csv)
Sortie : data/gold/jeu_prevision.parquet       une ligne par station × date d'origine J
         data/gold/climatologie_train.parquet  référence saisonnière (apprise sur train uniquement)
         data/gold/dictionnaire_variables.csv

Convention : à la date d'origine J, on connaît toutes les observations jusqu'à J inclus.
On prévoit tx et tn des jours J+1 … J+7 (colonnes cible_tx_h1 … cible_tn_h7).

Découpage chronologique (par date de la CIBLE, pas seulement de l'origine) :
  train : origines ≤ 2021-12-31        valid : 2022-01-01 … 2023-12-31        test : ≥ 2024-01-01
  Les 7 derniers jours avant chaque frontière sont marqués "purge" : leurs cibles J+h tomberaient
  dans la période suivante (fuite). Ils sont exclus de l'apprentissage et de l'évaluation.

Anti-fuite :
  - toutes les variables d'entrée n'utilisent que des données ≤ J (décalages vers le passé uniquement) ;
  - la climatologie est calculée sur la période train uniquement.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SILVER = ROOT / "data" / "silver"
GOLD = ROOT / "data" / "gold"

HORIZONS = range(1, 8)
VARS = ["tn", "tx"]
LAGS = [1, 2, 3, 4, 5, 6, 7, 14, 21, 365]   # lag k = valeur du jour J-k ; la valeur de J est en colonne directe
FENETRES = [3, 7, 14, 30]
FIN_TRAIN = pd.Timestamp("2021-12-31")
FIN_VALID = pd.Timestamp("2023-12-31")
FENETRE_CLIM_J = 15


def load_silver() -> pd.DataFrame:
    pq, csv = SILVER / "meteo_journaliere.parquet", SILVER / "meteo_journaliere.csv"
    df = pd.read_parquet(pq) if pq.exists() else pd.read_csv(csv, dtype={"ic_id": str, "mfid": str})
    df["date"] = pd.to_datetime(df["date"])
    keep = ["ic_id", "libelle", "latitude", "longitude", "altitude", "date", "tn", "tx", "tm", "rr", "flag_partiel"]
    return df[keep].sort_values(["ic_id", "date"]).reset_index(drop=True)


def doy(d: pd.Series) -> pd.Series:
    return d.dt.dayofyear.clip(upper=365)


def climatologie_train(df: pd.DataFrame) -> pd.DataFrame:
    """Moyenne et écart-type par station × jour de l'année, sur train uniquement, lissées ±15 j (circulaire)."""
    tr = df[df["date"] <= FIN_TRAIN].assign(doy=lambda x: doy(x["date"]))
    out = []
    for v in VARS:
        for stat in ["mean", "std"]:
            brut = tr.groupby(["ic_id", "doy"])[v].agg(stat).unstack("doy")
            k, w = FENETRE_CLIM_J, 2 * FENETRE_CLIM_J + 1
            ext = pd.concat([brut.iloc[:, -k:], brut, brut.iloc[:, :k]], axis=1)
            lis = ext.T.rolling(w, center=True, min_periods=1).mean().T.iloc[:, k:-k]
            lis.columns = brut.columns
            out.append(lis.stack().rename(f"clim_{v}" if stat == "mean" else f"clim_{v}_std"))
    return pd.concat(out, axis=1).reset_index()


def build_features(df: pd.DataFrame, clim: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["doy"] = doy(df["date"])
    df = df.merge(clim, on=["ic_id", "doy"], how="left")
    g = df.groupby("ic_id", group_keys=False)

    feats = {}
    # Anomalie du jour J par rapport à la climatologie train
    for v in VARS:
        feats[f"{v}_anom"] = df[v] - df[f"clim_{v}"]
    feats["amplitude"] = df["tx"] - df["tn"]
    # Décalages (passé uniquement)
    for v in ["tn", "tx", "tm", "rr"]:
        for k in LAGS:
            if v == "rr" and k > 7:
                continue
            feats[f"{v}_lag{k}"] = g[v].shift(k)
    # Tendances
    for v in VARS:
        feats[f"{v}_diff1"] = df[v] - g[v].shift(1)
    # Fenêtres glissantes se terminant à J (inclus)
    for w in FENETRES:
        for v in ["tn", "tx", "tm"]:
            feats[f"{v}_moy{w}"] = g[v].transform(lambda s, w=w: s.rolling(w, min_periods=max(1, w // 2)).mean())
        feats[f"tm_std{w}"] = g["tm"].transform(lambda s, w=w: s.rolling(w, min_periods=max(2, w // 2)).std())
        feats[f"rr_cumul{w}"] = g["rr"].transform(lambda s, w=w: s.rolling(w, min_periods=max(1, w // 2)).sum())
    feats["jours_secs_7"] = g["rr"].transform(lambda s: (s < 0.2).astype(float).where(s.notna()).rolling(7, min_periods=4).sum())
    # Contexte national : anomalie moyenne des 15 stations le jour J (information disponible à J)
    for v in VARS:
        feats[f"{v}_anom_nationale"] = feats[f"{v}_anom"].groupby(df["date"]).transform("mean")
    # Calendrier (cyclique)
    feats["doy_sin"] = np.sin(2 * np.pi * df["doy"] / 365.25)
    feats["doy_cos"] = np.cos(2 * np.pi * df["doy"] / 365.25)
    feats["mois"] = df["date"].dt.month
    df = pd.concat([df, pd.DataFrame(feats)], axis=1)

    # Cibles, baselines et climatologie du jour cible
    clim_idx = clim.set_index(["ic_id", "doy"])
    cibles = {}
    for h in HORIZONS:
        date_cible = df["date"] + pd.Timedelta(days=h)
        key = pd.MultiIndex.from_arrays([df["ic_id"], doy(date_cible)])
        for v in VARS:
            cible = g[v].shift(-h)
            # sécurité : la ligne J+h doit bien être h jours plus tard (calendrier complet en Silver)
            ok = g["date"].shift(-h) == date_cible
            cibles[f"cible_{v}_h{h}"] = cible.where(ok)
            cibles[f"base_persistance_{v}_h{h}"] = df[v]
            cibles[f"base_clim_{v}_h{h}"] = clim_idx[f"clim_{v}"].reindex(key).to_numpy()
    df = pd.concat([df, pd.DataFrame(cibles)], axis=1)

    # Découpage chronologique avec purge aux frontières
    h_max = max(HORIZONS)
    d = df["date"]
    split = np.select(
        [d <= FIN_TRAIN - pd.Timedelta(days=h_max), d <= FIN_TRAIN,
         d <= FIN_VALID - pd.Timedelta(days=h_max), d <= FIN_VALID],
        ["train", "purge", "valid", "purge"], default="test")
    df.insert(df.columns.get_loc("date") + 1, "split", split)
    df["ic_id"] = df["ic_id"].astype("category")
    return df.drop(columns=["doy"])


def dictionnaire(cols: list[str]) -> pd.DataFrame:
    def role(c: str) -> str:
        if c.startswith("cible_"): return "cible"
        if c.startswith("base_"): return "baseline"
        if c in ("ic_id", "date", "split", "libelle"): return "identifiant"
        if c == "flag_partiel": return "qualite"
        return "variable"
    desc = {
        "tn": "Minimale du jour J", "tx": "Maximale du jour J", "tm": "(tn+tx)/2 du jour J", "rr": "Pluie du jour J (mm)",
        "clim_tn": "Climatologie train de tn pour le jour J", "clim_tx": "Climatologie train de tx pour le jour J",
        "amplitude": "tx - tn du jour J", "jours_secs_7": "Nb de jours avec rr < 0,2 mm sur J-6..J",
        "doy_sin": "Saison (sinus du jour de l'année)", "doy_cos": "Saison (cosinus du jour de l'année)",
        "split": "train / valid / test / purge (frontières, exclues)",
    }
    rows = []
    for c in cols:
        d = desc.get(c, "")
        if not d:
            if "_lag" in c: v, k = c.split("_lag"); d = f"{v} du jour J-{k}"
            elif "_moy" in c: v, w = c.split("_moy"); d = f"Moyenne de {v} sur les {w} jours se terminant à J"
            elif "_std" in c and c.startswith("tm"): d = f"Écart-type de tm sur {c.split('std')[1]} jours"
            elif c.startswith("rr_cumul"): d = f"Cumul de pluie sur {c.split('cumul')[1]} jours"
            elif c.endswith("_anom_nationale"): d = f"Anomalie moyenne de {c[:2]} sur les 15 stations à J"
            elif c.endswith("_anom"): d = f"{c[:2]} de J moins sa climatologie train"
            elif c.endswith("_diff1"): d = f"{c[:2]}(J) - {c[:2]}(J-1)"
            elif c.startswith("cible_"): _, v, h = c.split("_"); d = f"{v} observée à J+{h[1:]}"
            elif c.startswith("base_persistance"): d = f"Persistance : {c.split('_')[2]}(J) utilisée comme prévision"
            elif c.startswith("base_clim"): d = "Climatologie train du jour cible J+h"
            elif c.endswith("_std"): d = "Écart-type climatologique (train)"
        rows.append({"colonne": c, "role": role(c), "description": d})
    return pd.DataFrame(rows)


def main() -> None:
    GOLD.mkdir(parents=True, exist_ok=True)
    silver = load_silver()
    clim = climatologie_train(silver)
    gold = build_features(silver, clim)

    gold.to_parquet(GOLD / "jeu_prevision.parquet", index=False)
    clim.to_parquet(GOLD / "climatologie_train.parquet", index=False)
    dictionnaire(list(gold.columns)).to_csv(GOLD / "dictionnaire_variables.csv", index=False)

    n_feat = sum(1 for c in gold.columns if dictionnaire([c]).role[0] == "variable")
    print(f"✓ Gold écrit : data/gold/jeu_prevision.parquet ({len(gold)} lignes, {gold.shape[1]} colonnes, {n_feat} variables)")
    print(gold.groupby("split", observed=True)["date"].agg(["min", "max", "size"]).to_string())


if __name__ == "__main__":
    main()
