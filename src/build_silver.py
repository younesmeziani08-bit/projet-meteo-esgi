"""
Étape 2 — Couche Silver (projet météo ESGI, prévision à court terme).

Entrée  : data/bronze/ (réponses brutes de l'API, extraction la plus récente)
Sortie  : data/silver/meteo_journaliere.parquet (ou .csv si pyarrow absent)
          data/silver/rapport_qualite.csv  (synthèse des flags par station)

Une ligne par station × jour, calendrier complet, avec :
  - les valeurs brutes (tn_brut, tx_brut, rr_brut) conservées pour l'audit ;
  - les valeurs nettoyées (tn, tx, tm, rr) : vides quand la donnée est jugée invalide ;
  - des indicateurs (flags) expliquant pourquoi une valeur a été invalidée.

Règles qualité (une valeur est invalidée si l'une d'elles est vraie) :
  1. flag_absent       : jour absent de l'API ou valeur vide
  2. flag_hors_bornes  : valeur physiquement implausible en France métropolitaine
  3. flag_incoherent   : tn > tx
  4. flag_spatial      : écart à la climatologie de la station très différent de celui
                         des autres stations le même jour (|score robuste| > SEUIL_SPATIAL).
                         Détecte les erreurs de capteur ponctuelles (ex. Lille 48,7 °C le 27/05/2005)
                         sans pénaliser les vagues de chaleur ou de froid, qui touchent toutes les stations.
Règle propre aux précipitations (n'invalide que rr) :
  5. flag_rr_invalide  : rr négatif ou > 1000 mm
Indicateur informatif (n'invalide pas) :
  6. flag_partiel      : journée incomplète (statut_extremes != "quotidien" ou peu d'observations)
"""
from __future__ import annotations

import glob
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
BRONZE = ROOT / "data" / "bronze"
SILVER = ROOT / "data" / "silver"

BORNES = {"tn": (-35.0, 35.0), "tx": (-25.0, 47.0)}
BORNES_RR = (0.0, 1000.0)  # les épisodes cévenols peuvent dépasser 500 mm/jour
SEUIL_SPATIAL = 10.0       # en écarts-types robustes. À 6, de vrais événements locaux
                           # (canicule 19/07/2022 à Orly, froid de déc. 2009 à Dijon) étaient rejetés.
FENETRE_CLIM_J = 15        # demi-fenêtre (jours) de lissage de la climatologie
MIN_OBS_RATIO = 0.5        # journée « partielle » si n_obs < 50 % de la médiane de la station


def load_bronze() -> pd.DataFrame:
    rows = []
    for f in sorted(glob.glob(str(BRONZE / "v2_journaliere" / "*" / "page_*.json"))):
        rows += json.loads(Path(f).read_text(encoding="utf-8"))["mesures"]
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["ic_id"] = df["ic_id"].astype(str)
    return df.drop(columns=["tntxm"], errors="ignore").drop_duplicates(["ic_id", "date"])


def load_stations() -> pd.DataFrame:
    ref_file = sorted(glob.glob(str(BRONZE / "ref" / "stations_*.json")))[-1]
    data = json.loads(Path(ref_file).read_text(encoding="utf-8"))
    ref = pd.DataFrame([f["properties"] for f in data["features"]])
    cols = ["ic_id", "libelle", "latitude", "longitude", "altitude", "departement"]
    return ref[cols].drop_duplicates("ic_id")


def complete_calendar(df: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for ic, g in df.groupby("ic_id"):
        idx = pd.date_range(g["date"].min(), g["date"].max(), freq="D")
        g = g.set_index("date").reindex(idx).rename_axis("date").reset_index()
        g["ic_id"] = ic
        g["mfid"] = g["mfid"].ffill().bfill()
        parts.append(g)
    return pd.concat(parts, ignore_index=True)


def climatologie(df: pd.DataFrame, col: str) -> pd.Series:
    """Moyenne par station et jour de l'année, lissée sur ±FENETRE_CLIM_J jours (circulaire)."""
    doy = df["date"].dt.dayofyear.clip(upper=365)
    brut = df.assign(doy=doy).groupby(["ic_id", "doy"])[col].mean().unstack("doy")
    w = 2 * FENETRE_CLIM_J + 1
    lisse = pd.concat([brut.iloc[:, -FENETRE_CLIM_J:], brut, brut.iloc[:, :FENETRE_CLIM_J]], axis=1)
    lisse = lisse.T.rolling(w, center=True, min_periods=1).mean().T.iloc[:, FENETRE_CLIM_J:-FENETRE_CLIM_J]
    lisse.columns = brut.columns
    stacked = lisse.stack().rename(f"{col}_clim")
    return df.assign(doy=doy).join(stacked, on=["ic_id", "doy"])[f"{col}_clim"]


def score_spatial(df: pd.DataFrame, col: str, valide: pd.Series) -> pd.Series:
    """Écart entre l'anomalie de la station et l'anomalie médiane des autres stations le même jour,
    normalisé par l'écart robuste (MAD) propre à chaque station."""
    anom = (df[col] - df[f"{col}_clim"]).where(valide)
    med_jour = anom.groupby(df["date"]).transform("median")
    resid = anom - med_jour
    mad = resid.abs().groupby(df["ic_id"]).transform("median") * 1.4826
    return resid / mad.replace(0, np.nan)


def appliquer_regles(df: pd.DataFrame) -> pd.DataFrame:
    """Applique les règles qualité à une table station × jour (calendrier complet). Fonction testée unitairement."""
    df = df.copy()
    for c in ["tn", "tx", "rr"]:
        df[f"{c}_brut"] = df[c]

    # 1. absent
    df["flag_absent"] = df[["tn", "tx"]].isna().any(axis=1)
    # 2. hors bornes
    hb = pd.Series(False, index=df.index)
    for c, (lo, hi) in BORNES.items():
        hb |= (df[c] < lo) | (df[c] > hi)
    df["flag_hors_bornes"] = hb
    # 3. incohérence
    df["flag_incoherent"] = df["tn"] > df["tx"]
    # 4. spatial (calculé sur les valeurs ayant passé les règles 1 à 3)
    base_ok = ~(df["flag_absent"] | df["flag_hors_bornes"] | df["flag_incoherent"])
    df["flag_spatial"] = False
    for c in ["tn", "tx"]:
        df[f"{c}_clim"] = climatologie(df.where(base_ok), c).values
        df[f"{c}_score_spatial"] = score_spatial(df, c, base_ok)
        df["flag_spatial"] |= df[f"{c}_score_spatial"].abs() > SEUIL_SPATIAL
    # 5. partiel (informatif)
    med_obs = df.groupby("ic_id")["n_obs"].transform("median")
    df["flag_partiel"] = (df["statut_extremes"].fillna("") != "quotidien") | (df["n_obs"] < MIN_OBS_RATIO * med_obs)

    # Valeurs nettoyées
    invalide = df["flag_absent"] | df["flag_hors_bornes"] | df["flag_incoherent"] | df["flag_spatial"]
    df["valide"] = ~invalide
    df.loc[invalide, ["tn", "tx"]] = np.nan
    df["flag_rr_invalide"] = df["rr"].notna() & ((df["rr"] < BORNES_RR[0]) | (df["rr"] > BORNES_RR[1]))
    df.loc[df["flag_rr_invalide"], "rr"] = np.nan
    df["tm"] = (df["tn"] + df["tx"]) / 2

    return df


def main() -> None:
    SILVER.mkdir(parents=True, exist_ok=True)
    bronze = load_bronze()
    df = complete_calendar(bronze)
    df = appliquer_regles(df)

    df = df.merge(load_stations(), on="ic_id", how="left")
    cols = ["ic_id", "mfid", "libelle", "latitude", "longitude", "altitude", "departement", "date",
            "tn", "tx", "tm", "rr", "tn_brut", "tx_brut", "rr_brut", "tn_clim", "tx_clim",
            "tn_score_spatial", "tx_score_spatial", "n_obs", "n_obs_rejetees", "statut_extremes",
            "flag_absent", "flag_hors_bornes", "flag_incoherent", "flag_spatial", "flag_rr_invalide", "flag_partiel", "valide"]
    df = df[cols].sort_values(["ic_id", "date"]).reset_index(drop=True)

    try:
        out = SILVER / "meteo_journaliere.parquet"
        df.to_parquet(out, index=False)
    except ImportError:
        out = SILVER / "meteo_journaliere.csv"
        df.to_csv(out, index=False)

    rapport = df.groupby(["ic_id", "libelle"]).agg(
        jours=("date", "size"), debut=("date", "min"), fin=("date", "max"),
        absent=("flag_absent", "sum"), hors_bornes=("flag_hors_bornes", "sum"),
        incoherent=("flag_incoherent", "sum"), spatial=("flag_spatial", "sum"),
        rr_invalide=("flag_rr_invalide", "sum"), partiel=("flag_partiel", "sum"), taux_valide=("valide", "mean"),
    ).reset_index()
    rapport["taux_valide"] = rapport["taux_valide"].round(5)
    rapport.to_csv(SILVER / "rapport_qualite.csv", index=False)

    print(f"✓ Silver écrit : {out.relative_to(ROOT)}  ({len(df)} lignes, {df['ic_id'].nunique()} stations)")
    print(rapport.drop(columns=["debut", "fin"]).to_string(index=False))
    invalide = ~df["valide"]
    print(f"\nValeurs invalidées : {invalide.sum()} jours sur {len(df)} ({invalide.mean():.3%})")


if __name__ == "__main__":
    main()
