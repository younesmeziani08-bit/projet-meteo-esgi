"""
Étape 4 — Modèles IA et évaluation (projet météo ESGI, prévision à court terme).

Pour chaque variable (tx, tn) et chaque horizon h = 1..7 :
  1. Ridge (régression linéaire régularisée) : alpha choisi sur valid
  2. LightGBM : nombre de feuilles choisi sur valid, nombre d'arbres par early stopping sur valid
  3. Réentraînement de chaque modèle retenu sur train + valid
  4. Évaluation UNE SEULE FOIS sur test (2024-01-01 → fin des données),
     face à la persistance et à la climatologie (références du Gold)

Sorties : results/metriques_test.csv, results/metriques_station.csv, results/importance_variables.csv,
          results/predictions_test.parquet, results/mae_par_horizon.png, results/mae_station_h1_h7.png,
          models/<modele>_<var>_h<h>.joblib
"""
from __future__ import annotations

import json
import time
import warnings
from pathlib import Path

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

ROOT = Path(__file__).resolve().parents[1]
GOLD, RES, MOD = ROOT / "data" / "gold", ROOT / "results", ROOT / "models"
VARS, HORIZONS = ["tx", "tn"], range(1, 8)
ALPHAS = [0.1, 1.0, 10.0, 100.0]
LGB_LEAVES = [15, 31, 63]
warnings.filterwarnings("ignore", message=".*eval_set.*deprecated")  # compatibilité anciennes versions
LGB_BASE = dict(learning_rate=0.05, n_estimators=3000, min_child_samples=50, subsample=0.8, subsample_freq=1,
                colsample_bytree=0.8, reg_lambda=1.0, n_jobs=4, verbose=-1, random_state=42)


def mae(y, p): return float(np.mean(np.abs(y - p)))
def rmse(y, p): return float(np.sqrt(np.mean((y - p) ** 2)))


def variables(gold: pd.DataFrame) -> list[str]:
    d = pd.read_csv(GOLD / "dictionnaire_variables.csv")
    return d.loc[d.role == "variable", "colonne"].tolist()


def ridge_pipe(num: list[str], alpha: float) -> Pipeline:
    pre = ColumnTransformer([
        ("num", make_pipeline(SimpleImputer(strategy="median"), StandardScaler()), num),
        ("sta", OneHotEncoder(handle_unknown="ignore"), ["ic_id"]),
    ])
    return Pipeline([("pre", pre), ("ridge", Ridge(alpha=alpha))])


PARTIEL = RES / "_partiel"


def entrainer_un(gold: pd.DataFrame, feats: list[str], v: str, h: int) -> None:
    """Entraîne, règle et évalue les modèles d'un couple (variable, horizon) ; sauvegarde le résultat partiel."""
    t0 = time.time()
    train, valid, test = (gold[gold.split == s] for s in ("train", "valid", "test"))
    trval = gold[gold.split.isin(["train", "valid"])]
    y = f"cible_{v}_h{h}"
    X = feats + [f"base_clim_{v}_h{h}", "ic_id"]   # climatologie du jour cible : connue à J
    num = [c for c in X if c != "ic_id"]
    tr, va, te, tv = (d[d[y].notna()] for d in (train, valid, test, trval))

    # --- Ridge : choix de alpha sur valid
    best_a = min(ALPHAS, key=lambda a: mae(va[y], ridge_pipe(num, a).fit(tr[X], tr[y]).predict(va[X])))
    ridge = ridge_pipe(num, best_a).fit(tv[X], tv[y])

    # --- LightGBM : choix des feuilles + early stopping sur valid
    best = None
    for leaves in LGB_LEAVES:
        m = lgb.LGBMRegressor(num_leaves=leaves, **LGB_BASE)
        m.fit(tr[X], tr[y], eval_set=[(va[X], va[y])], eval_metric="l1",
              callbacks=[lgb.early_stopping(100, verbose=False)])
        score = mae(va[y], m.predict(va[X], num_iteration=m.best_iteration_))
        if best is None or score < best[0]:
            best = (score, leaves, m.best_iteration_)
    _, leaves, n_iter = best
    n_arbres = int(n_iter * 1.1)
    gbm = lgb.LGBMRegressor(num_leaves=leaves, **{**LGB_BASE, "n_estimators": n_arbres})
    gbm.fit(tv[X], tv[y])
    joblib.dump(ridge, MOD / f"ridge_{v}_h{h}.joblib")
    joblib.dump(gbm, MOD / f"lightgbm_{v}_h{h}.joblib")

    # --- Évaluation unique sur test
    p = {"persistance": te[f"base_persistance_{v}_h{h}"].to_numpy(),
         "climatologie": te[f"base_clim_{v}_h{h}"].to_numpy(),
         "ridge": ridge.predict(te[X]), "lightgbm": gbm.predict(te[X])}
    yt = te[y].to_numpy()
    lignes = []
    for name, pr in p.items():
        ok = ~np.isnan(pr)
        lignes.append({"variable": v, "horizon": h, "modele": name, "n": int(ok.sum()),
                       "mae": mae(yt[ok], pr[ok]), "rmse": rmse(yt[ok], pr[ok]), "biais": float(np.mean(pr[ok] - yt[ok]))})
    imp = pd.Series(gbm.booster_.feature_importance("gain"), index=X)
    joblib.dump({
        "metriques": lignes,
        "predictions": pd.DataFrame({"ic_id": te.ic_id.astype(str).to_numpy(), "date_origine": te.date.to_numpy(),
                                     "variable": v, "horizon": h, "observe": yt, **p}),
        "importance": pd.DataFrame({"variable": v, "horizon": h, "colonne": imp.index, "gain": (imp / imp.sum()).to_numpy()}),
        "hyperparametres": {"ridge_alpha": best_a, "lgb_leaves": leaves, "lgb_arbres": n_arbres},
    }, PARTIEL / f"{v}_h{h}.joblib")
    m = {l["modele"]: l["mae"] for l in lignes}
    print(f"{v} J+{h}: ridge α={best_a}, lgb {leaves} feuilles/{n_arbres} arbres | MAE test lgb {m['lightgbm']:.2f} "
          f"ridge {m['ridge']:.2f} persist {m['persistance']:.2f} clim {m['climatologie']:.2f}  ({time.time()-t0:.0f}s)", flush=True)


def assembler() -> None:
    parts = [joblib.load(PARTIEL / f"{v}_h{h}.joblib") for v in VARS for h in HORIZONS]
    met = pd.DataFrame([l for p in parts for l in p["metriques"]])
    best_ref = met[met.modele.isin(["persistance", "climatologie"])].groupby(["variable", "horizon"]).mae.min().rename("mae_meilleure_ref")
    met = met.join(best_ref, on=["variable", "horizon"])
    met["gain_vs_meilleure_ref_pct"] = 100 * (1 - met.mae / met.mae_meilleure_ref)
    met.round(4).to_csv(RES / "metriques_test.csv", index=False)

    pr = pd.concat([p["predictions"] for p in parts], ignore_index=True)
    pr.to_parquet(RES / "predictions_test.parquet", index=False)
    long = pr.melt(id_vars=["ic_id", "date_origine", "variable", "horizon", "observe"], var_name="modele", value_name="prevision")
    long["erreur_abs"] = (long.prevision - long.observe).abs()
    st = long.groupby(["variable", "horizon", "ic_id", "modele"]).erreur_abs.mean().unstack("modele").reset_index()
    st.round(3).to_csv(RES / "metriques_station.csv", index=False)
    pd.concat([p["importance"] for p in parts]).round(5).to_csv(RES / "importance_variables.csv", index=False)
    hp = {f"{v}_h{h}": p["hyperparametres"] for (v, h), p in zip([(v, h) for v in VARS for h in HORIZONS], parts)}
    (RES / "hyperparametres.json").write_text(json.dumps(hp, indent=1))
    print("✓ Résultats assemblés dans results/ (metriques_test.csv, metriques_station.csv, ...)")


def main() -> None:
    """Reprenable : les couples (variable, horizon) déjà calculés dans results/_partiel/ sont sautés.
    Option --max N : ne traite que N couples puis s'arrête (utile si le temps d'exécution est limité)."""
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=None)
    ap.add_argument("--refaire", action="store_true", help="ignore les résultats partiels existants")
    args = ap.parse_args()
    RES.mkdir(exist_ok=True); MOD.mkdir(exist_ok=True); PARTIEL.mkdir(exist_ok=True)
    gold = pd.read_parquet(GOLD / "jeu_prevision.parquet")
    feats = variables(gold)
    todo = [(v, h) for v in VARS for h in HORIZONS if args.refaire or not (PARTIEL / f"{v}_h{h}.joblib").exists()]
    for v, h in todo[: args.max]:
        entrainer_un(gold, feats, v, h)
    if all((PARTIEL / f"{v}_h{h}.joblib").exists() for v in VARS for h in HORIZONS):
        assembler()
    else:
        print(f"… {sum(1 for v in VARS for h in HORIZONS if not (PARTIEL / f'{v}_h{h}.joblib').exists())} couple(s) restant(s) : relancer le script.")


if __name__ == "__main__":
    main()
