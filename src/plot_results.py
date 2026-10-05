"""Graphiques de l'étape 4 : MAE par horizon (test) et MAE par station."""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RES = ROOT / "results"
COULEURS = {"persistance": "#2a78d6", "climatologie": "#eb6834", "ridge": "#1baf7a", "lightgbm": "#4a3aa7"}
NOMS = {"persistance": "Persistance", "climatologie": "Climatologie", "ridge": "Ridge", "lightgbm": "LightGBM"}
TXT, TXT2, GRILLE = "#0b0b0b", "#52514e", "#e6e5e0"
TITRES = {"tx": "Température maximale (tx)", "tn": "Température minimale (tn)"}

plt.rcParams.update({"font.size": 10, "axes.edgecolor": GRILLE, "axes.labelcolor": TXT2,
                     "xtick.color": TXT2, "ytick.color": TXT2, "axes.spines.top": False, "axes.spines.right": False})


def mae_par_horizon(met: pd.DataFrame) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.4), sharey=True)
    for ax, v in zip(axes, ["tx", "tn"]):
        d = met[met.variable == v]
        for m, c in COULEURS.items():
            s = d[d.modele == m].sort_values("horizon")
            ax.plot(s.horizon, s.mae, color=c, lw=2, marker="o", ms=5, mec="white", mew=1.5, label=NOMS[m])
            if m in ("persistance", "climatologie"):  # Ridge et LightGBM se superposent : la légende suffit
                ax.annotate(NOMS[m], (7, s.mae.iloc[-1]), xytext=(6, 0), textcoords="offset points",
                            va="center", fontsize=9, color=TXT2)
        fin = d[d.modele.isin(["ridge", "lightgbm"]) & (d.horizon == 7)].mae.mean()
        ax.annotate("Ridge ≈ LightGBM", (7, fin), xytext=(6, -10), textcoords="offset points",
                    va="center", fontsize=9, color=TXT2)
        ax.set_title(TITRES[v], loc="left", color=TXT, fontsize=11)
        ax.set_xticks(range(1, 8), [f"J+{h}" for h in range(1, 8)])
        ax.set_xlim(0.7, 8.3)
        ax.grid(axis="y", color=GRILLE, lw=0.8); ax.set_axisbelow(True)
    axes[0].set_ylabel("MAE sur la période test (°C)")
    axes[0].set_ylim(bottom=0)
    fig.legend(*axes[0].get_legend_handles_labels(), loc="upper center", ncol=4, frameon=False, bbox_to_anchor=(0.5, 1.0))
    fig.suptitle("Erreur moyenne absolue par horizon — test 2024-2026, 15 stations", x=0.01, y=1.07, ha="left", color=TXT, fontsize=12)
    fig.tight_layout()
    fig.savefig(RES / "mae_par_horizon.png", dpi=160, bbox_inches="tight", facecolor="white")


def mae_station(st: pd.DataFrame) -> None:
    ref = pd.read_parquet(ROOT / "data" / "silver" / "meteo_journaliere.parquet", columns=["ic_id", "libelle"]).drop_duplicates()
    ref["nom"] = ref.libelle.str.split(" - |\\(", regex=True).str[0].str.strip()
    fig, axes = plt.subplots(1, 2, figsize=(11, 5.4), sharey=True)
    # même ordre de stations dans les deux panneaux (trié sur LightGBM à J+1)
    ordre = st[(st.variable == "tx") & (st.horizon == 1)].sort_values("lightgbm").ic_id.tolist()
    for ax, h in zip(axes, [1, 7]):
        d = st[(st.variable == "tx") & (st.horizon == h)].merge(ref, on="ic_id")
        d["ref"] = d[["persistance", "climatologie"]].min(axis=1)
        d = d.set_index("ic_id").loc[ordre].reset_index()
        y = range(len(d))
        ax.hlines(y, d.lightgbm, d.ref, color=GRILLE, lw=2)
        nom_ref = "persistance" if h == 1 else "climatologie"
        ax.scatter(d.ref, y, s=42, color=COULEURS[nom_ref], zorder=3,
                   edgecolor="white", lw=1.5, label=f"Meilleure référence ({NOMS[nom_ref]})")
        ax.scatter(d.lightgbm, y, s=42, color=COULEURS["lightgbm"], zorder=3, edgecolor="white", lw=1.5, label="LightGBM")
        if h == 1:
            ax.set_yticks(list(y), d.nom)
        ax.set_title(f"tx à J+{h}", loc="left", color=TXT, fontsize=11)
        ax.set_xlabel("MAE test (°C)")
        ax.grid(axis="x", color=GRILLE, lw=0.8); ax.set_axisbelow(True)
        ax.legend(frameon=False, loc="upper left", fontsize=9)
    fig.suptitle("MAE par station : LightGBM face à la meilleure référence", x=0.01, ha="left", color=TXT, fontsize=12)
    fig.tight_layout()
    fig.savefig(RES / "mae_station_h1_h7.png", dpi=160, bbox_inches="tight", facecolor="white")


if __name__ == "__main__":
    mae_par_horizon(pd.read_csv(RES / "metriques_test.csv"))
    mae_station(pd.read_csv(RES / "metriques_station.csv", dtype={"ic_id": str}))
    print("✓ Graphiques écrits dans results/")
