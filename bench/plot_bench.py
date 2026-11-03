"""Courbe de speedup et coût (S3 p27-30), à partir de results/bench_resume.csv -> results/speedup.png."""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RES = ROOT / "results"
TXT, TXT2, GRILLE = "#0b0b0b", "#52514e", "#e6e5e0"
C_INIT, C_OPT, C_IDEAL = "#2a78d6", "#1baf7a", "#b0afab"

plt.rcParams.update({"font.size": 10, "axes.edgecolor": GRILLE, "axes.labelcolor": TXT2,
                     "xtick.color": TXT2, "ytick.color": TXT2,
                     "axes.spines.top": False, "axes.spines.right": False})


def main() -> None:
    r = pd.read_csv(RES / "bench_resume.csv")
    init = r[r.version == "initiale"].sort_values("workers")
    opt = r[r.version == "optimisee"].sort_values("workers")
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.4))

    # Speedup
    nmax = init.workers.max()
    ax1.plot([1, nmax], [1, nmax], "--", color=C_IDEAL, lw=1.5, label="Idéal (linéaire)")
    ax1.plot(init.workers, init.speedup, "o-", color=C_INIT, lw=2, ms=6, mec="white", mew=1.5, label="Initiale")
    ax1.plot(opt.workers, opt.speedup, "s--", color=C_OPT, lw=2, ms=6, mec="white", mew=1.5, label="Optimisée")
    for _, row in init.iterrows():
        ax1.annotate(f"×{row.speedup:g}\nE={row.efficiency:g}", (row.workers, row.speedup),
                     xytext=(6, -2), textcoords="offset points", fontsize=8, color=TXT2)
    ax1.set_title("Speedup selon le nombre de workers", loc="left", color=TXT, fontsize=11)
    ax1.set_xlabel("workers (local[N])"); ax1.set_ylabel("speedup S(n) = t₁ / tₙ")
    ax1.set_xticks(init.workers); ax1.grid(axis="both", color=GRILLE, lw=0.8); ax1.set_axisbelow(True)
    ax1.legend(frameon=False, fontsize=9, loc="upper left")

    # Durée et coût
    ax2.bar(init.workers - 0.17, init.duree_med_s, width=0.34, color=C_INIT, label="Durée (s)")
    ax2b = ax2.twinx()
    ax2b.plot(init.workers, init.vcpu_h, "o-", color="#eb6834", lw=2, ms=5, label="Coût (vCPU·h)")
    ax2b.spines["top"].set_visible(False)
    ax2.set_title("Durée et coût selon les workers (initiale)", loc="left", color=TXT, fontsize=11)
    ax2.set_xlabel("workers (local[N])"); ax2.set_ylabel("durée médiane (s)")
    ax2b.set_ylabel("coût (vCPU·h)", color="#eb6834")
    ax2.set_xticks(init.workers); ax2.grid(axis="y", color=GRILLE, lw=0.8); ax2.set_axisbelow(True)
    lignes = ax2.get_legend_handles_labels()[0] + ax2b.get_legend_handles_labels()[0]
    ax2.legend(lignes, ["Durée (s)", "Coût (vCPU·h)"], frameon=False, fontsize=9, loc="upper right")

    fig.suptitle("Gold Spark — comparaison 1/N workers (test local, 15 stations)", x=0.01, ha="left",
                 color=TXT, fontsize=12)
    fig.tight_layout()
    fig.savefig(RES / "speedup.png", dpi=160, bbox_inches="tight", facecolor="white")
    print("✓ results/speedup.png")


if __name__ == "__main__":
    main()
