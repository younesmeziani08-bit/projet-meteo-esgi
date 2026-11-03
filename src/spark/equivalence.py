"""
Test d'équivalence : implémentation pandas (étalon) vs implémentation PySpark (S3 p23).

Le cours demande qu'une version « optimisée » (ici : distribuée, Spark) produise la **même** sortie que
la version initiale validée (ici : pandas), à une tolérance explicite près sur les flottants. Ce module
compare, colonne par colonne, les tables Silver et Gold des deux pipelines, et renvoie un rapport.

Lecture directe en pandas/pyarrow (le parquet Spark est partitionné mais lisible sans JVM) → rapide,
utilisable en CI et en test unitaire.

Usage : python3 src/spark/equivalence.py            (code de sortie 1 si une différence dépasse la tolérance)
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"

# Chaque table : clés, renommage Spark->pandas (aligner les colonnes sémantiquement identiques),
# et colonnes légitimement AJOUTÉES par la version Spark/cours (évolution additive = compatible, S1 p35).
TABLES = [
    {"nom": "Silver",
     "pandas": DATA / "silver" / "meteo_journaliere.parquet",
     "spark": DATA / "silver" / "observations",
     "cles": ["ic_id", "date"],
     "renommage": {"clim_tn": "tn_clim", "clim_tx": "tx_clim"},
     "ajouts_attendus": {"_batch_id", "_source", "annee", "mois", "qc_flag", "version"}},
    {"nom": "Gold",
     "pandas": DATA / "gold" / "jeu_prevision.parquet",
     "spark": DATA / "gold" / "jeu_prevision_spark",
     "cles": ["ic_id", "date"],
     "renommage": {},
     "ajouts_attendus": set()},
]
ATOL, RTOL = 1e-6, 1e-6


def _normaliser(df: pd.DataFrame, cles: list[str]) -> pd.DataFrame:
    df = df.copy()
    df["ic_id"] = df["ic_id"].astype(str)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df.sort_values(cles).reset_index(drop=True)


def comparer(nom: str, chemin_pa: Path, chemin_sp: Path, cles: list[str],
             renommage: dict | None = None, ajouts_attendus: set | None = None,
             atol: float = ATOL, rtol: float = RTOL) -> tuple[bool, pd.DataFrame]:
    """Compare deux tables sur leurs colonnes communes (après renommage). Renvoie (ok, rapport)."""
    renommage, ajouts_attendus = renommage or {}, ajouts_attendus or set()
    pa = _normaliser(pd.read_parquet(chemin_pa), cles)
    sp = _normaliser(pd.read_parquet(chemin_sp).rename(columns=renommage), cles)

    communes = [c for c in pa.columns if c in sp.columns and c not in cles]
    seule_pa = sorted(set(pa.columns) - set(sp.columns) - set(cles))
    # on retire les ajouts attendus (évolution additive documentée) des colonnes « seule spark »
    seule_sp = sorted(set(sp.columns) - set(pa.columns) - set(cles) - ajouts_attendus)
    ajouts = sorted((set(sp.columns) - set(pa.columns) - set(cles)) & ajouts_attendus)

    m = pa[cles + communes].merge(sp[cles + communes], on=cles, how="outer",
                                  suffixes=("_pa", "_sp"), indicator=True)
    non_apparies = int((m["_merge"] != "both").sum())

    lignes = []
    for c in communes:
        a, b = m[f"{c}_pa"], m[f"{c}_sp"]
        if pd.api.types.is_numeric_dtype(a) and not pd.api.types.is_bool_dtype(a):
            af, bf = a.astype("float64"), b.astype("float64")
            egal = (af.isna() & bf.isna()) | np.isclose(af, bf, atol=atol, rtol=rtol, equal_nan=True)
            dmax = float(np.nanmax(np.abs(af - bf))) if (~egal).any() else 0.0
        else:
            egal = (a.isna() & b.isna()) | (a.astype("object") == b.astype("object"))
            dmax = np.nan
        nd = int((~egal).sum())
        lignes.append({"colonne": c, "diffs": nd, "max_delta": dmax})

    rapport = pd.DataFrame(lignes).sort_values("diffs", ascending=False)
    ok = (rapport["diffs"].sum() == 0) and (non_apparies == 0) and not seule_pa and not seule_sp
    rapport.attrs.update(nom=nom, lignes_pa=len(pa), lignes_sp=len(sp), non_apparies=non_apparies,
                         seule_pa=seule_pa, seule_sp=seule_sp, ajouts=ajouts)
    return ok, rapport


def main() -> int:
    global_ok = True
    for t in TABLES:
        cpa, csp = t["pandas"], t["spark"]
        if not cpa.exists() or not csp.exists():
            print(f"[{t['nom']}] ignoré : sortie manquante (pandas={cpa.exists()}, spark={csp.exists()})")
            continue
        ok, rap = comparer(t["nom"], cpa, csp, t["cles"], t["renommage"], t["ajouts_attendus"])
        a = rap.attrs
        print(f"\n=== {t['nom']} : {'OK ✓' if ok else 'DIFFÉRENCES ✗'} ===")
        print(f"  lignes pandas={a['lignes_pa']} spark={a['lignes_sp']} | non appariées={a['non_apparies']}")
        print(f"  colonnes comparées={len(rap)} | ajouts attendus={a['ajouts']}")
        print(f"  seule_pandas={a['seule_pa']} | seule_spark(inattendu)={a['seule_sp']}")
        en_diff = rap[rap["diffs"] > 0]
        if len(en_diff):
            print("  colonnes en écart :")
            print(en_diff.to_string(index=False))
        else:
            print(f"  toutes identiques (tolérance atol={ATOL}, rtol={RTOL})")
        global_ok = global_ok and ok
    print("\n" + ("✓ Équivalence vérifiée : Spark == pandas." if global_ok
                  else "✗ Des différences dépassent la tolérance."))
    return 0 if global_ok else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
