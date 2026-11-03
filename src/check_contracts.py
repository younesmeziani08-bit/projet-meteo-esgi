"""
Vérificateur des data contracts ODCS du projet (contracts/*.odcs.yaml).

Pour chaque contrat, sur la table réelle (chargée dans DuckDB) :
  - schéma      : chaque colonne déclarée existe dans la table ;
  - required    : aucune valeur vide sur les colonnes obligatoires ;
  - primaryKey  : unicité de la clé ;
  - quality/sql : exécution de la requête et comparaison au seuil (mustBe, mustBeGreaterThan, …) ;
  - quality/custom : non exécutable ici, vérifié par les tests pytest (tests/test_gold_fuite.py).

Usage : python3 src/check_contracts.py            (code de sortie 1 si une règle échoue)
"""
from __future__ import annotations

import glob
import json
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts"
DATA = ROOT / "data"


def _bronze() -> pd.DataFrame:
    rows = []
    for f in sorted(glob.glob(str(DATA / "bronze" / "v2_journaliere" / "*" / "page_*.json"))):
        rows += json.loads(Path(f).read_text(encoding="utf-8"))["mesures"]
    if not rows:
        raise FileNotFoundError("aucune page Bronze")
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df


# nom de l'objet de schéma dans le contrat -> fonction de chargement de la table
TABLES = {
    "v2_journaliere": _bronze,
    "meteo_journaliere": lambda: pd.read_parquet(DATA / "silver" / "meteo_journaliere.parquet"),
    "jeu_prevision": lambda: pd.read_parquet(DATA / "gold" / "jeu_prevision.parquet"),
}

COMPARATEURS = {
    "mustBe": lambda v, s: v == s,
    "mustNotBe": lambda v, s: v != s,
    "mustBeGreaterThan": lambda v, s: v > s,
    "mustBeGreaterOrEqualTo": lambda v, s: v >= s,
    "mustBeLessThan": lambda v, s: v < s,
    "mustBeLessOrEqualTo": lambda v, s: v <= s,
    "mustBeBetween": lambda v, s: s[0] <= v <= s[1],
}
NOM_SIMPLE = re.compile(r"^\w+$")


def verifier_contrat(chemin: Path, con: duckdb.DuckDBPyConnection | None = None) -> list[dict]:
    """Renvoie une liste de résultats {contrat, table, controle, statut (OK|ECHEC|DELEGUE|ABSENT), detail}."""
    contrat = yaml.safe_load(chemin.read_text(encoding="utf-8"))
    con = con or duckdb.connect()
    res = []

    def ajout(table, controle, statut, detail=""):
        res.append({"contrat": chemin.name, "table": table, "controle": controle, "statut": statut, "detail": detail})

    for obj in contrat.get("schema", []):
        nom = obj["name"]
        try:
            df = TABLES[nom]()
        except (FileNotFoundError, KeyError) as e:
            ajout(nom, "chargement", "ABSENT", f"table introuvable ({type(e).__name__}) : lancer le pipeline d'abord")
            continue
        con.register(nom, df)
        props = [p for p in obj.get("properties", []) if NOM_SIMPLE.match(str(p["name"]))]

        manquantes = [p["name"] for p in props if p["name"] not in df.columns]
        ajout(nom, "schéma : colonnes déclarées présentes", "ECHEC" if manquantes else "OK",
              f"manquantes : {manquantes}" if manquantes else f"{len(props)} colonnes")

        for p in props:
            if p.get("required") and p["name"] in df.columns:
                n = int(df[p["name"]].isna().sum())
                ajout(nom, f"required : {p['name']}", "ECHEC" if n else "OK", f"{n} valeur(s) vide(s)")

        cle = [p["name"] for p in sorted((p for p in props if p.get("primaryKey")),
                                         key=lambda p: p.get("primaryKeyPosition", 0))]
        if cle:
            n = int(df.duplicated(cle).sum())
            ajout(nom, f"clé primaire ({', '.join(cle)})", "ECHEC" if n else "OK", f"{n} doublon(s)")

        for q in obj.get("quality", []):
            desc = q.get("description", q.get("rule", "règle"))
            if q.get("type") != "sql":
                ajout(nom, desc, "DELEGUE", "vérifié par les tests pytest")
                continue
            rows = con.execute(q["query"]).fetchall()   # fetchall : consomme tout le résultat
            ligne = rows[0] if rows else None            # (sinon DuckDB garde la connexion « in use »)
            valeur = 0 if ligne is None or ligne[0] is None else ligne[0]  # aucune ligne = aucun cas trouvé
            comp = next((k for k in COMPARATEURS if k in q), None)
            ok = COMPARATEURS[comp](valeur, q[comp]) if comp else True
            ajout(nom, desc, "OK" if ok else "ECHEC", f"valeur = {valeur} ({comp} {q.get(comp)})")
        con.unregister(nom)
    return res


def main() -> int:
    tous = []
    for chemin in sorted(CONTRACTS.glob("*.odcs.yaml")):
        tous += verifier_contrat(chemin)
    df = pd.DataFrame(tous)
    symb = {"OK": "✓", "ECHEC": "✗", "DELEGUE": "→", "ABSENT": "?"}
    for contrat, g in df.groupby("contrat", sort=False):
        print(f"\n{contrat}")
        for _, r in g.iterrows():
            ctl = r.controle if len(r.controle) <= 55 else r.controle[:52] + "…"
            print(f"  {symb[r.statut]} {ctl:<55} {r.detail}")
    n = df.statut.value_counts()
    print(f"\nBilan : {n.get('OK', 0)} OK, {n.get('ECHEC', 0)} échec(s), {n.get('DELEGUE', 0)} délégué(s) à pytest, {n.get('ABSENT', 0)} table(s) absente(s)")
    return 1 if n.get("ECHEC", 0) or n.get("ABSENT", 0) else 0


if __name__ == "__main__":
    sys.exit(main())
