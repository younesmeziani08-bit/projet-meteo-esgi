"""
Promotion vers la zone brute immuable data/raw/ (S2 p13-14).

Le pipeline historique télécharge l'API Infoclimat directement dans data/bronze/. Or l'API est vivante :
relancer un autre jour donnerait d'autres données → non reproductible. Ce script fige les réponses
déjà téléchargées dans une **zone brute immuable**, avec checksum SHA-256 et inscription au batch log.

    data/raw/source=infoclimat/
        batch=infoclimat-ref-<run>/        stations.json      + _checksums.sha256
        batch=infoclimat-<ic_id>-<run>/    page_001.json …    + _checksums.sha256

Désormais toute ré-ingestion (Bronze) lit data/raw/, jamais l'API : « on efface Bronze/Silver,
on relance, on retrouve exactement les mêmes tables » (jalon S2). L'acquisition réseau proprement dite
reste dans src/ingest_bronze.py et n'est rejouée que pour rafraîchir volontairement la zone brute.

Idempotent : un batch dont le checksum est déjà enregistré « en place » est ignoré.

Usage : python3 src/promote_to_raw.py
"""
from __future__ import annotations

import glob
import hashlib
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))   # pour « from spark.batch_log import … », quel que soit le cwd
from spark.batch_log import BatchLog     # noqa: E402
BRONZE_JSON = ROOT / "data" / "bronze"            # réponses API déjà téléchargées (source de départ)
RAW = ROOT / "data" / "raw"
SOURCE = "infoclimat"


def sha256_fichier(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for bloc in iter(lambda: f.read(1 << 20), b""):
            h.update(bloc)
    return h.hexdigest()


def checksum_batch(fichiers: list[Path]) -> str:
    """Checksum stable d'un batch = SHA-256 des (checksum, nom) de ses fichiers, triés."""
    parts = sorted(f"{sha256_fichier(f)}  {f.name}" for f in fichiers)
    return hashlib.sha256("\n".join(parts).encode()).hexdigest(), parts


def run_date() -> str:
    refs = sorted(glob.glob(str(BRONZE_JSON / "ref" / "stations_*.json")))
    if not refs:
        raise FileNotFoundError("aucun référentiel dans data/bronze/ref/ : lancer src/ingest_bronze.py d'abord")
    return Path(refs[-1]).stem.replace("stations_", "")


def lignes_json(p: Path) -> int:
    d = json.loads(p.read_text(encoding="utf-8"))
    if isinstance(d, dict) and "mesures" in d:
        return len(d["mesures"])
    if isinstance(d, dict) and d.get("type") == "FeatureCollection":
        return len(d.get("features", []))
    return 0


def promouvoir_batch(batch_id: str, kind: str, ic_id: str | None, fichiers: list[Path],
                     rdate: str, log: BatchLog) -> str:
    chk, per_file = checksum_batch(fichiers)
    if log.deja_charge(batch_id, chk):
        return "IGNORÉ"
    dest = RAW / f"source={SOURCE}" / f"batch={batch_id}"
    dest.mkdir(parents=True, exist_ok=True)
    log.marquer(batch_id, "RECU", source=SOURCE, kind=kind, ic_id=ic_id, run_date=rdate,
                uri=str(dest.relative_to(ROOT)), nb_fichiers=len(fichiers))
    nb_lignes = 0
    for f in fichiers:
        shutil.copy2(f, dest / f.name)
        nb_lignes += lignes_json(f)
    (dest / "_checksums.sha256").write_text("\n".join(per_file) + "\n", encoding="utf-8")
    # VERIFIE : on relit depuis la zone brute et on recompte le checksum (intégrité de la copie)
    copies = sorted(p for p in dest.glob("*") if p.name != "_checksums.sha256")
    chk2, _ = checksum_batch(copies)
    statut = "VERIFIE" if chk2 == chk else "ECHEC"
    log.marquer(batch_id, statut, checksum=chk, nb_lignes=nb_lignes)
    return statut


def main() -> None:
    rdate = run_date()
    log = BatchLog()
    resultats: dict[str, int] = {}

    # 1. Référentiel des stations
    ref = BRONZE_JSON / "ref" / f"stations_{rdate}.json"
    r = promouvoir_batch(f"{SOURCE}-ref-{rdate}", "ref", None, [ref], rdate, log)
    resultats[r] = resultats.get(r, 0) + 1
    print(f"  ref                     : {r}")

    # 2. Séries journalières, un batch par station
    for station_dir in sorted((BRONZE_JSON / "v2_journaliere").glob("*")):
        if not station_dir.is_dir():
            continue
        ic_id = station_dir.name
        pages = sorted(station_dir.glob("page_*.json"))
        if not pages:
            continue
        r = promouvoir_batch(f"{SOURCE}-{ic_id}-{rdate}", "v2_journaliere", ic_id, pages, rdate, log)
        resultats[r] = resultats.get(r, 0) + 1
        print(f"  {ic_id} ({len(pages)} page[s]) : {r}")

    print("\nBilan promotion :", ", ".join(f"{k}={v}" for k, v in sorted(resultats.items())))
    print(f"Zone brute : {RAW.relative_to(ROOT)}  ·  batch log : data/_meta/batchs.parquet")
    print("\nRéconciliation du log :")
    print(log.reconciliation().to_string(index=False))


if __name__ == "__main__":
    main()
