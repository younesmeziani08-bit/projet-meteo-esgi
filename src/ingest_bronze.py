"""
Étape 1 — Ingestion Bronze (projet météo ESGI, variante « Prévision à court terme »).

Récupère depuis l'API ouverte Infoclimat (https://climato.chom.engineering) :
  1. le référentiel des stations  (/ref/stations)       -> data/bronze/ref/stations_<date>.json
  2. les séries journalières      (/v2/journaliere)     -> data/bronze/v2_journaliere/<ic_id>/page_XXX.json

Principe Bronze : on stocke les réponses BRUTES de l'API, sans transformation,
avec un manifeste (date d'extraction, URL, nb de lignes) pour la traçabilité.

Aucune dépendance externe : bibliothèque standard Python uniquement.

Usage :
    python src/ingest_bronze.py                      # stations par défaut, 2000 -> aujourd'hui
    python src/ingest_bronze.py --from 2010-01-01    # autre date de début
    python src/ingest_bronze.py --stations 07149 07630
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE_URL = "https://climato.chom.engineering"
ROOT = Path(__file__).resolve().parents[1]
BRONZE = ROOT / "data" / "bronze"
TIMEOUT_S = 30
MAX_RETRIES = 5
PAUSE_S = 0.3  # politesse envers l'API entre deux appels

# Stations synoptiques (aéroports / stations principales) réparties sur la France,
# identifiées par leur code OMM, utilisé comme ic_id. Validées contre /ref/stations au lancement.
DEFAULT_STATIONS = [
    "07149",  # Paris-Orly
    "07015",  # Lille-Lesquin
    "07110",  # Brest-Guipavas
    "07130",  # Rennes-Saint-Jacques
    "07222",  # Nantes-Bouguenais
    "07190",  # Strasbourg-Entzheim
    "07280",  # Dijon-Longvic
    "07480",  # Lyon-Bron
    "07460",  # Clermont-Ferrand
    "07510",  # Bordeaux-Mérignac
    "07630",  # Toulouse-Blagnac
    "07643",  # Montpellier
    "07650",  # Marseille-Marignane
    "07690",  # Nice
    "07747",  # Perpignan
]


def http_get_json(path: str, params: dict | None = None) -> dict | list:
    """GET JSON avec reprise sur 503 (Retry-After) et erreurs réseau."""
    url = f"{BASE_URL}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json",
                                                       "User-Agent": "esgi-projet-meteo/1.0"})
            with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            if e.code == 503 and attempt < MAX_RETRIES:
                wait = int(e.headers.get("Retry-After", "2"))
                print(f"  503 (service occupé), nouvelle tentative dans {wait}s…")
                time.sleep(wait)
                continue
            raise RuntimeError(f"HTTP {e.code} sur {url} : {body[:300]}") from e
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt < MAX_RETRIES:
                time.sleep(2 * attempt)
                continue
            raise RuntimeError(f"Erreur réseau sur {url} : {e}") from e
    raise RuntimeError(f"Échec après {MAX_RETRIES} tentatives : {url}")


def extract_rows(data) -> list[dict]:
    """Renvoie les lignes d'une réponse : liste brute, {"mesures": [...]} ou GeoJSON {"features": [...]}."""
    if isinstance(data, list):
        return data
    if data.get("features") is not None:  # GeoJSON (ex. /ref/stations)
        return [f.get("properties", {}) for f in data["features"]]
    return data.get("mesures") or data.get("stations") or []


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=1), encoding="utf-8")


def ingest_ref_stations(run_date: str) -> list[dict]:
    print("→ Référentiel des stations (/ref/stations)")
    data = http_get_json("/ref/stations")
    rows = extract_rows(data)
    write_json(BRONZE / "ref" / f"stations_{run_date}.json", data)
    print(f"  {len(rows)} stations enregistrées")
    return rows


def resolve_stations(wanted: list[str], ref_rows: list[dict]) -> list[str]:
    """Garde les stations connues du référentiel (par ic_id, mfid ou alias)."""
    index: dict[str, str] = {}
    for r in ref_rows:
        ic = str(r.get("ic_id", ""))
        for key in [ic, str(r.get("mfid") or ""), *[str(a) for a in (r.get("aliases") or [])]]:
            if key:
                index.setdefault(key, ic)
    ok, unknown = [], []
    for s in wanted:
        if s not in index:
            unknown.append(s)
        elif index[s] not in ok:  # évite les doublons (même poste via ic_id, mfid ou alias)
            ok.append(index[s])
    if unknown:
        print(f"  ⚠ Stations absentes du référentiel, ignorées : {', '.join(unknown)}")
    return ok


def ingest_station_daily(ic_id: str, date_from: str, date_to: str) -> dict:
    """Télécharge /v2/journaliere pour une station, en suivant la pagination (tronque/suite)."""
    out_dir = BRONZE / "v2_journaliere" / ic_id
    for old in out_dir.glob("page_*.json"):  # une relance remplace l'extraction précédente
        old.unlink()
    page, total, cursor = 0, 0, date_from
    first, last = None, None
    while True:
        params = {"station": ic_id, "from": cursor, "to": date_to}
        data = http_get_json("/v2/journaliere", params)
        page += 1
        n = len(extract_rows(data))
        total += n
        write_json(out_dir / f"page_{page:03d}.json",
                   {"_requete": {"path": "/v2/journaliere", "params": params}, **data})
        fen = data.get("fenetre") or {}
        first = first or fen.get("debut")
        last = fen.get("fin") or last
        suite = data.get("suite")
        if data.get("tronque") and suite and suite.get("from"):
            cursor = suite["from"]
            time.sleep(PAUSE_S)
            continue
        if data.get("tronque") and not suite:
            print(f"  ⚠ {ic_id} : réponse tronquée sans point de reprise")
        break
    return {"ic_id": ic_id, "pages": page, "lignes": total, "debut": first, "fin": last}


def main() -> None:
    today = dt.date.today()
    p = argparse.ArgumentParser(description="Ingestion Bronze Infoclimat")
    p.add_argument("--stations", nargs="*", default=DEFAULT_STATIONS)
    p.add_argument("--from", dest="date_from", default="2000-01-01")
    p.add_argument("--to", dest="date_to", default=today.isoformat(), help="exclu")
    args = p.parse_args()

    run_date = today.isoformat()
    started = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

    ref_rows = ingest_ref_stations(run_date)
    stations = resolve_stations(args.stations, ref_rows)

    print(f"→ Séries journalières (/v2/journaliere) {args.date_from} → {args.date_to}")
    results = []
    for s in stations:
        try:
            r = ingest_station_daily(s, args.date_from, args.date_to)
            print(f"  {s} : {r['lignes']:>6} jours  ({r['debut']} → {r['fin']}, {r['pages']} page(s))")
        except RuntimeError as e:
            r = {"ic_id": s, "erreur": str(e)}
            print(f"  ✗ {s} : {e}")
        results.append(r)
        time.sleep(PAUSE_S)

    manifest = {
        "source": BASE_URL,
        "extraction_utc": started,
        "parametres": {"from": args.date_from, "to": args.date_to},
        "n_stations_referentiel": len(ref_rows),
        "stations": results,
        "licence": "Licence Ouverte Etalab 2.0 / politique opendata Infoclimat",
    }
    write_json(BRONZE / f"_manifest_{run_date}.json", manifest)
    ok = [r for r in results if "erreur" not in r]
    print(f"\n✓ Terminé : {len(ok)}/{len(results)} stations, "
          f"{sum(r['lignes'] for r in ok)} lignes. Manifeste : data/bronze/_manifest_{run_date}.json")


if __name__ == "__main__":
    main()
