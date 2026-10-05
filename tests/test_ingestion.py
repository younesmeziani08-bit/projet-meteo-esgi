"""Ingestion Bronze, testée hors ligne (l'API est simulée)."""
import ingest_bronze as ib


def _api_simulee(path, params=None):
    if path == "/ref/stations":  # format GeoJSON réel de l'API
        return {"type": "FeatureCollection", "features": [
            {"properties": {"ic_id": "07149", "mfid": "91027002", "aliases": ["LFPO"]}},
            {"properties": {"ic_id": "07630", "mfid": "31069001", "aliases": []}}]}
    if params["from"] == "2000-01-01":  # 1re page tronquée -> pagination
        return {"n": 2, "tronque": True, "fenetre": {"debut": "2000-01-01", "fin": "2000-01-02"},
                "suite": {"from": "2000-01-03"}, "mesures": [{}, {}]}
    return {"n": 1, "tronque": False, "fenetre": {"debut": "2000-01-03", "fin": "2000-01-03"},
            "suite": None, "mesures": [{}]}


def test_extract_rows_geojson_et_mesures():
    assert len(ib.extract_rows(_api_simulee("/ref/stations"))) == 2
    assert len(ib.extract_rows({"mesures": [{}, {}, {}]})) == 3
    assert ib.extract_rows([{"a": 1}]) == [{"a": 1}]


def test_resolution_stations_alias_mfid_doublons_inconnues():
    ref = ib.extract_rows(_api_simulee("/ref/stations"))
    assert ib.resolve_stations(["07149", "LFPO", "91027002", "31069001", "99999"], ref) == ["07149", "07630"]


def test_pagination_suit_suite_from(tmp_path, monkeypatch):
    monkeypatch.setattr(ib, "BRONZE", tmp_path)
    monkeypatch.setattr(ib, "PAUSE_S", 0)
    monkeypatch.setattr(ib, "http_get_json", _api_simulee)
    r = ib.ingest_station_daily("07149", "2000-01-01", "2000-02-01")
    assert (r["pages"], r["lignes"], r["debut"], r["fin"]) == (2, 3, "2000-01-01", "2000-01-03")
    assert sorted(p.name for p in (tmp_path / "v2_journaliere" / "07149").iterdir()) == ["page_001.json", "page_002.json"]
