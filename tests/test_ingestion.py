"""Offline tests for Phase 2 ingestion logic."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from pystac import Item

from delta_crops.ingestion import bronze, weather
from delta_crops.ingestion.earth_search import build_search_params, expected_assets, scene_from_item, select_best
from delta_crops.ingestion.raster import nodata_pct


def make_item(item_id: str, day: str, baseline: str, cloud: float = 5.0) -> Item:
    return Item.from_dict(
        {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": item_id,
            "collection": "sentinel-2-l2a",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[30.8, 30.7], [31.4, 30.7], [31.4, 31.3], [30.8, 31.3], [30.8, 30.7]]],
            },
            "bbox": [30.8, 30.7, 31.4, 31.3],
            "properties": {
                "datetime": f"{day}T08:51:41Z",
                "s2:processing_baseline": baseline,
                "eo:cloud_cover": cloud,
                "earthsearch:boa_offset_applied": baseline >= "04.00",
            },
            "links": [],
            "assets": {
                "red": {"href": f"https://example.test/{item_id}/B04.tif"},
                "nir": {"href": f"https://example.test/{item_id}/B08.tif"},
                "scl": {"href": f"https://example.test/{item_id}/SCL.tif"},
            },
        }
    )


def test_build_search_params_matches_the_earth_search_request() -> None:
    params = build_search_params([30.86, 30.75, 31.36, 31.2], date(2023, 5, 1), date(2023, 9, 30), 40.0)

    assert params["bbox"] == [30.86, 30.75, 31.36, 31.2]
    assert params["datetime"] == "2023-05-01/2023-09-30"  # inclusive on both ends
    assert params["collections"] == ["sentinel-2-l2a"]
    assert params["query"] == {"eo:cloud_cover": {"lt": 40.0}}  # strict <, not <=
    assert params["max_items"] is None  # paginate everything


def test_build_search_params_allows_a_different_collection_and_threshold() -> None:
    params = build_search_params(
        [30.0, 30.0, 30.5, 30.5], date(2019, 6, 15), date(2019, 6, 24), 20, collection="sentinel-2-c1-l2a"
    )
    assert params["collections"] == ["sentinel-2-c1-l2a"]
    assert params["query"]["eo:cloud_cover"]["lt"] == 20
    assert params["datetime"] == "2019-06-15/2019-06-24"


@pytest.mark.parametrize(
    "bbox,start,end",
    [
        ([30.86, 30.75, 31.36, 31.2], date(2023, 9, 30), date(2023, 5, 1)),  # inverted dates
        ([31.36, 30.75, 30.86, 31.2], date(2023, 5, 1), date(2023, 9, 30)),  # west > east
        ([30.86, 31.2, 31.36, 30.75], date(2023, 5, 1), date(2023, 9, 30)),  # south > north
        ([30.86, 30.75, 31.36], date(2023, 5, 1), date(2023, 9, 30)),  # bbox too short
    ],
)
def test_build_search_params_rejects_bad_input(bbox: list[float], start: date, end: date) -> None:
    with pytest.raises(ValueError):
        build_search_params(bbox, start, end, 40.0)


def test_select_best_keeps_highest_baseline_per_tile_and_day() -> None:
    items = [
        make_item("S2A_36RTV_20190507_1_L2A", "2019-05-07", "02.12"),
        make_item("S2A_36RTV_20190507_2_L2A", "2019-05-07", "05.00"),
        make_item("S2A_36RUV_20190507_1_L2A", "2019-05-07", "05.00"),
        make_item("S2A_36RTV_20190508_0_L2A", "2019-05-08", "05.09"),
    ]
    scenes = select_best(scene for item in items if (scene := scene_from_item(item)) is not None)

    by_key = {(scene.tile, scene.day): scene for scene in scenes}
    assert len(scenes) == 3
    assert by_key[("36RTV", date(2019, 5, 7))].baseline == (5, 0)
    assert by_key[("36RTV", date(2019, 5, 7))].item_id.endswith("_2_L2A")
    assert by_key[("36RUV", date(2019, 5, 7))].baseline == (5, 0)
    assert expected_assets(scenes) == {
        "S2A_36RTV_20190507_2_L2A:B04",
        "S2A_36RTV_20190507_2_L2A:B08",
        "S2A_36RTV_20190507_2_L2A:SCL",
        "S2A_36RUV_20190507_1_L2A:B04",
        "S2A_36RUV_20190507_1_L2A:B08",
        "S2A_36RUV_20190507_1_L2A:SCL",
        "S2A_36RTV_20190508_0_L2A:B04",
        "S2A_36RTV_20190508_0_L2A:B08",
        "S2A_36RTV_20190508_0_L2A:SCL",
    }


def test_scene_without_required_asset_is_skipped() -> None:
    item = make_item("S2A_36RTV_20190507_0_L2A", "2019-05-07", "05.00")
    item.assets.pop("scl")
    assert scene_from_item(item) is None


def test_jsonl_survives_a_torn_final_line(tmp_path: Path) -> None:
    log = tmp_path / "log.jsonl"
    bronze.append_record(log, {"asset_key": "a", "bytes": 1})
    with log.open("a", encoding="utf-8") as handle:
        handle.write('{"asset_key": "torn", "byt')  # simulated crash mid-write

    bronze.append_record(log, {"asset_key": "b", "bytes": 2})
    records = bronze.read_records(log)

    assert [record["asset_key"] for record in records] == ["a", "b"]
    assert len(log.read_text(encoding="utf-8").splitlines()) == 3


def test_idempotent_skip_uses_log_and_checksum(tmp_path: Path) -> None:
    path = tmp_path / "B04.tif"
    path.write_bytes(b"content")
    record = {"asset_key": "x:B04", "bytes": 7, "sha256": bronze.sha256_file(path)}

    assert bronze.is_complete(record, path)
    path.write_bytes(b"tampered")
    assert not bronze.is_complete(record, path)
    assert not bronze.is_complete(record, tmp_path / "missing.tif")
    assert not bronze.is_complete(None, path)


def test_interrupted_run_is_resumed_without_duplicates(tmp_path: Path) -> None:
    log = tmp_path / "log.jsonl"
    first = {"asset_key": "x:B04", "bytes": 1, "sha256": "abc"}
    second = {"asset_key": "x:B08", "bytes": 2, "sha256": "def"}
    bronze.append_record(log, first)

    # Crash after the first asset: a new run reads the log and only needs the second.
    index = bronze.latest_records(bronze.read_records(log))
    assert "x:B04" in index and "x:B08" not in index

    bronze.append_record(log, second)
    index = bronze.latest_records(bronze.read_records(log))
    assert set(index) == {"x:B04", "x:B08"}
    assert len(bronze.read_records(log)) == 2
    assert len(index) == 2


def test_latest_record_wins_for_same_asset_key() -> None:
    records = [
        {"asset_key": "x:B04", "bytes": 1},
        {"asset_key": "x:B04", "bytes": 99},
    ]
    assert bronze.latest_records(records)["x:B04"]["bytes"] == 99


def test_compact_log_deduplicates_and_writes_parquet(tmp_path: Path) -> None:
    log = bronze.log_path(tmp_path)
    base = {
        "asset_key": "S2A_36RTV_20230715_0_L2A:B04",
        "ingested_at_utc": "2026-11-10T12:00:00+00:00",
        "source": "earth-search",
        "collection": "sentinel-2-l2a",
        "item_id": "S2A_36RTV_20230715_0_L2A",
        "tile": "36RTV",
        "acquired_at": "2023-07-15T08:51:41+00:00",
        "band": "B04",
        "path": "bronze/x.tif",
        "bytes": 10,
        "sha256": "abc",
        "cloud_cover": 1.0,
        "processing_baseline": "05.09",
        "boa_offset_applied": True,
        "stac_href": "https://example.test/x.tif",
        "nodata_pct": 0.0,
        "resolution_m": 10.0,
        "duration_s": 1.0,
        "http_requests": 3,
    }
    bronze.append_record(log, {**base, "bytes": 10})
    bronze.append_record(log, {**base, "bytes": 11, "asset_key": "S2A_36RTV_20230715_0_L2A:B04"})
    first_key = "S2A_36RUV_20230715_0_L2A:B04"
    bronze.append_record(log, {**base, "asset_key": first_key, "tile": "36RUV"})

    rows = bronze.compact_log(tmp_path)
    table = pq.read_table(bronze.parquet_path(tmp_path)).to_pylist()

    assert rows == 2
    assert {row["asset_key"] for row in table} == {"S2A_36RTV_20230715_0_L2A:B04", first_key}
    assert next(row for row in table if row["tile"] == "36RTV")["bytes"] == 11


def test_write_cog_roundtrips_pixels_without_overviews(tmp_path: Path) -> None:
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    from delta_crops.ingestion.raster import write_cog

    array = ((np.arange(200 * 300, dtype="uint16") * 37) % 4096).reshape(200, 300)
    path = tmp_path / "B04.tif"
    write_cog(path, array, from_origin(500000, 3400000, 10, 10), "EPSG:32636", 0)

    with rasterio.open(path) as src:
        assert np.array_equal(src.read(1), array)  # lossless
        assert src.overviews(1) == []  # no pyramids: Silver reads native windows
        assert src.profile["compress"] == "deflate"
        assert src.profile["tiled"] is True
        assert src.nodata == 0


def test_nodata_pct() -> None:
    import numpy as np

    array = np.array([[0, 0], [1, 2]], dtype="uint16")
    assert nodata_pct(array, 0) == 50.0
    assert nodata_pct(array, None) == 0.0


def test_weather_params_and_validation() -> None:
    params = weather.build_params(30.975, 31.11, date(2023, 5, 1), date(2023, 9, 30))
    assert params["timezone"] == "Africa/Cairo"
    assert params["daily"].startswith("temperature_2m_max")

    days = [d.isoformat() for d in _days(date(2023, 5, 1), date(2023, 5, 3))]
    payload = {"daily": {"time": days, **{name: [1.0, 2.0, 3.0] for name in weather.DAILY_VARIABLES}}}
    weather.validate_payload(payload, date(2023, 5, 1), date(2023, 5, 3))

    with pytest.raises(ValueError, match="days"):
        weather.validate_payload({"daily": {"time": days[:2]}}, date(2023, 5, 1), date(2023, 5, 3))


def _days(start: date, end: date):
    day = start
    while day <= end:
        yield day
        day = day.fromordinal(day.toordinal() + 1)


# ─── Phase 6a step 7: official RasterioIOError retry, no broad wrapping ───

import numpy as np
import rasterio
from rasterio.errors import CRSError, RasterioIOError

from delta_crops.config import load_config
from delta_crops.errors import AssetFailure
from delta_crops.ingestion.earth_search import Scene

CONFIG = load_config()


def make_scene(item_id: str = "S2A_36RTV_20230501_0_L2A") -> Scene:
    return Scene(
        item_id=item_id,
        tile="36RTV",
        acquired=datetime(2023, 5, 1, 8, 51, tzinfo=__import__("datetime").timezone.utc),
        baseline=(5, 0),
        cloud_cover=1.0,
        hrefs={
            "B04": f"https://example.test/{item_id}/B04.tif",
            "B08": f"https://example.test/{item_id}/B08.tif",
            "SCL": f"https://example.test/{item_id}/SCL.tif",
        },
    )


def fake_window(href, bbox):
    return np.ones((2, 2), dtype="uint16"), rasterio.Affine.identity(), "EPSG:32636", 0


def fake_write(path, array, transform, crs, nodata):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * 32)


def test_rasterio_io_error_twice_then_success_ingests_one_asset(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "BAND_ASSETS", {"B04": "red"})
    scene = make_scene()
    calls: list[str] = []

    def flaky(href, bbox):
        calls.append(href)
        if len(calls) < 3:
            raise RasterioIOError("temporary read failure")
        return fake_window(href, bbox)

    monkeypatch.setattr(bronze, "read_window", flaky)
    monkeypatch.setattr(bronze, "write_cog", fake_write)
    stats = bronze.ingest_scenes(
        [scene], CONFIG.bbox, CONFIG, tmp_path, workers=1, retry_base_delay_s=0.0
    )

    assert stats["downloaded"] == 1 and stats["failed"] == 0
    assert len(calls) == 3
    assert len(bronze.read_records(bronze.log_path(tmp_path))) == 1
    assert bronze._bronze_raster_path(tmp_path, scene, "B04").is_file()


def test_rasterio_io_error_exhausts_three_attempts_and_reports(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(bronze, "BAND_ASSETS", {"B04": "red"})
    scene = make_scene()
    calls: list[str] = []

    def always_fails(href, bbox):
        calls.append(href)
        raise RasterioIOError("connection reset")

    monkeypatch.setattr(bronze, "read_window", always_fails)
    with pytest.raises(AssetFailure):
        bronze.ingest_scenes(
            [scene], CONFIG.bbox, CONFIG, tmp_path, workers=1, retry_base_delay_s=0.0
        )

    assert len(calls) == 3
    assert not bronze._bronze_raster_path(tmp_path, scene, "B04").exists()
    assert not list(tmp_path.rglob("*.tmp"))
    assert bronze.read_records(bronze.log_path(tmp_path)) == []
    output = capsys.readouterr().out
    assert scene.item_id in output and "RasterioIOError" in output


def test_crse_error_is_not_retried(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "BAND_ASSETS", {"B04": "red"})
    calls: list[str] = []

    def bad_crs(href, bbox):
        calls.append(href)
        raise CRSError("invalid CRS")

    monkeypatch.setattr(bronze, "read_window", bad_crs)
    with pytest.raises(AssetFailure):
        bronze.ingest_scenes(
            [make_scene()], CONFIG.bbox, CONFIG, tmp_path, workers=1, retry_base_delay_s=0.0
        )
    assert len(calls) == 1
