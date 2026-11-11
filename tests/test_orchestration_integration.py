"""Phase 6a step 8b: real CLI end-to-end integration test.

The four CLI ``main()`` functions run unchanged. Only the three network
boundaries are mocked: STAC search, remote raster window reads, and Open-Meteo.
Everything else (Bronze COG writes, Silver NDVI, quality, Gold builds, the run
log) is the real code on a tiny temporary lake.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from prefect.testing.utilities import prefect_test_harness

from delta_crops.config import load_config
from delta_crops.gold.weekly import iso_week_start
from delta_crops.ingestion import bronze, weather
from delta_crops.ingestion.earth_search import Scene
from delta_crops.orchestration.flow import scheduled_flow
from delta_crops.transform.grid import aoi_polygon, cell_ids, cell_mask, make_grid
from delta_crops.transform.silver import ndvi_path

pytestmark = pytest.mark.flow

SEASON_YEAR = 2027
IMAGERY_DAYS = (11, 13, 16)          # ISO week 19 of 2027
WEATHER_DAYS = range(10, 17)         # season starts 10 May; 17 May is missing -> in progress


@pytest.fixture(scope="session", autouse=True)
def _prefect_harness():
    """One ephemeral Prefect server for this module; never a live server."""
    with prefect_test_harness():
        yield


def _write_config(path: Path) -> None:
    raw = yaml.safe_load(Path("config/aoi.yaml").read_text(encoding="utf-8"))
    raw["name"] = "integration-tiny"
    raw["bbox"] = {"west": 30.90, "south": 30.80, "east": 30.91, "north": 30.81}
    raw["seasons"] = {
        "focus": SEASON_YEAR,
        "years": [SEASON_YEAR],
        "window": {"start": "05-10", "end": "05-17"},
        "label": "integration",
    }
    raw["gold"]["weeks"] = {"first": 19, "last": 19}
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")


def _write_fixtures(root: Path, config) -> None:
    """cells.parquet, Silver weather and the orbit cache; no network required."""
    grid = make_grid(config.bbox)
    keep = cell_mask(grid, aoi_polygon(config.bbox))
    cells = pa.table({
        "cell_id": pa.array(cell_ids(grid)[keep.ravel()], pa.int32()),
        "is_cropland": pa.array(np.ones(int(keep.sum()), dtype=bool)),
    })
    (root / "silver").mkdir(parents=True, exist_ok=True)
    pq.write_table(cells, root / "silver" / "cells.parquet")

    days = [date(SEASON_YEAR, 5, day) for day in WEATHER_DAYS]
    pq.write_table(
        pa.table({
            "date": pa.array(days, pa.date32()),
            "temp_max_c": pa.array([35.0] * len(days), pa.float32()),
            "temp_min_c": pa.array([22.0] * len(days), pa.float32()),
            "precip_mm": pa.array([0.0] * len(days), pa.float32()),
            "solar_mj_m2": pa.array([25.0] * len(days), pa.float32()),
        }),
        root / "silver" / "weather.parquet",
    )

    orbit_days = [date(SEASON_YEAR, 5, day) for day in IMAGERY_DAYS]
    cache = root / "silver" / "_cache" / "orbits.parquet"
    cache.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({
            "date": pa.array(orbit_days, pa.date32()),
            "relative_orbit": pa.array([21] * len(orbit_days), pa.int16()),
        }),
        cache,
    )


def _scene(day: int) -> Scene:
    item_id = f"S2A_36RTV_202705{day:02d}_0_L2A"
    acquired = datetime(SEASON_YEAR, 5, day, 8, 51, tzinfo=timezone.utc)
    return Scene(
        item_id=item_id,
        tile="36RTV",
        acquired=acquired,
        baseline=(5, 0),
        cloud_cover=1.0,
        hrefs={
            band: f"https://example.test/{item_id}/{band}.tif"
            for band in ("B04", "B08", "SCL")
        },
    )


def _weather_payload() -> dict:
    days = [date(SEASON_YEAR, 5, day).isoformat() for day in WEATHER_DAYS]
    return {
        "daily": {
            "time": days,
            "temperature_2m_max": [35.0] * len(days),
            "temperature_2m_min": [22.0] * len(days),
            "temperature_2m_mean": [28.0] * len(days),
            "precipitation_sum": [0.0] * len(days),
            "shortwave_radiation_sum": [25.0] * len(days),
        }
    }


def _patch_network(monkeypatch, config):
    """Mock only STAC search, remote raster reads and the Open-Meteo request."""
    from delta_crops.ingestion import cli as ingest_cli

    grid = make_grid(config.bbox)
    read_calls: list[str] = []

    def fake_read_window(href: str, bbox):
        read_calls.append(href)
        band = href.rsplit("/", 1)[-1].split(".")[0]
        if band == "SCL":
            array, nodata = np.full(grid.pixel_shape, 4, dtype="uint8"), 0
        elif band == "B04":
            array, nodata = np.full(grid.pixel_shape, 1000, dtype="uint16"), 0
        else:
            array, nodata = np.full(grid.pixel_shape, 3000, dtype="uint16"), 0
        return array, grid.pixel_transform, "EPSG:32636", nodata

    monkeypatch.setattr(ingest_cli, "search_scenes", lambda **kwargs: [_scene(day) for day in IMAGERY_DAYS])
    monkeypatch.setattr(bronze, "read_window", fake_read_window)
    monkeypatch.setattr(weather, "fetch_daily", lambda *a, **k: _weather_payload())
    return read_calls


def _wrap_mains(monkeypatch, calls: dict[str, list[list[str]]]) -> None:
    """Record the real CLI argv, then call the real main()."""
    from delta_crops.gold import cli as gold_cli
    from delta_crops.ingestion import cli as ingest_cli
    from delta_crops.quality import cli as quality_cli
    from delta_crops.transform import cli as transform_cli

    for name, module in (
        ("ingest", ingest_cli), ("silver", transform_cli),
        ("quality", quality_cli), ("gold", gold_cli),
    ):
        original = module.main

        def wrapper(argv: list[str], _name=name, _original=original) -> int:
            calls.setdefault(_name, []).append(list(argv))
            return _original(argv)

        monkeypatch.setattr(module, "main", wrapper)


def _run(root: Path, config_path: Path):
    return scheduled_flow(
        data_dir=str(root),
        config_path=str(config_path),
        retries=3,
        retry_delays=[0.0, 0.0, 0.0],
    )


def _run_log(root: Path) -> list[dict]:
    import json

    path = root / "_logs" / "pipeline_runs.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def test_real_four_cli_pipeline_is_idempotent(monkeypatch, tmp_path):
    config_path = tmp_path / "aoi.yaml"
    _write_config(config_path)
    config = load_config(config_path)
    _write_fixtures(tmp_path, config)
    read_calls = _patch_network(monkeypatch, config)

    calls: dict[str, list[list[str]]] = {}
    _wrap_mains(monkeypatch, calls)

    assert _run(tmp_path, config_path)["status"] == "ok"
    first = _run_log(tmp_path)[-1]

    # All four real tasks ran with the flow's flags.
    assert set(calls) == {"ingest", "silver", "quality", "gold"}
    assert "--no-checksum" in calls["ingest"][0]
    assert ["--allow-partial-weather-year", str(SEASON_YEAR)] == calls["ingest"][0][
        calls["ingest"][0].index("--allow-partial-weather-year"):
        calls["ingest"][0].index("--allow-partial-weather-year") + 2
    ]
    assert "--no-verify-checksum" in calls["silver"][0]
    assert any("build" in argv for argv in calls["gold"])
    assert any("verify" in argv for argv in calls["gold"])
    assert all("--summary-json" in argv for argv in calls["ingest"] + calls["silver"])

    # Real Silver and Gold outputs exist.
    for day in IMAGERY_DAYS:
        assert ndvi_path(tmp_path, date(SEASON_YEAR, 5, day)).is_file()
    for table in ("weekly_ndvi", "weekly_weather", "ndvi_anomaly"):
        assert list((tmp_path / "gold" / table / f"year={SEASON_YEAR}").glob("*.parquet"))
    assert iso_week_start(date(SEASON_YEAR, 5, 16)) == date(SEASON_YEAR, 5, 10)

    # Real counts in the run log.
    assert first["status"] == "ok"
    assert first["ingest"]["new"] == len(IMAGERY_DAYS) * 3
    assert first["ingest"]["skipped"] == 0
    assert first["ingest"]["bytes"] > 0
    assert first["ingest"]["weather"] == {str(SEASON_YEAR): "partial"}
    assert first["silver"]["built"] == len(IMAGERY_DAYS)
    assert first["silver"]["failed"] == 0
    assert first["quality"]["status"] == "passed"
    assert first["gold"]["tables"]["weekly_ndvi"]["built"] == 1
    assert first["gold"]["tables"]["weekly_weather"]["built"] == 1
    assert first["gold"]["tables"]["ndvi_anomaly"]["built"] == 1
    assert first["gold_verify"] == "ok"
    assert first["bronze_verify"] == "not_run"

    # Second run: bundled answer from the ledger and verified fingerprints only.
    read_calls.clear()
    calls.clear()
    assert _run(tmp_path, config_path)["status"] == "ok"
    second = _run_log(tmp_path)[-1]

    assert read_calls == []
    assert second["ingest"]["new"] == 0
    assert second["ingest"]["skipped"] == len(IMAGERY_DAYS) * 3
    assert second["silver"]["built"] == 0
    assert second["silver"]["skipped"] == len(IMAGERY_DAYS)
    assert second["silver"]["rebuilt_due_to_input_change"] == 0
    assert second["gold"]["tables"]["weekly_ndvi"]["built"] == 0
    assert second["gold"]["tables"]["weekly_ndvi"]["skipped"] == 1
    assert second["gold"]["tables"]["weekly_weather"]["skipped"] == 1
    assert second["gold"]["tables"]["ndvi_anomaly"]["skipped"] == 1
    assert second["quality"]["status"] == "passed"
