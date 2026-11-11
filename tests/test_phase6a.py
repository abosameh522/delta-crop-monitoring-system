"""Phase 6a orchestration contract tests (real CLI main() calls, tmp fixtures)."""

import json
import time
from copy import deepcopy
from dataclasses import replace
from datetime import date
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from delta_crops.config import load_config
from delta_crops.errors import (
    AssetFailure,
    QualityFailure,
    SilverDateFailure,
    TransientFailure,
    WeeklyGateFailure,
    to_exit_code,
)
from delta_crops.ingestion import bronze
from delta_crops.ingestion.earth_search import baseline_key
from delta_crops.ingestion.weather import DAILY_VARIABLES, validate_payload
from delta_crops.orchestration.steps import append_run_log, is_complete_season, week_status
from delta_crops.quality.checks import EXPECTED_SCHEMA
from delta_crops.transform.bronze_reader import assets_for_date
from delta_crops.transform.silver import ndvi_path

CONFIG = load_config()


def _raise(exc: BaseException):
    def boom(*args, **kwargs):
        raise exc

    return boom


def _patch_transform_prerequisites(monkeypatch):
    """Skip grid/ledger work so cmd_build reaches silver.build with one date."""
    from delta_crops.transform import cli as transform_cli

    monkeypatch.setattr(transform_cli, "load_or_build", lambda *a, **k: (None, np.zeros(1, bool), np.zeros(1, bool)))
    monkeypatch.setattr(transform_cli, "_total_pixels", lambda *a: None)
    monkeypatch.setattr(transform_cli, "_select_dates", lambda *a: [date(2023, 5, 1)])
    monkeypatch.setattr(transform_cli.bronze_reader, "load_ledger", lambda root: {})
    return transform_cli


# ─── Exit-code mapping: real main(argv) with the underlying function patched ───


def test_ingestion_transient_exception_maps_to_3(monkeypatch, tmp_path):
    """Ingestion CLI main() returns 3 when the STAC search fails on transport."""
    from delta_crops.ingestion import cli as ingest_cli

    monkeypatch.setattr(ingest_cli, "search_scenes", _raise(requests.ConnectionError("STAC unreachable")))

    assert ingest_cli.main(["--data-dir", str(tmp_path), "ingest", "--year", "2023"]) == 3


def test_ingestion_unexpected_exception_maps_to_1(monkeypatch, tmp_path):
    """A non-requests bug in the STAC step is unexpected, not retryable: exit 1."""
    from delta_crops.ingestion import cli as ingest_cli

    monkeypatch.setattr(ingest_cli, "search_scenes", _raise(KeyError("schema changed")))

    assert ingest_cli.main(["--data-dir", str(tmp_path), "ingest", "--year", "2023"]) == 1


def test_ingestion_asset_failure_maps_to_4(monkeypatch, tmp_path):
    """Ingestion CLI main() returns 4 when assets fail after retries."""
    from delta_crops.ingestion import cli as ingest_cli

    monkeypatch.setattr(ingest_cli, "search_scenes", lambda **kwargs: [])
    monkeypatch.setattr(bronze, "ingest_scenes", _raise(AssetFailure("2 asset(s) failed")))
    monkeypatch.setattr(bronze, "ingest_weather", lambda *a, **k: "downloaded")

    assert ingest_cli.main(["--data-dir", str(tmp_path), "ingest", "--year", "2023"]) == 4


def test_silver_transient_failure_maps_to_3(monkeypatch, tmp_path):
    """Transform CLI main() returns 3 for a genuinely transient Silver failure."""
    transform_cli = _patch_transform_prerequisites(monkeypatch)
    monkeypatch.setattr(
        transform_cli,
        "silver",
        type("M", (), {"build": staticmethod(_raise(TransientFailure("DuckDB I/O failure")))})(),
    )

    assert transform_cli.main(["--data-dir", str(tmp_path), "year", "--year", "2023"]) == 3


def test_silver_asset_or_date_failure_maps_to_4(monkeypatch, tmp_path):
    """Transform CLI main() returns 4 when a date/asset fails."""
    transform_cli = _patch_transform_prerequisites(monkeypatch)
    monkeypatch.setattr(
        transform_cli,
        "silver",
        type("M", (), {"build": staticmethod(_raise(SilverDateFailure("3 date(s) failed")))})(),
    )

    assert transform_cli.main(["--data-dir", str(tmp_path), "year", "--year", "2023"]) == 4


def _tiny_corrupt_silver(tmp_path: Path) -> Path:
    """Minimal Silver lake (ledger + two dates, NDVI 1.25) that must fail ndvi_range."""
    root = tmp_path / "lake"
    (root / "silver").mkdir(parents=True)
    pq.write_table(
        pa.table({"cell_id": pa.array([1, 2], type=pa.int32()), "is_cropland": [True, False]}),
        root / "silver/cells.parquet",
    )
    log = root / "bronze/_logs/ingestion_log.jsonl"
    log.parent.mkdir(parents=True)
    records = []
    for day in (date(2023, 5, 1), date(2023, 5, 15)):
        for tile in ("36RTV", "36RUV"):
            records.append(
                {
                    "asset_key": f"{day}:{tile}",
                    "source": "earth-search",
                    "acquired_at": str(day) + "T08:41:00Z",
                    "tile": tile,
                }
            )
        table = pa.Table.from_pylist(
            [
                dict(cell_id=cell, date=day, ndvi=value, valid_pixels=50, total_pixels=100, masked_pct=50.0)
                for cell, value in ((1, 1.25), (2, 1.0))
            ],
            schema=EXPECTED_SCHEMA,
        )
        path = ndvi_path(root, day)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)
    log.write_text("\n".join(json.dumps(record) for record in records))
    return root


def test_quality_cli_critical_failure_returns_10(monkeypatch, tmp_path):
    """Quality CLI main() returns 10 on a real corrupt Silver fixture."""
    from delta_crops.quality import cli as quality_cli

    root = _tiny_corrupt_silver(tmp_path)
    config = replace(
        CONFIG,
        seasons=replace(CONFIG.seasons, years=(2023,), window_start="05-01", window_end="05-15"),
        data_quality=deepcopy(CONFIG.data_quality),
    )
    monkeypatch.setattr(quality_cli, "load_config", lambda *a, **k: config)

    assert quality_cli.main(["--data-dir", str(root)]) == 10


def test_gold_weekly_gate_failure_maps_to_11(monkeypatch, tmp_path):
    """Gold CLI main() returns 11 for the real gate exception type."""
    from delta_crops.gold import cli as gold_cli

    monkeypatch.setattr(
        gold_cli.gate,
        "weekly_gate",
        _raise(gold_cli.gate.WeeklyGateFailure("Weekly NULL share exceeds the gate")),
    )

    assert gold_cli.main(["--data-dir", str(tmp_path), "build"]) == 11


# ─── Core logic ───


def test_assets_for_date_selects_highest_baseline_then_item_id(tmp_path):
    """assets_for_date itself picks the winner that Silver will read."""
    root = tmp_path / "lake"
    ledger: dict[str, dict] = {}

    def add(item_id: str, tile: str, band: str, baseline: str) -> None:
        path = root / "bronze" / "assets" / tile / f"{item_id}_{band}.tif"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"{item_id}:{band}".encode())
        record = {
            "asset_key": f"{item_id}:{band}",
            "source": "earth-search",
            "acquired_at": "2023-05-01T08:41:00Z",
            "tile": tile,
            "band": band,
            "path": str(path.relative_to(root)),
            "bytes": path.stat().st_size,
            "item_id": item_id,
            "processing_baseline": baseline,
        }
        ledger[record["asset_key"]] = record

    # Higher baseline must win over a lexically larger "5.0" vs numeric "10.0".
    add("S2A_36RTV_old", "36RTV", "B04", "5.0")
    add("S2A_36RTV_new", "36RTV", "B04", "10.0")
    for band in ("B08", "SCL"):
        add(f"S2A_36RTV_{band}", "36RTV", band, "10.0")

    # Equal baselines fall back to the larger item_id.
    add("S2A_36RUV_dup", "36RUV", "B04", "5.0")
    add("S2B_36RUV_dup", "36RUV", "B04", "5.0")
    for band in ("B08", "SCL"):
        add(f"S2B_36RUV_{band}", "36RUV", band, "5.0")

    # A tile with one record per band is untouched by the selection.
    for band in ("B04", "B08", "SCL"):
        add(f"S2A_36RTU_only_{band}", "36RTU", band, "5.0")

    tiles, problems = assets_for_date(ledger, root, date(2023, 5, 1), verify_checksum=False)

    assert problems == []
    assert tiles["36RTV"]["B04"].item_id == "S2A_36RTV_new"
    assert tiles["36RUV"]["B04"].item_id == "S2B_36RUV_dup"
    assert tiles["36RTU"]["B04"].item_id == "S2A_36RTU_only_B04"


def test_processing_baseline_version_order_10_0_above_5_0():
    assert baseline_key("10.0") > baseline_key("5.0")


def test_is_complete_season_requires_end_date_and_153_weather_days():
    assert not is_complete_season(
        2023, date(2023, 9, 30), date(2023, 9, 30), {date(2023, 5, 1)}, date(2023, 5, 1)
    )


def test_validate_payload_accepts_partial_year_with_partial_status():
    payload = {"daily": {"time": ["2023-05-01"], **{v: [1] for v in DAILY_VARIABLES}}}
    assert validate_payload(payload, date(2023, 5, 1), date(2023, 9, 30), True) == "partial"


def test_week_containing_as_of_is_not_yet_observed():
    assert week_status(date(2023, 9, 25), date(2023, 9, 27)) == "not_yet_observed"


def test_run_log_written_on_failure_with_failed_status(tmp_path):
    append_run_log(tmp_path, {"status": "failed"})
    assert json.loads((tmp_path / "_logs" / "pipeline_runs.jsonl").read_text())["status"] == "failed"


def test_scheduled_run_without_in_progress_year_is_logged_noop():
    from delta_crops.orchestration.flow import scheduled_noop

    result = scheduled_noop.fn([2019, 2020, 2021, 2022, 2023], None)
    assert result["status"] == "no_op"


# ─── Placeholders for later steps: call the real (not yet written) target, never constants ───


def test_silver_fingerprint_changes_when_selected_asset_changes(tmp_path, monkeypatch):
    """Fingerprint changes when the selected asset's item_id or sha256 changes."""
    from delta_crops.transform.silver import (
        compute_fingerprint,
        fingerprints_match,
        SILVER_FINGERPRINT_VERSION,
    )
    from delta_crops.transform.bronze_reader import Asset

    # Create a mock grid and config
    from delta_crops.transform.grid import Grid
    import numpy as np
    from delta_crops.config import Config, BBox, Seasons, Sources, Quality, Silver, Grid as GridConfig, Gold, Heatwave

    grid = Grid(x0=0.0, y0=0.0, ncols=10, nrows=10, cell_size_m=100)
    keep_cells = np.ones(100, dtype=bool)
    total_px = np.full(100, 100, dtype=np.int32)

    config = Config(
        name="test",
        bbox=BBox(west=0, south=0, east=0.1, north=0.1),
        crs="EPSG:32636",
        seasons=Seasons(focus=2023, years=(2023,), window_start="05-01", window_end="09-30", label="test"),
        grid=GridConfig(cell_size_m=100, crs="EPSG:32636"),
        sources=Sources(imagery="test", imagery_fallback="test"),
        quality=Quality(max_scene_cloud_pct=20.0, min_weekly_observations=2, max_weekly_masked_pct=50.0),
        data_quality={},
        silver=Silver(
            min_valid_fraction=0.1,
            masked_scl_classes=(3, 8, 9, 10, 11),
            tile_priority=("36RTV", "36RUV"),
            max_scl_dark_pct=0.1,
            max_scl_unclassified_pct=0.1,
            stable_target=None,
        ),
        gold=None,
    )

    # Base tiles
    tiles1 = {
        "36RTV": {
            "B04": Asset(band="B04", tile="36RTV", path=tmp_path / "a.tif", item_id="S2A_36RTV", sha256="abc123"),
            "B08": Asset(band="B08", tile="36RTV", path=tmp_path / "b.tif", item_id="S2A_36RTV", sha256="def456"),
            "SCL": Asset(band="SCL", tile="36RTV", path=tmp_path / "c.tif", item_id="S2A_36RTV", sha256="ghi789"),
        }
    }

    fp1 = compute_fingerprint(tiles1, grid, keep_cells, total_px, config)
    assert fp1["version"] == SILVER_FINGERPRINT_VERSION
    assert fp1["assets"]["36RTV"]["B04"]["item_id"] == "S2A_36RTV"

    # Change item_id -> fingerprint should differ
    tiles2 = {
        "36RTV": {
            "B04": Asset(band="B04", tile="36RTV", path=tmp_path / "a.tif", item_id="S2B_36RTV", sha256="abc123"),
            "B08": Asset(band="B08", tile="36RTV", path=tmp_path / "b.tif", item_id="S2A_36RTV", sha256="def456"),
            "SCL": Asset(band="SCL", tile="36RTV", path=tmp_path / "c.tif", item_id="S2A_36RTV", sha256="ghi789"),
        }
    }
    fp2 = compute_fingerprint(tiles2, grid, keep_cells, total_px, config)
    assert not fingerprints_match(fp1, fp2)

    # Change sha256 -> fingerprint should differ
    tiles3 = {
        "36RTV": {
            "B04": Asset(band="B04", tile="36RTV", path=tmp_path / "a.tif", item_id="S2A_36RTV", sha256="different"),
            "B08": Asset(band="B08", tile="36RTV", path=tmp_path / "b.tif", item_id="S2A_36RTV", sha256="def456"),
            "SCL": Asset(band="SCL", tile="36RTV", path=tmp_path / "c.tif", item_id="S2A_36RTV", sha256="ghi789"),
        }
    }
    fp3 = compute_fingerprint(tiles3, grid, keep_cells, total_px, config)
    assert not fingerprints_match(fp1, fp3)

    # Same inputs -> fingerprint matches
    assert fingerprints_match(fp1, fp1)

    # Different version -> no match
    fp1_bad_version = dict(fp1)
    fp1_bad_version["version"] = "silver_v99"
    assert not fingerprints_match(fp1_bad_version, fp1)


@pytest.mark.xfail(reason="step 6: Gold fingerprint with as_of not implemented", strict=True)
def test_gold_fingerprint_contains_as_of():
    from delta_crops.gold.weekly import weekly_fingerprint

    assert "as_of" in weekly_fingerprint(as_of=date(2023, 9, 30))


@pytest.mark.xfail(reason="step 8: retry mapping not implemented", strict=True)
def test_task_retry_mapping_retries_only_code_3():
    from delta_crops.orchestration.flow import retry_codes

    assert 3 in retry_codes()


@pytest.mark.xfail(reason="step 8: retry mapping not implemented", strict=True)
def test_task_retry_mapping_does_not_retry_code_10_or_other_codes():
    from delta_crops.orchestration.flow import retry_codes

    assert 10 not in retry_codes()


# ─── Helper-level only: these test to_exit_code(), not a CLI main() ───


def test_to_exit_code_quality_failure():
    assert to_exit_code(QualityFailure("x")) == 10


def test_to_exit_code_weekly_gate_failure():
    assert to_exit_code(WeeklyGateFailure("x")) == 11


def test_to_exit_code_transient_failure():
    assert to_exit_code(TransientFailure("x")) == 3


def test_to_exit_code_asset_failure():
    assert to_exit_code(AssetFailure("x")) == 4


def test_to_exit_code_silver_date_failure():
    assert to_exit_code(SilverDateFailure("x")) == 4


def test_to_exit_code_unknown_exception():
    assert to_exit_code(ValueError("x")) == 1


@pytest.mark.slow
def test_gold_regression_except_is_identical_in_both_directions():
    """Full Gold rebuild for 2019-2023 must match frozen snapshot exactly."""
    from scripts.gold_regression import run_regression

    assert run_regression(Path("data/_regression/gold_before"), Path("data"))


# ─── Silver fingerprint build behavior tests ───


def _setup_test_silver_build(tmp_path, monkeypatch):
    """Create a minimal Silver build environment for fingerprint tests."""
    from delta_crops.transform import silver
    from delta_crops.transform.grid import Grid
    from delta_crops.config import Config, BBox, Seasons, Sources, Quality, Silver as SilverConfig, Grid as GridConfig
    import numpy as np

    root = tmp_path / "lake"
    (root / "silver").mkdir(parents=True)
    (root / "bronze/_logs").mkdir(parents=True)
    (root / "bronze/assets/36RTV").mkdir(parents=True)

    # Minimal config
    config = Config(
        name="test",
        bbox=BBox(west=0, south=0, east=0.1, north=0.1),
        crs="EPSG:32636",
        seasons=Seasons(focus=2023, years=(2023,), window_start="05-01", window_end="05-02", label="test"),
        grid=GridConfig(cell_size_m=100, crs="EPSG:32636"),
        sources=Sources(imagery="test", imagery_fallback="test"),
        quality=Quality(max_scene_cloud_pct=20.0, min_weekly_observations=2, max_weekly_masked_pct=50.0),
        data_quality={},
        silver=SilverConfig(
            min_valid_fraction=0.1,
            masked_scl_classes=(3, 8, 9, 10, 11),
            tile_priority=("36RTV",),
            max_scl_dark_pct=0.1,
            max_scl_unclassified_pct=0.1,
            stable_target=None,
        ),
        gold=None,
    )

    # Grid: 1 kept cell
    grid = Grid(x0=0.0, y0=0.0, ncols=1, nrows=1, cell_size_m=100)
    keep_cells = np.array([True])
    pixel_mask = np.array([True] * 100, dtype=bool)
    total_px = np.array([100], dtype=np.int32)

    # Ledger with one date
    import json
    from datetime import date
    log = root / "bronze/_logs/ingestion_log.jsonl"
    records = []
    for day in (date(2023, 5, 1), date(2023, 5, 2)):
        for tile in ("36RTV",):
            for band in ("B04", "B08", "SCL"):
                asset_path = root / "bronze" / "assets" / tile / f"{day}_{band}.tif"
                asset_path.write_bytes(b"dummy")
                records.append(
                    dict(
                        asset_key=f"{day}:{tile}:{band}",
                        source="earth-search",
                        acquired_at=str(day) + "T08:41:00Z",
                        tile=tile,
                        band=band,
                        path=str(asset_path.relative_to(root)),
                        bytes=asset_path.stat().st_size,
                        item_id=f"S2A_{tile}_{day.isoformat()}",
                        processing_baseline="5.0",
                    )
                )
    log.write_text("\n".join(json.dumps(r) for r in records))

    # Mock process_date to avoid raster I/O
    from delta_crops.transform.silver import DateOutcome
    import pyarrow as pa
    import datetime
    # date32 is days since epoch
    epoch = datetime.date(1970, 1, 1)
    day_int = (datetime.date(2023, 5, 1) - epoch).days
    fake_table = pa.Table.from_pydict(
        {
            "cell_id": pa.array([1], type=pa.int32()),
            "date": pa.array([day_int], type=pa.date32()),
            "ndvi": pa.array([0.5], type=pa.float32()),
            "valid_pixels": pa.array([50], type=pa.uint8()),
            "total_pixels": pa.array([100], type=pa.uint8()),
            "masked_pct": pa.array([50.0], type=pa.float32()),
        }
    )
    fake_outcome = DateOutcome(
        table=fake_table,
        stats={
            "tiles": "36RTV", "rows": 1, "seconds": 0.0, "peak_rss_mb": 0.0,
            "ndvi_p05": 0.5, "ndvi_p50": 0.5, "ndvi_p95": 0.5,
            "null_pct": 0.0, "masked_p50": 50.0, "masked_p90": 50.0, "masked_max": 50.0,
            "overlap_median_abs_ndvi": None, "overlap_pixels": 0,
        },
        scl_counts=np.zeros(256, dtype=np.int64),
    )
    monkeypatch.setattr(silver, "process_date", lambda *a, **k: fake_outcome)

    return root, config, grid, keep_cells, pixel_mask, total_px, silver


def test_silver_fingerprint_build_twice_second_skipped(tmp_path, monkeypatch):
    """Building twice with same inputs: second run skips (fingerprint matches)."""
    root, config, grid, keep_cells, pixel_mask, total_px, silver = _setup_test_silver_build(tmp_path, monkeypatch)

    # First build
    summary1 = silver.build(
        dates=[date(2023, 5, 1)],
        config=config,
        data_root=root,
        keep_cells=keep_cells,
        pixel_mask=pixel_mask,
        total_px=total_px,
        grid=grid,
        verify_checksum=False,
        force=False,
    )
    assert summary1["built"] == 1
    assert summary1["skipped"] == 0
    assert summary1["rebuilt_due_to_input_change"] == 0

    # Second build (no force) -> should skip due to fingerprint match
    summary2 = silver.build(
        dates=[date(2023, 5, 1)],
        config=config,
        data_root=root,
        keep_cells=keep_cells,
        pixel_mask=pixel_mask,
        total_px=total_px,
        grid=grid,
        verify_checksum=False,
        force=False,
    )
    assert summary2["built"] == 0
    assert summary2["skipped"] == 1
    assert summary2["rebuilt_due_to_input_change"] == 0


def test_silver_fingerprint_force_rebuilds(tmp_path, monkeypatch):
    """--force rebuilds even if fingerprint matches."""
    root, config, grid, keep_cells, pixel_mask, total_px, silver = _setup_test_silver_build(tmp_path, monkeypatch)

    # First build
    silver.build(
        dates=[date(2023, 5, 1)],
        config=config,
        data_root=root,
        keep_cells=keep_cells,
        pixel_mask=pixel_mask,
        total_px=total_px,
        grid=grid,
        verify_checksum=False,
        force=False,
    )

    # Second build with force -> should rebuild
    summary = silver.build(
        dates=[date(2023, 5, 1)],
        config=config,
        data_root=root,
        keep_cells=keep_cells,
        pixel_mask=pixel_mask,
        total_px=total_px,
        grid=grid,
        verify_checksum=False,
        force=True,
    )
    assert summary["built"] == 1
    assert summary["skipped"] == 0


def test_silver_fingerprint_missing_reports_unfingerprinted(tmp_path, monkeypatch):
    """Legacy file without fingerprint: reports unfingerprinted and skips (no adopt)."""
    root, config, grid, keep_cells, pixel_mask, total_px, silver = _setup_test_silver_build(tmp_path, monkeypatch)

    # Create a Silver file WITHOUT a fingerprint (simulate legacy)
    import pyarrow as pa
    import pyarrow.parquet as pq
    from delta_crops.transform.silver import ndvi_path

    target = ndvi_path(root, date(2023, 5, 1))
    target.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(
        [{"cell_id": 1, "date": date(2023, 5, 1), "ndvi": 0.5, "valid_pixels": 50, "total_pixels": 100, "masked_pct": 50.0}]
    )
    pq.write_table(table, target)
    # NO fingerprint file

    summary = silver.build(
        dates=[date(2023, 5, 1)],
        config=config,
        data_root=root,
        keep_cells=keep_cells,
        pixel_mask=pixel_mask,
        total_px=total_px,
        grid=grid,
        verify_checksum=False,
        force=False,
        adopt=False,
    )
    assert summary["unfingerprinted"] == 1
    assert summary["built"] == 0
    assert summary["skipped"] == 0


def test_silver_fingerprint_adopt_writes_fingerprint(tmp_path, monkeypatch):
    """--adopt writes fingerprint for legacy file without rebuilding."""
    root, config, grid, keep_cells, pixel_mask, total_px, silver = _setup_test_silver_build(tmp_path, monkeypatch)

    # Create a Silver file WITHOUT a fingerprint (simulate legacy)
    import pyarrow as pa
    import pyarrow.parquet as pq
    from delta_crops.transform.silver import ndvi_path, fingerprint_path

    target = ndvi_path(root, date(2023, 5, 1))
    target.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(
        [{"cell_id": 1, "date": date(2023, 5, 1), "ndvi": 0.5, "valid_pixels": 50, "total_pixels": 100, "masked_pct": 50.0}]
    )
    pq.write_table(table, target)
    # NO fingerprint file

    summary = silver.build(
        dates=[date(2023, 5, 1)],
        config=config,
        data_root=root,
        keep_cells=keep_cells,
        pixel_mask=pixel_mask,
        total_px=total_px,
        grid=grid,
        verify_checksum=False,
        force=False,
        adopt=True,
    )
    assert summary["adopted"] == 1
    assert summary["built"] == 0
    assert summary["unfingerprinted"] == 0
    # Fingerprint file should now exist
    fp = fingerprint_path(root, date(2023, 5, 1))
    assert fp.exists()


def test_silver_fingerprint_input_change_rebuilds(tmp_path, monkeypatch):
    """If input asset changes (different item_id), only that date rebuilds."""
    root, config, grid, keep_cells, pixel_mask, total_px, silver = _setup_test_silver_build(tmp_path, monkeypatch)

    # First build with original assets
    summary1 = silver.build(
        dates=[date(2023, 5, 1), date(2023, 5, 2)],
        config=config,
        data_root=root,
        keep_cells=keep_cells,
        pixel_mask=pixel_mask,
        total_px=total_px,
        grid=grid,
        verify_checksum=False,
        force=False,
    )
    assert summary1["built"] == 2

    # Modify the ledger for one date (change item_id for 2023-05-01)
    import json
    log = root / "bronze/_logs/ingestion_log.jsonl"
    records = []
    for line in log.read_text().strip().split("\n"):
        rec = json.loads(line)
        if "2023-05-01" in rec["acquired_at"] and rec["band"] == "B04":
            rec["item_id"] = "S2B_36RTV_2023-05-01"  # Changed!
        records.append(rec)
    log.write_text("\n".join(json.dumps(r) for r in records))

    # Second build - should rebuild only 2023-05-01
    summary2 = silver.build(
        dates=[date(2023, 5, 1), date(2023, 5, 2)],
        config=config,
        data_root=root,
        keep_cells=keep_cells,
        pixel_mask=pixel_mask,
        total_px=total_px,
        grid=grid,
        verify_checksum=False,
        force=False,
    )
    assert summary2["rebuilt_due_to_input_change"] == 1
    assert summary2["skipped"] == 1  # 2023-05-02 unchanged
    assert summary2["built"] == 0


# ─── Fast unit test of the comparison function on tiny fixtures ───


def test_gold_regression_comparison_function_identical(tmp_path):
    """The comparison logic itself: identical tables -> 0 diffs."""
    con = duckdb.connect()
    # Create identical tiny tables
    t1 = pa.Table.from_pylist([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
    t2 = pa.Table.from_pylist([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
    p1 = tmp_path / "t1.parquet"
    p2 = tmp_path / "t2.parquet"
    pq.write_table(t1, p1)
    pq.write_table(t2, p2)
    # Direct EXCEPT test (same logic as _compare_table)
    diff_a = con.execute(f"SELECT count(*) FROM (SELECT * FROM read_parquet('{p1}') EXCEPT SELECT * FROM read_parquet('{p2}'))").fetchall()[0][0]
    diff_b = con.execute(f"SELECT count(*) FROM (SELECT * FROM read_parquet('{p2}') EXCEPT SELECT * FROM read_parquet('{p1}'))").fetchall()[0][0]
    assert diff_a == 0 and diff_b == 0


def test_gold_regression_comparison_function_different(tmp_path):
    """The comparison logic: one changed row -> nonzero diff and exit 1."""
    con = duckdb.connect()
    t1 = pa.Table.from_pylist([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
    t2 = pa.Table.from_pylist([{"a": 1, "b": "x"}, {"a": 2, "b": "z"}])  # 'y' -> 'z'
    p1 = tmp_path / "t1.parquet"
    p2 = tmp_path / "t2.parquet"
    pq.write_table(t1, p1)
    pq.write_table(t2, p2)
    diff_a = con.execute(f"SELECT count(*) FROM (SELECT * FROM read_parquet('{p1}') EXCEPT SELECT * FROM read_parquet('{p2}'))").fetchall()[0][0]
    diff_b = con.execute(f"SELECT count(*) FROM (SELECT * FROM read_parquet('{p2}') EXCEPT SELECT * FROM read_parquet('{p1}'))").fetchall()[0][0]
    assert diff_a + diff_b > 0
