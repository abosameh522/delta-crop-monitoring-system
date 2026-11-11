"""Phase 6a orchestration contract tests (real CLI main() calls, tmp fixtures)."""

import json
from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

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
from delta_crops.ingestion.earth_search import Scene, baseline_key, select_best
from delta_crops.ingestion.weather import DAILY_VARIABLES, validate_payload
from delta_crops.orchestration.steps import append_run_log, is_complete_season, week_status
from delta_crops.quality.checks import EXPECTED_SCHEMA
from delta_crops.transform.silver import ndvi_path

CONFIG = load_config()


def _scene(item_id: str, baseline: str, tile: str = "36RTV") -> Scene:
    return Scene(item_id, tile, datetime(2023, 1, 1), tuple(int(x) for x in baseline.split(".")), 0, {}, False)


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
    """Ingestion CLI main() returns 3 when the STAC search fails transiently."""
    from delta_crops.ingestion import cli as ingest_cli

    monkeypatch.setattr(ingest_cli, "search_scenes", _raise(TransientFailure("STAC timeout")))

    assert ingest_cli.main(["--data-dir", str(tmp_path), "ingest", "--year", "2023"]) == 3


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


def test_select_best_keeps_highest_baseline_and_item_id_tiebreak():
    assert select_best([_scene("b", "5.0"), _scene("a", "10.0")])[0].item_id == "a"


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

    result = scheduled_noop([2019, 2020, 2021, 2022, 2023], None)
    assert result["status"] == "no_op"


# ─── Placeholders for later steps: call the real (not yet written) target, never constants ───


@pytest.mark.xfail(reason="step 4: Silver input fingerprint not implemented", strict=True)
def test_silver_fingerprint_changes_when_selected_asset_changes():
    from delta_crops.transform.silver import silver_fingerprint

    assert silver_fingerprint(["S2A_36RTV_20230101_0_L2A"]) != silver_fingerprint(
        ["S2B_36RTV_20230101_0_L2A"]
    )


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
    # Real regression test will be added in Step 3 (scripts/gold_regression.py)
    pytest.skip("Implemented in scripts/gold_regression.py")
